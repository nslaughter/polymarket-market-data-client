"""The runner's steps, checks, and failure reports, against a fake client
that plays scripted records (spec/conformance.md, The runner)."""

import asyncio
import contextlib
import gc
import time
from collections.abc import Callable, Iterable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType
from typing import Any

import pytest

from polymarket_market_data import (
    BookEvent,
    ClientConfig,
    ClientStateError,
    ClientStats,
    Level,
    Market,
    MarketLookup,
    MarketNotFound,
    RecoveryFailed,
    TokenState,
    TokenStateChange,
)

from .conftest import ENABLED, enabled_names
from .failures import ScenarioFailed
from .matching import EVENT_RECORDS
from .notation import load, parse_scenario
from .runner import Client, build_config, run
from .synthetic import MARKETS, T0, TOKEN_IDS

_END = object()

Item = Callable[[], object]
"""Makes a record, or an exception the iterator raises, when it is due."""


class Fake:
    """A client that delivers each scripted item after its delay, counted
    from entering the block, and otherwise behaves as the contract says,
    unless an option makes it faulty:

    - ``leak``: a task it starts outlives the block;
    - ``stuck_leak``, ``stuck_exit``: for this many seconds, the leaked task,
      or leaving the block, ignores every cancellation;
    - ``swallow_cancel``: leaving the block swallows a ``CancelledError``.
    """

    def __init__(
        self,
        lookup: MarketLookup,
        script: Iterable[tuple[float, Item]] = (),
        *,
        leak: bool = False,
        stuck_leak: float = 0.0,
        stuck_exit: float = 0.0,
        swallow_cancel: bool = False,
        end_block_after: float | None = None,
    ) -> None:
        self.lookup = lookup
        self.script = list(script)
        self.leak = leak
        self.stuck_leak = stuck_leak
        self.stuck_exit = stuck_exit
        self.swallow_cancel = swallow_cancel
        self.end_block_after = end_block_after
        self.queue: asyncio.Queue[object] = asyncio.Queue()
        self.subscribed: list[Market] = []
        self.closed = False
        self.tasks: list[asyncio.Task[None]] = []

    async def __aenter__(self) -> "Fake":
        self.tasks.append(asyncio.create_task(self._play()))
        if self.leak:
            leaked = stuck(self.stuck_leak) if self.stuck_leak else asyncio.sleep(60)
            self.tasks.append(asyncio.create_task(leaked, name="leaked"))
        if self.end_block_after is not None:
            host = asyncio.current_task()
            assert host is not None
            asyncio.get_running_loop().call_later(self.end_block_after, host.cancel)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        await stuck(self.stuck_exit)
        stopping = self.tasks if not self.leak else self.tasks[:1]
        for task in stopping:
            task.cancel()
        await asyncio.gather(*stopping, return_exceptions=True)
        self.closed = True
        while not self.queue.empty():
            self.queue.get_nowait()
        self.queue.put_nowait(_END)
        return self.swallow_cancel and exc_type is asyncio.CancelledError

    async def _play(self) -> None:
        for delay, item in self.script:
            await asyncio.sleep(delay)
            self.queue.put_nowait(item())

    def records(self) -> "Fake":
        return self

    def __aiter__(self) -> "Fake":
        return self

    async def __anext__(self) -> object:
        item = await self.queue.get()
        if item is _END:
            self.queue.put_nowait(_END)
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item

    @property
    def backlog(self) -> int:
        return sum(isinstance(item, EVENT_RECORDS) for item in self.queue._queue)  # type: ignore[attr-defined]

    def stats(self) -> ClientStats:
        return ClientStats(
            frames=0,
            frames_after_interruption=0,
            events={},
            repeats=0,
            unknown=0,
            undecodable={},
            new_market_dropped=0,
            discarded_outside=0,
            pongs=0,
            pongs_unsolicited=0,
            pong_delay_last=0.0,
            pong_delay_max=0.0,
            connections=0,
            interruptions={"dropped": 1},
            lookups=0,
            lookup_failures=0,
            rest_book_not_found=0,
            hash_verified=0,
            hash_retried=0,
            hash_failed=0,
        )

    def subscribe(self, *markets: Market) -> None:
        if self.closed:
            raise ClientStateError("the client has shut down")
        self.subscribed.extend(markets)

    def unsubscribe(self, *condition_ids: str) -> None:
        if self.closed:
            raise ClientStateError("the client has shut down")

    async def resolve(self, slug: str) -> Market:
        info = await self.lookup.market(slug=slug)
        if info is None:
            raise MarketNotFound(slug)
        return Market(info.condition_id, info.token_ids, info.slug)


async def stuck(seconds: float) -> None:
    """Sleep for ``seconds``, however often cancelled, as a faulty client
    could."""
    loop = asyncio.get_running_loop()
    until = loop.time() + seconds
    while (left := until - loop.time()) > 0:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(left)


def scenario_run(
    text: str,
    script: Iterable[tuple[float, Item]] = (),
    *,
    step_timeout: float = 5.0,
    fake: type[Fake] = Fake,
    **options: Any,
) -> list[Fake]:
    """Run a scenario against a fake; the fake is returned for inspection."""
    fakes: list[Fake] = []

    def factory(
        config: ClientConfig, markets: tuple[Market, ...], lookup: MarketLookup
    ) -> Client:
        fakes.append(fake(lookup, script, **options))
        return fakes[-1]

    run(parse_scenario(text, first_line=500), factory, step_timeout=step_timeout)
    return fakes


def fails(
    text: str, script: Iterable[tuple[float, Item]] = (), **options: Any
) -> ScenarioFailed:
    with pytest.raises(ScenarioFailed) as failed:
        scenario_run(text, script, **options)
    return failed.value


def ready(token: str = "A1", reason: str = "book") -> Item:
    def make() -> TokenStateChange:
        return TokenStateChange(
            token_id=TOKEN_IDS[token],
            market=MARKETS[token[0]].condition_id,
            state=TokenState.READY,
            previous=TokenState.SYNCHRONIZING,
            reason=reason,
            at=datetime.now(UTC),
            connection=1,
            last_confirmed_at=None,
            winning_asset_id=None,
        )

    return make


def book(tick_size: Any = Decimal("0.01")) -> Item:
    def make() -> BookEvent:
        return BookEvent(
            event_type="book",
            market=MARKETS["A"].condition_id,
            source_timestamp_ms=T0 - 30000,
            received_at=datetime.now(UTC),
            connection=1,
            frame=1,
            index=0,
            repeat=False,
            raw=None,
            asset_id=TOKEN_IDS["A1"],
            hash="0" * 40,
            bids=(Level(Decimal("0.48"), Decimal("100")),),
            asks=(),
            tick_size=tick_size,
            last_trade_price=Decimal("0.500"),
            opening=True,
            held_book_matched=None,
        )

    return make


def raising(error: BaseException) -> Item:
    return lambda: error


def test_a_scenario_that_passes() -> None:
    (fake,) = scenario_run(
        "scenario example\n"
        "markets A\n"
        "o: wait 0\n"
        "expect token A1 ready reason=book within 0..0.3 of o\n"
        "expect book A1 opening=true tick_size=0.01\n"
        "expect-nothing 0.2\n"
        "expect-backlog 0\n"
        "expect-stats frames=0 interruptions.dropped=1\n"
        "subscribe B\n",
        [(0.1, ready()), (0, book())],
    )
    assert fake.subscribed == [MARKETS["B"].market()]
    assert fake.closed


def test_a_mismatch_reports_the_line_the_difference_and_the_record() -> None:
    failed = fails(
        "scenario example\n"
        "markets A\n"
        "expect token A1 ready\n"
        "  reason=book connection=1\n",
        [(0, ready(reason="subscribed"))],
    )
    assert (failed.scenario, failed.line) == ("example", 502)
    assert failed.failure.expected == "reason=book"
    assert failed.failure.actual == "reason=subscribed"
    report = failed.report()
    assert (
        "conformance.md:502: expect token A1 ready reason=book connection=1" in report
    )
    assert "record:   TokenStateChange(token_id=A1, " in report


def test_a_record_inside_its_window_passes_with_the_tolerance() -> None:
    scenario_run(
        "scenario example\no: wait 0\nexpect token A1 ready within 0..0.05 of o\n",
        [(0.15, ready())],
    )


@pytest.mark.parametrize(("delay", "window"), [(0.6, "0..0.1"), (0.1, "0.5..1")])
def test_a_record_outside_its_window_fails(delay: float, window: str) -> None:
    failed = fails(
        f"scenario example\no: wait 0\nexpect token A1 ready within {window} of o\n",
        [(delay, ready())],
    )
    assert failed.failure.expected == f"within {window} of o"
    assert "outside the window" in failed.failure.message
    assert failed.failure.record is not None
    assert failed.failure.record.startswith("TokenStateChange(token_id=A1, ")


def test_a_server_steps_time_is_a_label_too() -> None:
    scenario_run(
        "scenario example\n"
        "x: expect-no-connect 0.2\n"
        "expect token A1 ready within 0.1..0.3 of x\n",
        [(0.4, ready())],
    )


def test_expect_nothing_fails_when_a_record_arrives() -> None:
    failed = fails("scenario example\nexpect-nothing 0.5\n", [(0.1, ready())])
    assert failed.failure.message == "a record arrived"


@pytest.mark.parametrize("step", ["expect-nothing 0.5", "read-timeout 0.5"])
def test_a_timeout_the_client_raises_is_not_the_steps_own(step: str) -> None:
    failed = fails(
        f"scenario example\n{step}\nexpect token A1 ready\n",
        [(0, raising(TimeoutError())), (0.05, ready())],
    )
    assert failed.failure.message == "the read raised"
    assert failed.failure.actual == "TimeoutError()"


def test_a_read_that_times_out_loses_no_record() -> None:
    scenario_run(
        "scenario example\nread-timeout 0.2\nexpect token A1 ready\n",
        [(0.4, ready())],
    )


def test_read_timeout_fails_when_a_record_arrives() -> None:
    failed = fails("scenario example\nread-timeout 0.5\n", [(0, ready())])
    assert failed.failure.message == "the read did not time out"


def test_expect_error_then_expect_end() -> None:
    # The end of the steps reads the end again, as the iterator gives it.
    scenario_run(
        "scenario example\n"
        "expect-error RecoveryFailed\n"
        "expect-end\n"
        "expect-no-connect 0.2\n",
        [(0, raising(RecoveryFailed("bounds"))), (0, lambda: _END)],
    )


def test_expect_error_fails_on_another_exception() -> None:
    failed = fails(
        "scenario example\nexpect-error RecoveryFailed\n",
        [(0, raising(RuntimeError("defect")))],
    )
    assert failed.failure.actual == "RuntimeError('defect')"


def test_a_step_that_waits_fails_after_its_timeout() -> None:
    failed = fails("scenario example\nexpect token A1 ready\n", step_timeout=0.3)
    assert failed.failure.message == "no record arrived within 0.3 s"


def test_a_record_still_waiting_at_the_end_fails() -> None:
    failed = fails(
        "scenario example\nexpect token A1 ready\n", [(0, ready()), (0, ready("A2"))]
    )
    assert failed.failure.message == "a record is waiting"
    assert failed.step.startswith("after the last step")


def test_an_iterator_that_ends_while_the_client_runs_fails() -> None:
    failed = fails(
        "scenario example\nexpect token A1 ready\nexpect-no-connect 0.2\n",
        [(0, ready()), (0, lambda: _END)],
    )
    assert failed.failure.message == "the iterator ended while the client was running"
    assert failed.step.startswith("after the last step")


def test_a_timeout_the_client_raises_at_the_end_fails() -> None:
    failed = fails("scenario example\nwait 0.1\n", [(0, raising(TimeoutError()))])
    assert (failed.failure.message, failed.failure.actual) == (
        "a read raised",
        "TimeoutError()",
    )


def test_a_backlog_at_the_end_fails() -> None:
    failed = fails("scenario example\nwait 0.1\nexpect-backlog 1\n", [(0, book())])
    assert failed.failure.message == "records are waiting"


def test_expect_backlog_and_stats_compare() -> None:
    failed = fails("scenario example\nexpect-backlog 1\n")
    assert (failed.failure.expected, failed.failure.actual) == ("backlog=1", "0")
    failed = fails("scenario example\nexpect-stats interruptions>=2\n")
    assert failed.failure.actual == "interruptions=1"


class Broken(Fake):
    """A client whose ``stats()`` and ``backlog`` raise."""

    @property
    def backlog(self) -> int:
        raise RuntimeError("no backlog")

    def stats(self) -> ClientStats:
        raise RuntimeError("no stats")


@pytest.mark.parametrize(
    ("step", "message", "actual"),
    [
        ("expect-stats frames=0", "the step raised", "RuntimeError('no stats')"),
        ("expect-backlog 0", "the step raised", "RuntimeError('no backlog')"),
        ("wait 0", "the end of the steps raised", "RuntimeError('no backlog')"),
    ],
)
def test_an_exception_from_a_step_is_reported_with_its_line(
    step: str, message: str, actual: str
) -> None:
    failed = fails(f"scenario example\n{step}\n", fake=Broken)
    assert (failed.line, failed.failure.message, failed.failure.actual) == (
        501,
        message,
        actual,
    )
    assert isinstance(failed.__cause__, RuntimeError)


def test_exit_leaves_the_block_and_checks_shutdown() -> None:
    (fake,) = scenario_run(
        "scenario example\nexit\nexpect-end\nsubscribe B raises ClientStateError\n"
    )
    assert fake.closed


def test_cancel_cancels_the_host_task() -> None:
    scenario_run("scenario example\ncancel\nexpect-end\n")


def test_a_block_that_swallows_the_cancellation_fails() -> None:
    failed = fails("scenario example\ncancel\nexpect-end\n", swallow_cancel=True)
    assert failed.failure.message == "the cancellation did not leave the client's block"
    assert failed.line == 501


def test_a_task_that_outlives_the_block_fails() -> None:
    failed = fails("scenario example\nexit\n", leak=True)
    assert failed.failure.message == "tasks the client started outlive its block"
    assert failed.failure.actual == "leaked"


def fails_abandoning(text: str, **options: Any) -> str:
    """The message of a failure for which the runner abandoned tasks, once
    they are gone: asyncio logs each as destroyed while pending, in this
    test rather than a later one."""
    message = fails(text, **options).failure.message
    gc.collect()
    return message


def test_a_block_that_ignores_cancellation_fails_without_hanging() -> None:
    # The clean-up waits at most a step's timeout for each task it cancels,
    # well before the client gives in.
    start = time.monotonic()
    assert (
        fails_abandoning("scenario example\nexit\n", step_timeout=0.3, stuck_exit=30)
        == "the client's block did not end within 0.3 s"
    )
    assert time.monotonic() - start < 10


def test_a_task_that_ignores_cancellation_fails_without_hanging() -> None:
    start = time.monotonic()
    assert (
        fails_abandoning(
            "scenario example\nexit\n", step_timeout=0.3, leak=True, stuck_leak=30
        )
        == "tasks the client started outlive its block"
    )
    assert time.monotonic() - start < 10


def test_the_block_ending_on_its_own_fails() -> None:
    failed = fails("scenario example\nexpect-nothing 2\n", end_block_after=0.2)
    assert (
        failed.failure.message == "the client's block ended before the scenario left it"
    )
    assert failed.line == 501


def test_a_float_in_a_decimal_field_fails() -> None:
    failed = fails("scenario example\nexpect book A1\n", [(0, book(tick_size=0.01))])
    assert "decimal.Decimal" in failed.failure.message
    assert failed.failure.actual == "BookEvent.tick_size is a float: 0.01"


def test_none_in_a_decimal_field_that_does_not_allow_it_fails() -> None:
    def make() -> object:
        record: Any = book()()
        return replace(record, bids=(replace(record.bids[0], price=None),))

    failed = fails("scenario example\nexpect book A1\n", [(0, make)])
    assert failed.failure.actual == "BookEvent.bids[0].price is a NoneType: None"


def test_resolve_returns_the_synthetic_market_or_raises() -> None:
    scenario_run(
        "scenario example\n"
        "resolve synthetic-a\n"
        "resolve synthetic-u raises MarketNotFound\n"
        "lookup A missing\n"
        "resolve synthetic-a raises MarketNotFound\n"
    )
    failed = fails("scenario example\nresolve synthetic-u\n")
    assert failed.failure.actual is not None
    assert failed.failure.actual.startswith("MarketNotFound")
    failed = fails("scenario example\nresolve synthetic-a raises MarketNotFound\n")
    assert failed.failure.message == "resolve did not raise"


def test_a_call_that_should_raise_and_does_not_fails() -> None:
    failed = fails("scenario example\nsubscribe B raises ClientStateError\n")
    assert failed.failure.message == "the call did not raise"


def test_a_lookup_call_market_lookup_does_not_define_fails_the_step() -> None:
    def undefined_call(fake: list[Fake]) -> Item:
        def make() -> object:
            with contextlib.suppress(TypeError):
                fake[0].lookup.by_condition_id("0x")  # type: ignore[attr-defined]
            return ready()()

        return make

    holder: list[Fake] = []

    def factory(
        config: ClientConfig, markets: tuple[Market, ...], lookup: MarketLookup
    ) -> Client:
        holder.append(Fake(lookup, [(0, undefined_call(holder))]))
        return holder[0]

    with pytest.raises(ScenarioFailed) as failed:
        run(parse_scenario("scenario example\nexpect token A1 ready\n"), factory)
    assert "a call MarketLookup does not define" in failed.value.failure.message


SCENARIOS = load()


def test_every_scenarios_configuration_builds() -> None:
    for scenario in SCENARIOS:
        config = build_config(scenario, "ws://127.0.0.1:1")
        assert config.url == "ws://127.0.0.1:1"
    by_name = {scenario.name: scenario for scenario in SCENARIOS}
    config = build_config(by_name["consumer-pause-outlasts-recovery-time"], "ws://x")
    assert (config.queue_size, config.backlog_warning, config.resume_below) == (
        4,
        0.75,
        0.25,
    )
    assert config.reconnect.max_recovery_time == 0.5
    assert config.reconnect.max_attempts == 3
    assert (
        build_config(by_name["new-market-delivered"], "ws://x").new_market == "deliver"
    )
    assert build_config(by_name["hash-checks-pass"], "ws://x").verify_hash is True


def test_enabled_txt_lists_scenarios_once() -> None:
    known = {scenario.name for scenario in SCENARIOS}
    assert set(enabled_names(ENABLED.read_text(), known)) <= known
    assert enabled_names("initial-books\n\nread-timeout\n", known) == [
        "initial-books",
        "read-timeout",
    ]
    with pytest.raises(ValueError, match="not a scenario"):
        enabled_names("no-such-scenario\n", known)
    with pytest.raises(ValueError, match="twice"):
        enabled_names("initial-books\ninitial-books\n", known)
