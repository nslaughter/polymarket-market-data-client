"""The runner (spec/conformance.md, The runner).

``run`` starts the scripted server, builds the configuration and the
scripted lookup, enters the client's block in a host task, and runs the
scenario's steps in a step task. A step that awaits the client does so in a
task of its own, so that its timeout holds whatever the client does. It
raises ``ScenarioFailed`` for the first failing step.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Protocol

from polymarket_market_data import (
    ClientConfig,
    ClientStats,
    Market,
    MarketDataClient,
    MarketLookup,
    ReconnectPolicy,
)

from .failures import ScenarioFailed, StepFailed
from .lookup import ScriptedLookup
from .matching import RecordPattern, misplaced_decimal, render_record
from .notation import (
    Accept,
    Action,
    Cancel,
    Close,
    Drop,
    Exit,
    Expect,
    ExpectBacklog,
    ExpectClientClose,
    ExpectEnd,
    ExpectError,
    ExpectNoConnect,
    ExpectNothing,
    ExpectStats,
    IdleClose,
    Pong,
    ReadTimeout,
    RecvPing,
    RecvSubscribe,
    Refuse,
    RefuseAll,
    ReleasePongs,
    Resolve,
    Scenario,
    Send,
    SendAgain,
    SendBinary,
    SendBurst,
    SendText,
    SetLookup,
    Silent,
    Step,
    Subscribe,
    Trade,
    Unsubscribe,
    Wait,
    Within,
)
from .server import ScriptedServer
from .synthetic import MARKETS, PROFILE, PROFILE_RECONNECT

STEP_TIMEOUT = 5.0
"""A step that waits fails after this many seconds unless it says otherwise."""

WAITING_CHECK = 0.05
"""How long the end of a scenario waits to see that no record is waiting."""

EARLY = 0.05
LATE = 0.25
"""A ``within`` window's tolerance, in seconds, at each end."""


class Client(Protocol):
    """What the runner uses of ``MarketDataClient``."""

    async def __aenter__(self) -> object: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...

    def subscribe(self, *markets: Market) -> None: ...

    def unsubscribe(self, *condition_ids: str) -> None: ...

    def records(self) -> AsyncIterator[object]: ...

    @property
    def backlog(self) -> int: ...

    def stats(self) -> ClientStats: ...

    async def resolve(self, slug: str) -> Market: ...


ClientFactory = Callable[[ClientConfig, tuple[Market, ...], MarketLookup], Client]


def market_data_client(
    config: ClientConfig, markets: tuple[Market, ...], lookup: MarketLookup
) -> Client:
    return MarketDataClient(config, markets=markets, lookup=lookup)


def build_config(scenario: Scenario, url: str) -> ClientConfig:
    """The profile, with the scenario's ``config`` lines and ``url`` pointing
    at the server."""
    policy = ReconnectPolicy(**{**PROFILE_RECONNECT, **scenario.reconnect})
    return ClientConfig(url=url, reconnect=policy, **{**PROFILE, **scenario.config})


def run(
    scenario: Scenario,
    client: ClientFactory = market_data_client,
    *,
    step_timeout: float = STEP_TIMEOUT,
) -> None:
    """Run one scenario against a client."""
    with ScriptedServer() as server:
        _Run(scenario, server, client, step_timeout).execute()


def now() -> datetime:
    return datetime.now(UTC)


class _Run:
    def __init__(
        self,
        scenario: Scenario,
        server: ScriptedServer,
        factory: ClientFactory,
        step_timeout: float,
    ) -> None:
        self.scenario = scenario
        self.server = server
        self.factory = factory
        self.step_timeout = step_timeout
        self.lookup = ScriptedLookup(scenario.lookup)
        self.times: dict[str, datetime] = {}
        self.step: Step | None = None
        self.leaving = False
        self.left = False
        self.ended_early = False
        self.finishing = False
        self.iterator_ended = False
        """An ``expect-end`` read the iterator's end."""
        self.matched: object | None = None
        """The record the current ``expect`` step matched."""

    def execute(self) -> None:
        """Run the scenario on an event loop of its own, as ``asyncio.run``
        does, except that the tasks left when it ends get ``step_timeout``
        seconds to end once cancelled, not forever: a client that resists
        cancellation fails the scenario instead of hanging it."""
        loop = asyncio.new_event_loop()
        try:
            try:
                loop.run_until_complete(self.run())
            finally:
                stuck = loop.run_until_complete(self._end_tasks())
        finally:
            loop.close()
        if stuck:
            raise self._failed(
                StepFailed(
                    "tasks the client started did not end once cancelled",
                    actual=", ".join(stuck),
                )
            )

    async def run(self) -> None:
        try:
            config = build_config(self.scenario, self.server.url)
            markets = tuple(MARKETS[name].market() for name in self.scenario.markets)
            self.client = self.factory(config, markets, self.lookup)
            self.records = self.client.records()
        except Exception as error:
            raise self._failed(
                StepFailed("the client could not be built", actual=repr(error))
            ) from error
        self.before = asyncio.all_tasks()
        self.leave = asyncio.Event()
        self.host = asyncio.create_task(self._host(), name="conformance-host")
        self.steps = asyncio.create_task(self._steps(), name="conformance-steps")
        self.host.add_done_callback(self._host_done)
        try:
            await self.steps
        except asyncio.CancelledError:
            if not self.ended_early:
                raise
            raise self._failed(self._ended_early()) from None
        finally:
            await self._clean_up()

    async def _host(self) -> None:
        async with self.client:
            await self.leave.wait()

    def _host_done(self, host: asyncio.Task[None]) -> None:
        if not self.leaving:
            self.ended_early = True
            self.steps.cancel()

    async def _steps(self) -> None:
        for step in self.scenario.steps:
            self.step = step
            self.matched = None
            try:
                at = await self._step(step.action, step.label)
                if step.within is not None:
                    self._check_within(step.within, at)
                self._check_violations(self.server.violations())
            except StepFailed as failure:
                raise self._failed(self._step_failure(failure)) from None
            except Exception as error:
                # Reported as a failed step, with its line, all the same.
                raised = StepFailed("the step raised", actual=repr(error))
                raise self._failed(self._step_failure(raised)) from error
            if step.label is not None:
                self.times[step.label] = at
        self.step = None
        self.finishing = True
        try:
            await self._finish()
        except StepFailed as failure:
            raise self._failed(failure) from None
        except Exception as error:
            raised = StepFailed("the end of the steps raised", actual=repr(error))
            raise self._failed(raised) from error

    def _step_failure(self, failure: StepFailed) -> StepFailed:
        """A step that failed because the block ended says so."""
        ended = self.host.done() and not self.leaving
        return self._ended_early() if ended else failure

    def _ended_early(self) -> StepFailed:
        return StepFailed(
            "the client's block ended before the scenario left it",
            actual=_outcome(self.host),
        )

    def _failed(self, failure: StepFailed) -> ScenarioFailed:
        if self.step is not None:
            return ScenarioFailed(
                self.scenario.name, self.step.line, self.step.text, failure
            )
        if self.finishing:
            last = self.scenario.steps[-1]
            return ScenarioFailed(
                self.scenario.name,
                last.line,
                f"after the last step, {last.text}",
                failure,
            )
        return ScenarioFailed(
            self.scenario.name,
            self.scenario.line,
            f"scenario {self.scenario.name}",
            failure,
        )

    async def _step(self, action: Action, label: str | None) -> datetime:
        server = self.server
        match action:
            case Accept():
                return await self._server(server.accept())
            case Refuse(n=n):
                return await self._server(server.refuse(n))
            case RefuseAll():
                return await self._server(server.refuse_all())
            case RecvSubscribe(tokens=tokens):
                return await self._server(server.recv_subscribe(tokens))
            case RecvPing():
                return await self._server(server.recv_ping())
            case Send(frame=frame):
                return await self._server(server.send(frame, label))
            case SendText(text=text):
                return await self._server(server.send_text(text, label))
            case SendBinary(data=data):
                return await self._server(server.send_binary(data, label))
            case SendAgain(label=again, reverse=reverse):
                return await self._server(server.send_again(again, reverse, label))
            case SendBurst(market=market, t=t, every=every, entries=entries):
                duration = every * (len(entries) - 1)
                return await self._server(
                    server.send_burst(market, t, every, entries), duration
                )
            case Pong(mode=mode):
                return await self._server(server.pong(mode))
            case ReleasePongs():
                return await self._server(server.release_pongs())
            case Silent(token=token, t=t, entry=entry):
                return await self._server(server.silent(token, t, entry))
            case Trade(market=market, price=price):
                return await self._server(server.trade(market, price))
            case IdleClose(seconds=seconds):
                return await self._server(server.idle_close(seconds))
            case Close(code=code, reason=reason):
                return await self._server(server.close(code, reason))
            case Drop():
                return await self._server(server.drop())
            case ExpectClientClose(code=code, reason=reason):
                return await self._server(server.expect_client_close(code, reason))
            case ExpectNoConnect(seconds=seconds):
                return await self._server(server.expect_no_connect(seconds), seconds)
            case Wait(seconds=seconds):
                await asyncio.sleep(seconds)
            case Subscribe(market=market):
                added = MARKETS[market].market()
                self._call(lambda: self.client.subscribe(added))
            case Unsubscribe(market=market):
                condition_id = MARKETS[market].condition_id
                self._call(lambda: self.client.unsubscribe(condition_id))
            case Resolve(slug=slug):
                await self._resolve(slug)
            case SetLookup(market=market, answer=answer):
                self.lookup.answers[market] = answer
            case Exit():
                await self._leave(cancel=False)
            case Cancel():
                await self._leave(cancel=True)
            case ReadTimeout(seconds=seconds):
                await self._nothing(seconds, "the read did not time out")
            case Expect(pattern=pattern):
                return await self._expect(pattern)
            case ExpectNothing(seconds=seconds):
                await self._nothing(seconds, "a record arrived")
            case ExpectError(exception=exception):
                await self._expect_error(exception)
            case ExpectEnd():
                await self._expect_end()
            case ExpectBacklog(n=n):
                backlog = self.client.backlog
                if backlog != n:
                    raise StepFailed(
                        "the backlog differs",
                        expected=f"backlog={n}",
                        actual=f"{backlog}",
                    )
            case ExpectStats(checks=checks):
                stats = self.client.stats()
                for check in checks:
                    mismatch = check.check(stats)
                    if mismatch is not None:
                        raise StepFailed(
                            f"the counter {mismatch.field} differs",
                            expected=mismatch.expected,
                            actual=mismatch.actual,
                        )
        return now()

    # Server steps.

    async def _server[T](
        self, step: Coroutine[Any, Any, T], duration: float = 0.0
    ) -> T:
        timeout = self.step_timeout + duration
        try:
            async with asyncio.timeout(timeout) as scope:
                return await self.server.call(step)
        except TimeoutError:
            if scope.expired():
                raise StepFailed(f"the step timed out after {timeout:g} s") from None
            raise

    # Runner steps.

    def _call(self, call: Callable[[], object]) -> None:
        assert self.step is not None
        raises = self.step.raises
        try:
            call()
        except Exception as error:
            if raises is not None and isinstance(error, raises):
                return
            expected = f"raises {raises.__name__}" if raises else "no exception"
            raise StepFailed(
                "the call raised", expected=expected, actual=repr(error)
            ) from None
        if raises is not None:
            raise StepFailed(
                "the call did not raise", expected=f"raises {raises.__name__}"
            )

    async def _resolve(self, slug: str) -> None:
        assert self.step is not None
        raises = self.step.raises
        try:
            result = await self._bounded(
                "resolve", lambda: self.client.resolve(slug), self.step_timeout
            )
        except _Expired:
            raise StepFailed(
                f"resolve did not return within {self.step_timeout:g} s"
            ) from None
        except _Raised as raised:
            error = raised.error
            if raises is not None and isinstance(error, raises):
                return
            expected = f"raises {raises.__name__}" if raises else "no exception"
            raise StepFailed(
                "resolve raised", expected=expected, actual=repr(error)
            ) from None
        if raises is not None:
            raise StepFailed(
                "resolve did not raise",
                expected=f"raises {raises.__name__}",
                actual=repr(result),
            )
        expected_market = next(m for m in MARKETS.values() if m.slug == slug).market()
        if result != expected_market:
            raise StepFailed(
                "resolve returned another market",
                expected=repr(expected_market),
                actual=repr(result),
            )

    async def _leave(self, *, cancel: bool) -> None:
        """Leave the client's block, by ``exit`` or ``cancel``, then check
        shutdown."""
        if self.left:
            raise StepFailed("the client's block was already left")
        self.leaving = True
        if cancel:
            self.host.cancel()
        else:
            self.leave.set()
        await asyncio.wait({self.host}, timeout=self.step_timeout)
        if not self.host.done():
            raise StepFailed(
                f"the client's block did not end within {self.step_timeout:g} s"
            )
        self.left = True
        if self.host.cancelled():
            if not cancel:
                raise StepFailed(
                    "the client's block ended cancelled", actual="cancelled"
                )
        elif self.host.exception() is not None:
            raise StepFailed(
                "leaving the client's block raised", actual=_outcome(self.host)
            )
        elif cancel:
            # The client's __aexit__ swallowed the CancelledError, which the
            # contract says leaves the block (client.md, Cancellation and
            # shutdown).
            raise StepFailed(
                "the cancellation did not leave the client's block",
                expected="cancelled",
                actual="returned",
            )
        running = [
            task
            for task in asyncio.all_tasks()
            if task not in self.before and task not in (self.host, self.steps)
        ]
        if running:
            raise StepFailed(
                "tasks the client started outlive its block",
                actual=", ".join(sorted(task.get_name() for task in running)),
            )
        try:
            record = await self._next(self.step_timeout)
        except StopAsyncIteration:
            return
        except _Expired:
            raise StepFailed("the iterator did not end after shutdown") from None
        except _Raised as raised:
            raise StepFailed(
                "the iterator raised after shutdown",
                expected="StopAsyncIteration",
                actual=repr(raised.error),
            ) from None
        raise StepFailed(
            "a record arrived after shutdown", record=render_record(record)
        )

    # Expectation steps.

    async def _expect(self, pattern: RecordPattern) -> datetime:
        record = await self._read()
        mismatch = pattern.match(record)
        if mismatch is not None:
            raise StepFailed(
                f"the record differs in {mismatch.field}",
                expected=mismatch.expected,
                actual=mismatch.actual,
                record=render_record(record),
            )
        self.matched = record
        return _record_time(record)

    async def _next(self, seconds: float) -> object:
        """The next record, read within ``seconds``, as ``_bounded`` reads
        it."""
        return await self._bounded("the read", lambda: anext(self.records), seconds)

    async def _bounded[T](
        self, what: str, call: Callable[[], Awaitable[T]], seconds: float
    ) -> T:
        """Await ``call()`` inside ``asyncio.timeout(seconds)``, as a consumer
        would, but in a task of the runner's, so that the deadline holds even
        if the client suppresses the cancellation the timeout acts through.

        ``_Expired`` means the timeout ended the call; ``StopAsyncIteration``,
        that the iterator ended; ``_Raised``, that the client raised anything
        else, a ``TimeoutError`` of its own included. A call that returns
        although its timeout expired, or that has not ended ``step_timeout``
        after it, fails the step."""
        task = asyncio.create_task(_timed(call, seconds), name="conformance-call")
        try:
            await asyncio.wait({task}, timeout=seconds + self.step_timeout)
        finally:
            if not task.done():
                # Abandoned: the end of the run cancels it again and waits
                # for it, as long as for any task left, and nothing reads
                # its outcome.
                task.cancel()
                task.add_done_callback(_discard)
        if not task.done():
            raise StepFailed(
                f"{what} did not end once its {seconds:g} s timeout expired",
                actual=f"still running {self.step_timeout:g} s later",
            )
        if task.cancelled():
            # A CancelledError of the client's own, not the timeout's.
            raise _Raised(asyncio.CancelledError())
        error = task.exception()
        if isinstance(error, _Late):
            raise StepFailed(
                f"{what} returned although its {seconds:g} s timeout expired",
                actual=render_record(error.result),
            )
        return task.result()

    async def _read(self) -> object:
        try:
            record = await self._next(self.step_timeout)
        except _Expired:
            raise StepFailed(
                f"no record arrived within {self.step_timeout:g} s"
            ) from None
        except StopAsyncIteration:
            raise StepFailed("the iterator ended") from None
        except _Raised as raised:
            raise StepFailed("the read raised", actual=repr(raised.error)) from None
        self._check_decimals(record)
        return record

    async def _nothing(self, seconds: float, message: str) -> None:
        try:
            record = await self._next(seconds)
        except _Expired:
            return
        except StopAsyncIteration:
            raise StepFailed("the iterator ended") from None
        except _Raised as raised:
            raise StepFailed("the read raised", actual=repr(raised.error)) from None
        self._check_decimals(record)
        raise StepFailed(message, record=render_record(record))

    async def _expect_error(self, exception: type[BaseException]) -> None:
        try:
            record = await self._next(self.step_timeout)
        except _Expired:
            raise StepFailed(
                f"nothing was raised within {self.step_timeout:g} s",
                expected=exception.__name__,
            ) from None
        except StopAsyncIteration:
            raise StepFailed(
                "the iterator ended", expected=exception.__name__
            ) from None
        except _Raised as raised:
            if isinstance(raised.error, exception):
                return
            raise StepFailed(
                "the read raised another exception",
                expected=exception.__name__,
                actual=repr(raised.error),
            ) from None
        self._check_decimals(record)
        raise StepFailed(
            "a record arrived",
            expected=exception.__name__,
            record=render_record(record),
        )

    async def _expect_end(self) -> None:
        try:
            record = await self._next(self.step_timeout)
        except StopAsyncIteration:
            self.iterator_ended = True
            return
        except _Expired:
            raise StepFailed(
                f"the iterator did not end within {self.step_timeout:g} s"
            ) from None
        except _Raised as raised:
            raise StepFailed(
                "the read raised",
                expected="StopAsyncIteration",
                actual=repr(raised.error),
            ) from None
        self._check_decimals(record)
        raise StepFailed(
            "a record arrived",
            expected="StopAsyncIteration",
            record=render_record(record),
        )

    # Checks.

    def _check_decimals(self, record: object) -> None:
        misplaced = misplaced_decimal(record)
        if misplaced is not None:
            raise StepFailed(
                "a Decimal field holds something other than a decimal.Decimal",
                actual=misplaced,
                record=render_record(record),
            )

    def _check_within(self, within: Within, at: datetime) -> None:
        start = self.times[within.label]
        # An expect step's failure prints the record it read.
        record = None if self.matched is None else render_record(self.matched)
        try:
            delta = (at - start).total_seconds()
        except TypeError as error:
            raise StepFailed(
                f"the time {at!r} cannot be compared: {error}", record=record
            ) from None
        if not within.lo - EARLY <= delta <= within.hi + LATE:
            raise StepFailed(
                f"the time is outside the window, {within.lo - EARLY:.2f} to "
                f"{within.hi + LATE:.2f} s after {within.label}",
                expected=f"within {within.lo:g}..{within.hi:g} of {within.label}",
                actual=f"{delta:.3f} s after {within.label}",
                record=record,
            )

    def _check_violations(self, violations: list[str]) -> None:
        problems = violations + self.lookup.violations
        if problems:
            raise StepFailed("; ".join(problems))

    async def _finish(self) -> None:
        """When the steps run out: no record is waiting, the block is left as
        ``exit`` leaves it, and no frame went unconsumed."""
        backlog = self.client.backlog
        if backlog != 0:
            raise StepFailed(
                "records are waiting", expected="backlog=0", actual=f"{backlog}"
            )
        try:
            record = await self._next(WAITING_CHECK)
        except _Expired:
            pass
        except StopAsyncIteration:
            # The iterator ends only once the client has shut down
            # (client.md, Cancellation and shutdown): here, once the block
            # was left or an expect-end read the end.
            if not (self.left or self.iterator_ended):
                raise StepFailed(
                    "the iterator ended while the client was running"
                ) from None
        except _Raised as raised:
            raise StepFailed("a read raised", actual=repr(raised.error)) from None
        else:
            raise StepFailed("a record is waiting", record=render_record(record))
        if not self.left:
            await self._leave(cancel=False)
        self._check_violations(await self.server.call(self.server.finish()))

    async def _clean_up(self) -> None:
        """Leave the block, whatever failed, so that nothing of the client's
        outlives the scenario. Each wait is bounded, so that a client that
        resists cancellation cannot hide the failure by hanging."""
        if not hasattr(self, "host"):
            return
        if not self.host.done() and not self.leaving:
            self.leaving = True
            self.leave.set()
            await asyncio.wait({self.host}, timeout=self.step_timeout)
        if not self.host.done():
            self.host.cancel()
            await asyncio.wait({self.host}, timeout=self.step_timeout)
        if self.host.done() and not self.host.cancelled():
            self.host.exception()
        if not self.steps.done():
            self.steps.cancel()
            await asyncio.wait({self.steps}, timeout=self.step_timeout)

    async def _end_tasks(self) -> list[str]:
        """Cancel every task left on the loop, then end its asynchronous
        generators and its executor, as ``asyncio.run`` does, waiting at most
        ``step_timeout`` for each; the names of the tasks still running."""
        loop = asyncio.get_running_loop()
        tasks = asyncio.all_tasks() - {asyncio.current_task()}
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=self.step_timeout)
        await asyncio.wait(
            {
                asyncio.ensure_future(loop.shutdown_asyncgens()),
                asyncio.ensure_future(loop.shutdown_default_executor()),
            },
            timeout=self.step_timeout,
        )
        return sorted(task.get_name() for task in tasks if not task.done())


async def _timed[T](call: Callable[[], Awaitable[T]], seconds: float) -> T:
    """``call()`` inside ``asyncio.timeout(seconds)``, its outcome told apart
    as ``_Run._bounded`` says."""
    try:
        async with asyncio.timeout(seconds) as scope:
            result = await call()
    except StopAsyncIteration:
        raise
    except TimeoutError as error:
        if scope.expired():
            raise _Expired from None
        raise _Raised(error) from None
    except Exception as error:
        raise _Raised(error) from None
    if scope.expired():
        # Only a call that suppressed its cancellation can get here.
        raise _Late(result)
    return result


def _discard(task: asyncio.Task[Any]) -> None:
    """Read an abandoned task's outcome, so that asyncio does not log it."""
    if not task.cancelled():
        task.exception()


class _Expired(Exception):
    """The runner's timeout around a call into the client expired."""


class _Raised(Exception):
    """A call into the client raised ``error``."""

    def __init__(self, error: BaseException) -> None:
        super().__init__(repr(error))
        self.error = error


class _Late(Exception):
    """A call into the client returned ``result`` although its timeout had
    expired."""

    def __init__(self, result: object) -> None:
        super().__init__()
        self.result = result


def _record_time(record: object) -> datetime:
    """``at`` on a status record, ``received_at`` on an event record."""
    at = getattr(record, "at", None)
    if at is None:
        at = getattr(record, "received_at", None)
    if not isinstance(at, datetime):
        raise StepFailed("the record has no time", record=render_record(record))
    return at


def _outcome(task: asyncio.Task[None]) -> str:
    if task.cancelled():
        return "cancelled"
    error = task.exception()
    return "returned" if error is None else repr(error)
