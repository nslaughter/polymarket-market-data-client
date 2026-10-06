"""The client against the scripted server, for behavior no conformance
scenario covers: shutdown, the heartbeat's counters, ``keep_raw``, ends of a
connection the scenarios leave out, the backoff's jitter, a defect in the
client's own code, and the stopgaps that end the client until later plan
steps replace them (spec/client.md, Cancellation and shutdown, Heartbeat,
Detecting an interruption, Reconnecting, and Errors).

Most are scenarios in the conformance notation, which the runner runs as it
runs those in spec/conformance.md; a test that needs the client itself
drives it step by step.
"""

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from polymarket_market_data import (
    ClientConfig,
    ClientError,
    ClientStateError,
    ConnectionState,
    ConnectionStateChange,
    MarketDataClient,
    ReconnectPolicy,
    RecoveryFailed,
    TokenStateChange,
    _connection,
)

from .notation import parse_scenario
from .runner import run
from .server import ScriptedServer
from .synthetic import MARKETS, PROFILE, PROFILE_RECONNECT

MARKET_A = MARKETS["A"].market()
MARKET_B = MARKETS["B"].market()


def config(url: str, **overrides: Any) -> ClientConfig:
    """The conformance profile, with ``url`` and these fields set."""
    policy = ReconnectPolicy(**PROFILE_RECONNECT)
    return ClientConfig(url=url, reconnect=policy, **{**PROFILE, **overrides})


async def read_all(records: AsyncIterator[object]) -> tuple[list[object], object]:
    """Every record up to the iterator's end, and what ended it: the
    exception it raised, or ``StopAsyncIteration``."""
    read: list[object] = []
    while True:
        try:
            async with asyncio.timeout(5):
                read.append(await anext(records))
        except StopAsyncIteration:
            return read, StopAsyncIteration
        except ClientError as error:
            return read, error


# Scenarios in the conformance notation, for behavior the conformance
# scenarios do not cover.

SCENARIOS = {
    "an-empty-desired-set-connects-nowhere": """
expect-no-connect 0.5
expect-nothing 0.1
exit
expect-end
""",
    "cancelling-the-block-closes-the-connection": """
markets A
start A
cancel
expect-client-close 1000 "client exit"
expect-end
""",
    "leaving-the-block-discards-queued-records": """
markets A
start A
send pc A t=100 A1:BUY:0.49:10
wait 0.2
expect-backlog 1
exit
expect-client-close 1000 "client exit"
expect-end
""",
    "pongs-are-counted-and-are-not-frames": """
markets A
start A
send-text PONG
expect-nothing 0.1
expect-stats pongs=1 pongs_unsolicited=1 frames=1
send pc A t=100 A1:BUY:0.49:10
expect price_change A t=100 frame=2 raw=none
recv-ping
recv-ping
expect-nothing 0.1
expect-stats pongs>=3 pongs_unsolicited=1 pong_delay_max<=0.2 frames=2
""",
    "keep-raw-attaches-the-frame-text": """
markets A
config keep_raw=true
start A
send bba A1 t=100
expect best_bid_ask A1 t=100 raw=set
""",
    # The library ends a connection whose frame exceeds max_message_bytes,
    # a protocol error: an interruption with no close frame, and, since the
    # frame was never delivered, a failed attempt (spec/client.md, Detecting
    # an interruption and Reconnecting).
    "a-frame-larger-than-max-message-bytes-drops-the-connection": """
markets A
config max_message_bytes=64
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
send opening A1 A2
expect conn interrupted reason=dropped connection=1 close_code=none
expect token A1 uncertain previous=synchronizing reason=interrupted
expect token A2 uncertain previous=synchronizing reason=interrupted
expect conn recovering attempt=2 retry_in=0.2 reason=backoff detail=set
expect conn connecting attempt=2
expect-stats interruptions.dropped=1 frames=0
""",
    # Once no desired market is left on a connection, as after the last one
    # settled, its end is not an interruption, and the client does not
    # reconnect (spec/client.md, Detecting an interruption).
    "a-connection-ending-with-no-desired-market-is-not-an-interruption": """
markets A
start A
send resolved A t=100 winner=A1
expect market_resolved A
expect token A1 settled reason=market_resolved
expect token A2 settled reason=market_resolved
expect conn idle connection=1
drop
expect-no-connect 0.5
expect-nothing 0.1
expect-stats interruptions=0
""",
    # Stopgaps: until plan step 7 settles on the all-resolved close, and
    # step 9 responds at the queue's limit, each ends the client with a
    # ClientError, raised after the records already queued.
    "the-all-resolved-close-ends-the-client": """
markets A
start A
close 1000 "all subscribed assets resolved"
expect-error ClientError
expect-end
expect-stats interruptions=0
subscribe B raises ClientStateError
""",
    "a-frame-reaching-the-queue-limit-ends-the-client": """
markets A
config queue_size=1
start A
send pc A t=100 A1:BUY:0.49:10
send pc A t=101 A1:BUY:0.49:20
expect-client-close 1000 "client exit"
expect price_change A t=100
expect-error ClientError
expect-end
expect-stats events.price_change=1
""",
}


@pytest.mark.parametrize("name", SCENARIOS)
def test_scenario(name: str) -> None:
    run(parse_scenario(f"scenario {name}\n{SCENARIOS[name]}"))


# The backoff's jitter (D2). One run of a scenario cannot show a random
# delay, so these read the retry_in of every recovering record of a client
# whose every attempt is refused, until it fails.

BASE_DELAY, MAX_DELAY, MAX_ATTEMPTS = 0.01, 0.04, 8


def delay(attempt: int) -> float:
    """The delay before an attempt, without jitter: min(max_delay,
    base_delay × 2^(k − 1))."""
    return min(MAX_DELAY, BASE_DELAY * 2.0 ** (attempt - 1))


def retries(jitter: bool) -> list[tuple[int, float]]:
    """The attempt and ``retry_in`` of each recovering record."""
    read: list[object] = []

    async def main(server: ScriptedServer) -> None:
        policy = ReconnectPolicy(
            base_delay=BASE_DELAY,
            max_delay=MAX_DELAY,
            max_attempts=MAX_ATTEMPTS,
            max_recovery_time=10.0,
            jitter=jitter,
        )
        configuration = ClientConfig(url=server.url, reconnect=policy, **PROFILE)
        client = MarketDataClient(configuration, markets=[MARKET_A])
        records = client.records()
        await server.call(server.refuse_all())
        async with client:
            records_read, ended = await read_all(records)
        read.extend(records_read)
        assert isinstance(ended, RecoveryFailed)

    run_with(main)
    found = []
    for record in read:
        if (
            isinstance(record, ConnectionStateChange)
            and record.state is ConnectionState.RECOVERING
        ):
            assert record.attempt is not None
            assert record.retry_in is not None
            found.append((record.attempt, record.retry_in))
    assert [attempt for attempt, _ in found] == list(range(2, MAX_ATTEMPTS + 1))
    return found


def test_with_jitter_each_retry_in_lies_below_its_delay_and_they_vary() -> None:
    found = retries(jitter=True)
    assert all(0 <= retry_in < delay(attempt) for attempt, retry_in in found)
    assert len({retry_in for _, retry_in in found}) > 1


def test_without_jitter_each_retry_in_is_its_delay() -> None:
    expected = [(k, delay(k)) for k in range(2, MAX_ATTEMPTS + 1)]
    assert retries(jitter=False) == expected


# A defect in the client's own code ends the client: the exception is raised
# once, as the __cause__ of a ClientError.


def broken(*args: object, **kwargs: object) -> list[Any]:
    raise RuntimeError("a defect")


async def connect_and_send(server: ScriptedServer, text: str) -> None:
    await server.call(server.accept())
    await server.call(server.recv_subscribe(("A1", "A2")))
    await server.call(server.send_text(text))


def run_with(main: Callable[[ScriptedServer], Any]) -> None:
    with ScriptedServer() as server:
        asyncio.run(main(server))


def test_a_defect_is_raised_by_the_iterator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_connection, "decode_frame", broken)

    async def main(server: ScriptedServer) -> None:
        client = MarketDataClient(config(server.url), markets=[MARKET_A])
        records = client.records()
        async with client:
            await connect_and_send(server, "[]\n")
            read, ended = await read_all(records)
            # The client closed its connection as it ended.
            await server.call(server.expect_client_close(1000, "client exit"))
            with pytest.raises(ClientStateError):
                client.subscribe(MARKET_B)
        assert isinstance(ended, ClientError)
        assert isinstance(ended.__cause__, RuntimeError)
        # The records queued before it came first.
        assert [r.state for r in read if isinstance(r, ConnectionStateChange)] == [
            ConnectionState.CONNECTING,
            ConnectionState.OPEN,
            ConnectionState.SUBSCRIBED,
        ]
        assert len([r for r in read if isinstance(r, TokenStateChange)]) == 2
        assert await read_all(records) == ([], StopAsyncIteration)

    run_with(main)


@pytest.mark.parametrize(
    ("leaving", "context"),
    [
        ("normally", type(None)),
        ("by an exception", ValueError),
        ("by cancellation", asyncio.CancelledError),
    ],
)
def test_a_defect_no_consumer_read_is_raised_when_the_block_is_left(
    monkeypatch: pytest.MonkeyPatch, leaving: str, context: type
) -> None:
    # However the block is left, as TaskGroup raises a task's error; what
    # was leaving it is the failure's __context__.
    monkeypatch.setattr(_connection, "decode_frame", broken)

    async def main(server: ScriptedServer) -> None:
        client = MarketDataClient(config(server.url), markets=[MARKET_A])
        records = client.records()
        failed = asyncio.Event()

        async def hold() -> None:
            async with client:
                await connect_and_send(server, "[]\n")
                await server.call(server.expect_client_close(1000, "client exit"))
                failed.set()
                if leaving == "by an exception":
                    raise ValueError("leaving")
                if leaving == "by cancellation":
                    await asyncio.Event().wait()

        holder = asyncio.create_task(hold())
        async with asyncio.timeout(5):
            await failed.wait()
        if leaving == "by cancellation":
            holder.cancel()
        await asyncio.wait({holder}, timeout=5)
        raised = holder.exception()
        assert isinstance(raised, ClientError)
        assert isinstance(raised.__cause__, RuntimeError)
        assert isinstance(raised.__context__, context)
        # Raised once: the iterator then only ends.
        assert await read_all(records) == ([], StopAsyncIteration)

    run_with(main)
