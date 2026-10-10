"""The client against the scripted server, for behavior no conformance
scenario covers: shutdown, the heartbeat's counters, ``keep_raw``, ends of a
connection the scenarios leave out, the backoff's jitter, the last desired
market leaving before a connection is subscribed or while an interrupted
one closes, frames arriving while the client closes the connection the last
market left, subscription changes on a connection opened during a recovery
or while an interrupted one closes, settlement with no lookup available, a
defect in the client's own code, and the stopgap that ends the client until
plan step 9 replaces it (spec/client.md, Cancellation and shutdown,
Heartbeat, Detecting an interruption, Reconnecting, Connection states,
Settlement, and Errors).

Most are scenarios in the conformance notation, which the runner runs as it
runs those in spec/conformance.md; a test that needs the client itself
drives it step by step.
"""

import asyncio
import sys
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from websockets.asyncio.client import ClientConnection

from polymarket_market_data import (
    ClientConfig,
    ClientError,
    ClientStateError,
    ConnectionState,
    ConnectionStateChange,
    Market,
    MarketDataClient,
    MarketLookup,
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
    # Once the last desired market settles on a connection, the client closes
    # it, its end is not an interruption, and the client does not reconnect
    # (spec/client.md, Detecting an interruption and Connection states).
    "the-last-market-settling-closes-the-connection": """
markets A
start A
send resolved A t=100 winner=A1
expect market_resolved A
expect token A1 settled reason=market_resolved
expect token A2 settled reason=market_resolved
expect conn idle connection=1
expect-client-close 1000 "no subscriptions"
expect-no-connect 0.5
expect-nothing 0.1
expect-stats interruptions=0
""",
    # When the desired set empties during recovery, the client stops: a
    # backoff is abandoned, and a market added then connects at once, as
    # attempt 1 (spec/client.md, Connection states and Reconnecting).
    "the-last-market-settling-during-a-backoff-stops-it": """
markets A
config reconnect.base_delay=5.0 reconnect.max_delay=5.0
lookup A error
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
send opening
expect token A1 uncertain reason=no_book
expect token A2 uncertain reason=no_book
drop
expect conn interrupted reason=dropped connection=1
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 retry_in=5.0
l: lookup A closed winner=A2
expect token A1 settled previous=uncertain reason=lookup_closed
  winning_asset_id=A2 within 0..0.6 of l
expect token A2 settled reason=lookup_closed
expect conn idle reason=no_subscriptions connection=none
expect-nothing 0.3
a: subscribe B
expect conn connecting attempt=1 within 0..0.2 of a
accept
recv-subscribe B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token B1 synchronizing previous=none
expect token B2 synchronizing previous=none
""",
    # Likewise an attempt the server holds is abandoned.
    "the-last-market-settling-during-an-attempt-abandons-it": """
markets A
config connect_timeout=5.0
lookup A error
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
send opening
expect token A1 uncertain reason=no_book
expect token A2 uncertain reason=no_book
drop
expect conn interrupted reason=dropped connection=1
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1
expect conn connecting attempt=1
lookup A closed winner=A2
expect token A1 settled reason=lookup_closed
expect token A2 settled reason=lookup_closed
expect conn idle reason=no_subscriptions connection=none
wait 0.2
expect-no-connect 0.5
a: subscribe B
expect conn connecting attempt=1 within 0..0.2 of a
accept
recv-subscribe B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token B1 synchronizing previous=none
expect token B2 synchronizing previous=none
""",
    # A connection the client closes to apply a subscription change is never
    # a failed attempt, so the next attempt is the one that opened it again,
    # attempt 2 here, and max_attempts=2 is not exhausted (D7;
    # spec/client.md, Reconnecting).
    "a-subscription-change-repeats-the-attempt-that-opened-the-connection": """
markets A
config reconnect.max_attempts=2
expect conn connecting attempt=1
refuse 1
expect conn recovering attempt=2 reason=backoff retry_in=0.2 detail=set
expect conn connecting attempt=2
accept
recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing previous=none
expect token A2 synchronizing previous=none
add: subscribe B
expect conn interrupted reason=subscription_change connection=1
  within 0..0.5 of add
expect token A1 uncertain previous=synchronizing reason=interrupted
expect token A2 uncertain previous=synchronizing reason=interrupted
expect conn recovering attempt=2 reason=subscription_change retry_in=0
expect-client-close 1000 "subscription change"
expect conn connecting attempt=2
accept
recv-subscribe A1 A2 B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
expect token B1 synchronizing previous=none
expect token B2 synchronizing previous=none
send opening A1 A2 B1 B2
expect book A1 held_book_matched=none
expect token A1 ready
expect book A2
expect token A2 ready
expect book B1
expect token B1 ready
expect book B2
expect token B2 ready
expect-stats connections=2 interruptions.subscription_change=1
""",
    # The bounds are checked as for any interruption. Connection 2 delivers
    # no frame, so the recovery that connection 1's end began goes on, and a
    # change after max_recovery_time has passed exhausts it, with no wait
    # (spec/client.md, Detecting an interruption and Reconnecting).
    "a-subscription-change-after-max-recovery-time-fails": """
markets A
config reconnect.max_recovery_time=1.0 book_timeout=5.0
start A
drop
expect conn interrupted reason=dropped connection=1
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 reason=backoff retry_in=0.1
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
wait 1.0
add: subscribe B
expect conn interrupted reason=subscription_change connection=2
expect token A1 uncertain previous=synchronizing reason=interrupted
expect token A2 uncertain previous=synchronizing reason=interrupted
expect gap A1 cause=dropped end=recovery_failed resumed=false
expect gap A2 cause=dropped end=recovery_failed resumed=false
expect conn failed reason=max_recovery_time detail=none within 0..0.5 of add
expect-client-close 1000 "subscription change"
expect-error RecoveryFailed
expect-end
expect-no-connect 0.5
""",
    # Stopgap: until plan step 9 responds at the queue's limit, a frame that
    # reaches it ends the client with a ClientError, raised after the records
    # already queued.
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


# The last market settling while the client closes a connection it
# interrupted also stops the recovery: a market added then connects once the
# close has ended, at once, as attempt 1. The interrupted connection's failed
# attempt, since it delivered no frame, would exhaust max_attempts=1 if the
# recovery went on (spec/client.md, Reconnecting and Connection states). The
# scripted server answers a close at once, so the client's close is held for
# HOLD, as a server slow to answer it would hold it.

HOLD = 1.0

ADDED_WHILE_CLOSING = """
scenario a-market-added-while-an-interrupted-connection-closes
markets A
config close_timeout=2.0 reconnect.max_attempts=1 settlement_poll_interval=0.1
  settlement_confirm_timeout=10.0
lookup A error
pong off
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
expect token A1 uncertain reason=no_book
expect token A2 uncertain reason=no_book
expect conn interrupted reason=pong_timeout connection=1
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
lookup A closed winner=A2
expect token A1 settled reason=lookup_closed
expect token A2 settled reason=lookup_closed
expect conn idle reason=no_subscriptions connection=none
subscribe B
pong auto
c: expect-client-close 1000 "pong timeout"
expect conn connecting attempt=1 within 0..0.2 of c
accept
recv-subscribe B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token B1 synchronizing previous=none
expect token B2 synchronizing previous=none
"""


def hold_close(monkeypatch: pytest.MonkeyPatch, held_reason: str) -> None:
    """Hold the client's close with ``held_reason`` for ``HOLD``."""
    close = ClientConnection.close

    async def held(self: ClientConnection, code: int = 1000, reason: str = "") -> None:
        if reason == held_reason:
            await asyncio.sleep(HOLD)
        await close(self, code, reason)

    monkeypatch.setattr(ClientConnection, "close", held)


def test_a_market_added_while_an_interrupted_connection_closes_connects_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hold_close(monkeypatch, _connection.PONG_TIMEOUT)
    run(parse_scenario(ADDED_WHILE_CLOSING))


# To apply a subscription change, recovering comes with the interruption,
# before the client's close has ended, unlike after a pong_timeout; the
# attempt starts once the close has ended; and the frames that arrive
# meanwhile are counted and discarded (D7; spec/client.md, Detecting an
# interruption). The client's close is held as above.

CHANGE_WHILE_CLOSING = """
scenario a-subscription-change-announces-recovering-before-its-close-ends
markets A
config close_timeout=2.0
start A
add: subscribe B
expect conn interrupted reason=subscription_change connection=1
  within 0..0.3 of add
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 reason=subscription_change retry_in=0
  within 0..0.3 of add
send pc A t=100 A1:BUY:0.49:10
c: expect-client-close 1000 "subscription change"
expect conn connecting attempt=1 within 0..0.2 of c
accept
recv-subscribe A1 A2 B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
expect token B1 synchronizing previous=none
expect token B2 synchronizing previous=none
# The PONGs of the PINGs sent meanwhile are counted too.
expect-stats frames=2 frames_after_interruption>=1 events.price_change=0
"""


def test_a_subscription_change_announces_recovering_before_its_close_ends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hold_close(monkeypatch, _connection.SUBSCRIPTION_CHANGE)
    run(parse_scenario(CHANGE_WHILE_CLOSING))


# A market added while the client closes a connection it has interrupted
# joins the next subscription frame: no connection is subscribed then, so
# the addition causes no second interruption and no extra reconnect (D7;
# spec/client.md, Connection states). The client's close is held as above.

ADDED_AFTER_PONG_TIMEOUT = """
scenario a-market-added-while-the-client-closes-after-a-pong-timeout
markets A
config close_timeout=2.0
start A
pong off
expect conn interrupted reason=pong_timeout connection=1
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
subscribe B
pong auto
c: expect-client-close 1000 "pong timeout"
expect conn recovering attempt=1 reason=backoff retry_in=0.1 within 0..0.2 of c
expect conn connecting attempt=1
accept
recv-subscribe A1 A2 B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
expect token B1 synchronizing previous=none
expect token B2 synchronizing previous=none
expect-stats connections=2 interruptions=1 interruptions.pong_timeout=1
"""


def test_a_market_added_while_the_client_closes_after_a_pong_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hold_close(monkeypatch, _connection.PONG_TIMEOUT)
    run(parse_scenario(ADDED_AFTER_PONG_TIMEOUT))


# Frames that arrive while the client closes the connection the last market
# left are handled as on any connection, and the idle record follows its
# end, after them, even when a market was added meanwhile (spec/client.md,
# Connection states and Events outside the desired set). The client's close
# is held as above.

FRAMES_WHILE_CLOSING = """
scenario frames-while-the-client-closes-with-no-subscriptions
markets A
config close_timeout=2.0
start A
send resolved A t=100 winner=A1
expect market_resolved A
expect token A1 settled reason=market_resolved
expect token A2 settled reason=market_resolved
send-text {"market":"${A}","asset_id":"${A1}","timestamp":"${t:101}",
  "event_type":"something_new"}
send pc A t=102 A1:BUY:0.49:10
expect unknown event_type=something_new connection=1
subscribe B
c: expect-client-close 1000 "no subscriptions"
expect conn idle reason=no_subscriptions connection=1
expect conn connecting attempt=1 within 0..0.2 of c
accept
recv-subscribe B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token B1 synchronizing previous=none
expect token B2 synchronizing previous=none
expect-stats unknown=1 discarded_outside=1 interruptions=0
"""


def test_frames_while_the_client_closes_come_before_the_idle_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hold_close(monkeypatch, _connection.NO_SUBSCRIPTIONS)
    run(parse_scenario(FRAMES_WHILE_CLOSING))


# With no lookup available, passed or installed, no settlement can be
# confirmed, so T13 follows T4 at once, and resolve raises ClientStateError
# (spec/client.md, Settlement and Market lookup). Every conformance scenario
# passes the scripted lookup.

WITHOUT_LOOKUP = """
scenario settlement-without-a-lookup
markets A
resolve synthetic-a raises ClientStateError
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
send opening
n: expect token A1 uncertain previous=synchronizing reason=no_book
expect token A2 uncertain previous=synchronizing reason=no_book
expect token A1 uncertain previous=uncertain reason=settlement_unconfirmed
  within 0..0.1 of n
expect token A2 uncertain previous=uncertain reason=settlement_unconfirmed
expect-nothing 0.5
expect-stats lookups=0 lookup_failures=0
"""


def test_settlement_without_a_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "polymarket", None)  # no SDK installed

    def without_lookup(
        config: ClientConfig, markets: tuple[Market, ...], lookup: MarketLookup
    ) -> MarketDataClient:
        return MarketDataClient(config, markets=markets)

    run(parse_scenario(WITHOUT_LOOKUP), without_lookup)


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
