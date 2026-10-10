"""Reconnection and the end of a connection, for what no conformance
scenario can show (D1; D2; spec/client.md, Detecting an interruption,
Reconnecting, and Connection states): the backoff's delays and bounds;
frames that arrive while the client closes a connection after a
``pong_timeout``, which a scenario cannot time; what the scripted server
cannot bring about: a ``PING`` held by flow control, a connection left open
after its closing handshake, a connection that ends before its subscription
frame, and an attempt that raises; and the last market leaving in the turn
of the event loop in which a backoff, an attempt, or a subscription frame's
send ends, and the frames that arrive on a connection whose subscription
frame the client abandoned before it ends.
"""

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from typing import Any, Self

import pytest
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as websockets_connect
from websockets.exceptions import ConnectionClosed
from websockets.frames import Frame, Opcode
from websockets.http11 import Request
from websockets.protocol import State
from websockets.server import ServerProtocol

from polymarket_market_data import (
    BookEvent,
    BookParameters,
    ClientConfig,
    ClientError,
    ConnectionState,
    ConnectionStateChange,
    Market,
    MarketDataClient,
    MarketInfo,
    ReconnectPolicy,
    RecoveryFailed,
    TokenStateChange,
    _connection,
)
from polymarket_market_data._connection import Recovery

# Synthetic market A of spec/conformance.md.
PREFIX = "1" + "0" * 74
MARKET_A = Market(
    "0x" + "a1".rjust(64, "0"), (PREFIX + "11", PREFIX + "12"), "synthetic-a"
)
A1, A2 = MARKET_A.token_ids
T0 = 1791200000000


def policy(**overrides: Any) -> ReconnectPolicy:
    return ReconnectPolicy(**{"jitter": False, **overrides})


# The backoff (D2).


def test_each_delay_doubles_up_to_max_delay() -> None:
    recovery = Recovery(policy())
    delays = []
    for _ in range(9):
        delays.append(recovery.delay())
        recovery.failed()
    # min(30, 0.5 × 2^(k − 1)) for attempts 1 to 9.
    assert delays == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]


def test_a_large_attempt_number_keeps_max_delay() -> None:
    recovery = Recovery(policy(max_attempts=10_000))
    for _ in range(5_000):
        recovery.failed()
    assert recovery.delay() == 30.0


@pytest.mark.parametrize("factor", [0.0, 0.25, 0.999999])
def test_jitter_multiplies_the_delay_by_a_uniform_factor(factor: float) -> None:
    recovery = Recovery(policy(jitter=True), uniform=lambda: factor)
    recovery.failed()
    recovery.failed()
    # Attempt 3: the delay before jitter is 2 s.
    assert recovery.delay() == 2.0 * factor


def test_attempt_numbers_follow_on_until_a_frame_is_delivered() -> None:
    recovery = Recovery(policy())
    assert recovery.attempt == 1
    recovery.failed()
    recovery.failed()
    assert recovery.attempt == 3
    recovery.delivered()
    assert (recovery.attempt, recovery.delay()) == (1, 0.5)


def test_a_restart_starts_attempt_numbers_and_the_clock_again() -> None:
    # At startup, when a market is added to an empty desired set, or after
    # the all-resolved close leaves desired markets (spec/client.md,
    # Reconnecting and Connection states).
    recovery = Recovery(policy(max_attempts=100, max_recovery_time=10.0))
    recovery.begin(0.0)
    recovery.failed()
    recovery.restart(50.0)
    assert (recovery.attempt, recovery.delay()) == (1, 0.5)
    assert recovery.exhausted(55.0, 5.0) is None
    assert recovery.exhausted(55.0, 5.5) == "max_recovery_time"


def test_max_attempts_failed_in_a_row_exhaust_the_bounds() -> None:
    recovery = Recovery(policy(max_attempts=3))
    recovery.begin(0.0)
    for _ in range(2):
        recovery.failed()
        assert recovery.exhausted(0.0, recovery.delay()) is None
    recovery.failed()
    assert recovery.exhausted(0.0, recovery.delay()) == "max_attempts"


def test_a_wait_ending_after_max_recovery_time_exhausts_the_bounds() -> None:
    recovery = Recovery(policy(max_attempts=100, max_recovery_time=10.0))
    recovery.begin(100.0)
    assert recovery.exhausted(105.0, 5.0) is None  # ends 10 s after it began
    assert recovery.exhausted(105.0, 5.5) == "max_recovery_time"


def test_the_recovery_clock_runs_from_the_interruption_that_began_it() -> None:
    recovery = Recovery(policy(max_attempts=100, max_recovery_time=10.0))
    recovery.begin(0.0)
    recovery.delivered()
    # A new connection delivered a frame; its interruption, at 50 s, begins a
    # recovery. A connection that then delivers nothing leaves it running.
    recovery.begin(50.0)
    recovery.failed()
    recovery.begin(58.0)
    assert recovery.exhausted(58.0, 2.0) is None
    assert recovery.exhausted(58.0, 2.5) == "max_recovery_time"


# Frames that arrive while the client closes a connection after a
# pong_timeout are neither delivered nor applied, and are counted
# (spec/client.md, Detecting an interruption).


def opening(*levels: tuple[str, str]) -> str:
    """An opening frame with a book for A1 and A2, each with these bids."""
    return json.dumps(
        [
            {
                "market": MARKET_A.condition_id,
                "asset_id": token,
                "timestamp": str(T0 - 30000),
                "hash": "0" * 40,
                "bids": [{"price": p, "size": s} for p, s in levels],
                "asks": [{"price": "0.52", "size": "120"}],
                "tick_size": "0.01",
                "event_type": "book",
                "last_trade_price": "0.500",
            }
            for token in MARKET_A.token_ids
        ],
        separators=(",", ":"),
    )


def price_change(t: int) -> str:
    """A ``price_change`` that removes A1's only bid."""
    return json.dumps(
        {
            "market": MARKET_A.condition_id,
            "price_changes": [
                {
                    "asset_id": A1,
                    "price": "0.48",
                    "size": "0",
                    "side": "BUY",
                    "hash": "0" * 40,
                    "best_bid": "0",
                    "best_ask": "0.52",
                }
            ],
            "timestamp": str(T0 + t),
            "event_type": "price_change",
        },
        separators=(",", ":"),
    )


class SmallServer:
    """A server that sends the opening frame for every subscription and
    answers no ``PING``. A subclass can act on the client's ``PING``s and
    close frame."""

    ends_tcp = True
    """Whether the server ends the TCP connection once the closing handshake
    is over."""

    def __init__(self, opening: str) -> None:
        self.opening = opening
        self.port = 0
        self._writers: list[asyncio.StreamWriter] = []

    async def __aenter__(self) -> Self:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self._server.close()
        for writer in self._writers:
            writer.transport.abort()
        await self._server.wait_closed()

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    def pinged(self, protocol: ServerProtocol) -> None:
        """The client sent ``PING``."""

    async def client_closed(
        self, protocol: ServerProtocol, writer: asyncio.StreamWriter
    ) -> None:
        """The client's close frame arrived."""

    async def _serve(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writers.append(writer)
        protocol = ServerProtocol()
        while data := await reader.read(65536):
            protocol.receive_data(data)
            for event in protocol.events_received():
                if isinstance(event, Request):
                    protocol.send_response(protocol.accept(event))
                elif isinstance(event, Frame) and event.opcode is Opcode.TEXT:
                    if bytes(event.data) == b"PING":
                        self.pinged(protocol)
                    else:
                        protocol.send_text(self.opening.encode())
                elif isinstance(event, Frame) and event.opcode is Opcode.CLOSE:
                    await self.client_closed(protocol, writer)
            for chunk in protocol.data_to_send():
                if chunk:
                    writer.write(chunk)
                elif self.ends_tcp:
                    writer.close()
                    return
        writer.close()


class LateServer(SmallServer):
    """When the client's close frame arrives, the server sends the ``late``
    frames, apart, before answering it, as a stream still sending data
    would."""

    def __init__(self, opening: str, late: list[str]) -> None:
        super().__init__(opening)
        self.late = late
        self.closes: list[tuple[int, str]] = []

    async def client_closed(
        self, protocol: ServerProtocol, writer: asyncio.StreamWriter
    ) -> None:
        assert protocol.close_rcvd is not None
        self.closes.append((protocol.close_rcvd.code, protocol.close_rcvd.reason))
        # The protocol has queued its answer: the late frames go ahead of it.
        for text in self.late:
            frame = Frame(Opcode.TEXT, text.encode())
            writer.write(frame.serialize(mask=False))
            await writer.drain()
            await asyncio.sleep(0.05)


class ClosingServer(SmallServer):
    """The server answers the first ``PING`` with a close frame, and leaves
    the TCP connection open once the closing handshake is over, as a server
    slow to end it would."""

    ends_tcp = False

    def __init__(self, opening: str, code: int, reason: str) -> None:
        super().__init__(opening)
        self.code, self.reason = code, reason

    def pinged(self, protocol: ServerProtocol) -> None:
        if protocol.state is State.OPEN:
            protocol.send_close(self.code, self.reason)


def profile(
    url: str, *, reconnect: Mapping[str, Any] | None = None, **fields: Any
) -> ClientConfig:
    """The conformance profile's timings, with ``url``, these fields, and
    these fields of the reconnect policy set."""
    policy = {"base_delay": 0.1, "max_delay": 0.4, "max_attempts": 3, "jitter": False}
    return ClientConfig(
        **{
            "url": url,
            "ping_interval": 0.2,
            "pong_timeout": 0.5,
            "connect_timeout": 1.0,
            "close_timeout": 0.5,
            "verify_hash": False,
            **fields,
            "reconnect": ReconnectPolicy(**{**policy, **(reconnect or {})}),
        }
    )


async def read_until(
    records: AsyncIterator[object], done: Callable[[object], bool]
) -> list[object]:
    read: list[object] = []
    while not read or not done(read[-1]):
        async with asyncio.timeout(5):
            read.append(await anext(records))
    return read


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


def is_state(state: ConnectionState) -> Callable[[object], bool]:
    def check(record: object) -> bool:
        return isinstance(record, ConnectionStateChange) and record.state is state

    return check


def test_frames_arriving_while_the_client_closes_after_a_pong_timeout() -> None:
    late = [price_change(100), price_change(200), "PONG"]

    async def main() -> None:
        async with LateServer(opening(("0.48", "100")), late) as server:
            client = MarketDataClient(profile(server.url), markets=[MARKET_A])
            records = client.records()
            async with client:
                read = await read_until(records, is_state(ConnectionState.RECOVERING))
                # Nothing from the late frames, and the next attempt follows.
                after = await read_until(records, is_state(ConnectionState.CONNECTING))
                assert [type(r) for r in after] == [ConnectionStateChange]
                # The books were not changed by them: the next connection's
                # books match the books held.
                again = await read_until(records, lambda r: isinstance(r, BookEvent))
                stats = client.stats()
            assert server.closes[0] == (1000, "pong timeout")
        interrupted = [
            r.reason
            for r in read
            if isinstance(r, ConnectionStateChange)
            and r.state is ConnectionState.INTERRUPTED
        ]
        assert interrupted == ["pong_timeout"]
        book = again[-1]
        assert isinstance(book, BookEvent)
        assert (book.connection, book.held_book_matched) == (2, True)
        # The two late price_change frames and the late PONG are counted, and
        # none was decoded into an event: the books are the two opening
        # frames'. The late PONG still answered the oldest PING, and its
        # delay is how late it was.
        assert stats.frames_after_interruption == 3
        assert stats.events == {"book": 4}
        assert (stats.pongs, stats.pongs_unsolicited) == (1, 0)
        assert stats.pong_delay_last >= 0.5
        assert stats.interruptions == {"pong_timeout": 1}

    asyncio.run(main())


def test_a_ping_held_by_flow_control_does_not_hold_back_the_pong_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sockets: list[ClientConnection] = []

    async def connect(*args: Any, **kwargs: Any) -> ClientConnection:
        socket = await websockets_connect(*args, **kwargs)
        sockets.append(socket)
        return socket

    monkeypatch.setattr(_connection, "connect", connect)

    async def main() -> None:
        async with SmallServer(opening(("0.48", "100"))) as server:
            client = MarketDataClient(profile(server.url), markets=[MARKET_A])
            records = client.records()
            async with client:
                read = await read_until(records, is_state(ConnectionState.SUBSCRIBED))
                # As the transport does once its buffer is full: every send
                # now waits for it to drain, which here it never does.
                sockets[0].pause_writing()
                read += await read_until(records, is_state(ConnectionState.INTERRUPTED))
        subscribed, interrupted = read[2], read[-1]
        assert isinstance(subscribed, ConnectionStateChange)
        assert isinstance(interrupted, ConnectionStateChange)
        assert interrupted.reason == "pong_timeout"
        # The first PING went 0.2 s after the subscription, and D1 measures
        # the 0.5 s timeout from it.
        assert (interrupted.at - subscribed.at).total_seconds() < 0.7 + 0.25

    asyncio.run(main())


def test_a_close_frame_before_the_pong_deadline_is_not_a_pong_timeout() -> None:
    # The first PING, 0.5 s after the subscription, brings the server's close
    # frame, and its PONG deadline comes 0.3 s later, before the next PING,
    # while the server has yet to end the connection.
    async def main() -> None:
        reason = "slow consumer: send buffer full"
        async with ClosingServer(opening(("0.48", "100")), 1013, reason) as server:
            config = profile(server.url, ping_interval=0.5, pong_timeout=0.3)
            client = MarketDataClient(config, markets=[MARKET_A])
            records = client.records()
            async with client:
                read = await read_until(records, is_state(ConnectionState.INTERRUPTED))
        interrupted = read[-1]
        assert isinstance(interrupted, ConnectionStateChange)
        assert (
            interrupted.reason,
            interrupted.close_code,
            interrupted.close_reason,
        ) == ("close_frame", 1013, reason)

    asyncio.run(main())


def test_the_close_after_a_pong_timeout_counts_toward_max_recovery_time() -> None:
    # The late frames hold the close for at least 0.15 s, so an attempt
    # 0.01 s after it would start after the 0.05 s bound.
    late = [price_change(100), price_change(200), "PONG"]

    async def main() -> None:
        async with LateServer(opening(("0.48", "100")), late) as server:
            config = profile(
                server.url, reconnect={"base_delay": 0.01, "max_recovery_time": 0.05}
            )
            client = MarketDataClient(config, markets=[MARKET_A])
            records = client.records()
            async with client:
                read = await read_until(
                    records,
                    lambda r: (
                        is_state(ConnectionState.RECOVERING)(r)
                        or is_state(ConnectionState.FAILED)(r)
                    ),
                )
                with pytest.raises(RecoveryFailed):
                    async with asyncio.timeout(5):
                        await anext(records)
        failed = read[-1]
        assert isinstance(failed, ConnectionStateChange)
        assert (failed.state, failed.reason) == (
            ConnectionState.FAILED,
            "max_recovery_time",
        )

    asyncio.run(main())


# A connection that ends before its subscription frame is sent is a failed
# attempt, not an interruption (spec/client.md, Connecting and subscribing).


class EndedSocket:
    """A connection that has ended by the time the subscription frame is
    sent."""

    async def send(self, message: str) -> None:
        raise ConnectionClosed(None, None)


def test_a_connection_ending_before_its_subscription_frame_is_a_failed_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def connect(*args: object, **kwargs: object) -> EndedSocket:
        return EndedSocket()

    monkeypatch.setattr(_connection, "connect", connect)

    async def main() -> None:
        client = MarketDataClient(profile("ws://127.0.0.1:9"), markets=[MARKET_A])
        records = client.records()
        async with client:
            read, ended = await read_all(records)
        assert isinstance(ended, RecoveryFailed)
        # No token was subscribed, so nothing was interrupted.
        assert not [r for r in read if isinstance(r, TokenStateChange)]
        states = [
            (r.state.value, r.attempt, r.connection)
            for r in read
            if isinstance(r, ConnectionStateChange)
        ]
        assert states == [
            ("connecting", 1, None),
            ("open", None, 1),
            ("recovering", 2, None),
            ("connecting", 2, None),
            ("open", None, 2),
            ("recovering", 3, None),
            ("connecting", 3, None),
            ("open", None, 3),
            ("failed", None, None),
        ]
        recovering = [
            r
            for r in read
            if isinstance(r, ConnectionStateChange)
            and r.state is ConnectionState.RECOVERING
        ]
        assert all(
            r.detail is not None and "before its subscription frame" in r.detail
            for r in recovering
        )
        # The failure says how the last attempt failed too.
        failed = read[-1]
        assert isinstance(failed, ConnectionStateChange)
        assert failed.detail is not None
        assert "connection 3 ended before its subscription frame" in failed.detail
        assert isinstance(ended.__cause__, ConnectionClosed)

    asyncio.run(main())


# An attempt that raises (spec/client.md, Reconnecting and Errors).


def test_the_failure_keeps_how_the_last_attempt_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refused = ConnectionRefusedError(61, "Connect call failed")

    async def connect(*args: object, **kwargs: object) -> None:
        raise refused

    monkeypatch.setattr(_connection, "connect", connect)

    async def main() -> None:
        config = profile("ws://127.0.0.1:9", reconnect={"max_attempts": 1})
        client = MarketDataClient(config, markets=[MARKET_A])
        records = client.records()
        async with client:
            read, ended = await read_all(records)
        # No recovering record follows the attempt that exhausted the
        # bounds, so failed and RecoveryFailed are the ones to report it.
        failed = read[-1]
        assert isinstance(failed, ConnectionStateChange)
        assert (failed.state, failed.reason, failed.detail) == (
            ConnectionState.FAILED,
            "max_attempts",
            "ConnectionRefusedError: [Errno 61] Connect call failed",
        )
        assert isinstance(ended, RecoveryFailed)
        assert ended.__cause__ is refused

    asyncio.run(main())


def test_a_defect_in_an_attempt_ends_the_client_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    defect = TypeError("connect() got an unexpected keyword argument")

    async def connect(*args: object, **kwargs: object) -> None:
        raise defect

    monkeypatch.setattr(_connection, "connect", connect)

    async def main() -> None:
        client = MarketDataClient(profile("ws://127.0.0.1:9"), markets=[MARKET_A])
        records = client.records()
        async with client:
            read, ended = await read_all(records)
        # Not a failed attempt: it is not retried, and the client ends with
        # the defect as the cause of a ClientError.
        states = [
            (r.state.value, r.attempt)
            for r in read
            if isinstance(r, ConnectionStateChange)
        ]
        assert (len(read), states) == (1, [("connecting", 1)])
        assert type(ended) is ClientError
        assert ended.__cause__ is defect

    asyncio.run(main())


# The last market leaving as a wait before the subscription frame ends
# (spec/client.md, Connection states and Reconnecting). Once no desired
# market is left, the client abandons a backoff, an attempt, or the
# subscription frame's send, but that takes effect in a later turn of the
# event loop, so a wait that ends in the turn the set empties ends first.
# These bring that turn about: lookup shows market A closed, which settles it
# and empties the set, then the application adds market B, and then the wait
# ends. The wait is abandoned all the same: the connection the set emptied
# on, if one is open, ends and gives its idle record, after the frames that
# arrived on it, and B starts attempt 1.

MARKET_B = Market(
    "0x" + "b2".rjust(64, "0"), (PREFIX + "21", PREFIX + "22"), "synthetic-b"
)
B1, B2 = MARKET_B.token_ids
NAMES = {A1: "A1", A2: "A2", B1: "B1", B2: "B2"}
CLOSED_A = MarketInfo(
    condition_id=MARKET_A.condition_id,
    slug=MARKET_A.slug,
    question=None,
    token_ids=MARKET_A.token_ids,
    outcomes=("Yes", "No"),
    closed=True,
    end_date=None,
)


class HeldLookup:
    """A market lookup that answers each call when the test does."""

    def __init__(self) -> None:
        self.calls: list[asyncio.Future[MarketInfo | None]] = []
        self.called = asyncio.Event()

    async def market(self, *, slug: str) -> MarketInfo | None:
        call: asyncio.Future[MarketInfo | None] = (
            asyncio.get_running_loop().create_future()
        )
        self.calls.append(call)
        self.called.set()
        return await call

    async def book_parameters(self, token_id: str) -> BookParameters | None:
        return None


class FakeSocket:
    """A connection that delivers no frame but ``last``. Its sends complete
    once ``release`` is done, at once unless ``held``, and then end the
    connection instead if ``ends``. It ends without a close frame once
    ``drop`` is set, the client closes it, or a send ends it, and the frames
    in ``last``, which arrived before then, can still be read."""

    def __init__(
        self, *, held: bool = False, ends: bool = False, last: Sequence[str] = ()
    ) -> None:
        self.release: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if not held:
            self.release.set_result(None)
        self.ends = ends
        self.last = list(last)
        self.sending = asyncio.Event()
        self.drop = asyncio.Event()
        self.closes: list[tuple[int, str]] = []

    async def send(self, message: str) -> None:
        self.sending.set()
        await self.release
        if self.ends:
            self.drop.set()
            raise ConnectionClosed(None, None)

    async def recv(self) -> str:
        await self.drop.wait()
        if self.last:
            return self.last.pop(0)
        raise ConnectionClosed(None, None)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closes.append((code, reason))
        self.drop.set()


class Endpoint:
    """Stands in for ``connect``: attempt *n* opens ``sockets[n - 1]``, at
    once, or, if *n* is ``held``, once ``release`` is done."""

    def __init__(self, sockets: list[FakeSocket], *, held: int | None = None) -> None:
        self.sockets = sockets
        self.held = held
        self.attempts = 0
        self.release: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.waiting = asyncio.Event()

    async def connect(self, *args: object, **kwargs: object) -> FakeSocket:
        self.attempts += 1
        if self.attempts == self.held:
            self.waiting.set()
            await self.release
        return self.sockets[self.attempts - 1]


def settle_and_add(
    client: MarketDataClient,
    lookup: HeldLookup,
    wait: asyncio.Future[None] | None,
    *,
    add: bool = True,
) -> None:
    """In one turn of the event loop, and in this order: lookup shows A
    closed, which settles it and empties the desired set; the application
    adds B, if ``add``; and ``wait`` ends, if one is given. All three come
    before the turn in which the client would abandon the wait."""
    [call] = lookup.calls
    call.set_result(CLOSED_A)
    if add:
        asyncio.get_running_loop().call_soon(client.subscribe, MARKET_B)
    if wait is not None:
        wait.set_result(None)


def summary(record: object) -> tuple[object, ...]:
    match record:
        case ConnectionStateChange(state=state, attempt=attempt, connection=gen):
            return (state.value, attempt, gen)
        case TokenStateChange(token_id=token_id, state=state, reason=reason):
            return (NAMES[token_id], state.value, reason)
    return (type(record).__name__,)


async def read(records: AsyncIterator[object], count: int) -> list[tuple[object, ...]]:
    summaries: list[tuple[object, ...]] = []
    for _ in range(count):
        async with asyncio.timeout(5):
            summaries.append(summary(await anext(records)))
    return summaries


# Connection 1 subscribes A, gets no book, and is dropped once A's
# confirmation has asked lookup, which holds its answer. Since it delivered
# no frame, the next attempt is attempt 2.
DROPPED = [
    ("connecting", 1, None),
    ("open", None, 1),
    ("subscribed", None, 1),
    ("A1", "synchronizing", "subscribed"),
    ("A2", "synchronizing", "subscribed"),
    ("A1", "uncertain", "no_book"),
    ("A2", "uncertain", "no_book"),
    ("interrupted", None, 1),
    ("A1", "uncertain", "interrupted"),
    ("A2", "uncertain", "interrupted"),
    ("recovering", 2, None),
]
SETTLED = [("A1", "settled", "lookup_closed"), ("A2", "settled", "lookup_closed")]


def subscribed_b(connection: int) -> list[tuple[object, ...]]:
    """B connects at once, as attempt 1."""
    return [
        ("connecting", 1, None),
        ("open", None, connection),
        ("subscribed", None, connection),
        ("B1", "synchronizing", "subscribed"),
        ("B2", "synchronizing", "subscribed"),
    ]


def held_config() -> ClientConfig:
    """No PING, and confirmations and lookups that outlast the test."""
    return profile(
        "ws://127.0.0.1:9",
        book_timeout=0.05,
        ping_interval=60.0,
        pong_timeout=60.0,
        settlement_confirm_timeout=60.0,
        lookup_timeout=60.0,
    )


async def until_dropped(
    records: AsyncIterator[object], lookup: HeldLookup, first: FakeSocket
) -> list[tuple[object, ...]]:
    got = await read(records, 7)
    await lookup.called.wait()
    first.drop.set()
    return got + await read(records, len(DROPPED) - 7)


def test_a_send_completing_as_the_last_market_leaves_subscribes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def main() -> None:
        lookup = HeldLookup()
        first, second, third = FakeSocket(), FakeSocket(held=True), FakeSocket()
        endpoint = Endpoint([first, second, third])
        monkeypatch.setattr(_connection, "connect", endpoint.connect)
        client = MarketDataClient(held_config(), markets=[MARKET_A], lookup=lookup)
        records = client.records()
        async with client:
            got = await until_dropped(records, lookup, first)
            got += await read(records, 2)
            await second.sending.wait()
            settle_and_add(client, lookup, second.release)
            got += await read(records, 3 + 5)
        # Connection 2's frame named only A, so subscribing on it would leave
        # B desired and never subscribed. Its end gives its idle record first.
        assert got == [
            *DROPPED,
            ("connecting", 2, None),
            ("open", None, 2),
            *SETTLED,
            ("idle", None, 2),
            *subscribed_b(3),
        ]
        assert second.closes == [(1000, "no subscriptions")]

    asyncio.run(main())


@pytest.mark.parametrize("add", [False, True], ids=["nothing added", "B added"])
def test_a_connection_ending_as_the_last_market_leaves_gives_its_idle_record(
    monkeypatch: pytest.MonkeyPatch, add: bool
) -> None:
    async def main() -> None:
        lookup = HeldLookup()
        first, second = FakeSocket(), FakeSocket(held=True, ends=True)
        endpoint = Endpoint([first, second, FakeSocket()])
        monkeypatch.setattr(_connection, "connect", endpoint.connect)
        client = MarketDataClient(held_config(), markets=[MARKET_A], lookup=lookup)
        records = client.records()
        async with client:
            got = await until_dropped(records, lookup, first)
            got += await read(records, 2)
            await second.sending.wait()
            settle_and_add(client, lookup, second.release, add=add)
            got += await read(records, 3 + (5 if add else 0))
            if not add:
                # Idle: no attempt follows.
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.3):
                        got.append(summary(await anext(records)))
        # Not a failed attempt: no desired market was left before it ended.
        assert got == [
            *DROPPED,
            ("connecting", 2, None),
            ("open", None, 2),
            *SETTLED,
            ("idle", None, 2),
            *(subscribed_b(3) if add else []),
        ]
        assert endpoint.attempts == (3 if add else 2)

    asyncio.run(main())


@pytest.mark.parametrize("send", ["abandoned", "completed", "ended"])
def test_frames_arriving_as_an_unsubscribed_connection_closes_come_before_idle(
    monkeypatch: pytest.MonkeyPatch, send: str
) -> None:
    # Whether the send was abandoned, completed, or ended the connection,
    # the frames that arrived on it before its end are handled as during the
    # close of a subscribed connection: the unknown event is delivered, and
    # A's price change is discarded as outside the desired set
    # (spec/client.md, Events outside the desired set and Connection states).
    unknown = json.dumps(
        {
            "market": MARKET_A.condition_id,
            "asset_id": A1,
            "timestamp": str(T0 + 101),
            "event_type": "something_new",
        },
        separators=(",", ":"),
    )

    async def main() -> None:
        lookup = HeldLookup()
        first = FakeSocket()
        second = FakeSocket(
            held=True, ends=send == "ended", last=[unknown, price_change(102)]
        )
        endpoint = Endpoint([first, second, FakeSocket()])
        monkeypatch.setattr(_connection, "connect", endpoint.connect)
        client = MarketDataClient(held_config(), markets=[MARKET_A], lookup=lookup)
        records = client.records()
        async with client:
            got = await until_dropped(records, lookup, first)
            got += await read(records, 2)
            await second.sending.wait()
            # An abandoned send is still held when the abandonment takes
            # effect.
            ending = None if send == "abandoned" else second.release
            settle_and_add(client, lookup, ending)
            last = subscribed_b(3)[-1]
            got += [
                summary(record)
                for record in await read_until(
                    records, lambda record: summary(record) == last
                )
            ]
            stats = client.stats()
        assert got == [
            *DROPPED,
            ("connecting", 2, None),
            ("open", None, 2),
            *SETTLED,
            ("UnknownEvent",),
            ("idle", None, 2),
            *subscribed_b(3),
        ]
        assert (stats.unknown, stats.discarded_outside) == (1, 1)
        assert second.closes == [(1000, "no subscriptions")]

    asyncio.run(main())


def test_a_handshake_completing_as_the_last_market_leaves_opens_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def main() -> None:
        lookup = HeldLookup()
        first, second, third = FakeSocket(), FakeSocket(), FakeSocket()
        endpoint = Endpoint([first, second, third], held=2)
        monkeypatch.setattr(_connection, "connect", endpoint.connect)
        client = MarketDataClient(held_config(), markets=[MARKET_A], lookup=lookup)
        records = client.records()
        async with client:
            got = await until_dropped(records, lookup, first)
            got += await read(records, 1)
            await endpoint.waiting.wait()
            settle_and_add(client, lookup, endpoint.release)
            got += await read(records, 3 + 5)
        # Connection 2 was opened as the set emptied: it is closed unused,
        # though it keeps its generation, and no open record follows idle
        # without an attempt before it.
        assert got == [
            *DROPPED,
            ("connecting", 2, None),
            *SETTLED,
            ("idle", None, None),
            *subscribed_b(3),
        ]
        assert second.closes == [(1000, "no subscriptions")]

    asyncio.run(main())


def test_a_backoff_ending_as_the_last_market_leaves_restarts_the_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def main() -> None:
        lookup = HeldLookup()
        first, second = FakeSocket(), FakeSocket()
        endpoint = Endpoint([first, second])
        monkeypatch.setattr(_connection, "connect", endpoint.connect)
        client = MarketDataClient(held_config(), markets=[MARKET_A], lookup=lookup)

        def delay(self: Recovery) -> float:
            # As the backoff after connection 1 is decided: a wait of 0, as
            # jitter can give, which ends in the next turn of the event loop.
            settle_and_add(client, lookup, None)
            return 0.0

        monkeypatch.setattr(Recovery, "delay", delay)
        records = client.records()
        async with client:
            got = await until_dropped(records, lookup, first)
            got += await read(records, 3 + 5)
        # Not attempt 2 of the recovery that connection 1's end began.
        assert got == [
            *DROPPED,
            *SETTLED,
            ("idle", None, None),
            *subscribed_b(2),
        ]

    asyncio.run(main())
