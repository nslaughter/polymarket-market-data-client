"""Reconnection and the end of a connection, for what no conformance
scenario can show (D1; D2; spec/client.md, Detecting an interruption and
Reconnecting): the backoff's delays and bounds, frames that arrive while the
client closes a connection after a ``pong_timeout``, which a scenario cannot
time, a connection that ends before its subscription frame, which the
scripted server cannot end at that moment, and an attempt that raises, which
the scripted server cannot make it do.
"""

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosed
from websockets.frames import Frame, Opcode
from websockets.http11 import Request
from websockets.server import ServerProtocol

from polymarket_market_data import (
    BookEvent,
    ClientConfig,
    ClientError,
    ConnectionState,
    ConnectionStateChange,
    Market,
    MarketDataClient,
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


class LateServer:
    """A server that sends the opening frame for every subscription and
    answers no ``PING``. When the client's close frame arrives, it sends the
    ``late`` frames, apart, before answering it, as a stream still sending
    data would."""

    def __init__(self, opening: str, late: list[str]) -> None:
        self.opening = opening
        self.late = late
        self.closes: list[tuple[int, str]] = []
        self.port = 0
        self._writers: list[asyncio.StreamWriter] = []

    async def __aenter__(self) -> "LateServer":
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
                    if bytes(event.data) != b"PING":
                        protocol.send_text(self.opening.encode())
                elif isinstance(event, Frame) and event.opcode is Opcode.CLOSE:
                    assert protocol.close_rcvd is not None
                    self.closes.append(
                        (protocol.close_rcvd.code, protocol.close_rcvd.reason)
                    )
                    # The protocol has queued its answer: the late frames go
                    # ahead of it.
                    for text in self.late:
                        frame = Frame(Opcode.TEXT, text.encode())
                        writer.write(frame.serialize(mask=False))
                        await writer.drain()
                        await asyncio.sleep(0.05)
            for chunk in protocol.data_to_send():
                if chunk:
                    writer.write(chunk)
                else:
                    writer.close()
                    return
        writer.close()


def profile(url: str, **reconnect: Any) -> ClientConfig:
    """The conformance profile's timings, with ``url`` and these fields of
    the reconnect policy set."""
    return ClientConfig(
        url=url,
        ping_interval=0.2,
        pong_timeout=0.5,
        connect_timeout=1.0,
        close_timeout=0.5,
        verify_hash=False,
        reconnect=ReconnectPolicy(
            **{
                "base_delay": 0.1,
                "max_delay": 0.4,
                "max_attempts": 3,
                "jitter": False,
                **reconnect,
            }
        ),
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
        # The two late price_change frames are counted; the late PONG is not
        # a frame. Neither was decoded into an event: the books are the two
        # opening frames'.
        assert stats.frames_after_interruption == 2
        assert stats.events == {"book": 4}
        assert stats.interruptions == {"pong_timeout": 1}

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
        config = profile("ws://127.0.0.1:9", max_attempts=1)
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
