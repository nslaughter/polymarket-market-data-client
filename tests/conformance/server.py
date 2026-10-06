"""The scripted server (spec/conformance.md, The scripted server).

It runs on its own event loop in a separate thread, so its tasks never mix
with the client's, and speaks WebSocket through the ``websockets`` library's
Sans-I/O protocol, which lets it hold a handshake, refuse one with HTTP 503,
and end a connection without a close frame. The runner calls its steps from
the client's loop with ``call``; each returns the step's time, as the
notation defines it.
"""

import asyncio
import json
import threading
from collections import deque
from collections.abc import Callable, Coroutine, Sequence
from datetime import UTC, datetime
from typing import Any, Literal

from websockets.frames import Frame, Opcode
from websockets.http11 import Request
from websockets.protocol import State
from websockets.server import ServerProtocol

from .failures import StepFailed
from .frames import Entry, Source, dumps, reverse_entries
from .frames import Frame as FrameSpec
from .synthetic import TOKEN_IDS

PongMode = Literal["auto", "off", "hold"]


def now() -> datetime:
    return datetime.now(UTC)


class _Changes:
    """Wakes every waiter when the server's state changes."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def notify(self) -> None:
        self._event.set()
        self._event = asyncio.Event()

    async def wait_for(self, predicate: Callable[[], object]) -> None:
        while not predicate():
            await self._event.wait()


class _Peer:
    """One TCP connection: a connection attempt until the script accepts or
    refuses it, then a WebSocket connection if accepted."""

    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, at: float
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.protocol = ServerProtocol(max_size=None)
        self.request: Request | None = None
        self.arrived = now()
        self.status: Literal["reading", "held", "refused", "accepted", "abandoned"] = (
            "reading"
        )
        self.ended = False
        self.number = 0
        """The connection's number, once accepted."""
        self.inbox: deque[tuple[datetime, str]] = deque()
        """Text frames other than PING, waiting for ``recv-subscribe``."""
        self.pings: list[datetime] = []
        self.owed = 0
        """PONGs held back by ``pong hold``."""
        self.client_close: tuple[int, str, datetime] | None = None
        self.close_sent: datetime | None = None
        """When the server sent its close frame: by ``close``, or in reply to
        the client's, which the protocol sends as the client's arrives."""
        self.last_frame = at
        """Loop time of the last frame from the client."""


class ScriptedServer:
    def __init__(self) -> None:
        self.source = Source()
        self.port = 0
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="scripted-server", daemon=True
        )
        self._server: asyncio.Server | None = None
        self._changes: _Changes | None = None
        self._peers: list[_Peer] = []
        self._held: deque[_Peer] = deque()
        self._arrivals: list[datetime] = []
        self._refuse_all = False
        self._current: _Peer | None = None
        self._accepted = 0
        self._pong: PongMode = "auto"
        self._sent: dict[str, str | bytes] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._lock = threading.Lock()
        self._violations: list[str] = []

    # Lifecycle, from the runner's thread.

    def __enter__(self) -> "ScriptedServer":
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    def start(self) -> None:
        self._thread.start()
        asyncio.run_coroutine_threadsafe(self._open(), self._loop).result(timeout=5)

    def stop(self) -> None:
        try:
            asyncio.run_coroutine_threadsafe(self._close(), self._loop).result(
                timeout=5
            )
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)
            self._loop.close()

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    async def call[T](self, step: Coroutine[Any, Any, T]) -> T:
        """Run a step on the server's loop, from the caller's loop."""
        future = asyncio.run_coroutine_threadsafe(step, self._loop)
        return await asyncio.wrap_future(future)

    def violations(self) -> list[str]:
        """What the client did that fails the scenario whatever the step."""
        with self._lock:
            return list(self._violations)

    # Server steps, on the server's loop.

    async def accept(self) -> datetime:
        self._refuse_all = False
        peer = await self._next_attempt()
        if self._current is not None:
            # Frames left on the connection before can no longer be consumed.
            self._record_unconsumed(self._current)
        assert peer.request is not None
        response = peer.protocol.accept(peer.request)
        peer.protocol.send_response(response)
        self._flush(peer)
        if response.status_code != 101:
            raise StepFailed(
                "the handshake failed",
                expected="101",
                actual=f"{response.status_code} {response.reason_phrase}",
            )
        self._accepted += 1
        peer.number = self._accepted
        peer.status = "accepted"
        peer.last_frame = self._loop.time()
        self._current = peer
        return peer.arrived

    async def refuse(self, n: int) -> datetime:
        sent = now()
        for _ in range(n):
            sent = self._reject(await self._next_attempt())
        return sent

    async def refuse_all(self) -> datetime:
        self._refuse_all = True
        while self._held:
            self._reject(self._held.popleft())
        return now()

    async def recv_subscribe(self, tokens: Sequence[str]) -> datetime:
        peer = self._connection()
        await self._wait_for(lambda: peer.inbox or peer.ended)
        if not peer.inbox:
            raise StepFailed("the connection ended before a subscription frame")
        arrived, text = peer.inbox.popleft()
        expected = {
            "type": "market",
            "assets_ids": [TOKEN_IDS[token] for token in tokens],
            "custom_feature_enabled": True,
        }
        try:
            frame = json.loads(text)
        except ValueError:
            frame = None
        if not (
            isinstance(frame, dict)
            and frame.keys() == expected.keys()
            and frame["type"] == "market"
            and frame["assets_ids"] == expected["assets_ids"]
            and frame["custom_feature_enabled"] is True
        ):
            raise StepFailed(
                "the subscription frame differs", expected=dumps(expected), actual=text
            )
        return arrived

    async def recv_ping(self) -> datetime:
        peer = self._connection()
        count = len(peer.pings)
        await self._wait_for(lambda: len(peer.pings) > count or peer.ended)
        if len(peer.pings) == count:
            raise StepFailed("the connection ended before a PING")
        return peer.pings[count]

    async def send(self, frame: FrameSpec, label: str | None = None) -> datetime:
        peer = self._open_connection()
        return self._send(peer, self.source.frame(frame), label)

    async def send_text(self, text: str, label: str | None = None) -> datetime:
        return self._send(self._open_connection(), text, label)

    async def send_binary(self, data: bytes, label: str | None = None) -> datetime:
        return self._send(self._open_connection(), data, label)

    async def send_again(
        self, again: str, reverse: bool = False, label: str | None = None
    ) -> datetime:
        peer = self._open_connection()
        frame = self._sent[again]
        if reverse:
            if isinstance(frame, bytes):
                raise StepFailed("a binary frame cannot be sent reversed")
            frame = reverse_entries(frame)
        return self._send(peer, frame, label)

    async def send_burst(
        self, market: str, t: int, every: float, entries: Sequence[Entry]
    ) -> datetime:
        self._open_connection()
        sent = now()
        for count, text in enumerate(self.source.burst(market, t, entries)):
            if count:
                await asyncio.sleep(every)
            sent = self._send(self._open_connection(), text, None)
        return sent

    async def pong(self, mode: PongMode) -> datetime:
        self._pong = mode
        return now()

    async def release_pongs(self) -> datetime:
        peer = self._open_connection()
        sent = now()
        while peer.owed:
            peer.owed -= 1
            sent = self._send(peer, "PONG", None)
        return sent

    async def silent(self, token: str, t: int, entry: Entry) -> datetime:
        self.source.silent(token, t, entry)
        return now()

    async def trade(self, market: str, price: str) -> datetime:
        self.source.trade(market, price)
        return now()

    async def idle_close(self, seconds: float) -> datetime:
        peer = self._open_connection()
        peer.last_frame = self._loop.time()
        self._start(self._watch_idle(peer, seconds))
        return now()

    async def close(self, code: int, reason: str) -> datetime:
        peer = self._connection()
        if peer.protocol.state is State.OPEN:
            peer.protocol.send_close(code, reason)
            self._flush(peer)
        elif peer.client_close is None:
            raise StepFailed("the connection is not open")
        # If the client started closing, the server's close frame completed
        # its handshake as the client's arrived, and that is the step's time.
        sent = peer.close_sent
        if sent is None:
            raise StepFailed(
                "the connection ended before the server's close frame was sent"
            )
        await self._wait_for(lambda: peer.ended)
        return sent

    async def drop(self) -> datetime:
        peer = self._connection()
        if peer.ended:
            raise StepFailed("the connection has already ended")
        peer.writer.transport.abort()
        return now()

    async def expect_client_close(self, code: int, reason: str) -> datetime:
        peer = self._connection()
        await self._wait_for(lambda: peer.client_close is not None or peer.ended)
        if peer.client_close is None:
            raise StepFailed(
                "the connection ended without a close frame from the client"
            )
        got_code, got_reason, arrived = peer.client_close
        if (got_code, got_reason) != (code, reason):
            raise StepFailed(
                "the client's close frame differs",
                expected=f"{code} {dumps(reason)}",
                actual=f"{got_code} {dumps(got_reason)}",
            )
        return arrived

    async def expect_no_connect(self, seconds: float) -> datetime:
        start, count = now(), len(self._arrivals)
        try:
            async with asyncio.timeout(seconds):
                await self._wait_for(lambda: len(self._arrivals) > count)
        except TimeoutError:
            return now()
        late = (self._arrivals[count] - start).total_seconds()
        raise StepFailed(f"a connection attempt arrived {late:.3f} s into the step")

    async def finish(self) -> list[str]:
        """Every violation, including text frames no step consumed."""
        for peer in self._peers:
            self._record_unconsumed(peer)
        return self.violations()

    # Internals, on the server's loop.

    async def _open(self) -> None:
        self._changes = _Changes()
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def _close(self) -> None:
        assert self._server is not None
        self._server.close()
        for peer in self._peers:
            peer.writer.transport.abort()
        current = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if task is not current]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._server.wait_closed()

    async def _serve(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = _Peer(reader, writer, self._loop.time())
        self._peers.append(peer)
        try:
            while data := await reader.read(65536):
                peer.protocol.receive_data(data)
                self._process(peer)
            peer.protocol.receive_eof()
            self._process(peer)
        except OSError:
            # A reset ends the connection as an end of file does.
            pass
        finally:
            peer.ended = True
            if peer.status == "held":
                # Abandoned while held: discarded, never accepted.
                peer.status = "abandoned"
                self._held.remove(peer)
            if not writer.is_closing():
                writer.close()
            self._notify()

    def _process(self, peer: _Peer) -> None:
        for event in peer.protocol.events_received():
            if isinstance(event, Request):
                self._arrive(peer, event)
            elif isinstance(event, Frame) and peer.status == "accepted":
                self._receive(peer, event)
            else:
                self._violation(f"a frame before the handshake: {event}")
        self._flush(peer)
        self._notify()

    def _arrive(self, peer: _Peer, request: Request) -> None:
        peer.request = request
        peer.arrived = now()
        self._arrivals.append(peer.arrived)
        if self._refuse_all:
            self._reject(peer)
        else:
            peer.status = "held"
            self._held.append(peer)

    def _receive(self, peer: _Peer, frame: Frame) -> None:
        arrived = now()
        peer.last_frame = self._loop.time()
        connection = f"connection {peer.number}"
        if frame.opcode is Opcode.TEXT and frame.fin:
            try:
                text = bytes(frame.data).decode()
            except UnicodeDecodeError:
                self._violation(f"text that is not UTF-8 on {connection}")
                return
            if text == "PING":
                peer.pings.append(arrived)
                if self._pong == "auto" and peer.protocol.state is State.OPEN:
                    self._send(peer, "PONG", None)
                elif self._pong == "hold":
                    peer.owed += 1
            else:
                peer.inbox.append((arrived, text))
        elif frame.opcode is Opcode.CLOSE:
            close = peer.protocol.close_rcvd
            assert close is not None
            peer.client_close = (close.code, close.reason, arrived)
        elif frame.opcode in (Opcode.TEXT, Opcode.BINARY, Opcode.CONT):
            self._violation(f"a {frame.opcode.name.lower()} frame on {connection}")

    def _send(self, peer: _Peer, frame: str | bytes, label: str | None) -> datetime:
        if isinstance(frame, str):
            peer.protocol.send_text(frame.encode())
        else:
            peer.protocol.send_binary(frame)
        self._flush(peer)
        sent = now()
        if label is not None:
            self._sent[label] = frame
        return sent

    def _reject(self, peer: _Peer) -> datetime:
        peer.status = "refused"
        peer.protocol.send_response(peer.protocol.reject(503, "Service Unavailable\n"))
        self._flush(peer)
        return now()

    def _flush(self, peer: _Peer) -> None:
        # Whether the server's close frame waits among the data: queued, by
        # close or in reply to the client's, and not yet written.
        closing = (
            peer.close_sent is None
            and peer.protocol.close_sent is not None
            and not peer.writer.is_closing()
        )
        for data in peer.protocol.data_to_send():
            if peer.writer.is_closing():
                return
            if data:
                peer.writer.write(data)
            else:
                # The closing handshake is over, or the attempt was refused:
                # the server ends the TCP connection.
                peer.writer.close()
        if closing:
            peer.close_sent = now()

    async def _next_attempt(self) -> _Peer:
        await self._wait_for(lambda: self._held)
        return self._held.popleft()

    def _connection(self) -> _Peer:
        if self._current is None:
            raise StepFailed("no connection has been accepted")
        return self._current

    def _open_connection(self) -> _Peer:
        peer = self._connection()
        if peer.ended or peer.protocol.state is not State.OPEN:
            raise StepFailed(f"connection {peer.number} is not open")
        return peer

    async def _watch_idle(self, peer: _Peer, seconds: float) -> None:
        while not peer.ended:
            remaining = peer.last_frame + seconds - self._loop.time()
            if remaining <= 0:
                peer.writer.transport.abort()
                return
            await asyncio.sleep(remaining)

    def _start(self, coroutine: Coroutine[Any, Any, None]) -> None:
        task = self._loop.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _record_unconsumed(self, peer: _Peer) -> None:
        while peer.inbox:
            _, text = peer.inbox.popleft()
            self._violation(
                f"connection {peer.number}: a text frame no recv-subscribe consumed: "
                f"{text}"
            )

    async def _wait_for(self, predicate: Callable[[], object]) -> None:
        assert self._changes is not None
        await self._changes.wait_for(predicate)

    def _notify(self) -> None:
        assert self._changes is not None
        self._changes.notify()

    def _violation(self, message: str) -> None:
        with self._lock:
            self._violations.append(message)
