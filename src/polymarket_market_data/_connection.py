"""The connection to the market endpoint (spec/client.md, The connection).

``Connector`` opens a connection with the ``websockets`` asyncio client, its
own keepalive turned off, so that the application heartbeat is the only
liveness check. It sends one subscription frame built from the desired set,
then a ``PING`` every ``ping_interval``, and reads every frame: each is
decoded and fed to the state machine, and the records the machine produces
are queued for the consumer. The task that reads the socket never waits for
the consumer.

Until plan step 6, a failed attempt and any end of the connection end the
client with a ``ClientError``, where reconnection will follow; until step 9,
so does a frame that reaches the queue's limit. Neither loses anything
silently.
"""

import asyncio
import json
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from ._config import ClientConfig
from ._decode import decode_frame
from ._errors import ClientError
from ._queue import RecordQueue
from ._state import StateMachine

CLIENT_EXIT = "client exit"
"""The reason of the close frame the client sends when it shuts down."""


def now() -> datetime:
    return datetime.now(UTC)


def subscription_frame(tokens: Sequence[str]) -> str:
    """The subscription frame naming ``tokens``: ``custom_feature_enabled``
    is always true, since it adds ``market_resolved``."""
    return json.dumps(
        {"type": "market", "assets_ids": list(tokens), "custom_feature_enabled": True},
        separators=(",", ":"),
    )


@dataclass(slots=True)
class PongCounts:
    """The heartbeat's counters (spec/client.md, Statistics)."""

    pongs: int = 0
    pongs_unsolicited: int = 0
    delay_last: float = 0.0
    delay_max: float = 0.0


class _Heartbeat:
    """The ``PING``s sent on one connection and the ``PONG``s answering
    them. ``PONG`` carries no identifier, so each answers the oldest
    unanswered ``PING``. Times are seconds on the event loop's clock."""

    def __init__(self, counts: PongCounts) -> None:
        self._counts = counts
        self._unanswered: deque[float] = deque()

    def sent(self, at: float) -> None:
        self._unanswered.append(at)

    def answered(self, at: float) -> None:
        counts = self._counts
        counts.pongs += 1
        if not self._unanswered:
            counts.pongs_unsolicited += 1  # counted and ignored
            return
        counts.delay_last = at - self._unanswered.popleft()
        counts.delay_max = max(counts.delay_max, counts.delay_last)


class Connector:
    """Runs the client's connection, feeding the state machine and queueing
    the records it produces."""

    def __init__(
        self, config: ClientConfig, state: StateMachine, queue: RecordQueue
    ) -> None:
        self._config = config
        self._state = state
        self._queue = queue
        self._generation = 0
        self._socket: ClientConnection | None = None
        self.pongs = PongCounts()

    async def run(self) -> None:
        """Connect, subscribe, and read until the connection ends, which
        raises ``ClientError``, or the task is cancelled. The connection
        stays open until ``close``, so that the client can record why it
        ended before closing it."""
        self._state.connecting(1, now())
        self._publish()
        config = self._config
        try:
            socket = await connect(
                config.url,
                open_timeout=config.connect_timeout,
                ping_interval=None,
                close_timeout=config.close_timeout,
                max_size=config.max_message_bytes,
            )
        except Exception as error:
            raise ClientError(
                "the connection attempt failed, and reconnecting comes with plan step 6"
            ) from error
        self._socket = socket
        await self._serve(socket)

    async def close(self) -> None:
        """Close the open connection, if any, as the client shuts down."""
        socket, self._socket = self._socket, None
        if socket is not None:
            await self._close(socket, CLIENT_EXIT)

    async def _serve(self, socket: ClientConnection) -> None:
        self._generation += 1
        generation = self._generation
        self._state.opened(generation, now())
        self._publish()
        tokens = self._state.subscription()
        try:
            await socket.send(subscription_frame(tokens))
        except ConnectionClosed as closed:
            raise _ended(closed) from closed
        subscribed = asyncio.get_running_loop().time()
        self._state.subscribed(tokens, now())
        self._publish()
        heartbeat = _Heartbeat(self.pongs)
        async with asyncio.TaskGroup() as group:
            group.create_task(
                self._ping(socket, heartbeat, subscribed), name="heartbeat"
            )
            await self._read(socket, generation, heartbeat)

    async def _ping(
        self, socket: ClientConnection, heartbeat: _Heartbeat, start: float
    ) -> None:
        """Send ``PING`` every ``ping_interval``, the first one
        ``ping_interval`` after the subscription frame, whatever other
        traffic there is."""
        loop = asyncio.get_running_loop()
        due = start
        while True:
            due = max(due + self._config.ping_interval, loop.time())
            await asyncio.sleep(due - loop.time())
            heartbeat.sent(loop.time())
            try:
                await socket.send("PING")
            except ConnectionClosed:
                return  # the reader reports the end

    async def _read(
        self, socket: ClientConnection, generation: int, heartbeat: _Heartbeat
    ) -> None:
        loop = asyncio.get_running_loop()
        frame = 0
        while True:
            try:
                data = await socket.recv()
            except ConnectionClosed as closed:
                raise _ended(closed) from closed
            received_at, clock = now(), loop.time()
            if data == "PONG":
                heartbeat.answered(clock)
                self._state.pong(generation, received_at)
                continue
            frame += 1
            items = decode_frame(
                data,
                received_at=received_at,
                connection=generation,
                frame=frame,
                keep_raw=self._config.keep_raw,
            )
            if self._queue.full() and self._state.delivers(generation, items):
                raise ClientError(
                    f"{self._queue.backlog} market-event records are waiting for "
                    "the consumer, and the response at the limit comes with plan "
                    "step 9"
                )
            self._state.frame(generation, items, received_at=received_at, clock=clock)
            self._publish()

    async def _close(self, socket: ClientConnection, reason: str) -> None:
        """Close the connection with code 1000 and ``reason``, waiting at
        most ``close_timeout`` for the closing handshake."""
        try:
            async with asyncio.timeout(self._config.close_timeout):
                await socket.close(1000, reason)
        except TimeoutError:
            socket.transport.abort()
        except asyncio.CancelledError:
            # Cancelled again while closing: end the connection at once.
            socket.transport.abort()
            raise

    def _publish(self) -> None:
        self._queue.put(self._state.take())


def _ended(closed: ConnectionClosed) -> ClientError:
    received = closed.rcvd
    how = (
        "without a close frame"
        if received is None
        else f"with close frame {received.code} {received.reason!r}"
    )
    return ClientError(
        f"the connection ended {how}, and reconnecting comes with plan step 6"
    )
