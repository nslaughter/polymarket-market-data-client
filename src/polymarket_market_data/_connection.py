"""The connection to the market endpoint (spec/client.md, The connection).

``Connector`` opens a connection with the ``websockets`` asyncio client, its
own keepalive turned off, so that the application heartbeat is the only
liveness check. It sends one subscription frame built from the desired set,
then a ``PING`` every ``ping_interval``, and reads every frame: each is
decoded and fed to the state machine, and the records the machine produces
are queued for the consumer. The task that reads the socket never waits for
the consumer.

A subscribed connection that ends, or whose oldest unanswered ``PING`` has
waited ``pong_timeout`` (D1), is interrupted, unless no desired market is
left on it; an attempt that fails is retried. Either way, once the
connection has ended, after any close the client started, the next attempt
waits its backoff, within the bounds D2 specifies, and once they are
exhausted the client fails with ``RecoveryFailed`` (spec/client.md,
Detecting an interruption and Reconnecting).

Two stopgaps end the client with a ``ClientError`` until later plan steps
replace them, so that neither loses anything silently: the close ``1000 all
subscribed assets resolved``, which settles in step 7, and a frame that
reaches the queue's limit, until step 9.
"""

import asyncio
import json
import math
import random
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from ._config import ClientConfig, ReconnectPolicy
from ._decode import decode_frame
from ._errors import ClientError, RecoveryFailed
from ._queue import RecordQueue
from ._state import ALL_RESOLVED, StateMachine

CLIENT_EXIT = "client exit"
"""The reason of the close frame the client sends when it shuts down."""

PONG_TIMEOUT = "pong timeout"
"""The reason of the close frame the client sends after a ``pong_timeout``."""


def now() -> datetime:
    return datetime.now(UTC)


def subscription_frame(tokens: Sequence[str]) -> str:
    """The subscription frame naming ``tokens``: ``custom_feature_enabled``
    is always true, since it adds ``market_resolved``."""
    return json.dumps(
        {"type": "market", "assets_ids": list(tokens), "custom_feature_enabled": True},
        separators=(",", ":"),
    )


class Recovery:
    """Attempt numbers, the attempts that failed in a row, and the recovery
    clock, which decide how long the client waits before its next attempt,
    or whether it fails instead (D2; spec/client.md, Reconnecting).

    No I/O and no timers: times are seconds on the event loop's clock,
    passed in.
    """

    def __init__(
        self, policy: ReconnectPolicy, *, uniform: Callable[[], float] = random.random
    ) -> None:
        self._policy = policy
        self._uniform = uniform
        """A uniform random factor in [0, 1), for jitter."""
        self._failed = 0
        self._began: float | None = None

    @property
    def attempt(self) -> int:
        """The next attempt's number, which follows on from the attempts
        that failed in a row."""
        return self._failed + 1

    def begin(self, clock: float) -> None:
        """A recovery begins: at startup, as the first attempt begins, or at
        an interruption. One already running keeps its clock, as after a
        connection that delivered no frame."""
        if self._began is None:
            self._began = clock

    def delivered(self) -> None:
        """A connection delivered its first frame after subscribing: attempt
        numbers and the recovery clock start again (spec/client.md,
        Decisions)."""
        self._failed = 0
        self._began = None

    def failed(self) -> None:
        """An attempt failed."""
        self._failed += 1

    def delay(self) -> float:
        """The wait before the next attempt, *k*: ``min(max_delay,
        base_delay × 2^(k − 1))``, multiplied by a uniform random factor in
        [0, 1) when ``jitter`` is on."""
        policy = self._policy
        # Doubled step by step, so that a large k cannot overflow a float.
        delay = policy.base_delay
        for _ in range(self.attempt - 1):
            if delay >= policy.max_delay:
                break
            delay *= 2
        delay = min(delay, policy.max_delay)
        return delay * self._uniform() if policy.jitter else delay

    def exhausted(self, clock: float, wait: float) -> str | None:
        """Why the client fails instead of waiting ``wait`` for the next
        attempt, if it does: ``max_attempts`` once that many attempts in a
        row have failed, or ``max_recovery_time`` if the wait would end more
        than that after the recovery began."""
        policy = self._policy
        if self._failed >= policy.max_attempts:
            return "max_attempts"
        began = clock if self._began is None else self._began
        if clock + wait - began > policy.max_recovery_time:
            return "max_recovery_time"
        return None


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

    @property
    def oldest(self) -> float | None:
        """When the oldest unanswered ``PING`` was sent, if one is."""
        return self._unanswered[0] if self._unanswered else None

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


@dataclass(frozen=True, slots=True)
class _Retry:
    """The attempt or connection that just ended calls for another
    attempt."""

    detail: str | None
    """How it failed, if it was a failed attempt."""
    error: BaseException | None
    """The exception that ended it, if one did."""


@dataclass(slots=True)
class _Live:
    """A subscribed connection, as its reader and its heartbeat share it."""

    generation: int
    heartbeat: _Heartbeat
    delivered: bool = False
    """Whether it delivered a frame before its end was recorded."""
    ended: bool = False
    """Whether the client has recorded its end: nothing that arrives on it
    afterwards is delivered or applied."""
    closing: bool = False
    """Whether the client is closing it after a ``pong_timeout``."""


class Connector:
    """Runs the client's connections, feeding the state machine and
    queueing the records it produces."""

    def __init__(
        self, config: ClientConfig, state: StateMachine, queue: RecordQueue
    ) -> None:
        self._config = config
        self._state = state
        self._queue = queue
        self._generation = 0
        self._socket: ClientConnection | None = None
        self._recovery = Recovery(config.reconnect)
        # What follows the attempt or connection that just ended: another
        # attempt, or, if this is None, nothing.
        self._retry: _Retry | None = None
        self.pongs = PongCounts()

    async def run(self) -> None:
        """Connect, subscribe, and read, and after a failed attempt or an
        interruption connect again, until the client is cancelled. Raises
        ``RecoveryFailed`` once reconnection has exhausted its bounds, and
        returns once no connection is needed. A connection still open when
        the task is cancelled stays open until ``close``, so that the client
        can record why it ended before closing it."""
        loop = asyncio.get_running_loop()
        config = self._config
        # The first attempt starts at once, and at startup the recovery clock
        # runs from it (spec/client.md, Reconnecting).
        self._recovery.begin(loop.time())
        while True:
            self._retry = None
            self._state.connecting(self._recovery.attempt, now())
            self._publish()
            try:
                socket = await connect(
                    config.url,
                    open_timeout=config.connect_timeout,
                    ping_interval=None,
                    close_timeout=config.close_timeout,
                    max_size=config.max_message_bytes,
                )
            except (OSError, WebSocketException) as error:
                # A failed attempt, retried within the bounds. The
                # TimeoutError of connect_timeout is an OSError. Any other
                # exception is a defect, which ends the client
                # (spec/client.md, Errors).
                self._attempt_failed(f"{type(error).__name__}: {error}", error)
            else:
                self._socket = socket
                await self._serve(socket)
                self._socket = None
            if self._retry is None:
                return
            # Decided only now that the connection has ended, so that a close
            # the client started cannot hold the attempt past the wait that
            # recovering announced, or past the bound it was checked against.
            await asyncio.sleep(self._next(self._retry))

    async def close(self) -> None:
        """Close the open connection, if any, as the client shuts down."""
        socket, self._socket = self._socket, None
        if socket is not None:
            await self._close(socket, CLIENT_EXIT)

    async def _serve(self, socket: ClientConnection) -> None:
        """Subscribe on a new connection, then read it and send its
        heartbeat until it ends."""
        loop = asyncio.get_running_loop()
        self._generation += 1
        live = _Live(self._generation, _Heartbeat(self.pongs))
        self._state.opened(live.generation, now())
        self._publish()
        tokens = self._state.subscription()
        try:
            await socket.send(subscription_frame(tokens))
        except ConnectionClosed as closed:
            # A failed attempt, not an interruption: no token was subscribed
            # on it (spec/client.md, Connecting and subscribing).
            self._attempt_failed(
                f"connection {live.generation} ended before its subscription "
                f"frame was sent: {closed}",
                closed,
            )
            return
        subscribed = loop.time()
        self._state.subscribed(tokens, now())
        self._publish()
        async with asyncio.TaskGroup() as group:
            heartbeat = group.create_task(
                self._heartbeat(socket, live, subscribed), name="heartbeat"
            )
            await self._read(socket, live)
            if not live.closing:
                heartbeat.cancel()

    async def _heartbeat(
        self, socket: ClientConnection, live: _Live, start: float
    ) -> None:
        """Send ``PING`` every ``ping_interval``, the first one
        ``ping_interval`` after the subscription frame, whatever other
        traffic there is; and once the oldest unanswered ``PING`` has waited
        ``pong_timeout``, interrupt the connection and close it (D1)."""
        loop = asyncio.get_running_loop()
        interval, timeout = self._config.ping_interval, self._config.pong_timeout
        due = start + interval
        while True:
            oldest = live.heartbeat.oldest
            expires = math.inf if oldest is None else oldest + timeout
            await asyncio.sleep(min(due, expires) - loop.time())
            clock = loop.time()
            # A PONG may have answered the oldest PING meanwhile.
            oldest = live.heartbeat.oldest
            if oldest is not None and clock >= oldest + timeout:
                break
            if clock >= due:
                live.heartbeat.sent(clock)
                due = max(due + interval, clock)
                try:
                    await socket.send("PING")
                except ConnectionClosed:
                    return  # the reader records the end
        if live.ended:
            return
        live.closing = True
        self._interrupt(live, "pong_timeout", now(), clock)
        # The reader goes on reading while the close completes, so that the
        # frames still arriving are counted and discarded.
        await self._close(socket, PONG_TIMEOUT)

    async def _read(self, socket: ClientConnection, live: _Live) -> None:
        """Read every frame until the connection ends. Once its end is
        recorded, what still arrives is counted and discarded
        (spec/client.md, Detecting an interruption)."""
        loop = asyncio.get_running_loop()
        frame = 0
        while True:
            try:
                data = await socket.recv()
            except ConnectionClosed as closed:
                if not live.ended:
                    self._ended(live, closed)
                return
            received_at, clock = now(), loop.time()
            if data == "PONG":
                live.heartbeat.answered(clock)
                self._state.pong(live.generation, received_at)
                continue
            frame += 1
            if not (live.ended or live.delivered):
                live.delivered = True
                self._recovery.delivered()
            items = decode_frame(
                data,
                received_at=received_at,
                connection=live.generation,
                frame=frame,
                keep_raw=self._config.keep_raw,
            )
            if self._queue.full() and self._state.delivers(live.generation, items):
                raise ClientError(
                    f"{self._queue.backlog} market-event records are waiting for "
                    "the consumer, and the response at the limit comes with plan "
                    "step 9"
                )
            self._state.frame(
                live.generation, items, received_at=received_at, clock=clock
            )
            self._publish()

    def _ended(self, live: _Live, closed: ConnectionClosed) -> None:
        """The connection ended without the client closing it: with the
        server's close frame, or without one, as by a reset, an end of file,
        or a protocol error (spec/client.md, Detecting an interruption)."""
        at, clock = now(), asyncio.get_running_loop().time()
        close = closed.rcvd
        if close is None:
            # On a protocol error, such as a frame larger than
            # max_message_bytes, the library sends its own close frame and
            # ignores the server's answer, so none is received.
            self._interrupt(live, "dropped", at, clock, error=closed)
        elif (close.code, close.reason) == (1000, ALL_RESOLVED) and self._state.desired:
            raise ClientError(
                f"the server closed connection {live.generation} with 1000 "
                f"{ALL_RESOLVED!r}, and settling by it comes with plan step 7"
            )
        else:
            self._interrupt(
                live,
                "close_frame",
                at,
                clock,
                close_code=close.code,
                close_reason=close.reason,
                error=closed,
            )

    def _interrupt(
        self,
        live: _Live,
        cause: str,
        at: datetime,
        clock: float,
        *,
        close_code: int | None = None,
        close_reason: str | None = None,
        error: BaseException | None = None,
    ) -> None:
        """Record the end of a subscribed connection that may have lost
        events, and whether another attempt follows it (T14; spec/client.md,
        Record order, rule 4). One that delivered no frame is also a failed
        attempt. ``error`` is the exception that ended it, if one did."""
        live.ended = True
        if not self._state.desired:
            # Not an interruption: no desired market is left on it, as after
            # the last one settled, whose idle record came then. No
            # connection is needed (spec/client.md, Connection states).
            return
        self._state.interrupted(
            cause, at, close_code=close_code, close_reason=close_reason
        )
        self._publish()
        self._recovery.begin(clock)
        detail = None
        if not live.delivered:
            self._recovery.failed()
            detail = f"connection {live.generation} ended before delivering a frame"
        self._retry = _Retry(detail, error)

    def _attempt_failed(self, detail: str, error: BaseException) -> None:
        """An attempt failed before its connection was subscribed."""
        self._recovery.failed()
        self._retry = _Retry(detail, error)

    def _next(self, retry: _Retry) -> float:
        """Emit ``recovering`` and return the backoff before the next
        attempt, or fail if the bounds are exhausted (D2). Failing emits
        each open gap's ``CaptureGap``, then ``failed``, with no
        ``recovering`` record for the attempt the client will not make
        (T18; spec/client.md, Record order, rule 4). Either record says how
        the last attempt failed, if it did, and ``RecoveryFailed`` keeps the
        exception that ended it as its ``__cause__``, since no later record
        reports that attempt."""
        at, clock = now(), asyncio.get_running_loop().time()
        recovery = self._recovery
        wait = recovery.delay()
        exhausted = recovery.exhausted(clock, wait)
        if exhausted is None:
            self._state.recovering(
                recovery.attempt, "backoff", at, retry_in=wait, detail=retry.detail
            )
            self._publish()
            return wait
        self._state.failed(exhausted, at, detail=retry.detail)
        self._publish()
        raise RecoveryFailed(_exhausted(exhausted, self._config)) from retry.error

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


def _exhausted(reason: str, config: ClientConfig) -> str:
    policy = config.reconnect
    if reason == "max_attempts":
        return (
            f"reconnection failed: {policy.max_attempts} attempts in a row failed "
            "(max_attempts)"
        )
    return (
        "reconnection failed: the next attempt would start more than "
        f"{policy.max_recovery_time} s after the recovery began (max_recovery_time)"
    )
