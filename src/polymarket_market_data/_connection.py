"""The connection to the market endpoint (spec/client.md, The connection).

``Connector`` opens a connection with the ``websockets`` asyncio client, its
own keepalive turned off, so that the application heartbeat is the only
liveness check. It sends one subscription frame built from the desired set,
then a ``PING`` every ``ping_interval``, and reads every frame: each is
decoded and fed to the state machine, and the records the machine produces
are queued for the consumer. The task that reads the socket never waits for
the consumer.

A subscribed connection that ends, or whose oldest unanswered ``PING`` has
waited ``pong_timeout`` (D1), is interrupted; an attempt that fails is
retried. Either way, once the connection has ended, after any close the
client started, the next attempt waits its backoff, within the bounds D2
specifies, and once they are exhausted the client fails with
``RecoveryFailed`` (spec/client.md, Detecting an interruption and
Reconnecting).

Settlement comes three ways (spec/client.md, Settlement): ``market_resolved``,
which the state machine applies as the reader feeds it; the close ``1000 all
subscribed assets resolved``, which ends the connection without interrupting
it; and a token without a book once ``book_timeout`` has passed, which starts
a confirmation through lookup. Confirmations outlive connections, so they
run in the connector's own task group. Once no desired market is left, no
connection is needed: the client closes a subscribed connection, or
abandons a backoff, an attempt, or a subscription frame, and waits until a
market is added (spec/client.md, Connection states).

A frame that reaches the queue's limit ends the client with a
``ClientError``, a stopgap until plan step 9 responds at the limit, so that
nothing is lost silently.
"""

import asyncio
import json
import math
import random
from collections import deque
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException
from websockets.protocol import State

from ._config import ClientConfig, ReconnectPolicy
from ._decode import decode_frame
from ._errors import ClientError, LookupFailed, RecoveryFailed
from ._lookup import Lookups
from ._queue import RecordQueue
from ._state import ALL_RESOLVED, EndConfirmation, StartConfirmation, StateMachine

CLIENT_EXIT = "client exit"
"""The reason of the close frame the client sends when it shuts down."""

PONG_TIMEOUT = "pong timeout"
"""The reason of the close frame the client sends after a ``pong_timeout``."""

NO_SUBSCRIPTIONS = "no subscriptions"
"""The reason of the close frame the client sends once no desired market is
left."""


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

    def restart(self, clock: float) -> None:
        """A first attempt starts at once: at startup, when a market is added
        to an empty desired set, or after the all-resolved close left desired
        markets that were not on the connection (spec/client.md,
        Reconnecting and Connection states). Attempt numbers start again, and
        the recovery clock runs from it."""
        self._failed = 0
        self._began = clock

    def begin(self, clock: float) -> None:
        """A recovery begins at an interruption. One already running keeps its
        clock, as after a connection that delivered no frame."""
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


class _Emptied(Exception):
    """No desired market is left, so a wait before a subscription frame was
    abandoned."""


@dataclass(slots=True)
class _Live:
    """A subscribed connection, as its reader and its own tasks share it."""

    generation: int
    heartbeat: _Heartbeat
    delivered: bool = False
    """Whether it delivered a frame before its end was recorded."""
    ended: bool = False
    """Whether the client has recorded its end: nothing that arrives on it
    afterwards is delivered or applied."""
    closing: bool = False
    """Whether the client is closing it after a ``pong_timeout``."""
    idle: asyncio.Event = field(default_factory=asyncio.Event)
    """Set once no desired market is left: the client closes it, and its end
    is no interruption."""


class Connector:
    """Runs the client's connections and settlement confirmations, feeding
    the state machine and queueing the records it produces."""

    def __init__(
        self,
        config: ClientConfig,
        state: StateMachine,
        queue: RecordQueue,
        lookups: Lookups | None,
    ) -> None:
        self._config = config
        self._state = state
        self._queue = queue
        self._lookups = lookups
        self._generation = 0
        self._socket: ClientConnection | None = None
        self._recovery = Recovery(config.reconnect)
        # What follows the attempt or connection that just ended: another
        # attempt after a backoff, or, if this is None, a first attempt at
        # once if a desired market needs one.
        self._retry: _Retry | None = None
        # The tasks that outlive a connection, settlement confirmations, by
        # market; they belong to the group run() holds.
        self._tasks: asyncio.TaskGroup | None = None
        self._confirmations: dict[str, asyncio.Task[None]] = {}
        # A market was added to an empty desired set.
        self._wanted = asyncio.Event()
        # The wait before a subscription frame, if one is under way, and the
        # subscribed connection, if one is open: what an empty desired set
        # abandons or closes.
        self._pending: asyncio.Timeout | None = None
        self._live: _Live | None = None
        self.pongs = PongCounts()

    async def run(self) -> None:
        """Run connections while a desired market needs one, until the client
        is cancelled. Raises ``RecoveryFailed`` once reconnection has
        exhausted its bounds. A connection still open when the task is
        cancelled stays open until ``close``, so that the client can record
        why it ended before closing it."""
        async with asyncio.TaskGroup() as tasks:
            self._tasks = tasks
            while True:
                while not self._state.desired:
                    # Idle: no connection is needed until a market is added
                    # (spec/client.md, Connection states).
                    self._wanted.clear()
                    await self._wanted.wait()
                await self._connections()

    def wanted(self) -> None:
        """A market was added to an empty desired set: connect at once."""
        self._wanted.set()

    async def close(self) -> None:
        """Close the open connection, if any, as the client shuts down."""
        socket, self._socket = self._socket, None
        if socket is not None:
            await self._close(socket, CLIENT_EXIT)

    def publish(self) -> None:
        """Queue the records the state machine has produced, carry out the
        effects it asked for, and, once no desired market is left, let go of
        the connection (spec/client.md, Connection states)."""
        self._queue.put(self._state.take())
        for effect in self._state.take_effects():
            match effect:
                case StartConfirmation(condition_id=condition_id, slug=slug):
                    assert self._tasks is not None
                    started = asyncio.get_running_loop().time()
                    self._confirmations[condition_id] = self._tasks.create_task(
                        self._confirm(condition_id, slug, started),
                        name=f"confirm-settlement-{condition_id}",
                    )
                case EndConfirmation(condition_id=condition_id):
                    task = self._confirmations.pop(condition_id, None)
                    # A confirmation that ended itself returns on its own.
                    if task is not None and task is not asyncio.current_task():
                        task.cancel()
        if not self._state.desired:
            self._emptied()

    # Connections.

    async def _connections(self) -> None:
        """Connect, subscribe, and read, and after a failed attempt or an
        interruption connect again, until no desired market is left. Raises
        ``RecoveryFailed`` once reconnection has exhausted its bounds."""
        loop = asyncio.get_running_loop()
        # The first attempt starts at once, and the recovery clock runs from
        # it (spec/client.md, Reconnecting).
        self._recovery.restart(loop.time())
        while self._state.desired:
            self._retry = None
            self._state.connecting(self._recovery.attempt, now())
            self.publish()
            socket = await self._attempt()
            if socket is not None:
                self._socket = socket
                await self._serve(socket)
                self._socket = None
            if not self._state.desired:
                return  # its idle record came when the last market left
            if self._retry is None:
                # The all-resolved close left desired markets that were not
                # on the connection, or a market was added after the last one
                # left: attempt 1, at once (spec/client.md, Connection
                # states).
                self._recovery.restart(loop.time())
                continue
            # Decided only now that the connection has ended, so that a close
            # the client started cannot hold the attempt past the wait that
            # recovering announced, or past the bound it was checked against.
            wait = self._next(self._retry)
            try:
                async with self._unless_emptied():
                    await asyncio.sleep(wait)
            except _Emptied:
                return

    async def _attempt(self) -> ClientConnection | None:
        """Open a connection, or return ``None`` if the attempt failed or no
        desired market is left."""
        config = self._config
        try:
            async with self._unless_emptied():
                return await connect(
                    config.url,
                    open_timeout=config.connect_timeout,
                    ping_interval=None,
                    close_timeout=config.close_timeout,
                    max_size=config.max_message_bytes,
                )
        except _Emptied:
            return None
        except (OSError, WebSocketException) as error:
            # A failed attempt, retried within the bounds. The TimeoutError of
            # connect_timeout is an OSError. Any other exception is a defect,
            # which ends the client (spec/client.md, Errors).
            self._attempt_failed(f"{type(error).__name__}: {error}", error)
            return None

    async def _serve(self, socket: ClientConnection) -> None:
        """Subscribe on a new connection, then read it, send its heartbeat,
        and time its books until it ends."""
        loop = asyncio.get_running_loop()
        self._generation += 1
        generation = self._generation
        if not self._state.desired:
            # The last market left as the handshake completed.
            await self._close(socket, NO_SUBSCRIPTIONS)
            return
        self._state.opened(generation, now())
        self.publish()
        tokens = self._state.subscription()
        try:
            async with self._unless_emptied():
                await socket.send(subscription_frame(tokens))
        except _Emptied:
            await self._close(socket, NO_SUBSCRIPTIONS)
            return
        except ConnectionClosed as closed:
            # A failed attempt, not an interruption: no token was subscribed
            # on it (spec/client.md, Connecting and subscribing).
            self._attempt_failed(
                f"connection {generation} ended before its subscription frame "
                f"was sent: {closed}",
                closed,
            )
            return
        if not self._state.desired:
            await self._close(socket, NO_SUBSCRIPTIONS)
            return
        subscribed = loop.time()
        self._state.subscribed(tokens, now())
        self.publish()
        live = _Live(generation, _Heartbeat(self.pongs))
        self._live = live
        try:
            async with asyncio.TaskGroup() as group:
                heartbeat = group.create_task(
                    self._heartbeat(socket, live, subscribed), name="heartbeat"
                )
                books = group.create_task(
                    self._book_timeout(live, subscribed), name="book-timeout"
                )
                idle = group.create_task(
                    self._close_when_idle(socket, live), name="close-when-idle"
                )
                await self._read(socket, live)
                books.cancel()
                # A close the client started completes on its own.
                if not live.closing:
                    heartbeat.cancel()
                if not live.idle.is_set():
                    idle.cancel()
        finally:
            self._live = None

    @asynccontextmanager
    async def _unless_emptied(self) -> AsyncIterator[None]:
        """Wrap a wait before a subscription frame is sent: a backoff, an
        attempt, or the frame's send. If no desired market is left meanwhile,
        the wait is abandoned and ``_Emptied`` raised, since no connection is
        needed (spec/client.md, Connection states)."""
        # A timeout that never expires unless publish() reschedules it to
        # now: a cancel scope, which tells its own cancellation from the
        # client's, and its expiry from a TimeoutError of the wait.
        scope = asyncio.timeout(None)
        try:
            async with scope:
                self._pending = scope
                try:
                    yield
                finally:
                    self._pending = None
        except TimeoutError:
            if scope.expired():
                raise _Emptied from None
            raise

    def _emptied(self) -> None:
        """No desired market is left: abandon the wait before a subscription
        frame, or close the subscribed connection (spec/client.md, Connection
        states). The state machine has emitted the idle record."""
        pending = self._pending
        if pending is not None and not pending.expired():
            pending.reschedule(asyncio.get_running_loop().time())
        live = self._live
        if live is not None and not live.ended:
            live.idle.set()

    async def _close_when_idle(self, socket: ClientConnection, live: _Live) -> None:
        """Close the connection once no desired market is left on it."""
        await live.idle.wait()
        await self._close(socket, NO_SUBSCRIPTIONS)

    async def _book_timeout(self, live: _Live, subscribed: float) -> None:
        """``book_timeout`` after the subscription frame, the tokens still
        waiting for a book become uncertain, and their markets' settlement
        confirmations start (T4)."""
        loop = asyncio.get_running_loop()
        await asyncio.sleep(subscribed + self._config.book_timeout - loop.time())
        # The machine ignores it once the connection has been interrupted.
        self._state.book_timeout(live.generation, now())
        self.publish()

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
                expires = (clock if oldest is None else oldest) + timeout
                try:
                    # A send waits while the transport's buffer is full; the
                    # oldest PING's deadline bounds it, so that the timeout
                    # is checked when it is due all the same.
                    async with asyncio.timeout_at(expires):
                        await socket.send("PING")
                except ConnectionClosed:
                    return  # the reader records the end
                except TimeoutError:
                    pass  # the loop checks the deadline, which a PONG may move
        if live.ended or live.idle.is_set():
            return
        if socket.state is not State.OPEN:
            # Already ending, by the server's close frame or a protocol error,
            # which the reader records as the cause once the connection has
            # ended: not a pong_timeout (spec/client.md, Detecting an
            # interruption). The server may be slow to end it.
            await self._await_end(socket)
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
            self.publish()

    def _ended(self, live: _Live, closed: ConnectionClosed) -> None:
        """The connection ended before the client recorded its end: by the
        client's own close once no desired market was left, with the
        server's close frame, or without one, as by a reset, an end of file,
        or a protocol error (spec/client.md, Detecting an interruption)."""
        if live.idle.is_set():
            # No desired market is left on it, so its end, however it came,
            # interrupts nothing; its idle record came then.
            live.ended = True
            return
        at, clock = now(), asyncio.get_running_loop().time()
        close = closed.rcvd
        if close is None:
            # On a protocol error, such as a frame larger than
            # max_message_bytes, the library sends its own close frame and
            # ignores the server's answer, so none is received.
            self._interrupt(live, "dropped", at, clock, error=closed)
        elif (close.code, close.reason) == (1000, ALL_RESOLVED):
            # Settlement, not an interruption (T16): no attempt follows for
            # the tokens it settles. Desired markets that were not on it
            # connect again at once.
            live.ended = True
            self._state.all_resolved(at)
            self.publish()
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
        events, and that another attempt follows it (T14; spec/client.md,
        Record order, rule 4). One that delivered no frame is also a failed
        attempt. ``error`` is the exception that ended it, if one did."""
        live.ended = True
        self._state.interrupted(
            cause, at, close_code=close_code, close_reason=close_reason
        )
        self.publish()
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
            self.publish()
            return wait
        self._state.failed(exhausted, at, detail=retry.detail)
        self.publish()
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

    async def _await_end(self, socket: ClientConnection) -> None:
        """Wait at most ``close_timeout`` for a connection that is closing
        to end, and then end it."""
        try:
            async with asyncio.timeout(self._config.close_timeout):
                await socket.wait_closed()
        except TimeoutError:
            socket.transport.abort()

    # Settlement confirmation.

    async def _confirm(self, condition_id: str, slug: str, started: float) -> None:
        """Confirm a market's settlement through lookup: ask at once, then
        every ``settlement_poll_interval``, until lookup shows the market
        closed (T11) or finds no market (T12), or until
        ``settlement_confirm_timeout`` has passed since the T4 that started
        it (T13). A failed call is counted and treated as still open
        (spec/client.md, Settlement and Market lookup)."""
        lookups = self._lookups
        assert lookups is not None  # the machine confirms only with a lookup
        loop = asyncio.get_running_loop()
        config = self._config
        scope = asyncio.timeout_at(started + config.settlement_confirm_timeout)
        try:
            async with scope:
                due = started
                while True:
                    await asyncio.sleep(due - loop.time())
                    due = max(due + config.settlement_poll_interval, loop.time())
                    try:
                        info = await lookups.market(slug)
                    except LookupFailed:
                        continue
                    self._state.confirmation(condition_id, info, now())
                    self.publish()
                    if condition_id not in self._confirmations:
                        return  # T11 or T12 ended it
        except TimeoutError:
            if not scope.expired():
                raise
        self._state.confirmation_timeout(condition_id, now())
        self.publish()


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
