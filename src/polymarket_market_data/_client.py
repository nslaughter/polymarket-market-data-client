"""``MarketDataClient``: the desired set, the queue, the statistics, and the
tasks (spec/client.md, Public interface).

The client owns one task while it runs, which runs the connection; the
connection's own tasks belong to a ``TaskGroup`` inside it. Leaving the
``async with`` block cancels that task and waits for it, and the task closes
an open connection as it ends, so nothing the client started outlives the
block.

Not yet here: changing the desired set while the client runs (plan steps 7
and 8), ``resolve`` and the default lookup (step 7), and refusing
``verify_hash`` without a lookup (step 10).
"""

import asyncio
from collections.abc import AsyncIterator, Iterable
from enum import Enum, auto
from types import TracebackType
from typing import Self

from pydantic import ValidationError

from ._config import ClientConfig
from ._connection import Connector, now
from ._errors import ClientError, ClientStateError, ConfigError
from ._lookup import MarketLookup
from ._queue import AnyRecord, RecordIterator, RecordQueue
from ._records import ClientStats, Market
from ._state import StateMachine

_DEFAULT_CONFIG = ClientConfig()


class _Stage(Enum):
    NEW = auto()
    """Built, and the block not yet entered."""
    RUNNING = auto()
    """In the block."""
    ENDED = auto()
    """Ended on its own, by a failure, and the block not yet left."""
    SHUT_DOWN = auto()
    """The block has been left."""


class MarketDataClient:
    """The client for Polymarket's market WebSocket (spec/client.md, Public
    interface).

    ``async with client`` starts it: it connects once the desired set is
    non-empty. ``client.records()`` gives every record, in order. Leaving the
    block, by any path, shuts it down.
    """

    def __init__(
        self,
        config: ClientConfig = _DEFAULT_CONFIG,
        *,
        markets: Iterable[Market] = (),
        lookup: MarketLookup | None = None,
    ) -> None:
        self._config = _validated(config)
        self._lookup = lookup
        self._state = StateMachine(
            repeat_window=self._config.repeat_window,
            deliver_new_market=self._config.new_market == "deliver",
            can_look_up=lookup is not None,
            markets=markets,
        )
        self._queue = RecordQueue(self._config.queue_size)
        self._connector = Connector(self._config, self._state, self._queue)
        self._stage = _Stage.NEW
        self._task: asyncio.Task[None] | None = None
        self._iterating = False

    # The lifecycle (spec/client.md, Cancellation and shutdown).

    async def __aenter__(self) -> Self:
        if self._stage is not _Stage.NEW:
            raise ClientStateError("a client can be entered once")
        self._stage = _Stage.RUNNING
        if self._state.desired:
            self._task = asyncio.create_task(self._run(), name="market-data-client")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stage = _Stage.SHUT_DOWN
        task, self._task = self._task, None
        if task is not None:
            await _stop(task)
        self._queue.close()
        # A failure no consumer has read is raised here, once, however the
        # block is left. As TaskGroup does with a task's error, it takes the
        # place of an exception or a cancellation leaving the block, but not
        # of KeyboardInterrupt or SystemExit, which leave it to the iterator.
        if exc is None or isinstance(exc, Exception | asyncio.CancelledError):
            failure = self._queue.take_failure()
            if failure is not None:
                raise failure

    async def _run(self) -> None:
        try:
            await self._connector.run()
        except Exception as error:
            # The client ends, and the failure is raised once: by the
            # iterator, after the records already queued, or when the block
            # is left. It is recorded before the connection is closed, which
            # leaving the block can cut short.
            if self._stage is _Stage.RUNNING:
                self._stage = _Stage.ENDED
            self._queue.fail(_failure(error))
        finally:
            await self._connector.close()

    # The desired set.

    @property
    def desired(self) -> tuple[Market, ...]:
        """The desired set, in the order the markets were added."""
        return self._state.desired

    def subscribe(self, *markets: Market) -> None:
        """Add markets to the end of the desired set; a market already in it
        is left where it is. Raises ``ValueError`` if a token would belong to
        two markets of the desired set."""
        self._changing()
        self._state.subscribe(markets)

    def unsubscribe(self, *condition_ids: str) -> None:
        """Remove markets from the desired set; an ID not in it is
        ignored."""
        self._changing()
        self._state.unsubscribe(condition_ids, now())
        self._queue.put(self._state.take())

    def _changing(self) -> None:
        if self._stage in (_Stage.ENDED, _Stage.SHUT_DOWN):
            raise ClientStateError("the client has shut down")
        if self._stage is _Stage.RUNNING:
            raise NotImplementedError(
                "changing the desired set while the client runs comes with plan "
                "steps 7 and 8"
            )

    # The consumer's side.

    def records(self) -> AsyncIterator[AnyRecord]:
        """Every record, in order. It can be called once."""
        if self._iterating:
            raise ClientStateError("records() can be called once")
        self._iterating = True
        return RecordIterator(self._queue)

    @property
    def backlog(self) -> int:
        """The market-event records waiting for the consumer."""
        return self._queue.backlog

    def stats(self) -> ClientStats:
        """A snapshot of the client's counters, cumulative since it started."""
        counts = self._state.counts
        pongs = self._connector.pongs
        return ClientStats(
            frames=counts.frames,
            frames_after_interruption=counts.frames_after_interruption,
            events=dict(counts.events),
            repeats=counts.repeats,
            unknown=counts.unknown,
            undecodable=dict(counts.undecodable),
            new_market_dropped=counts.new_market_dropped,
            discarded_outside=counts.discarded_outside,
            pongs=pongs.pongs,
            pongs_unsolicited=pongs.pongs_unsolicited,
            pong_delay_last=pongs.delay_last,
            pong_delay_max=pongs.delay_max,
            connections=counts.connections,
            interruptions=dict(counts.interruptions),
            lookups=0,
            lookup_failures=0,
            rest_book_not_found=0,
            hash_verified=0,
            hash_retried=0,
            hash_failed=0,
        )

    async def resolve(self, slug: str) -> Market:
        """Look a market up by slug and return its ``Market``."""
        raise NotImplementedError("resolve comes with plan step 7")


def _validated(config: ClientConfig) -> ClientConfig:
    """The configuration, validated again, since ``model_copy(update=...)``
    and ``model_construct`` build a model without validating it."""
    if not isinstance(config, ClientConfig):
        raise ConfigError(f"config must be a ClientConfig, not {type(config).__name__}")
    try:
        return ClientConfig.model_validate(config)
    except ValidationError as error:
        raise ConfigError(str(error)) from error


async def _stop(task: asyncio.Task[None]) -> None:
    """Cancel the client's task and wait until it has ended. A cancellation
    of the caller meanwhile cancels the task again, so that it ends at once,
    and is raised once the task has ended."""
    task.cancel()
    cancelled: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.wait({task})
        except asyncio.CancelledError as error:
            cancelled = error
            task.cancel()
    if cancelled is not None:
        raise cancelled


def _failure(error: Exception) -> ClientError:
    """The ``ClientError`` the client ends with: the error itself, or, for a
    defect in the client's own code, a ``ClientError`` whose ``__cause__``
    it is."""
    while isinstance(error, ExceptionGroup):
        error = error.exceptions[0]
    if isinstance(error, ClientError):
        return error
    failure = ClientError(f"the client stopped on an internal error: {error!r}")
    failure.__cause__ = error
    return failure
