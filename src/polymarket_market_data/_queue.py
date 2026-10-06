"""The handoff of records to the consumer (spec/client.md, Consumer handoff).

One FIFO queue holds every record the consumer has yet to read. Market-event
records, ``UnknownEvent`` and ``UndecodableFrame`` included, count toward
``queue_size``; status records never do and are never dropped. Putting never
waits, so the task that reads the socket never waits for the consumer.

No I/O and no timers: a read waits on an ``asyncio.Event`` until a record
arrives or the queue ends, and a read that is cancelled takes nothing, so the
next read returns the record it was waiting for.
"""

import asyncio
from collections import deque
from collections.abc import Iterable

from ._errors import ClientError
from ._records import Backlog, CaptureGap, ConnectionStateChange, TokenStateChange
from ._state import Record

AnyRecord = Record | Backlog
"""Any record the client delivers."""

_STATUS_RECORDS = (TokenStateChange, ConnectionStateChange, CaptureGap, Backlog)


class RecordQueue:
    """The records waiting for the consumer, and how the queue ends.

    It ends when the client fails, with ``fail``: the consumer reads every
    record already queued, then the failure is raised, then the iterator
    ends. Or it ends when the client shuts down, with ``close``: records
    still queued are discarded, and the iterator ends after any failure it
    has not raised yet.
    """

    def __init__(self, limit: int) -> None:
        self.limit = limit
        """``queue_size``: the market-event records waiting at which the next
        frame reaches the limit."""
        self._records: deque[AnyRecord] = deque()
        self._backlog = 0
        self._failure: ClientError | None = None
        self._ended = False
        self._changed = asyncio.Event()

    @property
    def backlog(self) -> int:
        """The market-event records waiting for the consumer."""
        return self._backlog

    def full(self) -> bool:
        """Whether a frame whose records include a market-event record would
        reach the limit: ``queue_size`` or more are waiting."""
        return self._backlog >= self.limit

    def put(self, records: Iterable[AnyRecord]) -> None:
        """Queue records, in order, without waiting. Once the queue has
        ended, nothing more is queued."""
        if self._ended:
            return
        for record in records:
            self._records.append(record)
            if not isinstance(record, _STATUS_RECORDS):
                self._backlog += 1
            self._changed.set()

    def fail(self, failure: ClientError) -> None:
        """End the queue with ``failure``, raised once the consumer has read
        the records already queued."""
        if self._ended:
            return
        self._failure = failure
        self._ended = True
        self._changed.set()

    def close(self) -> None:
        """The client has shut down: discard the records still queued."""
        self._records.clear()
        self._backlog = 0
        self._ended = True
        self._changed.set()

    def take_failure(self) -> ClientError | None:
        """The failure the queue has not raised yet, if any, which the caller
        then raises instead, so that it is raised once."""
        failure, self._failure = self._failure, None
        return failure

    async def get(self) -> AnyRecord:
        """The next record. Raises the failure the queue ended with, once,
        and then ``StopAsyncIteration``."""
        while not self._records and not self._ended:
            self._changed.clear()
            await self._changed.wait()
        if self._records:
            record = self._records.popleft()
            if not isinstance(record, _STATUS_RECORDS):
                self._backlog -= 1
            return record
        failure = self.take_failure()
        if failure is not None:
            raise failure
        raise StopAsyncIteration


class RecordIterator:
    """``client.records()``: every record, in the order the client produced
    them."""

    def __init__(self, queue: RecordQueue) -> None:
        self._queue = queue

    def __aiter__(self) -> "RecordIterator":
        return self

    async def __anext__(self) -> AnyRecord:
        return await self._queue.get()
