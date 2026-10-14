"""The handoff of records to the consumer (spec/client.md, Consumer handoff;
D3).

One FIFO queue holds every record the consumer has yet to read. Market-event
records, ``UnknownEvent`` and ``UndecodableFrame`` included, count toward
``queue_size``; status records never do and are never dropped. Putting never
waits, so the task that reads the socket never waits for the consumer. The
queue says when a frame would reach the limit, and how many status records
are waiting; the connection decides what to do about either.

The queue adds its own ``Backlog`` records, since only it sees the consumer's
reading: one with ``rising=True`` where the backlog reached the warning
level, and one with ``rising=False`` when a read brings it back below.

No I/O and no timers: a read waits on an ``asyncio.Event`` until a record
arrives or the queue ends, and a read that is cancelled takes nothing, so the
next read returns the record it was waiting for. The connection waits for
the consumer the same way.
"""

import asyncio
import math
from collections import deque
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from decimal import Decimal

from ._errors import ClientError
from ._records import Backlog, CaptureGap, ConnectionStateChange, TokenStateChange
from ._state import Record

AnyRecord = Record | Backlog
"""Any record the client delivers."""

_STATUS_RECORDS = (TokenStateChange, ConnectionStateChange, CaptureGap, Backlog)


def _wall_clock() -> datetime:
    return datetime.now(UTC)


def _of(fraction: float, limit: int) -> Decimal:
    """``fraction × limit``, with the fraction as it was written: in floats,
    ``0.7 × 10`` is a little over 7, and would round up to 8."""
    return Decimal(repr(fraction)) * limit


class RecordQueue:
    """The records waiting for the consumer, and how the queue ends.

    It ends when the client fails, with ``fail``: the consumer reads every
    record already queued, then the failure is raised, then the iterator
    ends. Or it ends when the client shuts down, with ``close``: records
    still queued are discarded, and the iterator ends after any failure it
    has not raised yet. Once it has ended, nothing more is queued, its own
    ``Backlog`` records included.
    """

    def __init__(
        self,
        limit: int,
        *,
        backlog_warning: float,
        resume_below: float,
        clock: Callable[[], datetime] = _wall_clock,
    ) -> None:
        self.limit = limit
        """``queue_size``: the market-event records waiting at which the next
        frame reaches the limit, and the status records waiting at which the
        client starts no connection attempt."""
        self.warning = math.ceil(_of(backlog_warning, limit))
        """The backlog at which ``Backlog`` records are emitted:
        ``backlog_warning × queue_size``, rounded up."""
        self.resume = math.floor(_of(resume_below, limit))
        """The backlog the consumer's reading must bring the count down to
        before the client reconnects after ``consumer_overflow``:
        ``resume_below × queue_size``, rounded down."""
        self._clock = clock
        self._records: deque[AnyRecord] = deque()
        self._backlog = 0
        self._status = 0
        # Whether a Backlog record with rising=True has been queued and not
        # yet followed by one with rising=False: only one is outstanding at a
        # time.
        self._rising = False
        self._failure: ClientError | None = None
        self._ended = False
        self._changed = asyncio.Event()
        self._taken = asyncio.Event()

    @property
    def backlog(self) -> int:
        """The market-event records waiting for the consumer."""
        return self._backlog

    @property
    def status(self) -> int:
        """The status records waiting for the consumer."""
        return self._status

    def full(self) -> bool:
        """Whether a frame whose records include a market-event record would
        reach the limit: ``queue_size`` or more are waiting."""
        return self._backlog >= self.limit

    def holds_back(self, adding: int = 0) -> bool:
        """Whether ``queue_size`` or more status records are waiting, or will
        be once ``adding`` more are queued, so that the client starts no
        connection attempt (spec/client.md, Reconnecting)."""
        return self._status + adding >= self.limit

    def put(self, records: Iterable[AnyRecord]) -> None:
        """Queue records, in order, without waiting: the records one cause
        produced, such as a frame, whole.

        A market-event record that brings the backlog to the warning level
        is followed by every status record after it, which its event caused,
        and then by a ``Backlog`` record with ``rising=True`` (spec/client.md,
        Record order, rule 5). Within one cause, every status record comes
        after the event that caused it (rule 1), so the next market-event
        record, or the end of the records, ends that event's."""
        reached: int | None = None
        for record in records:
            if not isinstance(record, _STATUS_RECORDS) and reached is not None:
                self._warn(reached)
                reached = None
            self._append(record)
            if (
                not isinstance(record, _STATUS_RECORDS)
                and not self._rising
                and self._backlog >= self.warning
            ):
                self._rising = True
                reached = self._backlog
        if reached is not None:
            self._warn(reached)

    def _warn(self, queued: int) -> None:
        self._append(
            Backlog(queued=queued, limit=self.limit, rising=True, at=self._clock())
        )

    def _append(self, record: AnyRecord) -> None:
        if self._ended:
            return
        self._records.append(record)
        if isinstance(record, _STATUS_RECORDS):
            self._status += 1
        else:
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
        self._status = 0
        self._ended = True
        self._changed.set()

    def take_failure(self) -> ClientError | None:
        """The failure the queue has not raised yet, if any, which the caller
        then raises instead, so that it is raised once."""
        failure, self._failure = self._failure, None
        return failure

    async def get(self) -> AnyRecord:
        """The next record. Raises the failure the queue ended with, once,
        and then ``StopAsyncIteration``.

        A read that brings the backlog below the warning level, while a
        ``Backlog`` record with ``rising=True`` is outstanding, appends one
        with ``rising=False``. It does so before the client can resume on
        the same read, so it comes before that ``connecting`` record
        (spec/client.md, Consumer handoff)."""
        while not self._records and not self._ended:
            self._changed.clear()
            await self._changed.wait()
        if self._records:
            record = self._records.popleft()
            if isinstance(record, _STATUS_RECORDS):
                self._status -= 1
            else:
                self._backlog -= 1
                if self._rising and self._backlog < self.warning:
                    self._rising = False
                    self._append(
                        Backlog(
                            queued=self._backlog,
                            limit=self.limit,
                            rising=False,
                            at=self._clock(),
                        )
                    )
            self._taken.set()
            return record
        failure = self.take_failure()
        if failure is not None:
            raise failure
        raise StopAsyncIteration

    async def caught_up(self, *, drained: bool) -> None:
        """Wait until the consumer's reading has left fewer than
        ``queue_size`` status records waiting, and, with ``drained``, has
        brought the backlog down to ``resume`` or lower, in whichever order
        those happen (spec/client.md, Reconnecting)."""
        while self.holds_back() or (drained and self._backlog > self.resume):
            self._taken.clear()
            await self._taken.wait()


class RecordIterator:
    """``client.records()``: every record, in the order the client produced
    them."""

    def __init__(self, queue: RecordQueue) -> None:
        self._queue = queue

    def __aiter__(self) -> "RecordIterator":
        return self

    async def __anext__(self) -> AnyRecord:
        return await self._queue.get()
