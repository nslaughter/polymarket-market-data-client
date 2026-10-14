"""The consumer handoff's queue, driven directly (D3; spec/client.md,
Consumer handoff and Record order, rule 5): the levels its fractions give,
where ``Backlog`` records go, the status records it counts, and the wait for
the consumer.
"""

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from polymarket_market_data import (
    Backlog,
    ClientError,
    ConnectionState,
    ConnectionStateChange,
    TokenState,
    TokenStateChange,
    UnknownEvent,
)
from polymarket_market_data._queue import AnyRecord, RecordQueue

AT = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)


def queue(limit: int, warning: float = 0.5, resume: float = 0.1) -> RecordQueue:
    return RecordQueue(
        limit, backlog_warning=warning, resume_below=resume, clock=lambda: AT
    )


def event(n: int) -> UnknownEvent:
    """A market-event record."""
    return UnknownEvent(
        event_type="something_new",
        payload={"n": n},
        raw=f'{{"n":{n}}}',
        received_at=AT,
        connection=1,
        frame=n,
        index=0,
    )


def status(n: int) -> TokenStateChange:
    """A status record."""
    return TokenStateChange(
        token_id=str(n),
        market="m",
        state=TokenState.READY,
        previous=TokenState.SYNCHRONIZING,
        reason="book",
        at=AT,
        connection=1,
        last_confirmed_at=None,
        winning_asset_id=None,
    )


def backlog(queued: int, limit: int, rising: bool) -> Backlog:
    return Backlog(queued=queued, limit=limit, rising=rising, at=AT)


async def take(records: RecordQueue, count: int) -> list[AnyRecord]:
    return [await records.get() for _ in range(count)]


def drain(records: RecordQueue) -> list[AnyRecord]:
    """Every record waiting, read as the consumer reads them."""

    async def main() -> list[AnyRecord]:
        read = []
        while records.backlog or records.status:
            read.append(await records.get())
        return read

    return asyncio.run(main())


@pytest.mark.parametrize(
    ("limit", "warning", "resume", "levels"),
    [
        (10000, 0.5, 0.1, (5000, 1000)),
        (4, 0.75, 0.25, (3, 1)),
        (1, 1.0, 0.5, (1, 0)),
        (3, 0.5, 0.1, (2, 0)),
        # In floats, 0.7 × 10 is a little over 7 and 0.3 × 10 a little over
        # 3; the fractions as written give 7 and 3.
        (10, 0.7, 0.3, (7, 3)),
        (10, 0.35, 0.15, (4, 1)),
    ],
)
def test_the_levels_round_the_warning_up_and_the_resume_level_down(
    limit: int, warning: float, resume: float, levels: tuple[int, int]
) -> None:
    records = queue(limit, warning, resume)
    assert (records.warning, records.resume) == levels


def test_rising_follows_the_records_its_event_caused() -> None:
    records = queue(10, warning=0.1)  # the level is 1
    records.put([status(0), event(1), status(1), status(2), event(2), status(3)])
    assert drain(records) == [
        status(0),
        event(1),
        status(1),
        status(2),
        backlog(1, 10, rising=True),
        event(2),
        status(3),
        # event(1)'s read left 1 waiting; event(2)'s brought it below.
        backlog(0, 10, rising=False),
    ]


def test_rising_ends_the_records_when_the_last_event_reaches_the_level() -> None:
    records = queue(10, warning=0.2)  # the level is 2
    records.put([event(1), status(1), event(2), status(2)])
    assert drain(records)[:5] == [
        event(1),
        status(1),
        event(2),
        status(2),
        backlog(2, 10, rising=True),
    ]


def test_only_one_rising_record_is_outstanding_at_a_time() -> None:
    async def main() -> None:
        records = queue(10, warning=0.2)  # the level is 2
        records.put([event(1), event(2)])
        records.put([event(3)])
        records.put([event(4)])
        assert await take(records, 3) == [
            event(1),
            event(2),
            backlog(2, 10, rising=True),
        ]
        # Still at 2 or more: no record yet.
        assert records.status == 0
        await records.get()
        assert records.backlog == 1
        # Below the level, and the next rising record may come.
        records.put([event(5)])
        assert await take(records, 4) == [
            event(4),
            backlog(1, 10, rising=False),
            event(5),
            backlog(2, 10, rising=True),
        ]

    asyncio.run(main())


def test_the_falling_record_is_appended_after_the_records_waiting() -> None:
    async def main() -> None:
        records = queue(4, warning=0.75)  # the level is 3
        records.put([event(1), event(2), event(3), status(1)])
        records.put([status(2)])
        assert await take(records, 1) == [event(1)]
        assert await take(records, 5) == [
            event(2),
            event(3),
            status(1),
            backlog(3, 4, rising=True),
            status(2),
        ]
        assert await take(records, 1) == [backlog(2, 4, rising=False)]

    asyncio.run(main())


def test_status_records_count_toward_holding_back_and_not_toward_the_limit() -> None:
    records = queue(2, warning=1.0, resume=0.5)
    records.put([status(1), event(1)])
    assert (records.backlog, records.status) == (1, 1)
    assert not records.full()
    assert not records.holds_back()
    records.put([status(2), event(2)])
    assert records.full()
    # Backlog records are status records too.
    assert records.status == 3
    assert records.holds_back()


def test_a_frame_is_queued_whole_past_the_limit() -> None:
    records = queue(1, warning=1.0, resume=0.5)
    records.put([event(1), event(2), event(3)])
    assert records.backlog == 3
    assert records.full()


def test_the_wait_for_the_consumer_ends_once_fewer_status_records_wait() -> None:
    async def main() -> None:
        records = queue(2, warning=1.0, resume=0.5)
        records.put([status(1), status(2), status(3)])
        waiting = asyncio.create_task(records.caught_up(drained=False))
        await asyncio.sleep(0)
        await records.get()
        await asyncio.sleep(0)
        assert not waiting.done()
        await records.get()
        await asyncio.sleep(0)
        assert waiting.done()

    asyncio.run(main())


def test_after_the_limit_the_wait_also_needs_the_backlog_drained() -> None:
    async def main() -> None:
        records = queue(4, warning=0.75, resume=0.25)  # resume at 1
        records.put([event(1), event(2), event(3), event(4)])
        waiting = asyncio.create_task(records.caught_up(drained=True))
        reads: list[AnyRecord] = []
        while not waiting.done():
            reads.append(await records.get())
            await asyncio.sleep(0)
        # Three reads bring the backlog to 1; the rising record went after
        # the third event.
        assert reads == [event(1), event(2), event(3)]
        assert records.backlog == 1
        # Without draining, the status records alone decide: none is waiting
        # beyond the limit.
        await records.caught_up(drained=False)

    asyncio.run(main())


def test_nothing_is_appended_once_the_queue_has_ended() -> None:
    async def main() -> None:
        records = queue(2, warning=0.5)  # the level is 1
        records.put([event(1)])
        records.fail(ClientError("failed"))
        records.put([event(2)])
        assert await take(records, 2) == [event(1), backlog(1, 2, rising=True)]
        # The read that brought the backlog below the level added nothing:
        # the failure follows the records already queued.
        with pytest.raises(ClientError):
            await records.get()
        with pytest.raises(StopAsyncIteration):
            await records.get()

    asyncio.run(main())


def test_closing_discards_every_record() -> None:
    records = queue(2)
    records.put([status(1), event(1), event(2)])
    records.close()
    assert (records.backlog, records.status) == (0, 0)
    assert not records.holds_back()


def test_connection_and_backlog_records_are_status_records() -> None:
    records = queue(5)
    connecting = ConnectionStateChange(
        state=ConnectionState.CONNECTING,
        at=AT,
        connection=None,
        attempt=1,
        reason=None,
        detail=None,
        close_code=None,
        close_reason=None,
        retry_in=None,
        last_confirmed_at=None,
    )
    statuses: Sequence[AnyRecord] = [connecting, backlog(0, 5, rising=False)]
    records.put(statuses)
    assert (records.backlog, records.status) == (0, 2)
