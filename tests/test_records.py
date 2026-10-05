"""Records and the other public data types: frozen dataclasses with slots
that read back equal through TypeAdapter (D8)."""

import dataclasses
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, TypedDict

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from polymarket_market_data import (
    Backlog,
    BestBidAskEvent,
    BookEvent,
    BookParameters,
    CaptureGap,
    ClientStats,
    ConnectionState,
    ConnectionStateChange,
    LastTradePriceEvent,
    Level,
    Market,
    MarketInfo,
    MarketResolvedEvent,
    NewMarketEvent,
    PriceChange,
    PriceChangeEvent,
    Side,
    TickSizeChangeEvent,
    TokenState,
    TokenStateChange,
    UndecodableFrame,
    UnknownEvent,
)

# Synthetic market A and its tokens, from spec/conformance.md.
A = "0x00000000000000000000000000000000000000000000000000000000000000a1"
A1 = "10000000000000000000000000000000000000000000000000000000000000000000000000011"
A2 = "10000000000000000000000000000000000000000000000000000000000000000000000000012"
T0 = 1791200000000
AT = datetime(2026, 10, 5, 12, 0, 0, 123456, tzinfo=UTC)
A1_HASH = "b17e93a1f6202e13d8e0dd3aeb958b4a882dca3c"


class Common(TypedDict):
    market: str
    source_timestamp_ms: int
    received_at: datetime
    connection: int
    frame: int
    index: int
    repeat: bool
    raw: str | None


def common(**changes: Any) -> Common:
    fields = Common(
        market=A,
        source_timestamp_ms=T0,
        received_at=AT,
        connection=1,
        frame=1,
        index=0,
        repeat=False,
        raw=None,
    )
    fields.update(changes)  # type: ignore[typeddict-item]
    return fields


def level(price: str, size: str) -> Level:
    return Level(Decimal(price), Decimal(size))


OPENING_BOOK = BookEvent(
    event_type="book",
    **common(source_timestamp_ms=T0 - 30000, raw='[{"event_type":"book"}]'),
    asset_id=A1,
    hash=A1_HASH,
    bids=(level("0.47", "250"), level("0.48", "100")),
    asks=(level("0.53", "300"), level("0.52", "120")),
    tick_size=Decimal("0.01"),
    last_trade_price=Decimal("0.500"),
    opening=True,
    held_book_matched=None,
)

SAMPLES: list[object] = [
    level("0.48", "100"),
    PriceChange(
        asset_id=A1,
        side=Side.SELL,
        price=Decimal("0.52"),
        size=Decimal("0"),
        hash=A1_HASH,
        best_bid=None,
        best_ask=Decimal("1"),
        applied=True,
        before_book=False,
    ),
    OPENING_BOOK,
    BookEvent(
        event_type="book",
        **common(connection=2, frame=7),
        asset_id=A1,
        hash="0" * 40,
        bids=(),
        asks=(level("0.53", "300"),),
        tick_size=None,
        last_trade_price=None,
        opening=False,
        held_book_matched=False,
    ),
    PriceChangeEvent(
        event_type="price_change",
        **common(frame=2, repeat=True),
        changes=(
            PriceChange(
                asset_id=A1,
                side=Side.BUY,
                price=Decimal("0.49"),
                size=Decimal("10"),
                hash=A1_HASH,
                best_bid=Decimal("0.49"),
                best_ask=Decimal("0.52"),
                applied=True,
                before_book=False,
            ),
            PriceChange(
                asset_id=A2,
                side=Side.SELL,
                price=Decimal("0.53"),
                size=Decimal("0"),
                hash="0" * 40,
                best_bid=None,
                best_ask=None,
                applied=False,
                before_book=True,
            ),
        ),
    ),
    BestBidAskEvent(
        event_type="best_bid_ask",
        **common(frame=3),
        asset_id=A1,
        best_bid=Decimal("0.48"),
        best_ask=Decimal("0.52"),
        spread=Decimal("0.04"),
    ),
    BestBidAskEvent(
        event_type="best_bid_ask",
        **common(frame=3, index=1),
        asset_id=A2,
        best_bid=Decimal("0.48"),
        best_ask=Decimal("0.52"),
        spread=None,
    ),
    LastTradePriceEvent(
        event_type="last_trade_price",
        **common(frame=4),
        asset_id=A1,
        price=Decimal("0.51"),
        size=Decimal("10"),
        side=Side.BUY,
        fee_rate_bps=Decimal("0"),
        transaction_hash="0x" + "0" * 63 + "1",
    ),
    TickSizeChangeEvent(
        event_type="tick_size_change",
        **common(frame=5),
        asset_id=A1,
        old_tick_size=Decimal("0.01"),
        new_tick_size=Decimal("0.001"),
    ),
    MarketResolvedEvent(
        event_type="market_resolved",
        **common(frame=6),
        id="9000001",
        assets_ids=(A1, A2),
        winning_asset_id=A2,
        winning_outcome="No",
        tags=(),
    ),
    NewMarketEvent(
        event_type="new_market",
        **common(market="", frame=8),
        id="9100001",
        condition_id=None,
        slug="synthetic-new-1",
        question="Synthetic new market 1?",
        assets_ids=(),
        payload={
            "id": "9100001",
            "slug": "synthetic-new-1",
            "game_start_time": "2026-10-11 14:00:00+00",
            "event_message": None,
            "active": True,
            "tags": [],
        },
    ),
    UnknownEvent(
        event_type="some_new_event",
        payload={"event_type": "some_new_event", "market": A, "count": 3},
        raw='{"event_type":"some_new_event","market":"' + A + '","count":3}',
        received_at=AT,
        connection=1,
        frame=9,
        index=0,
    ),
    UnknownEvent(
        event_type=None,
        payload={},
        raw="[{}]",
        received_at=AT,
        connection=1,
        frame=10,
        index=0,
    ),
    UndecodableFrame(
        reason="binary",
        event_type=None,
        error="binary frame",
        raw="00ff",
        received_at=AT,
        connection=1,
        frame=11,
        index=None,
        affected=(A1, A2),
    ),
    UndecodableFrame(
        reason="invalid_event",
        event_type="price_change",
        error="price_changes.1.price: Input should be a finite number",
        raw="[{},{}]",
        received_at=AT,
        connection=1,
        frame=12,
        index=1,
        affected=(),
    ),
    TokenStateChange(
        token_id=A1,
        market=A,
        state=TokenState.READY,
        previous=TokenState.SYNCHRONIZING,
        reason="book",
        at=AT,
        connection=1,
        last_confirmed_at=None,
        winning_asset_id=None,
    ),
    TokenStateChange(
        token_id=A1,
        market=A,
        state=TokenState.UNCERTAIN,
        previous=TokenState.READY,
        reason="interrupted",
        at=AT,
        connection=1,
        last_confirmed_at=AT - timedelta(seconds=1),
        winning_asset_id=None,
    ),
    TokenStateChange(
        token_id=A2,
        market=A,
        state=TokenState.SETTLED,
        previous=None,
        reason="market_resolved",
        at=AT,
        connection=None,
        last_confirmed_at=None,
        winning_asset_id=A2,
    ),
    ConnectionStateChange(
        state=ConnectionState.INTERRUPTED,
        at=AT,
        connection=1,
        attempt=None,
        reason="close_frame",
        detail=None,
        close_code=1013,
        close_reason="slow consumer: send buffer full",
        retry_in=None,
        last_confirmed_at=AT - timedelta(seconds=1),
    ),
    ConnectionStateChange(
        state=ConnectionState.RECOVERING,
        at=AT,
        connection=None,
        attempt=2,
        reason="backoff",
        detail="HTTP 503",
        close_code=None,
        close_reason=None,
        retry_in=0.2,
        last_confirmed_at=None,
    ),
    CaptureGap(
        token_id=A1,
        market=A,
        cause="dropped",
        close_code=None,
        close_reason=None,
        last_confirmed_at=AT - timedelta(seconds=2),
        detected_at=AT - timedelta(seconds=1),
        resumed_at=AT,
        end="book",
        connection_before=1,
        connection_after=2,
        held_book_matched=True,
        at=AT,
    ),
    CaptureGap(
        token_id=A2,
        market=A,
        cause="close_frame",
        close_code=1011,
        close_reason="going away",
        last_confirmed_at=AT - timedelta(seconds=2),
        detected_at=AT - timedelta(seconds=1),
        resumed_at=None,
        end="recovery_failed",
        connection_before=1,
        connection_after=None,
        held_book_matched=None,
        at=AT,
    ),
    Backlog(queued=500, limit=1000, rising=True, at=AT),
    Market(condition_id=A, token_ids=(A1, A2), slug="synthetic-a"),
    Market(condition_id=A, token_ids=(A1, A2), slug=None),
    ClientStats(
        frames=12,
        frames_after_interruption=1,
        events={"book": 2, "price_change": 1},
        repeats=1,
        unknown=2,
        undecodable={"binary": 1, "invalid_event": 1},
        new_market_dropped=0,
        discarded_outside=0,
        pongs=3,
        pongs_unsolicited=0,
        pong_delay_last=0.14,
        pong_delay_max=1.7,
        connections=2,
        interruptions={"dropped": 1},
        lookups=0,
        lookup_failures=0,
        rest_book_not_found=0,
        hash_verified=0,
        hash_retried=0,
        hash_failed=0,
    ),
    MarketInfo(
        condition_id=A,
        slug="synthetic-a",
        question="Synthetic market A?",
        token_ids=(A1, A2),
        outcomes=("Yes", "No"),
        closed=True,
        end_date=AT,
        winning_asset_id=A2,
        resolution_status="resolved",
    ),
    BookParameters(min_order_size=Decimal("5"), neg_risk=False),
]

RECORD_TYPES: set[type] = {
    BookEvent,
    PriceChangeEvent,
    BestBidAskEvent,
    LastTradePriceEvent,
    TickSizeChangeEvent,
    MarketResolvedEvent,
    NewMarketEvent,
    UnknownEvent,
    UndecodableFrame,
    TokenStateChange,
    ConnectionStateChange,
    CaptureGap,
    Backlog,
}
DATA_TYPES: set[type] = RECORD_TYPES | {
    Level,
    PriceChange,
    Market,
    ClientStats,
    MarketInfo,
    BookParameters,
}


def type_name(cls: type) -> str:
    return cls.__name__


def sample_id(sample: object) -> str:
    return type_name(type(sample))


SORTED_TYPES = sorted(DATA_TYPES, key=type_name)


def round_trip[T](value: T) -> T:
    adapter: TypeAdapter[T] = TypeAdapter(type(value))
    return adapter.validate_json(adapter.dump_json(value))


def assert_same_types(original: object, loaded: object) -> None:
    """Check that the value read back has the original's types throughout,
    so that no Decimal came back as a float or a string."""
    assert type(loaded) is type(original)
    if dataclasses.is_dataclass(original):
        for field in dataclasses.fields(original):
            assert_same_types(
                getattr(original, field.name), getattr(loaded, field.name)
            )
    elif isinstance(original, tuple | list):
        assert isinstance(loaded, tuple | list)
        for a, b in zip(original, loaded, strict=True):
            assert_same_types(a, b)
    elif isinstance(original, dict):
        assert isinstance(loaded, dict)
        for key in original:
            assert_same_types(original[key], loaded[key])


def test_samples_cover_every_type() -> None:
    assert {type(sample) for sample in SAMPLES} == DATA_TYPES


@pytest.mark.parametrize("sample", SAMPLES, ids=sample_id)
def test_is_frozen_dataclass_with_slots(sample: object) -> None:
    assert dataclasses.is_dataclass(sample)
    assert not isinstance(sample, BaseModel)
    assert not hasattr(sample, "__dict__")
    for field in dataclasses.fields(sample):
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(sample, field.name, None)


@pytest.mark.parametrize("cls", SORTED_TYPES, ids=type_name)
def test_takes_positional_match_patterns(cls: type) -> None:
    assert vars(cls)["__match_args__"] == tuple(f.name for f in dataclasses.fields(cls))


def test_positional_match() -> None:
    match OPENING_BOOK.bids[0]:
        case Level(price, size):
            assert (price, size) == (Decimal("0.47"), Decimal("250"))
        case _:
            pytest.fail("Level did not match positionally")
    match Market(A, (A1, A2), "synthetic-a"):
        case Market(condition_id, (first, _), slug):
            assert (condition_id, first, slug) == (A, A1, "synthetic-a")
        case _:
            pytest.fail("Market did not match positionally")


@pytest.mark.parametrize("sample", SAMPLES, ids=sample_id)
def test_reads_back_equal_through_type_adapter(sample: object) -> None:
    loaded = round_trip(sample)
    assert loaded == sample
    assert_same_types(sample, loaded)


@pytest.mark.parametrize("cls", SORTED_TYPES, ids=type_name)
def test_has_json_schema(cls: type) -> None:
    schema = TypeAdapter(cls).json_schema()
    assert schema["type"] == "object"
    assert set(schema["required"]) <= {f.name for f in dataclasses.fields(cls)}


def test_json_form() -> None:
    written = json.loads(TypeAdapter(BookEvent).dump_json(OPENING_BOOK))
    assert written["bids"] == [
        {"price": "0.47", "size": "250"},
        {"price": "0.48", "size": "100"},
    ]
    assert written["last_trade_price"] == "0.500"
    assert written["received_at"] == "2026-10-05T12:00:00.123456Z"
    assert written["held_book_matched"] is None

    change = TokenStateChange(
        token_id=A1,
        market=A,
        state=TokenState.READY,
        previous=TokenState.SYNCHRONIZING,
        reason="book",
        at=AT,
        connection=1,
        last_confirmed_at=None,
        winning_asset_id=None,
    )
    written = json.loads(TypeAdapter(TokenStateChange).dump_json(change))
    assert written["state"] == "ready"
    assert written["previous"] == "synchronizing"


def test_decimals_keep_their_digits() -> None:
    long = Decimal("0.12345678901234567890123456789")
    huge = Decimal("1E+400")
    book = dataclasses.replace(OPENING_BOOK, bids=(Level(long, huge),))
    loaded = round_trip(book)
    assert loaded == book
    assert str(loaded.bids[0].price) == "0.12345678901234567890123456789"
    assert str(loaded.bids[0].size) == "1E+400"
    assert isinstance(loaded.bids[0].price, Decimal)


def test_best_bid_ask_best_prices_are_never_none() -> None:
    # Decoding requires both best prices; only spread is optional.
    adapter = TypeAdapter(BestBidAskEvent)
    record = next(s for s in SAMPLES if isinstance(s, BestBidAskEvent))
    for field in ("best_bid", "best_ask"):
        written = json.loads(adapter.dump_json(record))
        written[field] = None
        with pytest.raises(ValidationError):
            adapter.validate_json(json.dumps(written))
    written = json.loads(adapter.dump_json(record))
    written["spread"] = None
    assert adapter.validate_json(json.dumps(written)).spread is None


def test_binary_undecodable_frame_reads_back_equal() -> None:
    frame = UndecodableFrame(
        reason="binary",
        event_type=None,
        error="binary frame",
        raw=bytes([0x00, 0xFF]).hex(),
        received_at=AT,
        connection=1,
        frame=3,
        index=None,
        affected=(A1, A2),
    )
    assert frame.raw == "00ff"
    assert round_trip(frame) == frame


def test_new_market_payload_decimals_read_back_as_strings() -> None:
    record = NewMarketEvent(
        event_type="new_market",
        **common(market="0x" + "0" * 61 + "f01"),
        id="9100001",
        condition_id="0x" + "0" * 61 + "f01",
        slug="synthetic-new-1",
        question="Synthetic new market 1?",
        assets_ids=("2" + "0" * 74 + "11", "2" + "0" * 74 + "12"),
        payload={
            "id": "9100001",
            "order_price_min_tick_size": Decimal("0.01"),
            "game_start_time": "2026-10-11 14:00:00+00",
            "fees": [Decimal("0"), {"maker": Decimal("1E+400")}],
            "active": True,
        },
    )
    loaded = round_trip(record)
    assert loaded == dataclasses.replace(
        record,
        payload={
            "id": "9100001",
            "order_price_min_tick_size": "0.01",
            "game_start_time": "2026-10-11 14:00:00+00",
            "fees": ["0", {"maker": "1E+400"}],
            "active": True,
        },
    )
    assert type(loaded.payload) is dict


def test_enums() -> None:
    assert [state.value for state in TokenState] == [
        "synchronizing",
        "ready",
        "uncertain",
        "settled",
        "removed",
    ]
    assert [state.value for state in ConnectionState] == [
        "connecting",
        "open",
        "subscribed",
        "interrupted",
        "recovering",
        "ended",
        "idle",
        "failed",
    ]
    # The conformance matcher names a member by its name in lower case.
    for state in [*TokenState, *ConnectionState]:
        assert state.name.lower() == state.value
    # Side's values are the source's.
    assert [side.value for side in Side] == ["BUY", "SELL"]
    assert Side("SELL") is Side.SELL


def test_market_info_defaults() -> None:
    info = MarketInfo(
        condition_id=A,
        slug="synthetic-a",
        question=None,
        token_ids=(A1, A2),
        outcomes=("Yes", "No"),
        closed=False,
        end_date=None,
    )
    assert info.winning_asset_id is None
    assert info.resolution_status is None
