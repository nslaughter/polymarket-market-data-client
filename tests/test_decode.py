"""Frame decoding, repeat detection, and the tokens an undecodable frame or
event may affect (spec/client.md, Decoding and Repeated messages).

Frames are written out as JSON in the shapes of spec/conformance.md's frame
notation, with its references: ${A} for a market's condition ID, ${A1} for
a token's ID, and ${t:<offset>} for T0 plus the offset.
"""

import dataclasses
import json
import re
from collections.abc import Hashable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import TypeAdapter

from polymarket_market_data import (
    BestBidAskEvent,
    BookEvent,
    LastTradePriceEvent,
    Level,
    Market,
    MarketResolvedEvent,
    NewMarketEvent,
    PriceChange,
    PriceChangeEvent,
    Side,
    TickSizeChangeEvent,
    UndecodableFrame,
    UnknownEvent,
)
from polymarket_market_data._decode import (
    EVERY_TOKEN,
    NO_IMPACT,
    Decoded,
    DecodedEvent,
    EventRecord,
    Impact,
    RepeatDetector,
    Undecodable,
    content_key,
    decode_frame,
)

# Synthetic markets A and B, from spec/conformance.md.
A = "0x00000000000000000000000000000000000000000000000000000000000000a1"
B = "0x00000000000000000000000000000000000000000000000000000000000000b2"
A1 = "10000000000000000000000000000000000000000000000000000000000000000000000000011"
A2 = "10000000000000000000000000000000000000000000000000000000000000000000000000012"
B1 = "10000000000000000000000000000000000000000000000000000000000000000000000000021"
B2 = "10000000000000000000000000000000000000000000000000000000000000000000000000022"
REFERENCES = {"A": A, "B": B, "A1": A1, "A2": A2, "B1": B1, "B2": B2}
MARKETS = (
    Market(A, (A1, A2), "synthetic-a"),
    Market(B, (B1, B2), "synthetic-b"),
)
T0 = 1791200000000
AT = datetime(2026, 10, 5, 12, 0, 0, 250000, tzinfo=UTC)
A1_HASH = "b17e93a1f6202e13d8e0dd3aeb958b4a882dca3c"
ZEROS = "0" * 40


def frame(text: str) -> str:
    """Replace the conformance notation's references in a frame's text."""
    text = re.sub(r"\$\{t:(-?\d+)\}", lambda m: str(T0 + int(m.group(1))), text)
    return re.sub(r"\$\{(\w+)\}", lambda m: REFERENCES[m.group(1)], text)


def decode(
    data: str | bytes, *, number: int = 2, keep_raw: bool = False
) -> list[Decoded]:
    return decode_frame(
        data, received_at=AT, connection=1, frame=number, keep_raw=keep_raw
    )


def decode_one(data: str | bytes, **options: Any) -> Decoded:
    items = decode(data, **options)
    assert len(items) == 1, items
    return items[0]


def event(data: str, **options: Any) -> EventRecord:
    item = decode_one(data, **options)
    assert isinstance(item, DecodedEvent), item
    return item.record


def undecodable(data: str | bytes, **options: Any) -> Undecodable:
    item = decode_one(data, **options)
    assert isinstance(item, Undecodable), item
    return item


def common(**changes: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "market": A,
        "received_at": AT,
        "connection": 1,
        "frame": 2,
        "index": 0,
        "repeat": False,
        "raw": None,
    }
    return fields | changes


# Frames in the conformance notation's shapes.

OPENING_A1 = frame(
    '{"market":"${A}","asset_id":"${A1}","timestamp":"${t:-30000}",'
    f'"hash":"{A1_HASH}",'
    '"bids":[{"price":"0.47","size":"250"},{"price":"0.48","size":"100"}],'
    '"asks":[{"price":"0.53","size":"300"},{"price":"0.52","size":"120"}],'
    '"tick_size":"0.01","event_type":"book","last_trade_price":"0.500"}'
)
OPENING_A2 = frame(
    '{"market":"${A}","asset_id":"${A2}","timestamp":"${t:-30000}",'
    '"hash":"84549e080ed1722e15d1891c5ee759b77760e125",'
    '"bids":[{"price":"0.47","size":"300"},{"price":"0.48","size":"120"}],'
    '"asks":[{"price":"0.53","size":"250"},{"price":"0.52","size":"100"}],'
    '"tick_size":"0.01","event_type":"book","last_trade_price":"0.500"}'
)
# The opening frame: an array of books, with the newline the excerpts keep.
OPENING = f"[{OPENING_A1},{OPENING_A2}]\n"
LATER_BOOK = frame(
    '{"market":"${A}","asset_id":"${A1}",'
    '"bids":[{"price":"0.47","size":"250"},{"price":"0.48","size":"100"}],'
    '"asks":[{"price":"0.53","size":"300"}],'
    f'"hash":"{ZEROS}","timestamp":"${{t:100}}","event_type":"book"}}'
)
PRICE_CHANGE = frame(
    '{"market":"${A}","price_changes":['
    '{"asset_id":"${A1}","price":"0.49","size":"50","side":"BUY",'
    f'"hash":"{A1_HASH}","best_bid":"0.49","best_ask":"0.52"}},'
    '{"asset_id":"${A2}","price":"0.51","size":"0","side":"SELL",'
    f'"hash":"{ZEROS}","best_bid":"0","best_ask":"1"}}],'
    '"timestamp":"${t:100}","event_type":"price_change"}'
)
BEST_BID_ASK = frame(
    '{"market":"${A}","asset_id":"${A1}","best_bid":"0.49","best_ask":"0.51",'
    '"spread":"0.02","timestamp":"${t:100}","event_type":"best_bid_ask"}'
)
LAST_TRADE_PRICE = frame(
    '{"market":"${A}","asset_id":"${A1}","price":"0.51","size":"10",'
    '"fee_rate_bps":"0","side":"BUY","timestamp":"${t:150}",'
    '"event_type":"last_trade_price",'
    '"transaction_hash":"0x0000000000000000000000000000000000000000000000000000000000000001"}'
)
TICK_SIZE_CHANGE = frame(
    '{"market":"${A}","asset_id":"${A1}","old_tick_size":"0.01",'
    '"new_tick_size":"0.001","timestamp":"${t:200}","event_type":"tick_size_change"}'
)
MARKET_RESOLVED = frame(
    '{"id":"9000001","market":"${A}","assets_ids":["${A1}","${A2}"],'
    '"winning_asset_id":"${A2}","winning_outcome":"No","event_message":null,'
    '"timestamp":"${t:300}","event_type":"market_resolved","tags":[]}'
)
NEW_MARKET_ID = "0x0000000000000000000000000000000000000000000000000000000000000f01"
NEW_MARKET_TOKENS = (
    "20000000000000000000000000000000000000000000000000000000000000000000000000011",
    "20000000000000000000000000000000000000000000000000000000000000000000000000012",
)
NEW_MARKET = (
    '{"id":"9100001","question":"Synthetic new market 1?",'
    f'"market":"{NEW_MARKET_ID}","slug":"synthetic-new-1",'
    '"description":"Synthetic.",'
    f'"assets_ids":["{NEW_MARKET_TOKENS[0]}","{NEW_MARKET_TOKENS[1]}"],'
    '"outcomes":["Yes","No"],"event_message":null,'
    f'"timestamp":"{T0 + 100}","event_type":"new_market","tags":[],'
    f'"condition_id":"{NEW_MARKET_ID}","active":true,'
    f'"clob_token_ids":["{NEW_MARKET_TOKENS[0]}","{NEW_MARKET_TOKENS[1]}"],'
    '"game_start_time":"2026-10-11 14:00:00+00",'
    '"order_price_min_tick_size":"0.01"}'
)
EVENT_FRAMES = [
    OPENING,
    LATER_BOOK,
    PRICE_CHANGE,
    BEST_BID_ASK,
    LAST_TRADE_PRICE,
    TICK_SIZE_CHANGE,
    MARKET_RESOLVED,
    NEW_MARKET,
]


def edit(text: str, change: Any) -> str:
    """Parse a frame, let ``change`` alter the object, and write it out
    compactly again."""
    value = json.loads(text, parse_float=Decimal)
    change(value)
    return json.dumps(value, separators=(",", ":"), default=str)


# Event types.


def test_opening_books() -> None:
    items = decode(OPENING, number=1)
    assert [type(item) for item in items] == [DecodedEvent, DecodedEvent]
    first, second = (item.record for item in items if isinstance(item, DecodedEvent))
    assert first == BookEvent(
        event_type="book",
        **common(frame=1, source_timestamp_ms=T0 - 30000),
        asset_id=A1,
        hash=A1_HASH,
        bids=(
            Level(Decimal("0.47"), Decimal("250")),
            Level(Decimal("0.48"), Decimal("100")),
        ),
        asks=(
            Level(Decimal("0.53"), Decimal("300")),
            Level(Decimal("0.52"), Decimal("120")),
        ),
        tick_size=Decimal("0.01"),
        last_trade_price=Decimal("0.500"),
        opening=True,
        held_book_matched=None,
    )
    assert isinstance(second, BookEvent)
    assert (second.asset_id, second.index, second.opening) == (A2, 1, True)


def test_later_book() -> None:
    assert event(LATER_BOOK, number=5) == BookEvent(
        event_type="book",
        **common(frame=5, source_timestamp_ms=T0 + 100),
        asset_id=A1,
        hash=ZEROS,
        bids=(
            Level(Decimal("0.47"), Decimal("250")),
            Level(Decimal("0.48"), Decimal("100")),
        ),
        asks=(Level(Decimal("0.53"), Decimal("300")),),
        tick_size=None,
        last_trade_price=None,
        opening=False,
        held_book_matched=None,
    )


def test_price_change() -> None:
    assert event(PRICE_CHANGE) == PriceChangeEvent(
        event_type="price_change",
        **common(source_timestamp_ms=T0 + 100),
        changes=(
            PriceChange(
                asset_id=A1,
                side=Side.BUY,
                price=Decimal("0.49"),
                size=Decimal("50"),
                hash=A1_HASH,
                best_bid=Decimal("0.49"),
                best_ask=Decimal("0.52"),
                applied=False,
                before_book=False,
            ),
            PriceChange(
                asset_id=A2,
                side=Side.SELL,
                price=Decimal("0.51"),
                size=Decimal("0"),
                hash=ZEROS,
                best_bid=Decimal("0"),
                best_ask=Decimal("1"),
                applied=False,
                before_book=False,
            ),
        ),
    )


def test_price_change_entry_without_best_prices() -> None:
    def drop_best_prices(value: dict[str, Any]) -> None:
        for entry in value["price_changes"]:
            del entry["best_bid"], entry["best_ask"]

    record = event(edit(PRICE_CHANGE, drop_best_prices))
    assert isinstance(record, PriceChangeEvent)
    assert [(c.best_bid, c.best_ask) for c in record.changes] == [(None, None)] * 2


def test_best_bid_ask() -> None:
    assert event(BEST_BID_ASK) == BestBidAskEvent(
        event_type="best_bid_ask",
        **common(source_timestamp_ms=T0 + 100),
        asset_id=A1,
        best_bid=Decimal("0.49"),
        best_ask=Decimal("0.51"),
        spread=Decimal("0.02"),
    )
    record = event(edit(BEST_BID_ASK, lambda v: v.pop("spread")))
    assert isinstance(record, BestBidAskEvent)
    assert record.spread is None


def test_last_trade_price() -> None:
    assert event(LAST_TRADE_PRICE) == LastTradePriceEvent(
        event_type="last_trade_price",
        **common(source_timestamp_ms=T0 + 150),
        asset_id=A1,
        price=Decimal("0.51"),
        size=Decimal("10"),
        side=Side.BUY,
        fee_rate_bps=Decimal("0"),
        transaction_hash="0x" + "0" * 63 + "1",
    )

    def drop_optional(value: dict[str, Any]) -> None:
        del value["fee_rate_bps"], value["transaction_hash"]

    record = event(edit(LAST_TRADE_PRICE, drop_optional))
    assert isinstance(record, LastTradePriceEvent)
    assert (record.fee_rate_bps, record.transaction_hash) == (None, None)


def test_tick_size_change() -> None:
    assert event(TICK_SIZE_CHANGE) == TickSizeChangeEvent(
        event_type="tick_size_change",
        **common(source_timestamp_ms=T0 + 200),
        asset_id=A1,
        old_tick_size=Decimal("0.01"),
        new_tick_size=Decimal("0.001"),
    )


def test_market_resolved() -> None:
    assert event(MARKET_RESOLVED) == MarketResolvedEvent(
        event_type="market_resolved",
        **common(source_timestamp_ms=T0 + 300),
        id="9000001",
        assets_ids=(A1, A2),
        winning_asset_id=A2,
        winning_outcome="No",
        tags=(),
    )

    def keep_required(value: dict[str, Any]) -> None:
        for name in ("id", "winning_outcome", "event_message", "tags"):
            del value[name]

    record = event(edit(MARKET_RESOLVED, keep_required))
    assert isinstance(record, MarketResolvedEvent)
    assert (record.id, record.winning_outcome, record.tags) == (None, None, ())


def test_new_market_with_string_game_start_time() -> None:
    record = event(NEW_MARKET)
    assert record == NewMarketEvent(
        event_type="new_market",
        **common(market=NEW_MARKET_ID, source_timestamp_ms=T0 + 100),
        id="9100001",
        condition_id=NEW_MARKET_ID,
        slug="synthetic-new-1",
        question="Synthetic new market 1?",
        assets_ids=NEW_MARKET_TOKENS,
        payload=json.loads(NEW_MARKET),
    )
    assert isinstance(record, NewMarketEvent)
    assert record.payload["game_start_time"] == "2026-10-11 14:00:00+00"
    assert type(record.payload) is dict


def test_new_market_with_only_its_required_fields() -> None:
    record = event(
        '{"id":"9100002","timestamp":"1791200000101","event_type":"new_market"}'
    )
    assert isinstance(record, NewMarketEvent)
    assert (record.market, record.condition_id, record.slug, record.question) == (
        "",
        None,
        None,
        None,
    )
    assert record.assets_ids == ()


def test_new_market_condition_id_falls_back_to_market() -> None:
    record = event(edit(NEW_MARKET, lambda v: v.pop("condition_id")))
    assert isinstance(record, NewMarketEvent)
    assert record.condition_id == NEW_MARKET_ID


def test_raw_is_kept_only_when_asked() -> None:
    for text in EVENT_FRAMES:
        for item in decode(text, keep_raw=True):
            assert isinstance(item, DecodedEvent)
            assert item.record.raw == text
        for item in decode(text):
            assert isinstance(item, DecodedEvent)
            assert item.record.raw is None


def add_unknown_member(value: Any) -> None:
    for item in value if isinstance(value, list) else [value]:
        item["something_new"] = {"nested": [1, "x"]}


def test_unknown_fields_are_ignored() -> None:
    for text in EVENT_FRAMES:
        items = decode(edit(text, add_unknown_member))
        assert all(isinstance(item, DecodedEvent) for item in items)


# Values.


def walk(value: object) -> list[object]:
    """Every value inside a record, recursively."""
    found: list[object] = [value]
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        for field in dataclasses.fields(value):
            found += walk(getattr(value, field.name))
    elif isinstance(value, tuple | list):
        for item in value:
            found += walk(item)
    elif isinstance(value, dict):
        for item in value.values():
            found += walk(item)
    return found


def test_decimals_keep_the_text_sent() -> None:
    record = event(edit(BEST_BID_ASK, lambda v: v.update(best_bid="0.480")))
    assert isinstance(record, BestBidAskEvent)
    assert str(record.best_bid) == "0.480"


def test_numbers_sent_as_json_numbers_keep_every_digit() -> None:
    text = frame(
        '{"market":"${A}","asset_id":"${A1}",'
        '"bids":[{"price":0.48,"size":100},'
        '{"price":0.12345678901234567890123456789,"size":1e400}],'
        f'"asks":[],"hash":"{ZEROS}","timestamp":"${{t:100}}","event_type":"book"}}'
    )
    record = event(text)
    assert isinstance(record, BookEvent)
    assert [(str(level.price), str(level.size)) for level in record.bids] == [
        ("0.48", "100"),
        ("0.12345678901234567890123456789", "1E+400"),
    ]


def test_no_value_is_a_float() -> None:
    numbers = frame(
        '{"market":"${A}","price_changes":[{"asset_id":"${A1}","price":0.49,'
        f'"size":50,"side":"BUY","hash":"{ZEROS}","best_bid":0.49,"best_ask":0.52}}],'
        '"timestamp":"${t:100}","event_type":"price_change","extra":1.5}'
    )
    for text in [*EVENT_FRAMES, numbers]:
        for item in decode(text):
            assert isinstance(item, DecodedEvent)
            assert not any(isinstance(value, float) for value in walk(item.record))


NON_FINITE = ['"NaN"', '"Infinity"', '"-Infinity"', "NaN", "Infinity", "-Infinity"]
# (frame, the member to replace in its first object, written as JSON text).
DECIMAL_FIELDS = [
    (LATER_BOOK, r'"bids":\[\{"price":"0.47"', '"bids":[{"price":{}'),
    (PRICE_CHANGE, r'"size":"50"', '"size":{}'),
    (PRICE_CHANGE, r'"best_bid":"0.49"', '"best_bid":{}'),
    (BEST_BID_ASK, r'"best_ask":"0.51"', '"best_ask":{}'),
    (BEST_BID_ASK, r'"spread":"0.02"', '"spread":{}'),
    (LAST_TRADE_PRICE, r'"price":"0.51"', '"price":{}'),
    (LAST_TRADE_PRICE, r'"fee_rate_bps":"0"', '"fee_rate_bps":{}'),
    (TICK_SIZE_CHANGE, r'"new_tick_size":"0.001"', '"new_tick_size":{}'),
]


@pytest.mark.parametrize("value", NON_FINITE)
@pytest.mark.parametrize(("text", "pattern", "replacement"), DECIMAL_FIELDS)
def test_non_finite_values_are_refused(
    text: str, pattern: str, replacement: str, value: str
) -> None:
    changed, count = re.subn(pattern, replacement.replace("{}", value), text, count=1)
    assert count == 1
    item = undecodable(changed)
    assert item.record.reason == "invalid_event"
    assert "finite" in item.record.error


@pytest.mark.parametrize("value", ['"1e400"', "1e400", '"-1E+400"'])
def test_large_finite_values_are_accepted(value: str) -> None:
    record = event(BEST_BID_ASK.replace('"spread":"0.02"', f'"spread":{value}'))
    assert isinstance(record, BestBidAskEvent)
    assert record.spread == Decimal(json.loads(value) if value[0] == '"' else value)
    assert isinstance(record.spread, Decimal)


@pytest.mark.parametrize("value", ['"1791200000100"', "1791200000100"])
def test_timestamps_are_integers(value: str) -> None:
    record = event(BEST_BID_ASK.replace(f'"{T0 + 100}"', value))
    assert record.source_timestamp_ms == 1791200000100
    assert type(record.source_timestamp_ms) is int


@pytest.mark.parametrize(
    "value",
    [
        '"12.5"',
        '"abc"',
        '""',
        '"-1"',
        '" 1"',
        '"\\u0661\\u0662"',
        "12.5",
        "1e3",
        "-1",
        "true",
        "null",
        "[]",
    ],
)
def test_timestamps_that_are_not_integers_are_refused(value: str) -> None:
    item = undecodable(BEST_BID_ASK.replace(f'"{T0 + 100}"', value))
    assert item.record.reason == "invalid_event"
    assert item.record.error.startswith("timestamp:")


@pytest.mark.parametrize("value", ['"buy"', '"Buy"', '""', "null", "1"])
@pytest.mark.parametrize("text", [PRICE_CHANGE, LAST_TRADE_PRICE])
def test_side_must_be_buy_or_sell(text: str, value: str) -> None:
    item = undecodable(text.replace('"side":"BUY"', f'"side":{value}', 1))
    assert item.record.reason == "invalid_event"


# Every required field of every type: (frame, path to the member to remove).
REQUIRED = [
    *(
        (LATER_BOOK, (name,))
        for name in ("market", "asset_id", "timestamp", "hash", "bids", "asks")
    ),
    (LATER_BOOK, ("bids", 0, "price")),
    (LATER_BOOK, ("asks", 0, "size")),
    *((PRICE_CHANGE, (name,)) for name in ("market", "timestamp", "price_changes")),
    *(
        (PRICE_CHANGE, ("price_changes", 0, name))
        for name in ("asset_id", "price", "size", "side", "hash")
    ),
    *(
        (BEST_BID_ASK, (name,))
        for name in ("market", "asset_id", "timestamp", "best_bid", "best_ask")
    ),
    *(
        (LAST_TRADE_PRICE, (name,))
        for name in ("market", "asset_id", "timestamp", "price", "size", "side")
    ),
    *(
        (TICK_SIZE_CHANGE, (name,))
        for name in (
            "market",
            "asset_id",
            "timestamp",
            "old_tick_size",
            "new_tick_size",
        )
    ),
    *(
        (MARKET_RESOLVED, (name,))
        for name in ("market", "timestamp", "assets_ids", "winning_asset_id")
    ),
    *((NEW_MARKET, (name,)) for name in ("id", "timestamp")),
]


def remove(path: tuple[str | int, ...]) -> Any:
    def change(value: Any) -> None:
        for part in path[:-1]:
            value = value[part]
        del value[path[-1]]

    return change


def set_to(path: tuple[str | int, ...], replacement: object) -> Any:
    def change(value: Any) -> None:
        for part in path[:-1]:
            value = value[part]
        value[path[-1]] = replacement

    return change


@pytest.mark.parametrize(("text", "path"), REQUIRED)
def test_a_missing_required_field_makes_the_event_undecodable(
    text: str, path: tuple[str | int, ...]
) -> None:
    item = undecodable(edit(text, remove(path)))
    assert item.record.reason == "invalid_event"
    assert item.record.event_type == json.loads(text)["event_type"]
    assert ".".join(map(str, path)) in item.record.error


@pytest.mark.parametrize(("text", "path"), REQUIRED)
def test_a_null_required_field_makes_the_event_undecodable(
    text: str, path: tuple[str | int, ...]
) -> None:
    assert undecodable(edit(text, set_to(path, None))).record.reason == "invalid_event"


@pytest.mark.parametrize(
    ("text", "path"),
    [
        (OPENING_A1, ("tick_size",)),
        (OPENING_A1, ("last_trade_price",)),
        (PRICE_CHANGE, ("price_changes", 0, "best_bid")),
        (BEST_BID_ASK, ("spread",)),
        (LAST_TRADE_PRICE, ("fee_rate_bps",)),
        (LAST_TRADE_PRICE, ("transaction_hash",)),
        (MARKET_RESOLVED, ("id",)),
        (MARKET_RESOLVED, ("winning_outcome",)),
        (MARKET_RESOLVED, ("tags",)),
        (NEW_MARKET, ("slug",)),
        (NEW_MARKET, ("assets_ids",)),
    ],
)
def test_an_optional_field_may_be_null(text: str, path: tuple[str | int, ...]) -> None:
    assert isinstance(decode_one(edit(text, set_to(path, None))), DecodedEvent)


@pytest.mark.parametrize(
    ("text", "path", "value"),
    [
        (LATER_BOOK, ("asset_id",), 11),
        (LATER_BOOK, ("bids",), {"price": "0.47", "size": "250"}),
        (LATER_BOOK, ("bids", 0), "0.47"),
        (PRICE_CHANGE, ("market",), ["x"]),
        (PRICE_CHANGE, ("price_changes", 0, "hash"), 0),
        (MARKET_RESOLVED, ("assets_ids",), [1, 2]),
        (MARKET_RESOLVED, ("tags",), "Crypto"),
        (NEW_MARKET, ("id",), 9100001),
    ],
)
def test_a_field_of_the_wrong_type_makes_the_event_undecodable(
    text: str, path: tuple[str | int, ...], value: object
) -> None:
    assert undecodable(edit(text, set_to(path, value))).record.reason == "invalid_event"


@pytest.mark.parametrize(
    ("name", "value", "field", "expected"),
    [
        ("slug", 7, "slug", None),
        ("question", ["Synthetic?"], "question", None),
        ("assets_ids", "token", "assets_ids", ()),
        ("assets_ids", [NEW_MARKET_TOKENS[0], 2], "assets_ids", ()),
        ("condition_id", 5, "condition_id", NEW_MARKET_ID),
        ("market", {"id": 1}, "market", ""),
    ],
)
def test_new_market_metadata_of_the_wrong_type_is_absent(
    name: str, value: object, field: str, expected: object
) -> None:
    # The record treats it as absent, and payload keeps it as sent
    # (spec/client.md, new_market). A condition_id falls back to market.
    record = event(edit(NEW_MARKET, set_to((name,), value)))
    assert isinstance(record, NewMarketEvent)
    assert getattr(record, field) == expected
    assert record.payload[name] == value
    assert record.id == "9100001"


def test_an_invalid_event_record() -> None:
    text = LATER_BOOK.replace('"price":"0.47"', '"price":"abc"')
    assert undecodable(text, number=4).record == UndecodableFrame(
        reason="invalid_event",
        event_type="book",
        error="bids.0.price: Input should be a valid decimal",
        raw=text,
        received_at=AT,
        connection=1,
        frame=4,
        index=None,
        affected=(),
    )
    in_array = undecodable(f"[{text}]").record
    assert in_array.index == 0


# Unknown events.


def test_unknown_event_type() -> None:
    text = frame(
        '{"market":"${A}","asset_id":"${A1}","timestamp":"${t:100}",'
        '"event_type":"something_new"}'
    )
    assert decode_one(text) == UnknownEvent(
        event_type="something_new",
        payload=json.loads(text),
        raw=text,
        received_at=AT,
        connection=1,
        frame=2,
        index=0,
    )


@pytest.mark.parametrize(
    "text",
    [
        '{"market":"x","note":"no event type"}',
        '{"event_type":5}',
        '{"event_type":null}',
        '{"event_type":["book"]}',
        '{"event_type":{"type":"book"}}',
    ],
)
def test_an_object_without_a_string_event_type_is_unknown(text: str) -> None:
    item = decode_one(text)
    assert isinstance(item, UnknownEvent)
    assert item.event_type is None
    assert item.payload == json.loads(text)


def test_items_of_an_array_frame_are_decoded_one_by_one() -> None:
    bad_book = LATER_BOOK.replace('"price":"0.47"', '"price":"abc"')
    text = f'[{LATER_BOOK},7,{{"event_type":"future"}},{bad_book}]'
    items = decode(text)
    assert [type(item) for item in items] == [
        DecodedEvent,
        Undecodable,
        UnknownEvent,
        Undecodable,
    ]
    first, not_object, unknown, invalid = items
    assert isinstance(first, DecodedEvent)
    assert first.record.index == 0
    assert isinstance(not_object, Undecodable)
    assert (not_object.record.reason, not_object.record.index) == ("not_object", 1)
    assert isinstance(unknown, UnknownEvent)
    assert unknown.index == 2
    assert isinstance(invalid, Undecodable)
    assert (invalid.record.reason, invalid.record.index) == ("invalid_event", 3)
    for item in (not_object, unknown, invalid):
        assert (item.record if isinstance(item, Undecodable) else item).raw == text


def test_an_empty_opening_frame_holds_nothing() -> None:
    assert decode("[]\n", number=1) == []


# Frames that cannot be decoded.


@pytest.mark.parametrize(
    "text",
    ["not json", "", "{", '[{"event_type":"book"},', '{"a":1}x', "PING", "1" * 5000],
)
def test_text_that_is_not_json(text: str) -> None:
    item = undecodable(text, number=3)
    assert item.record == UndecodableFrame(
        reason="invalid_json",
        event_type=None,
        error=item.record.error,
        raw=text,
        received_at=AT,
        connection=1,
        frame=3,
        index=None,
        affected=(),
    )
    assert item.record.error
    assert item.impact == EVERY_TOKEN


@pytest.mark.parametrize(
    "text",
    [
        '{"event_type":"future","n":1e9999999999999999999}',
        LATER_BOOK.replace('"price":"0.47"', '"price":-1e-9999999999999999999'),
    ],
)
def test_a_number_beyond_the_range_of_decimal_is_not_json(text: str) -> None:
    # parse_float cannot make a Decimal of it, so the frame cannot be parsed;
    # it is reported, never raised.
    item = undecodable(text)
    assert (item.record.reason, item.record.event_type, item.record.raw) == (
        "invalid_json",
        None,
        text,
    )
    assert item.record.error == "a number's exponent is beyond Decimal's range"
    assert item.impact == EVERY_TOKEN
    adapter = TypeAdapter(UndecodableFrame)
    assert adapter.validate_json(adapter.dump_json(item.record)) == item.record


def nested(pairs: int) -> str:
    """An array holding an object, ``pairs`` times over, around an empty
    array, as JSON text: ``2 * pairs + 1`` levels deep."""
    return '[{"a":' * pairs + "[]" + "}]" * pairs


def arrays(levels: int) -> str:
    """Arrays ``levels`` deep, as JSON text."""
    return "[" * levels + "]" * levels


def depth(text: str) -> int:
    """How deep arrays and objects nest in a frame, its outermost one
    counting as 1 (spec/client.md, Nesting depth)."""
    deepest, pending = 0, [(json.loads(text), 1)]
    while pending:
        value, level = pending.pop()
        if isinstance(value, dict | list):
            deepest = max(deepest, level)
            members = value.values() if isinstance(value, dict) else value
            pending.extend((member, level + 1) for member in members)
    return deepest


def test_json_nested_too_deeply_to_parse_is_not_json() -> None:
    # json.loads refuses nesting this deep on every supported Python; the
    # frame is reported, never raised.
    text = LATER_BOOK[:-1] + f',"x":{nested(500_000)}}}'
    item = undecodable(text)
    assert item.record.reason == "invalid_json"
    assert item.record.raw == text
    assert item.impact == EVERY_TOKEN


def with_member(text: str, levels: int) -> str:
    """The frame with an ignored member ``x`` of arrays ``levels`` deep in
    its first object."""
    return text.replace('"market":', f'"x":{arrays(levels)},"market":', 1)


def frames_at_depth(levels: int) -> dict[str, str]:
    """Frames nested ``levels`` deep, by where the depth comes from: a known
    event's ignored member, an unknown event's payload, a new_market's, and
    an array frame holding a book."""
    return {
        "book": with_member(LATER_BOOK, levels - 1),
        "price_change entry": PRICE_CHANGE.replace(
            '"side":"BUY",', f'"side":"BUY","x":{arrays(levels - 3)},', 1
        ),
        "unknown event": f'{{"event_type":"future","x":{arrays(levels - 1)}}}',
        "new_market": NEW_MARKET.replace(
            '"tags":[]', f'"tags":[],"x":{arrays(levels - 1)}'
        ),
        "array frame": f"[{with_member(LATER_BOOK, levels - 2)}]",
    }


# The most a frame may nest (spec/client.md, Nesting depth), and one more.
AT_THE_LIMIT = frames_at_depth(64)
PAST_THE_LIMIT = frames_at_depth(65)


@pytest.mark.parametrize("text", AT_THE_LIMIT.values(), ids=AT_THE_LIMIT.keys())
def test_a_frame_64_deep_decodes_and_reads_back(text: str) -> None:
    assert depth(text) == 64
    item = decode_one(text)
    assert not isinstance(item, Undecodable)
    record = item.record if isinstance(item, DecodedEvent) else item
    # The record holding the deepest payload a frame can carry reads back,
    # on every supported version of Pydantic.
    adapter: TypeAdapter[Any] = TypeAdapter(type(record))
    assert adapter.validate_json(adapter.dump_json(record)) == record


def test_a_known_event_64_deep_matches_the_plain_one() -> None:
    item = decode_one(AT_THE_LIMIT["book"])
    assert isinstance(item, DecodedEvent)
    assert item.record == event(LATER_BOOK)
    detector = RepeatDetector(1.0)
    assert detector.check(item.content, 0.0) is False
    assert detector.check(content(AT_THE_LIMIT["book"]), 0.5) is True
    assert item.content != content(LATER_BOOK)


@pytest.mark.parametrize("text", PAST_THE_LIMIT.values(), ids=PAST_THE_LIMIT.keys())
def test_a_frame_nested_more_than_64_deep_is_not_json(text: str) -> None:
    assert depth(text) == 65
    item = undecodable(text)
    assert item.record == UndecodableFrame(
        reason="invalid_json",
        event_type=None,
        error="JSON nested more than 64 deep",
        raw=text,
        received_at=AT,
        connection=1,
        frame=2,
        index=None,
        affected=(),
    )
    assert item.impact == EVERY_TOKEN


@pytest.mark.parametrize(
    ("text", "index"),
    [
        ("42", None),
        ('"text"', None),
        ("null", None),
        ("true", None),
        ("[7]", 0),
        ("[[]]", 0),
        ("[{},null]", 1),
    ],
)
def test_json_that_is_not_an_object(text: str, index: int | None) -> None:
    item = decode(text)[-1]
    assert isinstance(item, Undecodable)
    assert (item.record.reason, item.record.index, item.record.raw) == (
        "not_object",
        index,
        text,
    )
    assert item.record.event_type is None
    assert item.impact == EVERY_TOKEN


@pytest.mark.parametrize(("data", "raw"), [(b"\x00\xff", "00ff"), (b"", "")])
def test_binary_frame(data: bytes, raw: str) -> None:
    item = undecodable(data)
    assert item.record == UndecodableFrame(
        reason="binary",
        event_type=None,
        error=item.record.error,
        raw=raw,
        received_at=AT,
        connection=1,
        frame=2,
        index=None,
        affected=(),
    )
    assert item.impact == EVERY_TOKEN


# Unpaired surrogates (spec/client.md, Unpaired surrogates).


@pytest.mark.parametrize(
    "text",
    [
        '{"event_type":"future","note":"\\ud800"}',
        '{"event_type":"future","note":"\\uDC00 and more"}',
        '{"event_type":"future","\\ud800":"x"}',
        '{"event_type":"future","nested":[{"\\udbff":1}]}',
        f'[{LATER_BOOK},{{"event_type":"future","note":"\\ud800"}}]',
        # In a member that a later one of the same name overwrites.
        '{"event_type":"future","note":"\\ud800","note":"safe"}',
        '{"event_type":"future","note":{"inner":["\\udfff"]},"note":1}',
        '{"event_type":"future","note":{"inner":"\\udc00","inner":2},"note":3}',
        LATER_BOOK[:-1] + ',"note":"\\ud800","note":"safe"}',
    ],
)
def test_an_unpaired_surrogate_makes_the_frame_invalid_json(text: str) -> None:
    item = undecodable(text)
    record = item.record
    assert (record.reason, record.event_type, record.index) == (
        "invalid_json",
        None,
        None,
    )
    assert record.raw == text
    assert item.impact == EVERY_TOKEN
    record.error.encode("utf-8")
    adapter = TypeAdapter(UndecodableFrame)
    assert adapter.validate_json(adapter.dump_json(record)) == record


def test_a_paired_surrogate_escape_is_one_character() -> None:
    item = decode_one('{"event_type":"future","note":"\\ud83d\\ude00"}')
    assert isinstance(item, UnknownEvent)
    assert item.payload["note"] == "\U0001f600"


def test_a_repeated_name_keeps_its_last_value_in_a_frame_with_surrogates() -> None:
    # A surrogate escape makes the decoder search overwritten members; the
    # object is built as json.loads builds it.
    text = '{"event_type":"future","a":"\\ud83d\\ude00","b":1,"a":"last","c":2}'
    item = decode_one(text)
    assert isinstance(item, UnknownEvent)
    assert item.payload == json.loads(text)
    assert list(item.payload.items()) == list(json.loads(text).items())


def test_overwritten_members_are_searched_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Each object whose names repeat keeps its members, and each member holds
    # the objects nested in it, so the search must not go through those again
    # for every level. A frame may nest 64 deep (spec/client.md, Nesting
    # depth), so 63 levels below the book, each with one name, take under
    # twice as many searches, not about two thousand.
    searches = 0
    surrogate = re.compile("[\ud800-\udfff]")

    class Counting:
        def search(self, text: str) -> re.Match[str] | None:
            nonlocal searches
            searches += 1
            return surrogate.search(text)

    monkeypatch.setattr("polymarket_market_data._decode._SURROGATE", Counting())
    levels = 63
    deep = '{"a":0,"a":' * levels + "0" + "}" * levels
    text = LATER_BOOK[:-1] + f',"note":"\\ud83d\\ude00","x":{deep}}}'
    assert depth(text) == 64
    item = decode_one(text)
    assert isinstance(item, DecodedEvent)
    assert item.record == event(LATER_BOOK)
    assert levels <= searches < 2 * levels


def test_an_escaped_backslash_before_u_is_not_a_surrogate() -> None:
    item = decode_one('{"event_type":"future","note":"\\\\ud800"}')
    assert isinstance(item, UnknownEvent)
    assert item.payload["note"] == "\\ud800"


# What an undecodable event may affect, the cases of the invalid-known-event
# scenario first.


def impact(text: str) -> Impact:
    return undecodable(frame(text)).impact


def test_a_book_with_an_invalid_level_affects_its_token() -> None:
    found = impact(
        '{"market":"${A}","asset_id":"${A1}","timestamp":"${t:100}",'
        f'"hash":"{ZEROS}","bids":[{{"price":"abc","size":"1"}}],"asks":[],'
        '"event_type":"book"}'
    )
    assert found == Impact(tokens=(A1,))
    assert found.may_affect(MARKETS) == (A1,)


def test_a_price_change_with_an_invalid_entry_affects_its_token() -> None:
    found = impact(
        '{"market":"${A}","price_changes":[{"asset_id":"${A2}",'
        f'"price":"0.49","size":"NaN","side":"BUY","hash":"{ZEROS}"}}],'
        '"timestamp":"${t:110}","event_type":"price_change"}'
    )
    assert found.may_affect(MARKETS) == (A2,)


def test_types_that_change_no_book_affect_nothing() -> None:
    found = impact(
        '{"market":"${B}","asset_id":"${B1}","best_bid":"x","best_ask":"0.41",'
        '"spread":"0.02","timestamp":"${t:120}","event_type":"best_bid_ask"}'
    )
    assert found == NO_IMPACT
    assert found.may_affect(MARKETS) == ()
    for text in (LAST_TRADE_PRICE, MARKET_RESOLVED, NEW_MARKET):
        bad = edit(text, set_to(("timestamp",), "soon"))
        assert undecodable(bad).impact == NO_IMPACT


def test_an_entry_without_a_token_affects_its_market() -> None:
    found = impact(
        '{"market":"${B}","price_changes":[{"price":"0.39","size":"1",'
        f'"side":"BUY","hash":"{ZEROS}"}}],'
        '"timestamp":"${t:130}","event_type":"price_change"}'
    )
    assert found == Impact(markets=(B,))
    assert found.may_affect(MARKETS) == (B1, B2)


def test_named_tokens_and_the_market_of_an_unattributed_entry() -> None:
    found = impact(
        '{"market":"${A}","price_changes":[{"asset_id":"${A1}",'
        f'"price":"0.49","size":"10","side":"BUY","hash":"{ZEROS}"}},'
        f'{{"price":"0.51","size":"10","side":"SELL","hash":"{ZEROS}"}}],'
        '"timestamp":"${t:140}","event_type":"price_change"}'
    )
    assert found == Impact(tokens=(A1,), markets=(A,))
    assert found.may_affect(MARKETS) == (A1, A2)


def test_no_readable_token_or_market_affects_the_connection() -> None:
    found = impact(
        '{"price_changes":[{"price":"0.39","size":"1","side":"BUY",'
        f'"hash":"{ZEROS}"}}],"timestamp":"${{t:150}}","event_type":"price_change"}}'
    )
    assert found == Impact(connection=True)
    assert found.may_affect(MARKETS) == (A1, A2, B1, B2)


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (remove(("asset_id",)), Impact(markets=(A,))),
        (set_to(("asset_id",), 11), Impact(markets=(A,))),
        (lambda v: (v.pop("asset_id"), v.pop("market")), Impact(connection=True)),
        (set_to(("bids",), None), Impact(tokens=(A1,))),
        (set_to(("market",), None), Impact(tokens=(A1,))),
    ],
)
def test_book_impact(change: Any, expected: Impact) -> None:
    assert undecodable(edit(LATER_BOOK, change)).impact == expected


def test_tick_size_change_impact() -> None:
    bad = edit(TICK_SIZE_CHANGE, set_to(("new_tick_size",), "x"))
    assert undecodable(bad).impact == Impact(tokens=(A1,))
    no_token = edit(TICK_SIZE_CHANGE, remove(("asset_id",)))
    assert undecodable(no_token).impact == Impact(markets=(A,))


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (remove(("price_changes",)), Impact(markets=(A,))),
        (set_to(("price_changes",), {}), Impact(markets=(A,))),
        (set_to(("price_changes", 0), "entry"), Impact(tokens=(A2,), markets=(A,))),
        (set_to(("market",), 5), Impact(tokens=(A1, A2))),
        (
            lambda v: (v.update(market=5), v["price_changes"][1].pop("asset_id")),
            Impact(tokens=(A1,), connection=True),
        ),
        (
            lambda v: v["price_changes"].append(
                dict(v["price_changes"][0], size="NaN")
            ),
            Impact(tokens=(A1, A2)),
        ),
    ],
)
def test_price_change_impact(change: Any, expected: Impact) -> None:
    assert undecodable(edit(PRICE_CHANGE, change)).impact == expected


def test_may_affect_keeps_the_desired_set_order() -> None:
    assert Impact(tokens=(B1, A2)).may_affect(MARKETS) == (A2, B1)
    assert Impact(tokens=(B2,), markets=(A,)).may_affect(MARKETS) == (A1, A2, B2)


def test_may_affect_only_tokens_of_the_given_markets() -> None:
    assert Impact(tokens=("999",), markets=("0xother",)).may_affect(MARKETS) == ()
    assert EVERY_TOKEN.may_affect(MARKETS[:1]) == (A1, A2)


def test_the_error_names_where_validation_failed() -> None:
    text = PRICE_CHANGE.replace('"size":"0"', '"size":"NaN"')
    error = undecodable(text).record.error
    assert error == "price_changes.1.size: Input should be a finite number"


# Repeats (spec/client.md, Repeated messages).


def content(text: str) -> Hashable:
    item = decode_one(text)
    assert isinstance(item, DecodedEvent)
    return item.content


def test_reordered_entries_have_the_same_content() -> None:
    reversed_entries = edit(PRICE_CHANGE, lambda v: v["price_changes"].reverse())
    assert reversed_entries != PRICE_CHANGE
    assert content(reversed_entries) == content(PRICE_CHANGE)


def test_member_order_does_not_matter() -> None:
    value = json.loads(BEST_BID_ASK)
    reordered = json.dumps(dict(reversed(value.items())), separators=(",", ":"))
    assert content(reordered) == content(BEST_BID_ASK)


@pytest.mark.parametrize(
    "change",
    [
        set_to(("price_changes", 0, "size"), "51"),
        set_to(("price_changes", 0, "hash"), "1" * 40),
        set_to(("timestamp",), str(T0 + 101)),
        lambda v: v["price_changes"].append(v["price_changes"][0]),
        lambda v: v.update(extra=True),
    ],
)
def test_changed_content_is_not_the_same(change: Any) -> None:
    assert content(edit(PRICE_CHANGE, change)) != content(PRICE_CHANGE)


def test_only_price_change_entries_are_a_multiset() -> None:
    reversed_bids = edit(LATER_BOOK, lambda v: v["bids"].reverse())
    assert content(reversed_bids) != content(LATER_BOOK)


@pytest.mark.parametrize(
    ("first", "second", "same"),
    [
        ({"v": "0.5"}, {"v": "0.50"}, False),
        ({"v": "0.5"}, {"v": Decimal("0.5")}, False),
        ({"v": Decimal("0.5")}, {"v": Decimal("0.50")}, True),
        ({"v": 1}, {"v": Decimal("1.0")}, True),
        ({"v": True}, {"v": 1}, False),
        ({"v": None}, {}, False),
        ({"v": Decimal("NaN")}, {"v": Decimal("NaN")}, True),
        ({"v": [1, 2]}, {"v": [2, 1]}, False),
    ],
)
def test_content_compares_json_values(
    first: dict[str, Any], second: dict[str, Any], same: bool
) -> None:
    assert (content_key(first) == content_key(second)) is same


def test_content_does_not_depend_on_the_frame() -> None:
    first = decode(PRICE_CHANGE, number=2)[0]
    again = decode_frame(
        PRICE_CHANGE,
        received_at=datetime.now(UTC),
        connection=1,
        frame=9,
        keep_raw=True,
    )[0]
    assert isinstance(first, DecodedEvent)
    assert isinstance(again, DecodedEvent)
    assert first.content == again.content


def test_a_repeat_inside_the_window() -> None:
    detector = RepeatDetector(1.0)
    assert detector.check("x", 10.0) is False
    assert detector.check("x", 10.12) is True
    assert detector.check("x", 11.12) is True


def test_not_a_repeat_outside_the_window() -> None:
    detector = RepeatDetector(1.0)
    assert detector.check("x", 10.0) is False
    assert detector.check("x", 11.2) is False
    assert detector.check("y", 11.3) is False


def test_each_arrival_extends_the_window() -> None:
    detector = RepeatDetector(1.0)
    assert [detector.check("x", t) for t in (0.0, 0.9, 1.8, 3.0)] == [
        False,
        True,
        True,
        False,
    ]


def test_expiry_keeps_the_latest_arrival() -> None:
    detector = RepeatDetector(1.0)
    detector.check("x", 0.0)
    detector.check("x", 0.9)
    detector.check("y", 1.5)  # expires the arrival at 0.0
    assert detector.check("x", 1.6) is True


def test_repeats_as_the_repeated_messages_scenario_sends_them() -> None:
    # x, a later change, x again with its entries reversed, a best_bid_ask
    # twice, and that best_bid_ask again 1.2 s later, outside the window.
    later = edit(PRICE_CHANGE, set_to(("timestamp",), str(T0 + 105)))
    again = edit(PRICE_CHANGE, lambda v: v["price_changes"].reverse())
    detector = RepeatDetector(1.0)
    arrivals = [
        (PRICE_CHANGE, 0.0),
        (later, 0.05),
        (again, 0.1),
        (BEST_BID_ASK, 0.15),
        (BEST_BID_ASK, 0.2),
        (BEST_BID_ASK, 1.45),
    ]
    assert [detector.check(content(text), now) for text, now in arrivals] == [
        False,
        False,
        True,
        False,
        True,
        False,
    ]


# Every record the decoder produces serializes (D8).


def test_every_decoded_record_reads_back_equal() -> None:
    frames: list[str | bytes] = [
        *EVENT_FRAMES,
        '{"event_type":"future","note":"\\ud800"}',
        "not json",
        "[7]",
        b"\x00\xff",
        LATER_BOOK.replace('"price":"0.47"', '"price":"abc"'),
        '{"event_type":"something_new","n":3}',
    ]
    for data in frames:
        for item in decode(data, keep_raw=True):
            record = (
                item.record if isinstance(item, DecodedEvent | Undecodable) else item
            )
            adapter: TypeAdapter[Any] = TypeAdapter(type(record))
            assert adapter.validate_json(adapter.dump_json(record)) == record
