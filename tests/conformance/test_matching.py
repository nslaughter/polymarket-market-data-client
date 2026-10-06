"""The matcher's rules, one test per rule (spec/conformance.md, Record
notation)."""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from polymarket_market_data import (
    Backlog,
    BestBidAskEvent,
    BookEvent,
    CaptureGap,
    ClientStats,
    ConnectionState,
    ConnectionStateChange,
    Level,
    NewMarketEvent,
    PriceChange,
    PriceChangeEvent,
    Side,
    TokenState,
    TokenStateChange,
    UndecodableFrame,
)

from .matching import compile_pattern, compile_stat, misplaced_decimal, render_record
from .synthetic import MARKETS, T0, TOKEN_IDS
from .words import NotationError, split_words

AT = datetime(2026, 10, 5, tzinfo=UTC)
A = MARKETS["A"].condition_id
A1, A2 = TOKEN_IDS["A1"], TOKEN_IDS["A2"]

TOKEN = TokenStateChange(
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
CONNECTION = ConnectionStateChange(
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
)
COMMON: dict[str, Any] = {
    "market": A,
    "source_timestamp_ms": T0 + 100,
    "received_at": AT,
    "connection": 1,
    "frame": 2,
    "index": 0,
    "repeat": False,
    "raw": None,
}
BOOK = BookEvent(
    event_type="book",
    **COMMON,
    asset_id=A1,
    hash="0" * 40,
    bids=(
        Level(Decimal("0.47"), Decimal("250")),
        Level(Decimal("0.48"), Decimal("100")),
    ),
    asks=(Level(Decimal("0.53"), Decimal("300")),),
    tick_size=Decimal("0.01"),
    last_trade_price=Decimal("0.500"),
    opening=True,
    held_book_matched=None,
)


def entry(token: str, side: Side, price: str, size: str, applied: bool) -> PriceChange:
    return PriceChange(
        asset_id=token,
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        hash="0" * 40,
        best_bid=None,
        best_ask=None,
        applied=applied,
        before_book=False,
    )


CHANGE = PriceChangeEvent(
    event_type="price_change",
    **COMMON,
    changes=(
        entry(A1, Side.BUY, "0.49", "50", True),
        entry(A2, Side.SELL, "0.51", "50", False),
    ),
)
GAP = CaptureGap(
    token_id=A1,
    market=A,
    cause="close_frame",
    close_code=1001,
    close_reason="going away",
    last_confirmed_at=AT,
    detected_at=AT,
    resumed_at=AT,
    end="book",
    connection_before=1,
    connection_after=2,
    held_book_matched=True,
    at=AT,
)


def altered(record: Any, **changes: Any) -> Any:
    """A record with values its type does not allow, as a faulty client
    could make."""
    return replace(record, **changes)


def matches(pattern: str, record: object) -> bool:
    return compile_pattern(split_words(pattern)).match(record) is None


def test_the_kind_subject_and_state_must_match() -> None:
    assert matches("token A1 ready", TOKEN)
    assert not matches("token A2 ready", TOKEN)
    assert not matches("token A1 uncertain", TOKEN)
    assert not matches("conn recovering", TOKEN)
    assert not matches("gap A1", TOKEN)
    assert matches("conn recovering", CONNECTION)
    assert matches("price_change A", CHANGE)
    assert not matches("price_change B", CHANGE)
    assert matches("book A1", BOOK)
    assert matches("backlog", Backlog(3, 4, True, AT))


def test_fields_not_named_are_not_compared() -> None:
    assert matches("token A1 ready", replace(TOKEN, reason="anything"))


def test_none_and_set_in_any_field() -> None:
    assert matches(
        "conn recovering connection=none close_code=none detail=set", CONNECTION
    )
    assert not matches("conn recovering detail=none", CONNECTION)
    assert not matches("conn recovering connection=set", CONNECTION)
    assert matches("token A1 ready last_confirmed_at=none", TOKEN)
    assert matches("book A1 held_book_matched=none tick_size=set", BOOK)


def test_a_bool_field_matches_true_or_false() -> None:
    assert matches("book A1 opening=true repeat=false", BOOK)
    assert not matches("book A1 opening=false", BOOK)
    with pytest.raises(NotationError):
        compile_pattern(split_words("book A1 opening=yes"))


def test_an_int_field_matches_the_number() -> None:
    assert matches("book A1 connection=1 frame=2 index=0", BOOK)
    assert not matches("book A1 frame=1", BOOK)
    assert matches("conn recovering attempt=2", CONNECTION)
    with pytest.raises(NotationError):
        compile_pattern(split_words("book A1 frame=1.0"))


def test_a_float_field_matches_within_a_thousandth() -> None:
    assert matches("conn recovering retry_in=0.2", CONNECTION)
    assert matches("conn recovering retry_in=0.2009", CONNECTION)
    assert not matches("conn recovering retry_in=0.202", CONNECTION)
    assert matches("conn recovering retry_in=0", replace(CONNECTION, retry_in=0.0))


def test_a_decimal_field_matches_its_exact_text() -> None:
    assert matches("book A1 last_trade_price=0.500", BOOK)
    assert not matches("book A1 last_trade_price=0.5", BOOK)
    lowered = replace(BOOK, last_trade_price=Decimal("0.5"))
    assert not matches("book A1 last_trade_price=0.500", lowered)
    # A float never matches.
    assert not matches("book A1 tick_size=0.01", altered(BOOK, tick_size=0.01))


def test_a_str_field_matches_the_word_or_the_json_string() -> None:
    assert matches('gap A1 close_reason="going away"', GAP)
    assert matches("gap A1 cause=close_frame end=book", GAP)
    assert not matches('gap A1 close_reason="going"', GAP)
    assert matches('gap A1 close_reason=""', replace(GAP, close_reason=""))
    assert not matches("gap A1 close_reason=none", replace(GAP, close_reason=""))


def test_a_name_stands_for_its_id() -> None:
    settled = replace(TOKEN, state=TokenState.SETTLED, winning_asset_id=A2)
    assert matches("token A1 settled winning_asset_id=A2", settled)
    assert not matches("token A1 settled winning_asset_id=A1", settled)
    assert matches("token A1 ready market=A", TOKEN)
    # A JSON string is taken as written.
    assert not matches('token A1 ready market="A"', TOKEN)


def test_an_enum_field_matches_the_members_name_in_lower_case() -> None:
    assert matches("token A1 ready previous=synchronizing", TOKEN)
    assert not matches("token A1 ready previous=ready", TOKEN)
    # The member itself, not a string equal to it.
    assert not matches("token A1 ready", altered(TOKEN, state="ready"))
    with pytest.raises(NotationError):
        compile_pattern(split_words("token A1 ready previous=Synchronizing"))


def test_a_tuple_of_ids_matches_names_in_order() -> None:
    frame = UndecodableFrame(
        reason="invalid_json",
        event_type=None,
        error="",
        raw="not json",
        received_at=AT,
        connection=1,
        frame=2,
        index=None,
        affected=(A1, A2),
    )
    assert matches("undecodable affected=A1,A2", frame)
    assert not matches("undecodable affected=A2,A1", frame)
    assert matches("undecodable affected=()", replace(frame, affected=()))
    assert not matches("undecodable affected=()", frame)
    assert matches('undecodable raw="not json" index=none', frame)


def test_levels_match_price_and_size_pairs_in_the_records_order() -> None:
    assert matches("book A1 bids=0.47:250,0.48:100 asks=0.53:300", BOOK)
    assert not matches("book A1 bids=0.48:100,0.47:250", BOOK)
    assert not matches("book A1 bids=0.47:250", BOOK)
    assert not matches("book A1 bids=0.47:250.0,0.48:100", BOOK)
    assert matches("book A1 asks=-", replace(BOOK, asks=()))


def test_changes_match_entries_in_the_frame_notation() -> None:
    assert matches("price_change A changes=A1:BUY:0.49:50,A2:SELL:0.51:50", CHANGE)
    assert not matches("price_change A changes=A1:BUY:0.49:50", CHANGE)
    assert not matches("price_change A changes=A1:SELL:0.49:50,A2:SELL:0.51:50", CHANGE)


def test_t_matches_the_source_timestamp_as_an_offset() -> None:
    assert matches("book A1 t=100", BOOK)
    assert not matches("book A1 t=101", BOOK)
    assert matches("book A1 t=-30000", replace(BOOK, source_timestamp_ms=T0 - 30000))


def test_resumed_matches_whether_resumed_at_is_set() -> None:
    assert matches("gap A1 resumed=true", GAP)
    assert not matches("gap A1 resumed=false", GAP)
    assert matches("gap A1 resumed=false", replace(GAP, resumed_at=None))


def test_applied_and_before_book_take_one_value_or_one_per_entry() -> None:
    assert matches("price_change A applied=true,false before_book=false", CHANGE)
    assert not matches("price_change A applied=true", CHANGE)
    assert not matches("price_change A applied=true,false,false", CHANGE)
    all_applied = replace(
        CHANGE, changes=tuple(replace(c, applied=True) for c in CHANGE.changes)
    )
    assert matches("price_change A applied=true", all_applied)


def test_payload_keys_match_as_json_values() -> None:
    new_market = NewMarketEvent(
        event_type="new_market",
        **{**COMMON, "market": ""},
        id="9100001",
        condition_id=None,
        slug="synthetic-new-1",
        question=None,
        assets_ids=(),
        payload={"game_start_time": "2026-10-11 14:00:00+00", "active": True},
    )
    assert matches(
        'new_market payload.game_start_time="2026-10-11 14:00:00+00"', new_market
    )
    assert matches("new_market payload.active=true id=9100001", new_market)
    assert not matches('new_market payload.active="true"', new_market)
    assert not matches("new_market payload.missing=1", new_market)


def test_an_unknown_field_is_a_notation_error() -> None:
    with pytest.raises(NotationError, match="no field"):
        compile_pattern(split_words("token A1 ready colour=red"))
    with pytest.raises(NotationError, match="record kind"):
        compile_pattern(split_words("tokens A1 ready"))
    with pytest.raises(NotationError):
        compile_pattern(split_words("token Z9 ready"))


def test_a_mismatch_names_the_first_difference() -> None:
    pattern = compile_pattern(split_words("token A1 ready reason=book connection=2"))
    mismatch = pattern.match(TOKEN)
    assert mismatch is not None
    assert (mismatch.expected, mismatch.actual) == ("connection=2", "connection=1")
    record = pattern.match(CONNECTION)
    assert record is not None
    assert (record.expected, record.actual) == (
        "TokenStateChange",
        "ConnectionStateChange",
    )
    assert "token_id=A1" in render_record(TOKEN)


def test_every_decimal_field_of_an_event_record_must_be_a_decimal() -> None:
    assert misplaced_decimal(BOOK) is None
    assert misplaced_decimal(CHANGE) is None
    assert (
        misplaced_decimal(altered(BOOK, tick_size=0.01))
        == "BookEvent.tick_size is a float: 0.01"
    )
    floated = altered(BOOK, bids=(altered(BOOK.bids[0], size=250.0),))
    assert misplaced_decimal(floated) == "BookEvent.bids[0].size is a float: 250.0"
    # None, where the annotation is Decimal alone, is misplaced too.
    emptied = altered(BOOK, bids=(altered(BOOK.bids[0], price=None),))
    assert misplaced_decimal(emptied) == "BookEvent.bids[0].price is a NoneType: None"
    assert misplaced_decimal(altered(BOOK, tick_size=None)) is None
    changed = replace(CHANGE, changes=(altered(CHANGE.changes[0], best_bid=0.49),))
    assert (
        misplaced_decimal(changed)
        == "PriceChangeEvent.changes[0].best_bid is a float: 0.49"
    )
    quote = BestBidAskEvent(
        event_type="best_bid_ask",
        **COMMON,
        asset_id=A1,
        best_bid=Decimal("0.49"),
        best_ask=Decimal("0.51"),
        spread=None,
    )
    assert misplaced_decimal(quote) is None
    # Status records hold no Decimal field.
    assert misplaced_decimal(TOKEN) is None


STATS = ClientStats(
    frames=3,
    frames_after_interruption=0,
    events={"book": 2, "price_change": 1},
    repeats=0,
    unknown=0,
    undecodable={"binary": 1, "invalid_json": 2},
    new_market_dropped=0,
    discarded_outside=0,
    pongs=2,
    pongs_unsolicited=0,
    pong_delay_last=0.1,
    pong_delay_max=1.25,
    connections=1,
    interruptions={},
    lookups=3,
    lookup_failures=1,
    rest_book_not_found=0,
    hash_verified=0,
    hash_retried=0,
    hash_failed=0,
)


@pytest.mark.parametrize(
    ("text", "passes"),
    [
        ("frames=3", True),
        ("frames=2", False),
        ("lookups>=3", True),
        ("lookups>=4", False),
        ("lookups<=3", True),
        ("lookups<=2", False),
        ("pong_delay_max>=1.0", True),
        ("pong_delay_max>=1.3", False),
        ("events.price_change=1", True),
        ("events.tick_size_change=0", True),
        ("undecodable=3", True),
        ("undecodable.binary=1", True),
        ("interruptions=0", True),
    ],
)
def test_stats_compare_as_stated(text: str, passes: bool) -> None:
    assert (compile_stat(text).check(STATS) is None) is passes


@pytest.mark.parametrize(
    "text", ["frame=3", "frames.book=1", "frames=1.5", "frames==3"]
)
def test_a_stat_the_notation_does_not_allow(text: str) -> None:
    with pytest.raises(NotationError):
        compile_stat(text)
