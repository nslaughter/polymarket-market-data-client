"""The frame notation and the harness's hashes (spec/conformance.md, Frame
notation; spec/client.md, Order-book hash)."""

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pytest

from .failures import SPEC
from .frames import (
    BAD_HASH,
    BestBidAskFrame,
    BookFrame,
    BookSpec,
    Entry,
    LastTradeFrame,
    NewMarketFrame,
    Opening,
    PriceChangeFrame,
    ResolvedFrame,
    Source,
    TickSizeFrame,
    reverse_entries,
)
from .synthetic import MARKETS, T0, TOKEN_IDS

CLIENT_SPEC = Path(__file__).resolve().parents[2] / "spec" / "client.md"
A = MARKETS["A"].condition_id
B = MARKETS["B"].condition_id


def documented_members() -> dict[str, list[tuple[str, Any]]]:
    """The table of each event's members, in order, with the fixed values
    it gives: the opening book, the later book, and each event type."""
    text = SPEC.read_text()
    table = text[text.index("| Event | Members, in order |") :].split("\n\n", 1)[0]
    members: dict[str, list[tuple[str, Any]]] = {}
    for row in table.splitlines()[2:]:
        event, cell = (part.strip() for part in row.strip("|").split("|"))
        nested = re.search(r"\(each: ([^)]*)\)", cell)
        if nested:
            members[f"{event.strip('`')} entry"] = _members(nested.group(1))
            cell = cell.replace(nested.group(0), "")
        members[event.strip("`")] = _members(cell)
    return members


def _members(cell: str) -> list[tuple[str, Any]]:
    found = re.findall(r"`([a-z_]+)`(?: `([^`]+)`)?", cell)
    return [(name, json.loads(value) if value else None) for name, value in found]


MEMBERS = documented_members()


def check_members(frame: dict[str, Any], event: str) -> None:
    documented = MEMBERS[event]
    assert list(frame) == [name for name, _ in documented]
    for name, value in documented:
        if value is not None:
            assert frame[name] == value, name


def frame(source: Source, spec: Any) -> Any:
    return json.loads(source.frame(spec))


def test_the_members_table_covers_every_frame() -> None:
    assert set(MEMBERS) == {
        "opening book",
        "later book",
        "price_change",
        "price_change entry",
        "best_bid_ask",
        "last_trade_price",
        "tick_size_change",
        "market_resolved",
        "new_market",
    }


def standard_book_hashes() -> dict[str, str]:
    text = SPEC.read_text()
    rows = re.findall(
        r"^\| `([AB][12])` \| [^|]+\| [^|]+\| `([0-9a-f]{40})` \|$", text, re.M
    )
    return dict(rows)


def test_the_four_standard_book_hashes() -> None:
    hashes = standard_book_hashes()
    assert set(hashes) == {"A1", "A2", "B1", "B2"}
    source = Source()
    for token, expected in hashes.items():
        assert source.hash(token, T0 - 30000) == expected, token


def test_the_client_contract_test_vector() -> None:
    text = CLIENT_SPEC.read_text()
    section = text[text.index("Test vector, computed") :]
    hashed = re.search(r"```text\n(.*?)\n```", section, re.S)
    digest = re.search(r"hashes to `([0-9a-f]{40})`", section)
    assert hashed is not None
    assert digest is not None
    source = Source()
    assert source.hashed_text("A1", 1791199970000) == hashed.group(1)
    assert hashlib.sha1(hashed.group(1).encode()).hexdigest() == digest.group(1)
    assert source.hash("A1", 1791199970000) == digest.group(1)


def test_opening_with_no_items_is_an_empty_array_and_a_newline() -> None:
    assert Source().frame(Opening(())) == "[]\n"


def test_opening_books_carry_tick_size_and_trade_price() -> None:
    source = Source()
    text = source.frame(Opening(("A1", "B2")))
    assert " " not in text
    first, second = json.loads(text)
    check_members(first, "opening book")
    assert first == {
        "market": A,
        "asset_id": TOKEN_IDS["A1"],
        "timestamp": str(T0 - 30000),
        "hash": "b17e93a1f6202e13d8e0dd3aeb958b4a882dca3c",
        "bids": [{"price": "0.47", "size": "250"}, {"price": "0.48", "size": "100"}],
        "asks": [{"price": "0.53", "size": "300"}, {"price": "0.52", "size": "120"}],
        "tick_size": "0.01",
        "event_type": "book",
        "last_trade_price": "0.500",
    }
    assert second["market"] == B
    assert second["last_trade_price"] == "0.400"
    assert second["hash"] == "b17904e76ff27a2131684e3764670c829d4c3e18"


def test_an_opening_item_can_replace_the_reference_book() -> None:
    source = Source()
    item = BookSpec("A1", 50, (("0.40", "1"),), ())
    (book,) = frame(source, Opening((item,)))
    assert book["timestamp"] == str(T0 + 50)
    assert book["bids"] == [{"price": "0.40", "size": "1"}]
    assert book["asks"] == []
    assert book["hash"] == source.hash("A1", T0 + 50)


def test_a_later_book() -> None:
    source = Source()
    book = frame(source, BookFrame("A2", None, False))
    check_members(book, "later book")
    assert book["hash"] == "84549e080ed1722e15d1891c5ee759b77760e125"
    assert book["timestamp"] == str(T0 - 30000)
    assert book["bids"] == [
        {"price": "0.47", "size": "300"},
        {"price": "0.48", "size": "120"},
    ]


def test_a_later_book_can_replace_the_reference_book() -> None:
    source = Source()
    spec = BookSpec("A1", 10, (("0.45", "5"), ("0.44", "6")), (("0.55", "7"),))
    book = frame(source, BookFrame("A1", spec, False))
    assert book["bids"] == [
        {"price": "0.44", "size": "6"},
        {"price": "0.45", "size": "5"},
    ]
    assert book["asks"] == [{"price": "0.55", "size": "7"}]
    assert book["timestamp"] == str(T0 + 10)
    assert frame(source, BookFrame("A1", None, False)) == book


def test_hash_bad_replaces_every_hash() -> None:
    source = Source()
    assert frame(source, BookFrame("A1", None, True))["hash"] == BAD_HASH
    entries = (Entry("A1", "BUY", "0.49", "1"), Entry("A2", "BUY", "0.49", "1"))
    change = frame(source, PriceChangeFrame("A", 100, entries, True))
    assert [entry["hash"] for entry in change["price_changes"]] == [BAD_HASH, BAD_HASH]


def test_a_price_change_carries_each_tokens_book_after_all_its_entries() -> None:
    source = Source()
    entries = (
        Entry("A1", "BUY", "0.49", "50"),
        Entry("A2", "SELL", "0.51", "40"),
        Entry("A1", "BUY", "0.49", "70"),
    )
    change = frame(source, PriceChangeFrame("A", 100, entries, False))
    check_members(change, "price_change")
    assert change["timestamp"] == str(T0 + 100)
    first, second, third = change["price_changes"]
    for entry in change["price_changes"]:
        check_members(entry, "price_change entry")
    assert (first["asset_id"], first["price"], first["size"], first["side"]) == (
        TOKEN_IDS["A1"],
        "0.49",
        "50",
        "BUY",
    )
    # A1's entries share the hash of its book after both.
    assert first["hash"] == third["hash"] == source.hash("A1", T0 + 100)
    assert (first["best_bid"], first["best_ask"]) == ("0.49", "0.52")
    assert second["hash"] == source.hash("A2", T0 + 100)
    assert (second["best_bid"], second["best_ask"]) == ("0.48", "0.51")
    assert source.books["A1"].bid_levels()[-1] == {"price": "0.49", "size": "70"}
    assert source.books["A1"].timestamp == T0 + 100


def test_an_empty_side_has_best_prices_0_and_1() -> None:
    source = Source()
    entries = tuple(
        Entry("A1", side, price, "0")
        for side, price in [
            ("BUY", "0.48"),
            ("BUY", "0.47"),
            ("SELL", "0.52"),
            ("SELL", "0.53"),
        ]
    )
    change = frame(source, PriceChangeFrame("A", 1000, entries, False))
    assert {(e["best_bid"], e["best_ask"]) for e in change["price_changes"]} == {
        ("0", "1")
    }
    book = frame(source, BookFrame("A1", None, False))
    assert (book["bids"], book["asks"]) == ([], [])


def test_a_level_compares_by_value_and_keeps_its_last_text() -> None:
    source = Source()
    frame(
        source, PriceChangeFrame("A", 100, (Entry("A1", "BUY", "0.480", "90"),), False)
    )
    assert source.books["A1"].bid_levels() == [
        {"price": "0.47", "size": "250"},
        {"price": "0.480", "size": "90"},
    ]


def test_a_reference_books_last_change_is_its_latest_t() -> None:
    source = Source()
    early = (Entry("A1", "BUY", "0.48", "100"),)
    change = frame(source, PriceChangeFrame("A", -30001, early, False))
    # The entry carries the message's timestamp; the book keeps the later one.
    assert change["price_changes"][0]["hash"] == source.hash("A1", T0 - 30001)
    assert frame(source, BookFrame("A1", None, False))["timestamp"] == str(T0 - 30000)


def test_best_bid_ask() -> None:
    source = Source()
    frame(
        source, PriceChangeFrame("A", 100, (Entry("A1", "SELL", "0.51", "75"),), False)
    )
    bba = frame(source, BestBidAskFrame("A1", 110))
    check_members(bba, "best_bid_ask")
    assert (bba["best_bid"], bba["best_ask"], bba["spread"]) == ("0.48", "0.51", "0.03")
    assert bba["timestamp"] == str(T0 + 110)
    # It changes no book.
    assert source.books["A1"].timestamp == T0 + 100


def test_last_trade_price_sets_the_trade_price_in_later_hashes() -> None:
    source = Source()
    first = frame(source, LastTradeFrame("A1", 120, "0.53", "5", "BUY"))
    check_members(first, "last_trade_price")
    assert (first["price"], first["size"], first["side"]) == ("0.53", "5", "BUY")
    assert first["transaction_hash"] == "0x" + "0" * 63 + "1"
    second = frame(source, LastTradeFrame("B2", 130, "0.6", "1", "SELL"))
    assert second["transaction_hash"] == "0x" + "0" * 63 + "2"
    assert '"last_trade_price":"0.530"' in source.hashed_text("A2", T0)
    assert '"last_trade_price":"0.600"' in source.hashed_text("B1", T0)
    (opening,) = frame(source, Opening(("A1",)))
    assert opening["last_trade_price"] == "0.530"


def test_trade_sets_the_trade_price_without_a_frame() -> None:
    source = Source()
    source.trade("A", "0.530")
    assert '"last_trade_price":"0.530"' in source.hashed_text("A1", T0)
    assert '"last_trade_price":"0.400"' in source.hashed_text("B1", T0)


def test_tick_size_change_gives_the_old_tick_size_and_sets_the_new() -> None:
    source = Source()
    change = frame(source, TickSizeFrame("A1", 200, "0.001"))
    check_members(change, "tick_size_change")
    assert (change["old_tick_size"], change["new_tick_size"]) == ("0.01", "0.001")
    assert frame(source, TickSizeFrame("A1", 210, "0.01"))["old_tick_size"] == "0.001"
    frame(source, TickSizeFrame("A1", 220, "0.001"))
    assert '"tick_size":"0.001"' in source.hashed_text("A1", T0)
    assert '"tick_size":"0.01"' in source.hashed_text("A2", T0)
    (opening,) = frame(source, Opening(("A1",)))
    assert opening["tick_size"] == "0.001"


def test_market_resolved() -> None:
    source = Source()
    resolved = frame(source, ResolvedFrame("A", 1001, "A2"))
    check_members(resolved, "market_resolved")
    assert resolved == {
        "id": "9000001",
        "market": A,
        "assets_ids": [TOKEN_IDS["A1"], TOKEN_IDS["A2"]],
        "winning_asset_id": TOKEN_IDS["A2"],
        "winning_outcome": "No",
        "event_message": None,
        "timestamp": str(T0 + 1001),
        "event_type": "market_resolved",
        "tags": [],
    }
    again = frame(source, ResolvedFrame("S", 1002, "S1"))
    assert (again["id"], again["winning_outcome"]) == ("9000002", "Up")


def test_new_market() -> None:
    new_market = frame(Source(), NewMarketFrame(1, 100))
    check_members(new_market, "new_market")
    condition_id = "0x" + "0" * 61 + "f01"
    tokens = ["2" + "0" * 74 + "11", "2" + "0" * 74 + "12"]
    assert new_market == {
        "id": "9100001",
        "question": "Synthetic new market 1?",
        "market": condition_id,
        "slug": "synthetic-new-1",
        "description": "Synthetic.",
        "assets_ids": tokens,
        "outcomes": ["Yes", "No"],
        "event_message": None,
        "timestamp": str(T0 + 100),
        "event_type": "new_market",
        "tags": [],
        "condition_id": condition_id,
        "active": True,
        "clob_token_ids": tokens,
        "game_start_time": "2026-10-11 14:00:00+00",
        "order_price_min_tick_size": "0.01",
    }
    assert len(tokens[0]) == 77


def test_a_burst_gives_one_frame_per_entry_with_the_hash_after_all() -> None:
    source = Source()
    entries = [
        Entry("A1", "BUY", "0.49", "50"),
        Entry("A1", "BUY", "0.47", "0"),
        Entry("A1", "SELL", "0.52", "60"),
    ]
    frames = [json.loads(text) for text in source.burst("A", 100, entries)]
    after = source.hash("A1", T0 + 100)
    assert [f["price_changes"][0]["price"] for f in frames] == ["0.49", "0.47", "0.52"]
    assert {f["price_changes"][0]["hash"] for f in frames} == {after}
    assert {f["timestamp"] for f in frames} == {str(T0 + 100)}
    book = frame(source, BookFrame("A1", None, False))
    assert book["bids"] == [
        {"price": "0.48", "size": "100"},
        {"price": "0.49", "size": "50"},
    ]
    assert book["asks"] == [
        {"price": "0.53", "size": "300"},
        {"price": "0.52", "size": "60"},
    ]


def test_silent_changes_the_reference_book_only() -> None:
    source = Source()
    source.silent("A1", 2000, Entry("A1", "BUY", "0.46", "500"))
    book = frame(source, BookFrame("A1", None, False))
    assert book["bids"][0] == {"price": "0.46", "size": "500"}
    assert book["timestamp"] == str(T0 + 2000)


def test_reversed_entries() -> None:
    source = Source()
    entries = (Entry("A1", "BUY", "0.49", "50"), Entry("A2", "SELL", "0.51", "50"))
    text = source.frame(PriceChangeFrame("A", 100, entries, False))
    reversed_ = json.loads(reverse_entries(text))
    assert reversed_["price_changes"] == json.loads(text)["price_changes"][::-1]
    assert list(reversed_) == list(json.loads(text))
    with pytest.raises(ValueError, match="price_change"):
        reverse_entries(source.frame(BookFrame("A1", None, False)))


def test_reversed_entries_keep_each_number_as_written() -> None:
    # A send-text frame can write prices and sizes as numbers.
    first = '{"asset_id":"1","price":0.12345678901234567890123456789,"size":1e400}'
    second = '{"asset_id":"2","price":-0.5E-3,"size":NaN,"side":"BUY"}'
    text = (
        f'{{"market":"0x1","price_changes":[{first},{second}],'
        '"timestamp":17,"event_type":"price_change"}'
    )
    assert reverse_entries(text) == (
        f'{{"market":"0x1","price_changes":[{second},{first}],'
        '"timestamp":17,"event_type":"price_change"}'
    )
