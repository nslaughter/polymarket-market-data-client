"""The order book (spec/client.md, Recovery contract)."""

from datetime import UTC, datetime
from decimal import Decimal

from polymarket_market_data import BookEvent, Level, PriceChange, Side
from polymarket_market_data._book import Book

A = "0x00000000000000000000000000000000000000000000000000000000000000a1"
A1 = "10000000000000000000000000000000000000000000000000000000000000000000000000011"
T0 = 1791200000000


def level(price: str, size: str) -> Level:
    return Level(Decimal(price), Decimal(size))


def change(side: Side, price: str, size: str) -> PriceChange:
    return PriceChange(
        asset_id=A1,
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        hash="0" * 40,
        best_bid=None,
        best_ask=None,
        applied=False,
        before_book=False,
    )


def standard() -> Book:
    # A1's standard book, from spec/conformance.md.
    return Book(
        [level("0.48", "100"), level("0.47", "250")],
        [level("0.52", "120"), level("0.53", "300")],
        timestamp=T0 - 30000,
    )


def test_a_book_from_a_book_event() -> None:
    event = BookEvent(
        event_type="book",
        market=A,
        source_timestamp_ms=T0 - 30000,
        received_at=datetime(2026, 10, 5, tzinfo=UTC),
        connection=1,
        frame=1,
        index=0,
        repeat=False,
        raw=None,
        asset_id=A1,
        hash="0" * 40,
        bids=(level("0.47", "250"), level("0.48", "100")),
        asks=(level("0.53", "300"), level("0.52", "120")),
        tick_size=Decimal("0.01"),
        last_trade_price=Decimal("0.500"),
        opening=True,
        held_book_matched=None,
    )
    book = Book.from_event(event)
    assert book.timestamp == T0 - 30000
    assert book.matches(standard())


def test_sides_are_in_the_hash_input_order() -> None:
    book = standard()
    # Bids ascending, asks descending.
    assert book.bids == (level("0.47", "250"), level("0.48", "100"))
    assert book.asks == (level("0.53", "300"), level("0.52", "120"))


def test_an_entry_sets_the_size_at_its_price_on_its_side() -> None:
    book = standard()
    book.apply(change(Side.BUY, "0.49", "50"), T0 + 100)
    book.apply(change(Side.SELL, "0.52", "60"), T0 + 100)
    book.apply(change(Side.BUY, "0.48", "110"), T0 + 110)
    assert book.bids == (
        level("0.47", "250"),
        level("0.48", "110"),
        level("0.49", "50"),
    )
    assert book.asks == (level("0.53", "300"), level("0.52", "60"))
    assert book.timestamp == T0 + 110


def test_a_size_of_zero_removes_the_level() -> None:
    book = standard()
    book.apply(change(Side.BUY, "0.47", "0"), T0 + 100)
    book.apply(change(Side.SELL, "0.53", "0.00"), T0 + 100)
    assert book.bids == (level("0.48", "100"),)
    assert book.asks == (level("0.52", "120"),)


def test_removing_a_level_that_is_not_there_changes_nothing() -> None:
    book = standard()
    book.apply(change(Side.BUY, "0.10", "0"), T0 + 100)
    assert book.matches(standard())


def test_prices_compare_by_value_and_keep_the_text_last_sent() -> None:
    book = standard()
    book.apply(change(Side.BUY, "0.480", "100"), T0 + 100)
    assert len(book.bids) == 2
    assert str(book.bids[1].price) == "0.480"
    assert book.matches(standard())


def test_matches_compares_every_level_on_both_sides() -> None:
    assert standard().matches(standard())
    changed = standard()
    changed.apply(change(Side.SELL, "0.52", "121"), T0 + 100)
    assert not changed.matches(standard())
    assert not standard().matches(changed)
    missing = standard()
    missing.apply(change(Side.BUY, "0.47", "0"), T0 + 100)
    assert not missing.matches(standard())
    # A level on the other side is not the same level.
    swapped = Book(standard().asks, standard().bids, timestamp=T0)
    assert not swapped.matches(standard())


def test_an_emptied_book_is_still_a_book() -> None:
    book = standard()
    for side, price in [
        (Side.BUY, "0.47"),
        (Side.BUY, "0.48"),
        (Side.SELL, "0.52"),
        (Side.SELL, "0.53"),
    ]:
        book.apply(change(side, price, "0"), T0 + 1000)
    assert (book.bids, book.asks) == ((), ())
    assert book.matches(Book(timestamp=T0))


def test_a_copy_is_independent() -> None:
    book = standard()
    copy = book.copy()
    book.apply(change(Side.BUY, "0.49", "50"), T0 + 100)
    assert copy.matches(standard())
    assert copy.timestamp == T0 - 30000
