"""The frame notation and the source state behind it (spec/conformance.md,
Frame notation).

``Source`` holds what the scripted server keeps: a reference book for each
token of ``A`` and ``B``, the tick sizes and trade prices that enter their
hashes, and the counts that number ``ltp`` and ``resolved`` frames. It builds
each frame from that state, with its hashes, by the harness's own
implementation of the recipe, separate from the client's.
"""

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .synthetic import (
    MARKETS,
    MIN_ORDER_SIZE,
    NEG_RISK,
    STANDARD_BOOKS,
    STANDARD_OFFSET,
    T0,
    TICK_SIZE,
    TOKEN_IDS,
    TOKEN_MARKET,
    TRADE_PRICES,
    TRADING,
    Levels,
    outcome,
)

BAD_HASH = "0" * 40


@dataclass(frozen=True, slots=True)
class Entry:
    """``<T>:<BUY or SELL>:<price>:<size>``, or, for ``silent``, the same
    without the token."""

    token: str
    side: str
    price: str
    size: str


@dataclass(frozen=True, slots=True)
class BookSpec:
    """A book that replaces a token's reference book: ``t=``, ``bids=``, and
    ``asks=``."""

    token: str
    t: int
    bids: Levels
    asks: Levels


@dataclass(frozen=True, slots=True)
class Opening:
    """``opening <item> ...``: a token name, for its current reference book,
    or a book that first replaces it. No items is the text ``[]``."""

    items: tuple[str | BookSpec, ...]


@dataclass(frozen=True, slots=True)
class BookFrame:
    token: str
    replace: BookSpec | None
    bad_hash: bool


@dataclass(frozen=True, slots=True)
class PriceChangeFrame:
    market: str
    t: int
    entries: tuple[Entry, ...]
    bad_hash: bool


@dataclass(frozen=True, slots=True)
class BestBidAskFrame:
    token: str
    t: int


@dataclass(frozen=True, slots=True)
class LastTradeFrame:
    token: str
    t: int
    price: str
    size: str
    side: str


@dataclass(frozen=True, slots=True)
class TickSizeFrame:
    token: str
    t: int
    new: str


@dataclass(frozen=True, slots=True)
class ResolvedFrame:
    market: str
    t: int
    winner: str


@dataclass(frozen=True, slots=True)
class NewMarketFrame:
    n: int
    t: int


Frame = (
    Opening
    | BookFrame
    | PriceChangeFrame
    | BestBidAskFrame
    | LastTradeFrame
    | TickSizeFrame
    | ResolvedFrame
    | NewMarketFrame
)


def dumps(value: object) -> str:
    """Compact JSON, as every frame is written."""
    return json.dumps(value, separators=(",", ":"))


def three_decimals(price: str) -> str:
    """A trade price as the hash and the opening books carry it."""
    return f"{Decimal(price):.3f}"


class _ReferenceBook:
    """A size for each price on each side, keyed by the price's value, each
    level keeping the text it was last set with."""

    def __init__(self, bids: Levels, asks: Levels, timestamp: int) -> None:
        self.bids: dict[Decimal, tuple[str, str]] = {}
        self.asks: dict[Decimal, tuple[str, str]] = {}
        self.timestamp = timestamp
        self.replace(bids, asks, timestamp)

    def replace(self, bids: Levels, asks: Levels, timestamp: int) -> None:
        self.bids = {Decimal(price): (price, size) for price, size in bids}
        self.asks = {Decimal(price): (price, size) for price, size in asks}
        self.timestamp = timestamp

    def apply(self, side: str, price: str, size: str, timestamp: int) -> None:
        levels = self.bids if side == "BUY" else self.asks
        if Decimal(size) == 0:
            levels.pop(Decimal(price), None)
        else:
            levels[Decimal(price)] = (price, size)
        # A reference book's last change is the latest t of the steps that
        # changed it.
        self.timestamp = max(self.timestamp, timestamp)

    def bid_levels(self) -> list[dict[str, str]]:
        """Ascending, as in the hash input and the captured books."""
        return [_level(self.bids[price]) for price in sorted(self.bids)]

    def ask_levels(self) -> list[dict[str, str]]:
        """Descending, as in the hash input and the captured books."""
        return [_level(self.asks[price]) for price in sorted(self.asks, reverse=True)]

    def best_bid(self) -> str:
        return self.bids[max(self.bids)][0] if self.bids else "0"

    def best_ask(self) -> str:
        return self.asks[min(self.asks)][0] if self.asks else "1"


def _level(level: tuple[str, str]) -> dict[str, str]:
    return {"price": level[0], "size": level[1]}


class Source:
    """The source's own state, from which the frames are built."""

    def __init__(self) -> None:
        self.books = {
            token: _ReferenceBook(bids, asks, T0 + STANDARD_OFFSET)
            for token, (bids, asks) in STANDARD_BOOKS.items()
        }
        self.tick_sizes = {
            token: TICK_SIZE for market in TRADING for token in MARKETS[market].tokens
        }
        self.trade_prices = dict(TRADE_PRICES)
        self.trades_sent = 0
        self.resolutions_sent = 0

    # The hash recipe (spec/client.md, Order-book hash).

    def hashed_text(self, token: str, timestamp: int) -> str:
        """The JSON text a token's hash covers, at a timestamp."""
        book = self.books[token]
        return dumps(
            {
                "market": MARKETS[TOKEN_MARKET[token]].condition_id,
                "asset_id": TOKEN_IDS[token],
                "timestamp": str(timestamp),
                "hash": "",
                "bids": book.bid_levels(),
                "asks": book.ask_levels(),
                "min_order_size": MIN_ORDER_SIZE,
                "tick_size": self.tick_sizes[token],
                "neg_risk": NEG_RISK,
                "last_trade_price": three_decimals(
                    self.trade_prices[TOKEN_MARKET[token]]
                ),
            }
        )

    def hash(self, token: str, timestamp: int) -> str:
        return hashlib.sha1(self.hashed_text(token, timestamp).encode()).hexdigest()

    # Steps that change the source without sending anything.

    def silent(self, token: str, t: int, entry: Entry) -> None:
        self.books[token].apply(entry.side, entry.price, entry.size, T0 + t)

    def trade(self, market: str, price: str) -> None:
        self.trade_prices[market] = price

    # Frames.

    def frame(self, spec: Frame) -> str:
        """The text of one frame, changing the source as the notation says."""
        match spec:
            case Opening(items=()):
                return "[]\n"
            case Opening(items=items):
                return dumps([self._opening_book(item) for item in items])
            case BookFrame():
                return dumps(self._book(spec))
            case PriceChangeFrame():
                return dumps(self._price_change(spec))
            case BestBidAskFrame():
                return dumps(self._best_bid_ask(spec))
            case LastTradeFrame():
                return dumps(self._last_trade(spec))
            case TickSizeFrame():
                return dumps(self._tick_size(spec))
            case ResolvedFrame():
                return dumps(self._resolved(spec))
            case NewMarketFrame():
                return dumps(_new_market(spec))

    def burst(self, market: str, t: int, entries: Sequence[Entry]) -> list[str]:
        """``send-burst``: applies every entry, then gives one ``pc`` frame
        for each, each carrying its token's book after all of them."""
        self._apply(entries, t)
        return [
            dumps(self._price_change_object(market, t, [entry], {entry.token}, False))
            for entry in entries
        ]

    def _opening_book(self, item: str | BookSpec) -> dict[str, Any]:
        if isinstance(item, BookSpec):
            self._replace(item)
            token = item.token
        else:
            token = item
        book = self.books[token]
        market = TOKEN_MARKET[token]
        return {
            "market": MARKETS[market].condition_id,
            "asset_id": TOKEN_IDS[token],
            "timestamp": str(book.timestamp),
            "hash": self.hash(token, book.timestamp),
            "bids": book.bid_levels(),
            "asks": book.ask_levels(),
            "tick_size": self.tick_sizes[token],
            "event_type": "book",
            "last_trade_price": three_decimals(self.trade_prices[market]),
        }

    def _book(self, spec: BookFrame) -> dict[str, Any]:
        if spec.replace is not None:
            self._replace(spec.replace)
        book = self.books[spec.token]
        return {
            "market": MARKETS[TOKEN_MARKET[spec.token]].condition_id,
            "asset_id": TOKEN_IDS[spec.token],
            "bids": book.bid_levels(),
            "asks": book.ask_levels(),
            "hash": BAD_HASH
            if spec.bad_hash
            else self.hash(spec.token, book.timestamp),
            "timestamp": str(book.timestamp),
            "event_type": "book",
        }

    def _price_change(self, spec: PriceChangeFrame) -> dict[str, Any]:
        self._apply(spec.entries, spec.t)
        tokens = {entry.token for entry in spec.entries}
        return self._price_change_object(
            spec.market, spec.t, spec.entries, tokens, spec.bad_hash
        )

    def _price_change_object(
        self,
        market: str,
        t: int,
        entries: Iterable[Entry],
        tokens: set[str],
        bad_hash: bool,
    ) -> dict[str, Any]:
        # Each entry carries its token's book after all of the message's
        # entries for that token, with the message's timestamp.
        after = {
            token: (
                BAD_HASH if bad_hash else self.hash(token, T0 + t),
                self.books[token].best_bid(),
                self.books[token].best_ask(),
            )
            for token in tokens
        }
        return {
            "market": MARKETS[market].condition_id,
            "price_changes": [
                {
                    "asset_id": TOKEN_IDS[entry.token],
                    "price": entry.price,
                    "size": entry.size,
                    "side": entry.side,
                    "hash": after[entry.token][0],
                    "best_bid": after[entry.token][1],
                    "best_ask": after[entry.token][2],
                }
                for entry in entries
            ],
            "timestamp": str(T0 + t),
            "event_type": "price_change",
        }

    def _best_bid_ask(self, spec: BestBidAskFrame) -> dict[str, Any]:
        book = self.books[spec.token]
        best_bid, best_ask = book.best_bid(), book.best_ask()
        return {
            "market": MARKETS[TOKEN_MARKET[spec.token]].condition_id,
            "asset_id": TOKEN_IDS[spec.token],
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": str(Decimal(best_ask) - Decimal(best_bid)),
            "timestamp": str(T0 + spec.t),
            "event_type": "best_bid_ask",
        }

    def _last_trade(self, spec: LastTradeFrame) -> dict[str, Any]:
        market = TOKEN_MARKET[spec.token]
        self.trade_prices[market] = spec.price
        self.trades_sent += 1
        return {
            "market": MARKETS[market].condition_id,
            "asset_id": TOKEN_IDS[spec.token],
            "price": spec.price,
            "size": spec.size,
            "fee_rate_bps": "0",
            "side": spec.side,
            "timestamp": str(T0 + spec.t),
            "event_type": "last_trade_price",
            "transaction_hash": f"0x{self.trades_sent:064x}",
        }

    def _tick_size(self, spec: TickSizeFrame) -> dict[str, Any]:
        old = self.tick_sizes[spec.token]
        self.tick_sizes[spec.token] = spec.new
        return {
            "market": MARKETS[TOKEN_MARKET[spec.token]].condition_id,
            "asset_id": TOKEN_IDS[spec.token],
            "old_tick_size": old,
            "new_tick_size": spec.new,
            "timestamp": str(T0 + spec.t),
            "event_type": "tick_size_change",
        }

    def _resolved(self, spec: ResolvedFrame) -> dict[str, Any]:
        market = MARKETS[spec.market]
        self.resolutions_sent += 1
        return {
            # A string, as the source sends it (docs/source-behavior.md, §6).
            "id": str(9000000 + self.resolutions_sent),
            "market": market.condition_id,
            "assets_ids": [TOKEN_IDS[token] for token in market.tokens],
            "winning_asset_id": TOKEN_IDS[spec.winner],
            "winning_outcome": outcome(spec.winner),
            "event_message": None,
            "timestamp": str(T0 + spec.t),
            "event_type": "market_resolved",
            "tags": [],
        }

    def _replace(self, spec: BookSpec) -> None:
        self.books[spec.token].replace(spec.bids, spec.asks, T0 + spec.t)

    def _apply(self, entries: Iterable[Entry], t: int) -> None:
        for entry in entries:
            self.books[entry.token].apply(entry.side, entry.price, entry.size, T0 + t)


def new_market_ids(n: int) -> tuple[str, tuple[str, str]]:
    """Synthetic new market ``n``'s condition ID and tokens."""
    return f"0x{0xF00 + n:064x}", (f"2{10 * n + 1:076d}", f"2{10 * n + 2:076d}")


def _new_market(spec: NewMarketFrame) -> dict[str, Any]:
    condition_id, tokens = new_market_ids(spec.n)
    return {
        "id": str(9100000 + spec.n),
        "question": f"Synthetic new market {spec.n}?",
        "market": condition_id,
        "slug": f"synthetic-new-{spec.n}",
        "description": "Synthetic.",
        "assets_ids": list(tokens),
        "outcomes": ["Yes", "No"],
        "event_message": None,
        "timestamp": str(T0 + spec.t),
        "event_type": "new_market",
        "tags": [],
        "condition_id": condition_id,
        "active": True,
        "clob_token_ids": list(tokens),
        # A string, as the source sends it (docs/source-behavior.md, §1).
        "game_start_time": "2026-10-11 14:00:00+00",
        "order_price_min_tick_size": "0.01",
    }


def reverse_entries(text: str) -> str:
    """A ``price_change`` frame's text with its entries in reverse order,
    for ``send-again <label> reversed``. A number keeps the text it was
    written with, as a ``send-text`` frame may write a price or a size as a
    number, which a ``float`` would round or overflow."""
    frame = json.loads(
        text, parse_float=_Number, parse_int=_Number, parse_constant=_Number
    )
    if not isinstance(frame, dict) or frame.get("event_type") != "price_change":
        raise ValueError("only a price_change frame can be sent reversed")
    frame["price_changes"] = frame["price_changes"][::-1]
    return _dumps_numbers(frame)


@dataclass(frozen=True, slots=True)
class _Number:
    """A JSON number, as its text."""

    text: str


def _dumps_numbers(value: object) -> str:
    """Compact JSON, writing each ``_Number`` as its text."""
    match value:
        case _Number(text=text):
            return text
        case dict():
            members = (
                f"{dumps(key)}:{_dumps_numbers(item)}" for key, item in value.items()
            )
            return "{" + ",".join(members) + "}"
        case list():
            return "[" + ",".join(_dumps_numbers(item) for item in value) + "]"
        case _:
            return dumps(value)
