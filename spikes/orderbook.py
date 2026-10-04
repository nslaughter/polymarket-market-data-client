# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Order-book replay and hash checks shared by the spike scripts.

Imported by check_hashes.py and snapshot_join.py; not run on its own.

The CLOB `hash` on a REST book, a stream `book` event, and each stream
`price_change` entry is the SHA-1 of the compact JSON of the token's book in
the REST response's key order, with `hash` set to "":

  market, asset_id, timestamp, hash, bids (ascending price),
  asks (descending price), min_order_size, tick_size, neg_risk,
  last_trade_price

This is not documented; it was found by reproducing REST and stream hashes.
Stream events omit min_order_size and neg_risk, and book events after the
first omit tick_size and last_trade_price, so those come from REST and from
earlier events. last_trade_price is per market, written with three decimals.
"""

import hashlib
import json
from dataclasses import dataclass, field
from decimal import Decimal


def book_hash(book: dict) -> str:
    """Hash a REST-shaped book dict (its own `hash` field is ignored)."""
    obj = {"market": book["market"], "asset_id": book["asset_id"],
           "timestamp": book["timestamp"], "hash": "", "bids": book["bids"],
           "asks": book["asks"], "min_order_size": book["min_order_size"],
           "tick_size": book["tick_size"], "neg_risk": book["neg_risk"],
           "last_trade_price": book["last_trade_price"]}
    return hashlib.sha1(json.dumps(obj, separators=(",", ":")).encode()).hexdigest()


def ltp_text(price: str) -> str:
    return f"{Decimal(price):.3f}"


@dataclass
class Book:
    market: str
    asset_id: str
    min_order_size: str
    neg_risk: bool
    tick_size: str
    bids: dict[str, str] = field(default_factory=dict)
    asks: dict[str, str] = field(default_factory=dict)

    def load(self, bids: list[dict], asks: list[dict]) -> None:
        self.bids = {lv["price"]: lv["size"] for lv in bids}
        self.asks = {lv["price"]: lv["size"] for lv in asks}

    def apply(self, side: str, price: str, size: str) -> None:
        levels = self.bids if side.upper() == "BUY" else self.asks
        if Decimal(size) == 0:
            levels.pop(price, None)
        else:
            levels[price] = size

    def as_rest(self, timestamp: str, last_trade_price: str) -> dict:
        return {
            "market": self.market, "asset_id": self.asset_id, "timestamp": timestamp,
            "bids": [{"price": p, "size": s}
                     for p, s in sorted(self.bids.items(), key=lambda kv: Decimal(kv[0]))],
            "asks": [{"price": p, "size": s}
                     for p, s in sorted(self.asks.items(), key=lambda kv: -Decimal(kv[0]))],
            "min_order_size": self.min_order_size, "tick_size": self.tick_size,
            "neg_risk": self.neg_risk, "last_trade_price": last_trade_price,
        }

    def hash(self, timestamp: str, last_trade_price: str) -> str:
        return book_hash(self.as_rest(timestamp, last_trade_price))

    def levels(self) -> tuple[dict, dict]:
        return dict(self.bids), dict(self.asks)


def events(records: list[dict]):
    """Yield (record, event) for every market event in received frames."""
    for r in records:
        if r.get("kind") not in ("recv", "ref_recv") or r["text"] == "PONG":
            continue
        d = json.loads(r["text"])
        for e in d if isinstance(d, list) else [d]:
            yield r, e
