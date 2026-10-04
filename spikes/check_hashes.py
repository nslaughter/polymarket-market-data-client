# /// script
# requires-python = ">=3.12"
# dependencies = ["polymarket-client==0.12.0"]
# ///
"""Check a capture's order-book hashes against locally replayed books.

Replays a capture from capture.py. Each token's book starts from its first
`book` event and takes every `price_change` entry in receipt order. A run of
consecutive entries for one token that share a hash is checked once, after
its last entry, by recomputing the hash locally (see orderbook.py). A later
`book` event is compared level by level with the replayed book, and its own
hash is checked, before it replaces the book.

Two retries are counted separately from checks that verify at once:

- A trade's price enters the hash before the stream's `last_trade_price`
  event announces it, so a check that fails with the current last trade price
  is retried with the market's next announced one, as a client could by
  holding the check until that event arrives.
- Some hashes already include the token's next change, which arrives a
  millisecond or two later with its own hash. A check that fails is retried
  once after the token's next change, with the failed entry's timestamp.

A check that fails both ways is reported as failed.

min_order_size and neg_risk are not on the stream; they are fetched once per
token from the REST /book endpoint through the SDK.

With --drop N, the Nth price_change entry is skipped, to show whether the
check detects a missed event and when the replayed book agrees again.
"""

import argparse
import asyncio
import json
from collections import Counter, defaultdict

from orderbook import Book, events, ltp_text


async def fetch_meta(assets: set[str]) -> dict[str, dict]:
    from polymarket import AsyncPublicClient

    meta = {}
    async with AsyncPublicClient() as client:
        for a in sorted(assets):
            ob = await client.get_order_book(token_id=a)
            meta[a] = {"min_order_size": str(ob.min_order_size), "neg_risk": ob.neg_risk,
                       "tick_size": str(ob.tick_size),
                       "last_trade_price": ltp_text(str(ob.last_trade_price or 0))}
    return meta


def replay(evs, meta, drop: int | None = None) -> dict:
    stats = Counter()
    books: dict[str, Book] = {}
    ltp: dict[str, str] = {}
    pending: dict[str, tuple] = {}
    late: dict[str, tuple] = {}  # failed checks to retry after the next change
    announced = defaultdict(list)  # market -> [(event index, ltp text)]
    for i, (_, e) in enumerate(evs):
        if e.get("event_type") == "last_trade_price":
            announced[e["market"]].append((i, ltp_text(e["price"])))
    failures, dropped_at = [], None

    def next_announced(market: str, i: int) -> str | None:
        for j, price in announced[market]:
            if j > i:
                return price
        return None

    def verify(asset: str) -> None:
        if asset not in pending:
            return
        want, ts, i, r = pending.pop(asset)
        b = books[asset]
        if b.hash(ts, ltp.get(b.market, meta[asset]["last_trade_price"])) == want:
            stats["change groups verified"] += 1
            return
        nxt = next_announced(b.market, i)
        if nxt and b.hash(ts, nxt) == want:
            stats["change groups verified with next trade price"] += 1
            return
        late[asset] = (want, ts, i, r)

    def retry_late(asset: str, applied: bool) -> None:
        if asset not in late:
            return
        want, ts, i, r = late.pop(asset)
        b = books[asset]
        if applied and b.hash(ts, ltp.get(b.market, meta[asset]["last_trade_price"])) == want:
            stats["change groups verified one change late"] += 1
            return
        stats["change groups failed"] += 1
        failures.append({"asset": asset, "event_index": i, "elapsed": r["elapsed"],
                         "ts": ts})

    changes = 0
    for i, (r, e) in enumerate(evs):
        et = e.get("event_type")
        if et == "book":
            a = e["asset_id"]
            verify(a)
            if a in late:
                retry_late(a, applied=False)
            if "last_trade_price" in e:
                ltp[e["market"]] = ltp_text(e["last_trade_price"])
            b = books.get(a)
            if b is None:
                m = meta[a]
                b = books[a] = Book(e["market"], a, m["min_order_size"], m["neg_risk"],
                                    e.get("tick_size", m["tick_size"]))
            else:
                new = ({lv["price"]: lv["size"] for lv in e["bids"]},
                       {lv["price"]: lv["size"] for lv in e["asks"]})
                stats["later book events matching replay" if b.levels() == new
                      else "later book events differing from replay"] += 1
            if "tick_size" in e:
                b.tick_size = e["tick_size"]
            b.load(e["bids"], e["asks"])
            cur = ltp.get(b.market, meta[a]["last_trade_price"])
            nxt = next_announced(b.market, i)
            if b.hash(e["timestamp"], cur) == e["hash"]:
                stats["book hashes verified"] += 1
            elif nxt and b.hash(e["timestamp"], nxt) == e["hash"]:
                stats["book hashes verified with next trade price"] += 1
            else:
                stats["book hashes failed"] += 1
        elif et == "price_change":
            for c in e["price_changes"]:
                a = c["asset_id"]
                if a not in books:
                    stats["changes before first book"] += 1
                    continue
                changes += 1
                if a in pending and pending[a][0] != c["hash"]:
                    verify(a)
                if changes == drop:
                    dropped_at = {"asset": a, "event_index": i, "elapsed": r["elapsed"],
                                  "change": c}
                    continue
                books[a].apply(c["side"], c["price"], c["size"])
                retry_late(a, applied=True)
                pending[a] = (c["hash"], e["timestamp"], i, r)
        elif et == "last_trade_price":
            ltp[e["market"]] = ltp_text(e["price"])
        elif et == "tick_size_change":
            if e["asset_id"] in books:
                books[e["asset_id"]].tick_size = e["new_tick_size"]
            stats["tick size changes"] += 1
    for a in list(pending):
        verify(a)
    for a in list(late):
        retry_late(a, applied=False)
    stats["price_change entries"] = changes
    return {"stats": stats, "failures": failures, "dropped_at": dropped_at}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("capture")
    p.add_argument("--drop", type=int, action="append",
                   help="skip the Nth price_change entry; repeatable, one replay each")
    args = p.parse_args()

    records = [json.loads(line) for line in open(args.capture)]
    evs = [(r, e) for r, e in events(records)
           if e.get("event_type") not in ("new_market", "best_bid_ask", "market_resolved")]
    assets = {e["asset_id"] for _, e in evs if e.get("event_type") == "book"}
    meta = asyncio.run(fetch_meta(assets))

    result = replay(evs, meta)
    print(f"== {args.capture}: {len(assets)} tokens, {len(evs)} book-related events")
    for k, v in sorted(result["stats"].items()):
        print(f"  {k}: {v}")
    for f in result["failures"][:10]:
        print(f"  failed: {f}")

    for n in args.drop or []:
        res = replay(evs, meta, drop=n)
        d = res["dropped_at"]
        print(f"\n== Replay skipping price_change entry {n}: token {d['asset'][:12]}…"
              f" at {d['elapsed']:.3f} s, {d['change']['side']} {d['change']['price']}"
              f" -> {d['change']['size']}")
        after = [f for f in res["failures"] if f["event_index"] >= d["event_index"]]
        same = [f for f in after if f["asset"] == d["asset"]]
        if same:
            print(f"  first failed check on that token at {same[0]['elapsed']:.3f} s"
                  f" ({same[0]['elapsed'] - d['elapsed']:.3f} s after the skipped entry);"
                  f" {len(same)} failed checks on it in all, last at"
                  f" {same[-1]['elapsed']:.3f} s")
        else:
            print("  no failed check on that token: the skipped entry left no trace in"
                  " the hashes")
        print(f"  later book events differing from replay:"
              f" {res['stats']['later book events differing from replay']}")


if __name__ == "__main__":
    main()
