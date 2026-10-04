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

With --any-trade-price, a check that still fails is retried with every last
trade price on a 0.001 grid. A match still confirms every level exactly; it
treats only the trade price as unknown, since on some markets the price in
the hash does not follow the stream's last_trade_price events. A check that
fails every way is reported as failed.

min_order_size and neg_risk are not on the stream; they are fetched once per
token from the REST /book endpoint through the SDK. A settled market has no
REST book, so for it they are recovered by trying likely values against the
first stream book's hash.

With --drop-sample K, each of K evenly spaced price_change entries is
skipped in its own replay, to show whether the checks detect a missed event,
how soon, and whether they verify again later.
"""

import argparse
import asyncio
import json
from collections import Counter, defaultdict

from orderbook import Book, book_hash, events, ltp_text


async def fetch_meta(first_books: dict[str, dict]) -> dict[str, dict]:
    from polymarket import AsyncPublicClient
    from polymarket.errors import RequestRejectedError

    meta = {}
    async with AsyncPublicClient() as client:
        for a, book in sorted(first_books.items()):
            try:
                ob = await client.get_order_book(token_id=a)
            except RequestRejectedError:
                # A settled market has no REST book; recover the two fields
                # from the first stream book's hash instead.
                meta[a] = infer_meta(book)
                continue
            meta[a] = {"min_order_size": str(ob.min_order_size), "neg_risk": ob.neg_risk,
                       "tick_size": str(ob.tick_size),
                       "last_trade_price": ltp_text(str(ob.last_trade_price or 0))}
    return meta


def infer_meta(book: dict) -> dict:
    for size in ("5", "1", "10", "15", "20", "50", "100"):
        for neg_risk in (False, True):
            trial = {**book, "min_order_size": size, "neg_risk": neg_risk,
                     "last_trade_price": ltp_text(book["last_trade_price"])}
            if book_hash(trial) == book["hash"]:
                return {"min_order_size": size, "neg_risk": neg_risk,
                        "tick_size": book["tick_size"],
                        "last_trade_price": trial["last_trade_price"]}
    raise SystemExit(f"no REST book and no inferred fields for token {book['asset_id']}")


def replay(evs, meta, drop: int | None = None, any_price_search: bool = False) -> dict:
    stats = Counter()
    books: dict[str, Book] = {}
    ltp: dict[str, str] = {}
    pending: dict[str, tuple] = {}
    late: dict[str, tuple] = {}  # failed checks to retry after the next change
    announced = defaultdict(list)  # market -> [(event index, ltp text)]
    for i, (_, e) in enumerate(evs):
        if e.get("event_type") == "last_trade_price":
            announced[e["market"]].append((i, ltp_text(e["price"])))
    failures, episodes, dropped_at = [], [], None
    open_episodes: dict[str, dict] = {}  # token -> consecutive failed checks

    def count(asset: str, key: str, r: dict, ok: bool, ts: str = "", i: int = 0) -> None:
        stats[key] += 1
        ep = open_episodes.get(asset)
        if ok:
            if ep:
                ep.update(end=r["elapsed"], ended_by=key)
                episodes.append(open_episodes.pop(asset))
            return
        failures.append({"asset": asset, "event_index": i, "elapsed": r["elapsed"], "ts": ts})
        if ep:
            ep["failed"] += 1
        else:
            open_episodes[asset] = {"asset": asset, "start": r["elapsed"], "failed": 1}

    def next_announced(market: str, i: int) -> str | None:
        for j, price in announced[market]:
            if j > i:
                return price
        return None

    def any_price(b: Book, ts: str, want: str) -> bool:
        price = b.any_trade_price(ts, want) if any_price_search else None
        if price is not None:
            ltp[b.market] = price
        return price is not None

    def current(b: Book) -> str:
        return ltp.get(b.market, meta[b.asset_id]["last_trade_price"])

    def verify(asset: str) -> None:
        if asset not in pending:
            return
        want, ts, i, r = pending.pop(asset)
        b = books[asset]
        nxt = next_announced(b.market, i)
        if b.hash(ts, current(b)) == want:
            count(asset, "change groups verified", r, True)
        elif nxt and b.hash(ts, nxt) == want:
            count(asset, "change groups verified with next trade price", r, True)
        elif any_price(b, ts, want):
            count(asset, "change groups verified with another trade price", r, True)
        else:
            late[asset] = (want, ts, i, r)

    def retry_late(asset: str, applied: bool) -> None:
        if asset not in late:
            return
        want, ts, i, r = late.pop(asset)
        b = books[asset]
        if applied and (b.hash(ts, current(b)) == want or any_price(b, ts, want)):
            count(asset, "change groups verified one change late", r, True)
        else:
            count(asset, "change groups failed", r, False, ts, i)

    changes = 0
    for i, (r, e) in enumerate(evs):
        et = e.get("event_type")
        if et == "book":
            a = e["asset_id"]
            verify(a)
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
            nxt = next_announced(b.market, i)
            if b.hash(e["timestamp"], current(b)) == e["hash"]:
                count(a, "book hashes verified", r, True)
            elif nxt and b.hash(e["timestamp"], nxt) == e["hash"]:
                count(a, "book hashes verified with next trade price", r, True)
            elif any_price(b, e["timestamp"], e["hash"]):
                count(a, "book hashes verified with another trade price", r, True)
            else:
                count(a, "book hashes failed", r, False, e["timestamp"], i)
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
    for ep in open_episodes.values():
        episodes.append({**ep, "end": None, "ended_by": "end of capture"})
    stats["price_change entries"] = changes
    return {"stats": stats, "failures": failures, "episodes": episodes,
            "dropped_at": dropped_at}


def describe_episodes(episodes: list[dict]) -> None:
    if not episodes:
        print("  failure episodes: none")
        return
    spans = sorted(ep["end"] - ep["start"] for ep in episodes if ep["end"] is not None)
    print(f"  failure episodes (consecutive failed checks on one token): {len(episodes)};"
          f" failed checks per episode max {max(ep['failed'] for ep in episodes)}")
    if spans:
        print(f"    time to the next verified check: median {spans[len(spans) // 2]:.3f} s,"
              f" max {spans[-1]:.3f} s")
    ended = Counter(ep["ended_by"] for ep in episodes)
    print(f"    ended by: {dict(ended)}")
    for ep in sorted(episodes, key=lambda ep: -ep["failed"])[:5]:
        end = "end of capture" if ep["end"] is None else f"{ep['end']:.3f} s"
        print(f"    token {ep['asset'][:12]}… from {ep['start']:.3f} s to {end},"
              f" {ep['failed']} failed checks, ended by {ep['ended_by']}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("capture")
    p.add_argument("--any-trade-price", action="store_true",
                   help="when a check fails, also try every trade price on a 0.001 grid")
    p.add_argument("--drop-sample", type=int, metavar="K",
                   help="skip each of K evenly spaced price_change entries, one replay each")
    args = p.parse_args()

    records = [json.loads(line) for line in open(args.capture)]
    evs = [(r, e) for r, e in events(records)
           if e.get("event_type") not in ("new_market", "best_bid_ask", "market_resolved")]
    first_books = {}
    for _, e in evs:
        if e.get("event_type") == "book":
            first_books.setdefault(e["asset_id"], e)
    assets = set(first_books)
    meta = asyncio.run(fetch_meta(first_books))

    result = replay(evs, meta, any_price_search=args.any_trade_price)
    print(f"== {args.capture}: {len(assets)} tokens, {len(evs)} book-related events")
    for k, v in sorted(result["stats"].items()):
        print(f"  {k}: {v}")
    describe_episodes(result["episodes"])

    if args.drop_sample:
        report_drops(evs, meta, result, args.drop_sample, args.any_trade_price)


def report_drops(evs, meta, baseline: dict, k: int, any_price_search: bool) -> None:
    """Skip each of k evenly spaced price_change entries in turn, and report
    whether the skip shows up as new failed checks on that token."""
    total = baseline["stats"]["price_change entries"]
    known = {(f["asset"], f["event_index"]) for f in baseline["failures"]}
    detected, delays, unresolved, undetected = 0, [], 0, {}
    print(f"\n== Skipping {k} of {total} price_change entries, one replay each")
    for j in range(k):
        n = int((j + 0.5) * total / k) + 1
        res = replay(evs, meta, drop=n, any_price_search=any_price_search)
        d = res["dropped_at"]
        new = [f for f in res["failures"] if f["asset"] == d["asset"]
               and f["event_index"] >= d["event_index"]
               and (f["asset"], f["event_index"]) not in known]
        if new:
            detected += 1
            delays.append(new[0]["elapsed"] - d["elapsed"])
            still = any(ep["asset"] == d["asset"] and ep["end"] is None
                        for ep in res["episodes"])
            unresolved += still
            outcome = (f"detected after {delays[-1]:.3f} s; {len(new)} new failed checks;"
                       f" {'still failing at end' if still else 'checks verify again later'}")
        else:
            outcome = "not detected: " + why_undetected(evs, d)
            undetected[outcome] = undetected.get(outcome, 0) + 1
        c = d["change"]
        print(f"  entry {n} at {d['elapsed']:.3f} s ({c['side']} {c['price']} -> {c['size']}):"
              f" {outcome}")
    delays.sort()
    print(f"  detected {detected} of {k}"
          + (f"; delay median {delays[len(delays) // 2]:.3f} s, max {delays[-1]:.3f} s;"
             f" still failing at end of capture: {unresolved}" if delays else ""))
    for outcome, n in sorted(undetected.items()):
        print(f"  {outcome}: {n}")


def why_undetected(evs, d: dict) -> str:
    """Say why a skipped entry left no failed check, where the capture shows it."""
    c = d["change"]
    for i, (_, e) in enumerate(evs):
        if i < d["event_index"] or e.get("event_type") != "price_change":
            continue
        for later in e["price_changes"]:
            if later is c or later["asset_id"] != c["asset_id"]:
                continue
            if (later["side"], later["price"]) == (c["side"], c["price"]):
                return "the token's next change set the same level again"
            return "unexplained"
    return "no later change on that token in the capture"


if __name__ == "__main__":
    main()
