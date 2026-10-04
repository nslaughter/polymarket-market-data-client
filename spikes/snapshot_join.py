# /// script
# requires-python = ">=3.12"
# dependencies = ["polymarket-client==0.12.0", "websockets==15.0.1"]
# ///
"""Join REST order-book snapshots to the market stream and check the joins.

Keeps one raw stream connection open (custom_feature_enabled set, PING every
10 seconds) and, every --every seconds, fetches each token's REST book
through the SDK, recording when each request started and finished. Both go
to one JSON Lines file.

--summarize FILE replays the stream (see orderbook.py) and, for each REST
snapshot:

- checks that its hash recomputes from its own contents;
- looks for the same hash among the stream's states for that token, and
  where the stream reached it relative to the request;
- compares the snapshot's levels with the replayed stream book at that
  state (a join by hash);
- compares them with the replayed book after the last stream entry whose
  timestamp is at or before the snapshot's (a join by timestamp), and counts
  joins where several entries share that timestamp, which makes the rule
  ambiguous.

A join that matches level for level neither loses nor repeats a change: the
stream's later entries apply to exactly the state the snapshot holds.
"""

import argparse
import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path

from websockets.asyncio.client import connect

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class Recorder:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("a", buffering=1)
        self.start = time.monotonic()

    def write(self, kind: str, **fields) -> None:
        now = time.monotonic()
        rec = {"t": time.time(), "mono": now, "elapsed": round(now - self.start, 6),
               "kind": kind, **fields}
        self._file.write(json.dumps(rec) + "\n")


async def stream(rec: Recorder, assets: list[str], stop: asyncio.Event) -> None:
    async with connect(URL, ping_interval=None, max_size=None) as ws:
        await ws.send(json.dumps({"type": "market", "assets_ids": assets,
                                  "custom_feature_enabled": True}))
        rec.write("open")

        async def ping():
            while True:
                await asyncio.sleep(10)
                await ws.send("PING")

        async def read():
            async for msg in ws:
                if msg != "PONG":
                    rec.write("recv", text=msg)

        tasks = [asyncio.create_task(ping()), asyncio.create_task(read())]
        await stop.wait()
        for t in tasks:
            t.cancel()


async def snapshots(rec: Recorder, assets: list[str], every: float,
                    stop: asyncio.Event) -> None:
    from polymarket import AsyncPublicClient

    async with AsyncPublicClient() as client:
        while not stop.is_set():
            for a in assets:
                t0, w0 = time.monotonic(), time.time()
                ob = await client.get_order_book(token_id=a)
                t1, w1 = time.monotonic(), time.time()
                rec.write("rest", asset=a, market=ob.market, req_mono=[t0, t1],
                          req_t=[w0, w1],
                          timestamp=str(round(ob.timestamp.timestamp() * 1000)),
                          hash=ob.hash,
                          bids=[{"price": str(lv.price), "size": str(lv.size)} for lv in ob.bids],
                          asks=[{"price": str(lv.price), "size": str(lv.size)} for lv in ob.asks],
                          min_order_size=str(ob.min_order_size), tick_size=str(ob.tick_size),
                          neg_risk=ob.neg_risk,
                          last_trade_price=None if ob.last_trade_price is None
                          else str(ob.last_trade_price))
            await asyncio.sleep(every)


async def run(args) -> None:
    rec = Recorder(Path(args.out))
    assets = []
    for line in Path(args.markets).read_text().splitlines():
        m = json.loads(line)
        if m["slug"] in args.slug:
            assets.extend(m["token_ids"])
    rec.write("note", assets=assets, every=args.every, duration=args.duration)
    stop = asyncio.Event()
    tasks = [asyncio.create_task(stream(rec, assets, stop))]
    await asyncio.sleep(3)  # let the stream deliver its first books
    tasks.append(asyncio.create_task(snapshots(rec, assets, args.every, stop)))
    await asyncio.sleep(args.duration)
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)


def summarize(path: str) -> None:
    from orderbook import Book, book_hash, events

    recs = [json.loads(line) for line in open(path)]
    rests = [r for r in recs if r["kind"] == "rest"]
    meta = {r["asset"]: r for r in rests}

    # Replay the stream; keep each token's states in receipt order.
    states = defaultdict(list)  # token -> [(mono, ts, hash, bids, asks)]
    books: dict[str, Book] = {}
    for r, e in events(recs):
        et = e.get("event_type")
        if et == "book":
            a = e["asset_id"]
            if a not in meta:
                continue
            m = meta[a]
            b = books.setdefault(a, Book(e["market"], a, m["min_order_size"], m["neg_risk"],
                                         m["tick_size"]))
            b.load(e["bids"], e["asks"])
            states[a].append((r["mono"], int(e["timestamp"]), e["hash"], *b.levels()))
        elif et == "price_change":
            for c in e["price_changes"]:
                a = c["asset_id"]
                if a in books:
                    books[a].apply(c["side"], c["price"], c["size"])
                    states[a].append((r["mono"], int(e["timestamp"]), c["hash"],
                                      *books[a].levels()))

    counts = defaultdict(int)
    examples = []
    ahead = []  # how long after the REST response the stream reached its state
    for s in rests:
        a, ts = s["asset"], int(s["timestamp"])
        rest_levels = ({lv["price"]: lv["size"] for lv in s["bids"]},
                       {lv["price"]: lv["size"] for lv in s["asks"]})
        counts["snapshots"] += 1
        counts["REST hash recomputes" if book_hash({**s, "asset_id": a}) == s["hash"]
               else "REST hash does not recompute"] += 1
        seq = states.get(a, [])
        hit = [st for st in seq if st[2] == s["hash"]]
        if hit:
            mono = hit[-1][0]
            when = ("before the request" if mono < s["req_mono"][0] else
                    "during the request" if mono <= s["req_mono"][1] else
                    "after the response")
            counts[f"hash found in stream, reached {when}"] += 1
            if mono > s["req_mono"][1]:
                ahead.append(mono - s["req_mono"][1])
            counts["hash join: levels match" if (hit[-1][3], hit[-1][4]) == rest_levels
                   else "hash join: levels differ"] += 1
        else:
            first = seq[0][1] if seq else None
            reason = ("snapshot older than the stream's first state"
                      if first is not None and ts < first else "no stream state with that hash")
            counts[f"hash not found in stream ({reason})"] += 1
            if len(examples) < 5:
                examples.append((s["elapsed"], a[:12], ts, s["hash"][:8]))
        upto = [st for st in seq if st[1] <= ts]
        if upto:
            same_ts = sum(1 for st in seq if st[1] == ts)
            if same_ts > 1:
                counts["timestamp join ambiguous (several entries share it)"] += 1
            counts["timestamp join: levels match" if (upto[-1][3], upto[-1][4]) == rest_levels
                   else "timestamp join: levels differ"] += 1
        lag = s["req_mono"][1] - s["req_mono"][0]
        counts["_request_ms_total"] += round(lag * 1000)
    n = counts["snapshots"]
    print(f"== {path}: {n} REST snapshots of {len(meta)} tokens")
    for k, v in sorted(counts.items()):
        if not k.startswith("_"):
            print(f"  {k}: {v}")
    print(f"  mean REST request time: {counts['_request_ms_total'] / max(n, 1):.0f} ms")
    if ahead:
        ahead.sort()
        print(f"  when REST was ahead, the stream caught up after: median"
              f" {ahead[len(ahead) // 2] * 1000:.0f} ms, max {ahead[-1] * 1000:.0f} ms")
    for x in examples:
        print(f"  not found: at {x[0]:.1f} s token {x[1]}… ts {x[2]} hash {x[3]}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--markets", help="JSON Lines file from select_markets.py")
    p.add_argument("--slug", action="append", help="market slug; repeatable")
    p.add_argument("--every", type=float, default=3, help="seconds between REST rounds")
    p.add_argument("--duration", type=float, default=300)
    p.add_argument("--out")
    p.add_argument("--summarize", metavar="FILE")
    args = p.parse_args()
    if args.summarize:
        summarize(args.summarize)
    elif args.markets and args.slug and args.out:
        asyncio.run(run(args))
    else:
        p.error("--markets, --slug and --out are required to run")


if __name__ == "__main__":
    main()
