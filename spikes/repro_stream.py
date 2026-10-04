# /// script
# requires-python = ">=3.12"
# dependencies = ["polymarket-client==0.12.0", "websockets==15.0.1"]
# ///
"""Reproduce market-stream behavior the documentation does not state.

idle-close
    Opens two connections to an already-settled market without
    custom_feature_enabled, so no market data flows: one sends no PING, one
    sends PING every 10 seconds. Reproduced when the server closes the quiet
    connection without a close frame while the one sending PING stays open.

no-replay
    Keeps a reference connection open to a busy long-dated market while a
    second connection closes and, 15 seconds later, reopens. Reproduced when
    the reference received book changes during the gap, none of them reach the
    reopened connection, and it starts again from fresh books.

slow-consumer
    Subscribes to the current 5- and 15-minute Bitcoin markets, reconnecting
    after each close, and records how each connection ends. Reproduced when
    the server ends a connection, with 1013 "slow consumer" or without a close
    frame. Whether it happens depends on the markets' activity.

duplicates
    Records the current 15-minute Bitcoin market and counts price_change
    messages received twice and events whose source timestamp is earlier than
    the previous event's for the same token. Reproduced when either occurs.

Each subcommand runs live and writes a capture under spikes/captures/repro/,
or with --capture FILE analyzes existing captures instead, including those
from capture.py.
"""

import argparse
import asyncio
import json
import time
from collections import Counter
from datetime import timedelta

import live


async def hold(sock: live.MarketSocket, seconds: float) -> None:
    try:
        await asyncio.wait_for(sock.closed.wait(), seconds)
    except TimeoutError:
        pass


def conns(records: list[dict]) -> list[str]:
    seen = []
    for r in records:
        c = r.get("conn")
        if r["kind"] == "open" and c not in seen:
            seen.append(c)
    return seen


def lifetime(records: list[dict], conn: str) -> tuple[float, float | None]:
    opened = next(r["t"] for r in records if r.get("conn") == conn and r["kind"] == "open")
    closed = [r["t"] for r in records if r.get("conn") == conn and r["kind"] == "close"]
    return opened, (closed[-1] if closed else None)


def sent_pings(records: list[dict], conn: str) -> int:
    return sum(1 for r in records if r.get("conn") == conn and r["kind"] == "sent"
               and r.get("text") == "PING")


# --- idle-close --------------------------------------------------------------


async def collect_idle_close(args) -> str:
    market = await live.select("settled")
    rec = live.Recorder("idle-close")
    rec.write("note", markets=[market])
    print(f"settled market: {market['slug']}; waiting up to {args.duration:.0f} s")
    quiet = await live.MarketSocket(rec, "no-ping", market["token_ids"], custom=False,
                                    ping=None).open()
    pinging = await live.MarketSocket(rec, "ping", market["token_ids"], custom=False,
                                      ping=10).open()
    await hold(quiet, args.duration)
    await hold(pinging, 20)
    await quiet.close()
    await pinging.close()
    return str(rec.path)


def analyze_idle_close(paths: list[str]) -> None:
    recs = live.load(paths)
    result = {}
    print(f"== {', '.join(paths)}")
    for c in conns(recs):
        opened, closed = lifetime(recs, c)
        data = [e for r, e in live.frames(recs, c)]
        first = next((r["text"].strip() for r in recs if r.get("conn") == c
                      and r["kind"] == "recv" and r["text"] != "PONG"), None)
        how = live.how_closed(recs, c)
        pings = sent_pings(recs, c)
        span = (closed - opened) if closed else None
        print(f"  {c}: sent {pings} PING; first frame {first!r}; {len(data)} market events;"
              f" {how}" + (f" after {span:.1f} s" if span else ""))
        result[c] = (pings, how, span)
    quiet = [v for v in result.values() if v[0] == 0]
    pinging = [v for v in result.values() if v[0] > 0]
    dropped = [v for v in quiet if v[1] == "closed by the server without a close frame"]
    if dropped and all(v[1] != dropped[0][1] for v in pinging):
        live.verdict("idle-close", "REPRODUCED",
                     f"a connection with no traffic was closed without a close frame after"
                     f" {dropped[0][2]:.1f} s; the one sending PING was not", paths)
    elif dropped:
        live.verdict("idle-close", "INCONCLUSIVE",
                     "the quiet connection was dropped, but so was the one sending PING",
                     paths)
    else:
        live.verdict("idle-close", "NOT REPRODUCED",
                     "no quiet connection was closed by the server", paths)


# --- no-replay ---------------------------------------------------------------


async def collect_no_replay(args) -> str:
    market = await live.select("long")
    rec = live.Recorder("no-replay")
    rec.write("note", markets=[market], gap=args.gap)
    print(f"market: {market['slug']}; gap of {args.gap:.0f} s")
    ref = await live.MarketSocket(rec, "reference", market["token_ids"]).open()
    test = await live.MarketSocket(rec, "test", market["token_ids"]).open()
    await asyncio.sleep(20)
    await test.close()
    await asyncio.sleep(args.gap)
    test2 = await live.MarketSocket(rec, "test-2", market["token_ids"]).open()
    await asyncio.sleep(25)
    await test2.close()
    await ref.close()
    return str(rec.path)


def book_states(recs: list[dict], conn: str) -> list[tuple[float, str, str, str]]:
    """(receipt time, token, hash, event type) for every book state on a connection."""
    out = []
    for r, e in live.frames(recs, conn):
        if e.get("event_type") == "price_change":
            out += [(r["t"], c["asset_id"], c["hash"], "price_change")
                    for c in e["price_changes"]]
        elif e.get("event_type") == "book":
            out.append((r["t"], e["asset_id"], e["hash"], "book"))
    return out


def analyze_no_replay(paths: list[str]) -> None:
    recs = live.load(paths)
    ref = book_states(recs, "reference")
    _, gap_start = lifetime(recs, "test")
    reopened, _ = lifetime(recs, "test-2")
    after = book_states(recs, "test-2")
    gap = [s for s in ref if gap_start < s[0] < reopened]
    # The reopened connection's opening books match the source's latest state;
    # a replay would deliver the changes in between as price_change entries.
    after_changes = {(a, h) for _, a, h, kind in after if kind == "price_change"}
    replayed = [s for s in gap if (s[1], s[2]) in after_changes]
    first = next(r for r in recs if r.get("conn") == "test-2" and r["kind"] == "recv")
    d = json.loads(first["text"])
    opening = [e.get("event_type") for e in (d if isinstance(d, list) else [d])]
    matched = latest = 0
    for _, e in live.frames([first]):
        if e.get("event_type") != "book":
            continue
        seen = [s for s in ref if s[1] == e["asset_id"] and s[0] <= first["t"]]
        matched += any(s[2] == e["hash"] for s in ref if s[1] == e["asset_id"])
        latest += bool(seen) and seen[-1][2] == e["hash"]
    print(f"== {', '.join(paths)}")
    print(f"  gap: {reopened - gap_start:.1f} s; the reference received {len(gap)} book states"
          f" in it; the reopened connection received {len(replayed)} of them as changes")
    print(f"  reopened connection's first frame: {opening}; {matched} of its books match a"
          f" reference state, {latest} the reference's latest")
    opening_states = {(e["asset_id"], e["hash"]) for _, e in live.frames([first])
                      if e.get("event_type") == "book"}
    lost = [s for s in gap if (s[1], s[2]) not in opening_states]
    if gap and not replayed and opening and set(opening) == {"book"}:
        live.verdict("no-replay", "REPRODUCED",
                     f"{len(lost)} of the gap's {len(gap)} book states never arrived after"
                     " reconnecting; the stream started again from fresh books holding"
                     " only the latest state", paths)
    elif not gap:
        live.verdict("no-replay", "INCONCLUSIVE",
                     "the market did not change during the gap", paths)
    else:
        live.verdict("no-replay", "NOT REPRODUCED",
                     "some gap events arrived after reconnecting, or the stream did not"
                     " start from books", paths)


# --- slow-consumer -----------------------------------------------------------


async def collect_slow_consumer(args) -> str:
    m15 = await live.select("ending", prefix="btc-updown-15m-",
                            min_ahead=timedelta(seconds=args.duration))
    m5 = await live.select("ending", prefix="btc-updown-5m-",
                           min_ahead=timedelta(seconds=60))
    rec = live.Recorder("slow-consumer")
    rec.write("note", markets=[m15, m5])
    assets = m15["token_ids"] + m5["token_ids"]
    print(f"markets: {m15['slug']}, {m5['slug']}; recording {args.duration:.0f} s")
    deadline = time.monotonic() + args.duration
    n = 0
    while time.monotonic() < deadline:
        n += 1
        sock = await live.MarketSocket(rec, f"c{n}", assets).open()
        await hold(sock, deadline - time.monotonic())
        await sock.close()
        await asyncio.sleep(1)
    return str(rec.path)


def analyze_slow_consumer(paths: list[str]) -> None:
    recs = live.load(paths)
    print(f"== {', '.join(paths)}")
    ended = []
    for c in conns(recs):
        opened, closed = lifetime(recs, c)
        how = live.how_closed(recs, c)
        pings = [r["mono"] for r in recs if r.get("conn") == c and r["kind"] == "sent"
                 and r.get("text") == "PING"]
        pongs = [r["mono"] for r in recs if r.get("conn") == c and r["kind"] == "recv"
                 and r["text"] == "PONG"]
        rtt = max((b - a for a, b in zip(pings, pongs)), default=0)
        lags = [r["t"] - int(e["timestamp"]) / 1000 for r, e in live.frames(recs, c)
                if e.get("timestamp") and e.get("event_type") != "book"]
        span = (closed or recs[-1]["t"]) - opened
        print(f"  {c}: {span:.0f} s, {how}; slowest PONG {rtt:.2f} s;"
              f" largest receipt lag {max(lags, default=0):.2f} s")
        if how.startswith("server") or how.endswith("without a close frame"):
            ended.append((c, how))
    if ended:
        live.verdict("slow-consumer", "REPRODUCED",
                     f"the server ended {len(ended)} connection(s): "
                     + "; ".join(f"{c} {how}" for c, how in ended), paths)
    else:
        live.verdict("slow-consumer", "NOT REPRODUCED",
                     "no connection was ended by the server in this period", paths)


# --- duplicates --------------------------------------------------------------


async def collect_duplicates(args) -> str:
    market = await live.select("ending", prefix="btc-updown-15m-",
                               min_ahead=timedelta(seconds=args.duration))
    rec = live.Recorder("duplicates")
    rec.write("note", markets=[market])
    print(f"market: {market['slug']}; recording {args.duration:.0f} s")
    sock = await live.MarketSocket(rec, "c1", market["token_ids"]).open()
    await hold(sock, args.duration)
    await sock.close()
    return str(rec.path)


def analyze_duplicates(paths: list[str]) -> None:
    recs = live.load(paths)
    seen: dict[tuple, float] = {}
    repeats, gaps = Counter(), []
    last: dict[tuple, tuple[int, str]] = {}
    regress = Counter()
    for r, e in live.frames(recs):
        et = e.get("event_type")
        if et in ("new_market", "market_resolved"):
            continue
        # A repeat can list the same price_change entries in another order.
        body = dict(e)
        if et == "price_change":
            body["price_changes"] = sorted(json.dumps(c, sort_keys=True)
                                           for c in e["price_changes"])
        key = (r.get("conn"), json.dumps(body, sort_keys=True))
        if key in seen:
            repeats[et] += 1
            gaps.append(r["t"] - seen[key])
        seen[key] = r["t"]
        if not e.get("timestamp"):
            continue
        ts = int(e["timestamp"])
        tokens = ([c["asset_id"] for c in e["price_changes"]] if et == "price_change"
                  else [e.get("asset_id")])
        for tok in tokens:
            k = (r.get("conn"), tok)
            prev = last.get(k)
            if prev and ts < prev[0]:
                regress[(prev[1], et)] += 1
            else:
                last[k] = (ts, et)
    book_types = {"book", "price_change"}
    book_regress = sum(n for (a, b), n in regress.items() if {a, b} <= book_types)
    print(f"== {', '.join(paths)}")
    print(f"  repeated messages: {dict(repeats) or 0}"
          + (f", up to {max(gaps) * 1000:.0f} ms apart" if gaps else ""))
    print(f"  per-token timestamp regressions by (earlier event, later event):"
          f" {dict(regress) or 0}")
    if repeats:
        live.verdict("duplicates", "REPRODUCED",
                     f"{sum(repeats.values())} messages arrived again with the same content",
                     paths)
    else:
        live.verdict("duplicates", "NOT REPRODUCED", "no message arrived twice", paths)
    if regress:
        live.verdict("out-of-order", "REPRODUCED",
                     f"{sum(regress.values())} events arrived after a later-stamped event for"
                     f" the same token; {book_regress} of them between book and price_change",
                     paths)
    else:
        live.verdict("out-of-order", "NOT REPRODUCED",
                     "every token's events arrived in timestamp order", paths)


CHECKS = {
    "idle-close": (collect_idle_close, analyze_idle_close, 240),
    "no-replay": (collect_no_replay, analyze_no_replay, 15),
    "slow-consumer": (collect_slow_consumer, analyze_slow_consumer, 600),
    "duplicates": (collect_duplicates, analyze_duplicates, 300),
}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="check", required=True)
    for name, (_, _, default) in CHECKS.items():
        s = sub.add_parser(name)
        s.add_argument("--capture", action="append",
                       help="analyze this capture instead of running live; repeatable")
        if name == "no-replay":
            s.add_argument("--gap", type=float, default=default, help="seconds disconnected")
        else:
            s.add_argument("--duration", type=float, default=default,
                           help="seconds to record")
    args = p.parse_args()
    collect, analyze, _ = CHECKS[args.check]
    analyze(args.capture or [asyncio.run(collect(args))])


if __name__ == "__main__":
    main()
