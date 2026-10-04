# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Report event ordering, duplicates, receipt lag, and PONG timing in captures.

Reads JSON Lines files from capture.py and prints, per file:

- event counts by type, and how many frames were JSON arrays of events;
- for each token, market, and the whole connection, how often an event's
  source timestamp is earlier than the previous event's (a regression), and
  by how much, in receipt order;
- events received more than once (same type, token, timestamp, and content);
- receipt time minus source timestamp, which mixes network delay, server
  queueing, and clock offset;
- time from each PING sent to the next PONG received.

new_market events are broadcast for markets that were not subscribed and are
left out of the ordering checks.
"""

import argparse
import json
import statistics
from collections import Counter


def pct(values: list[float], q: float) -> float:
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def summarize(values: list[float]) -> str:
    if not values:
        return "none"
    return (f"n={len(values)} median={statistics.median(values):.3f}"
            f" p95={pct(values, 0.95):.3f} p99={pct(values, 0.99):.3f}"
            f" max={max(values):.3f}")


def entries(e: dict):
    """Yield (token, market, timestamp ms, identity) for each part of an event."""
    et = e.get("event_type")
    ts = int(e["timestamp"]) if e.get("timestamp") else None
    if et == "price_change":
        for c in e["price_changes"]:
            yield c["asset_id"], e["market"], ts, (et, c["asset_id"], ts, c["price"],
                                                   c["side"], c["size"], c["hash"])
    elif et == "market_resolved":
        yield None, e["market"], ts, (et, e["market"], ts)
    else:
        yield e.get("asset_id"), e.get("market"), ts, (et, e.get("asset_id"), ts,
                                                       json.dumps(e, sort_keys=True))


def report(path: str) -> None:
    recs = [json.loads(line) for line in open(path)]
    types, frames, arrays = Counter(), 0, 0
    last = {"token": {}, "market": {}, "connection": {}}
    regress = {k: [] for k in last}
    regress_types = Counter()
    seen, dups = set(), Counter()
    lag = []
    pings, rtts, unanswered = [], [], 0
    conns = set()
    for r in recs:
        conn = r.get("conn")
        if r["kind"] == "open":
            unanswered += len(pings)
            pings = []
        if r["kind"] == "sent" and r.get("text") == "PING":
            pings.append(r["mono"])
        if r["kind"] != "recv":
            continue
        conns.add(conn)
        if r["text"] == "PONG":
            if pings:
                rtts.append(r["mono"] - pings.pop(0))
            continue
        frames += 1
        d = json.loads(r["text"])
        arrays += isinstance(d, list)
        for e in d if isinstance(d, list) else [d]:
            et = e.get("event_type")
            types[et] += 1
            if et == "new_market":
                continue
            for token, market, ts, ident in entries(e):
                if ident in seen:
                    dups[et] += 1
                seen.add(ident)
                if ts is None:
                    continue
                lag.append(r["t"] - ts / 1000)
                for scope, key, present in (("token", (conn, token), token),
                                            ("market", (conn, market), market),
                                            ("connection", conn, True)):
                    if not present:
                        continue
                    prev = last[scope].get(key)
                    if prev is not None and ts < prev[0]:
                        regress[scope].append((prev[0] - ts) / 1000)
                        if scope == "token":
                            regress_types[(prev[1], et)] += 1
                    else:
                        last[scope][key] = (ts, et)
    span = recs[-1]["elapsed"] - recs[0]["elapsed"]
    print(f"== {path}: {span / 60:.1f} min, connections {sorted(conns)}")
    print(f"  frames {frames}, of which arrays {arrays}; events {dict(types)}")
    for scope in ("token", "market", "connection"):
        print(f"  timestamp regressions per {scope}: {len(regress[scope])};"
              f" size (s) {summarize(regress[scope])}")
    if regress_types:
        print(f"  token regressions by (previous type, later type): {dict(regress_types)}")
    print(f"  duplicate events: {dict(dups) or 0}")
    print(f"  receipt minus source timestamp (s): {summarize(lag)}")
    print(f"  PING to PONG (s): {summarize(rtts)}; PINGs without PONG: {unanswered + len(pings)}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("captures", nargs="+")
    for path in p.parse_args().captures:
        report(path)


if __name__ == "__main__":
    main()
