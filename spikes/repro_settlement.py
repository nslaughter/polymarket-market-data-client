# /// script
# requires-python = ">=3.12"
# dependencies = ["polymarket-client==0.12.0", "websockets==15.0.1"]
# ///
"""Reproduce how settlement does and does not show up on the market stream.

settled-subscription
    Subscribes to a recently settled market together with an active one, and
    to the settled market alone, then asks REST for the settled market's book.
    Reproduced when the settled tokens are silently left out of the opening
    frame (an empty array when they are the only tokens), with no error, and
    REST rejects the book.

settlement
    Subscribes to the 5-minute Bitcoin market that ends next, reconnecting
    after any close, and watches until market_resolved arrives or --after
    seconds past the end date. Reports the timeline: trading after the end
    date, the burst that empties the book, market_resolved, and any
    disconnect. Reproduced, for the risk this checks, when the market settles
    without market_resolved reaching the client; otherwise it reports the
    settlement as announced.

Each subcommand runs live and writes a capture under spikes/captures/repro/,
or with --capture FILE analyzes existing captures instead. For captures from
capture.py, pass --markets with the select_markets.py file so end dates are
known.
"""

import argparse
import asyncio
import json
import time
from datetime import datetime, timedelta

import live


def subscribed(records: list[dict], conn: str) -> list[str]:
    for r in records:
        if r.get("conn") == conn and r["kind"] == "sent" and r["text"].startswith("{"):
            return json.loads(r["text"])["assets_ids"]
    return []


def markets_from(records: list[dict], markets_file: str | None) -> dict[str, dict]:
    """Condition ID -> market, from the capture's note or a select_markets file."""
    found = {}
    for r in records:
        if r["kind"] == "note":
            for m in r.get("markets", []):
                found[m["condition_id"]] = m
    if markets_file:
        for line in open(markets_file):
            m = json.loads(line)
            found.setdefault(m["condition_id"], m)
    return found


# --- settled-subscription ----------------------------------------------------


async def collect_settled_subscription(args) -> str:
    from polymarket import AsyncPublicClient
    from polymarket.errors import RequestRejectedError

    settled = await live.select("settled")
    active = await live.select("long")
    rec = live.Recorder("settled-subscription")
    rec.write("note", markets=[settled, active], settled=settled["condition_id"])
    print(f"settled: {settled['slug']}; active: {active['slug']}")
    both = await live.MarketSocket(rec, "settled+active",
                                   settled["token_ids"] + active["token_ids"]).open()
    alone = await live.MarketSocket(rec, "settled-only", settled["token_ids"]).open()
    await asyncio.sleep(args.duration)
    await both.close()
    await alone.close()
    async with AsyncPublicClient() as client:
        for token in settled["token_ids"]:
            try:
                ob = await client.get_order_book(token_id=token)
                rec.write("rest", token=token, outcome=f"book with {len(ob.bids)} bids")
            except RequestRejectedError as err:
                rec.write("rest", token=token, outcome=f"rejected: {err}")
    return str(rec.path)


def analyze_settled_subscription(paths: list[str]) -> None:
    recs = live.load(paths)
    print(f"== {', '.join(paths)}")
    silent = []
    for r in [r for r in recs if r["kind"] == "open"]:
        c = r.get("conn")
        assets = subscribed(recs, c)
        first = next((x for x in recs if x.get("conn") == c and x["kind"] == "recv"
                      and x["text"] != "PONG"), None)
        if first is None:
            print(f"  {c}: subscribed {len(assets)} tokens; nothing received")
            continue
        booked = {e["asset_id"] for _, e in live.frames([first]) if e.get("event_type") == "book"}
        missing = [a for a in assets if a not in booked]
        later = {e.get("asset_id") for _, e in live.frames(recs, c)} & set(missing)
        errors = [e for _, e in live.frames(recs, c) if "error" in json.dumps(e).lower()]
        opening = json.loads(first["text"])
        shape = (f"an array of {len(opening)} events" if isinstance(opening, list) and opening
                 else repr(first["text"].strip()))
        print(f"  {c}: subscribed {len(assets)} tokens; opening frame {shape},"
              f" with books for {len(booked)};"
              f" {len(missing)} left out, {len(later)} of them heard from later;"
              f" error events: {len(errors)}")
        if missing and not later and not errors:
            silent.append(c)
    for r in recs:
        if r["kind"] == "rest":
            print(f"  REST /book for {r['token'][:12]}…: {r['outcome']}")
    if silent:
        live.verdict("settled-subscription", "REPRODUCED",
                     f"tokens were left out of the opening frame without any error on"
                     f" {', '.join(silent)}", paths)
    else:
        live.verdict("settled-subscription", "NOT REPRODUCED",
                     "every subscribed token got a book, or the server reported an error",
                     paths)


# --- settlement --------------------------------------------------------------


async def collect_settlement(args) -> str:
    from polymarket import AsyncPublicClient

    market = await live.select("ending", prefix="btc-updown-5m-",
                               min_ahead=timedelta(seconds=60))
    rec = live.Recorder("settlement")
    rec.write("note", markets=[market])
    end = datetime.fromisoformat(market["end_date"]).timestamp()
    print(f"market: {market['slug']}, ends {market['end_date']}; watching until"
          f" market_resolved or {args.after:.0f} s after the end")
    deadline = end + args.after
    resolved = asyncio.Event()

    def watch(text: str) -> None:
        if '"market_resolved"' in text:
            resolved.set()

    n = 0
    while time.time() < deadline and not resolved.is_set():
        n += 1
        sock = await live.MarketSocket(rec, f"c{n}", market["token_ids"],
                                       on_text=watch).open()
        while (not sock.closed.is_set() and not resolved.is_set()
               and time.time() < deadline):
            await asyncio.sleep(0.5)
        if resolved.is_set():
            await asyncio.sleep(5)  # keep listening briefly after the event
        await sock.close()
        if not resolved.is_set() and time.time() < deadline:
            await asyncio.sleep(1)
    # Poll market lookup until it shows the market closed, to measure its lag.
    stop = time.time() + args.gamma_wait
    async with AsyncPublicClient() as client:
        while True:
            m = await client.get_market(slug=market["slug"])
            rec.write("gamma", slug=market["slug"], closed=m.state.closed,
                      closed_time=m.state.closed_time,
                      uma_resolution_status=m.resolution.uma_resolution_status)
            if m.state.closed or time.time() >= stop:
                break
            await asyncio.sleep(15)
    return str(rec.path)


def analyze_settlement(paths: list[str], markets_file: str | None) -> None:
    recs = live.load(paths)
    markets = markets_from(recs, markets_file)
    print(f"== {', '.join(paths)}")
    outcomes = []
    for cid, m in markets.items():
        mine = set(m["token_ids"])
        if not any(set(subscribed(recs, r.get("conn"))) & mine
                   for r in recs if r["kind"] == "open"):
            continue
        end = datetime.fromisoformat(m["end_date"]).timestamp()
        after, last, clear, resolved = 0, None, None, None
        for r, e in live.frames(recs):
            et = e.get("event_type")
            if et == "market_resolved" and e.get("market") == cid:
                resolved = (r, e)
            if e.get("market") != cid or et in ("new_market", "market_resolved"):
                continue
            if int(e.get("timestamp", 0)) / 1000 >= end:
                after += 1
            last = r
            if (clear is None and et == "price_change"
                    and all(c.get("best_bid") == "0" and c.get("best_ask") == "1"
                            for c in e["price_changes"])):
                clear = r
        closes = [(r.get("conn"), r["t"]) for r in recs if r["kind"] == "close"
                  and r["t"] >= end and set(subscribed(recs, r.get("conn"))) & mine]
        print(f"  {m['slug']}: ended {live.utc(end)}; {after} events stamped after the end;"
              f" book emptied at {live.utc(clear['t']) if clear else 'never seen'};"
              f" last event {live.utc(last['t']) if last else '-'}")
        if resolved:
            r, e = resolved
            print(f"    market_resolved at {live.utc(r['t'])}"
                  f" ({r['t'] - end:.0f} s after the end), winner {e.get('winning_outcome')}")
        for c, t in closes:
            how = live.how_closed(recs, c)
            if how != "closed by the client":
                print(f"    connection {c} {how} at {live.utc(t)}")
        reopened = [r for r in recs if r["kind"] == "recv" and clear and r["t"] > clear["t"]
                    and r["text"].startswith("[")]
        for r in reopened:
            booked = {e["asset_id"] for _, e in live.frames([r]) if e.get("event_type") == "book"}
            print(f"    reopened {r.get('conn')} at {live.utc(r['t'])}: books for"
                  f" {len(booked & mine)} of the market's tokens")
        polls = [r for r in recs if r["kind"] == "gamma" and r["slug"] == m["slug"]]
        shut = next((g for g in polls if g["closed"]), None)
        if polls:
            since = resolved[0]["t"] if resolved else end
            print(f"    Gamma polled {len(polls)} times: "
                  + (f"first showed it closed at {live.utc(shut['t'])},"
                     f" {shut['t'] - since:.0f} s after"
                     f" {'market_resolved' if resolved else 'the end date'}"
                     f" (closed_time {shut['closed_time']},"
                     f" status {shut['uma_resolution_status']})" if shut
                     else f"still open at {live.utc(polls[-1]['t'])}"))
        outcomes.append((m["slug"], bool(resolved), bool(clear)))
    unannounced = [s for s, res, clr in outcomes if clr and not res]
    if unannounced:
        live.verdict("settlement", "REPRODUCED",
                     f"{', '.join(unannounced)} emptied its book and settled, but"
                     " market_resolved never reached the client", paths)
    elif any(res for _, res, _ in outcomes):
        live.verdict("settlement", "NOT REPRODUCED",
                     "market_resolved reached the client for every market that settled",
                     paths)
    else:
        live.verdict("settlement", "INCONCLUSIVE",
                     "no market settled while it was watched", paths)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="check", required=True)
    s = sub.add_parser("settled-subscription")
    s.add_argument("--capture", action="append")
    s.add_argument("--duration", type=float, default=20, help="seconds to listen")
    s = sub.add_parser("settlement")
    s.add_argument("--capture", action="append")
    s.add_argument("--markets", help="select_markets.py output, for capture.py captures")
    s.add_argument("--after", type=float, default=600,
                   help="seconds past the end date to wait for market_resolved")
    s.add_argument("--gamma-wait", type=float, default=300,
                   help="seconds to poll market lookup for the market to show closed")
    args = p.parse_args()
    if args.check == "settled-subscription":
        analyze_settled_subscription(
            args.capture or [asyncio.run(collect_settled_subscription(args))])
    else:
        analyze_settlement(args.capture or [asyncio.run(collect_settlement(args))],
                           args.markets)


if __name__ == "__main__":
    main()
