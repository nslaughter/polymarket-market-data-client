# /// script
# requires-python = ">=3.12"
# dependencies = ["polymarket-client==0.12.0", "websockets==15.0.1"]
# ///
"""Reproduce what polymarket-client 0.12.0's market stream does not tell its
consumer.

silent-reconnect
    Runs the SDK through sdk_reconnect.py's proxy on the current 15-minute
    Bitcoin market, drops its connection, then withholds PONG, with a direct
    reference connection alongside. Reproduced when the SDK reconnects and
    resubscribes after a fault without delivering anything that marks the gap
    and without logging above DEBUG, so a consumer cannot tell that the source
    events in the gap never reached it.

drops-events
    Records the raw stream with custom_feature_enabled and runs every event
    through the SDK's own parser. Reproduced when the parser rejects events
    the server sent; the SDK drops those with only a DEBUG log record.

drops-events-live
    Runs a real SDK subscription with custom_feature_enabled and a direct
    reference connection to the same market at the same time, and matches
    their new_market events by ID. Reproduced when the reference received
    new_market events the SDK never delivered, the SDK's own parser rejects
    each of them, and the SDK neither reconnected nor reported anything above
    DEBUG. Inconclusive if the SDK reconnected, since a gap could also explain
    the missing events.

Each subcommand runs live and writes a capture under spikes/captures/repro/.
With --capture FILE it analyzes existing captures instead: sdk_reconnect.py
captures for silent-reconnect, any capture of received frames for
drops-events, and drops-events-live captures for drops-events-live.
"""

import argparse
import asyncio
import json
import logging
import re
from argparse import Namespace
from collections import Counter
from datetime import timedelta

import live


# --- silent-reconnect --------------------------------------------------------


async def collect_silent_reconnect(args) -> str:
    import sdk_reconnect

    # A market that changes every second, so the gaps have events to lose.
    market = await live.select("ending", prefix="btc-updown-15m-",
                               min_ahead=timedelta(minutes=3))
    rec = live.Recorder("silent-reconnect")
    markets_file = rec.path.with_suffix(".markets.jsonl")
    markets_file.write_text(json.dumps({"group": "long", **market}) + "\n")
    print(f"market: {market['slug']} ({market['condition_id']})")
    await sdk_reconnect.run(Namespace(
        markets=str(markets_file), slug=[market["slug"]], out=str(rec.path),
        faults=["kill", "withhold-pong"], kill_at=20, withhold_pong_at=50,
        withhold_pong_for=45, duration=110, stall_at=0, stall_for=0, close_at=0,
        outage_at=0, outage_for=0))
    return str(rec.path)


def analyze_silent_reconnect(paths: list[str]) -> None:
    for path in paths:
        recs = live.load([path])
        sdk = [r for r in recs if r["kind"] == "sdk_event" and r["type"] != "new_market"]
        resubs = [r["elapsed"] for r in recs if r["kind"] == "sdk_ws"
                  and r["msg"].startswith("> TEXT '{\"type\":\"market\"")]
        logs = [r for r in recs if r["kind"] == "sdk_log" and r["level"] != "DEBUG"]
        ref_states = []  # (elapsed, token, hash)
        for r, e in live.frames(recs):
            if e.get("event_type") == "price_change":
                ref_states += [(r["elapsed"], c["asset_id"], c["hash"])
                               for c in e["price_changes"]]
            elif e.get("event_type") == "book":
                ref_states.append((r["elapsed"], e["asset_id"], e["hash"]))
        delivered = set()
        for r in sdk:
            if r["type"] == "price_change":
                delivered.update((c[0], c[4]) for c in r["changes"])
            elif r["type"] == "book":
                delivered.add((r["asset"], r["hash"]))
        kinds = Counter(r["type"] for r in sdk)
        print(f"== {path}")
        print(f"  the handle delivered only market events: {dict(kinds)}")
        silent = []
        for f in [r for r in recs if r["kind"] == "fault"
                  and not r["action"].endswith("end")]:
            later = [t for t in resubs if t > f["elapsed"]]
            if not later:
                print(f"  {f['action']} at {f['elapsed']:.1f} s: no resubscription seen")
                continue
            resub = later[0]
            before = [r for r in sdk if r["elapsed"] < f["elapsed"]]
            after = [r for r in sdk if r["elapsed"] > resub]
            lo = before[-1]["elapsed"] if before else f["elapsed"]
            hi = after[0]["elapsed"] if after else resub
            missed = sum(1 for el, a, h in ref_states
                         if lo < el < hi and (a, h) not in delivered)
            window = [r for r in logs if f["elapsed"] <= r["elapsed"] <= resub + 1]
            said = "; ".join(sorted({f"{r['level']} {r['msg'][:60]}" for r in window}))
            print(f"  {f['action']} at {f['elapsed']:.1f} s: resubscribed after"
                  f" {resub - f['elapsed']:.1f} s; {missed} source book states never"
                  f" delivered; logged above DEBUG: {said or 'nothing'}")
            if not window:
                silent.append((f["action"], missed))
        if silent:
            lost = sum(m for _, m in silent)
            loss = (f"{lost} source book states in those gaps never reached the consumer"
                    if lost else "the market did not change during those gaps, so no"
                    " loss shows here")
            live.verdict("silent-reconnect", "REPRODUCED",
                         f"the SDK reconnected after {', '.join(a for a, _ in silent)} without"
                         f" any event or log above DEBUG to mark it; {loss}", path)
        else:
            live.verdict("silent-reconnect", "NOT REPRODUCED",
                         "each fault here was followed by a log record above DEBUG, or the"
                         " SDK did not resubscribe; the handle still carried no marker",
                         path)


# --- drops-events ------------------------------------------------------------


async def collect_drops_events(args) -> str:
    market = await live.select("long")
    rec = live.Recorder("drops-events")
    rec.write("note", markets=[market])
    print(f"market: {market['slug']}; recording for {args.duration:.0f} s")
    sock = await live.MarketSocket(rec, "c1", market["token_ids"]).open()
    try:
        await asyncio.wait_for(sock.closed.wait(), args.duration)
    except TimeoutError:
        pass
    await sock.close()
    return str(rec.path)


def analyze_drops_events(paths: list[str]) -> None:
    from pydantic import ValidationError
    from polymarket.models.clob.market_events import parse_market_event

    recs = live.load(paths)
    seen, rejected, examples = Counter(), Counter(), {}
    for _, e in live.frames(recs):
        et = e.get("event_type")
        seen[et] += 1
        try:
            parse_market_event(e)
        except ValidationError as err:
            first = err.errors()[0]
            key = (et, ".".join(str(x) for x in first["loc"][2:]) or "-", first["type"])
            rejected[key] += 1
            examples.setdefault(key, {"slug": e.get("slug"), "value": first.get("input"),
                                      "error": first["msg"][:90]})
    total = sum(seen.values())
    print(f"== {', '.join(paths)}")
    print(f"  events received: {total} {dict(seen)}")
    for (et, field, kind), n in rejected.most_common():
        print(f"  rejected {n} of {seen[et]} {et} events on field {field} ({kind})")
        print(f"    e.g. {examples[(et, field, kind)]}")
    if rejected:
        live.verdict("drops-events", "REPRODUCED",
                     f"the SDK's parser rejected {sum(rejected.values())} of {total} events"
                     " the server sent; the SDK drops these, logging only at DEBUG", paths)
    else:
        live.verdict("drops-events", "NOT REPRODUCED",
                     "the parser accepted every event received; rejections depend on which"
                     " new markets are created during the recording", paths)


# --- drops-events-live ---------------------------------------------------------


async def collect_drops_events_live(args) -> str:
    from polymarket import AsyncPublicClient
    from polymarket.streams import MarketSpec
    from sdk_reconnect import LogToRecorder

    market = await live.select("long")
    rec = live.Recorder("drops-events-live")
    rec.write("note", markets=[market])
    tokens = market["token_ids"]
    print(f"market: {market['slug']}; the SDK subscribes for {args.duration:.0f} s")

    sdk_logger = logging.getLogger("spike.sdk")
    sdk_logger.setLevel(logging.DEBUG)
    sdk_logger.addHandler(LogToRecorder(rec, "sdk_log"))
    sdk_logger.propagate = False
    # The SDK's socket logs to websockets' default logger; the reference
    # connection gets its own, so the SDK's connections can be told apart.
    ws_logger = logging.getLogger("websockets.client")
    ws_logger.setLevel(logging.DEBUG)
    ws_logger.addHandler(LogToRecorder(rec, "sdk_ws", lambda m: m.startswith(
        ("= connection is", "> TEXT '{\"type\":\"market\"", "x "))))
    ws_logger.propagate = False

    ref = await live.MarketSocket(rec, "reference", tokens,
                                  logger=logging.getLogger("reference.websockets")).open()
    await asyncio.sleep(2)  # the reference covers the SDK's whole window
    async with AsyncPublicClient(logger=sdk_logger) as client:
        handle = await client.subscribe(MarketSpec(token_ids=tokens,
                                                   custom_feature_enabled=True))
        rec.write("sdk", event="subscribed")
        try:
            async with asyncio.timeout(args.duration):
                async for event in handle:
                    rec.write("sdk_event", type=event.type,
                              id=event.payload.id if event.type == "new_market" else None)
        except TimeoutError:
            pass
        # Private: the public client does not expose the parse-drop count.
        manager = client._market_manager
        rec.write("sdk", event="done", handle_dropped=handle.dropped,
                  parse_dropped=None if manager is None else manager.dropped_events)
        await handle.close()
    await asyncio.sleep(2)
    await ref.close()
    return str(rec.path)


def analyze_drops_events_live(paths: list[str]) -> None:
    from pydantic import ValidationError
    from polymarket.models.clob.market_events import parse_market_event

    recs = live.load(paths)
    start = next(r["t"] for r in recs if r["kind"] == "sdk" and r["event"] == "subscribed")
    done = next(r for r in recs if r["kind"] == "sdk" and r["event"] == "done")
    # Compare inside the SDK's window, a second in from each edge, so events
    # that reached only one connection because of when it subscribed or
    # closed do not count.
    ref = {e["id"]: e for r, e in live.frames(recs, "reference")
           if e.get("event_type") == "new_market" and start + 1 <= r["t"] <= done["t"] - 1}
    delivered = {r["id"] for r in recs if r["kind"] == "sdk_event" and r["type"] == "new_market"}
    missing = [e for i, e in ref.items() if i not in delivered]
    reasons = Counter()
    for e in missing:
        try:
            parse_market_event(e)
            reasons["the SDK's parser accepts it"] += 1
        except ValidationError as err:
            first = err.errors()[0]
            reasons[f"rejected on {'.'.join(str(x) for x in first['loc'][2:])}"] += 1
    logged = sum(int(m.group(1)) for r in recs if r["kind"] == "sdk_log"
                 for m in [re.match(r"dropped (\d+) malformed", r["msg"])] if m)
    above_debug = sorted({r["msg"][:70] for r in recs
                          if r["kind"] == "sdk_log" and r["level"] != "DEBUG"})
    subscriptions = sum(1 for r in recs if r["kind"] == "sdk_ws"
                        and r["msg"].startswith("> TEXT '{\"type\":\"market\""))
    print(f"== {', '.join(paths)}")
    print(f"  SDK subscribed for {done['t'] - start:.0f} s; it sent {subscriptions}"
          f" subscription frame(s), so {max(subscriptions - 1, 0)} reconnect(s)")
    print(f"  new_market events the reference received in that window: {len(ref)}")
    print(f"  of those, delivered by the SDK: {len(ref) - len(missing)};"
          f" never delivered: {len(missing)} {dict(reasons)}")
    print(f"  the SDK logged {logged} dropped event(s) at DEBUG and counted"
          f" {done['parse_dropped']} on its stream manager; handle.dropped ="
          f" {done['handle_dropped']}")
    print(f"  SDK log records above DEBUG: {above_debug or 'none'}")
    explained = missing and all(k.startswith("rejected") for k in reasons)
    if subscriptions > 1:
        live.verdict("drops-events-live", "INCONCLUSIVE",
                     "the SDK reconnected during the run, so a gap could also explain"
                     " missing events", paths)
    elif explained and not above_debug:
        live.verdict("drops-events-live", "REPRODUCED",
                     f"the SDK delivered {len(ref) - len(missing)} of the {len(ref)} new_market"
                     f" events the reference received; its parser rejects each of the"
                     f" {len(missing)} it dropped, and nothing above DEBUG said so", paths)
    elif not missing:
        live.verdict("drops-events-live", "NOT REPRODUCED",
                     f"the SDK delivered all {len(ref)} new_market events the reference"
                     " received", paths)
    else:
        live.verdict("drops-events-live", "INCONCLUSIVE",
                     "some missing events are not explained by the parser, or the SDK"
                     " logged above DEBUG", paths)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="check", required=True)
    for name, default in (("silent-reconnect", 0), ("drops-events", 180),
                          ("drops-events-live", 600)):
        s = sub.add_parser(name)
        s.add_argument("--capture", action="append",
                       help="analyze this capture instead of running live; repeatable")
        if default:
            s.add_argument("--duration", type=float, default=default,
                           help="seconds to record")
    args = p.parse_args()
    collect = {"silent-reconnect": collect_silent_reconnect,
               "drops-events": collect_drops_events,
               "drops-events-live": collect_drops_events_live}[args.check]
    analyze = {"silent-reconnect": analyze_silent_reconnect,
               "drops-events": analyze_drops_events,
               "drops-events-live": analyze_drops_events_live}[args.check]
    paths = args.capture or [asyncio.run(collect(args))]
    analyze(paths)


if __name__ == "__main__":
    main()
