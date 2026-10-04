# /// script
# requires-python = ">=3.12"
# dependencies = ["polymarket-client==0.12.0", "websockets==15.0.1"]
# ///
"""Check that the committed evidence still demonstrates each behavior.

Runs the reproduction scripts' own analyses over the excerpts in
spikes/evidence/, offline, and compares each verdict and key figure with
what the findings report. Every file is first checked against the SHA-256 in
manifest.json. The hash check infers the two fields REST would supply from
the first book's own hash, so no check touches the network.

The excerpts record polymarket-client 0.12.0 and the service as of October
4, 2026, so these checks keep passing after either changes. That is the
point: they show the original behavior. Whether it persists is for the
repro_*.py scripts run live.

    uv run spikes/check_evidence.py          # exits 1 if any check fails
"""

import contextlib
import hashlib
import io
import json
import sys
from pathlib import Path

import check_hashes
import live
import orderbook
import repro_sdk
import repro_settlement
import repro_stream

EVIDENCE = Path(__file__).parent / "evidence"


def check_hash_recipe(paths: list[str]) -> None:
    """Replay the probe: the recipe verifies, a trade enters a hash before it
    is announced, and the Van Hollen book diverges for four checks."""
    recs = live.load(paths)
    evs = [(r, e) for r, e in orderbook.events(recs)
           if e.get("event_type") not in ("new_market", "best_bid_ask", "market_resolved")]
    first = {}
    for _, e in evs:
        if e.get("event_type") == "book":
            first.setdefault(e["asset_id"], e)
    meta = {a: check_hashes.infer_meta(b) for a, b in first.items()}
    res = check_hashes.replay(evs, meta)
    st = res["stats"]
    checks = sum(n for k, n in st.items() if k.startswith(("change groups", "book hashes")))
    good = checks - st["change groups failed"] - st["book hashes failed"]
    print(f"  {good} of {checks} hash checks verified; {dict(st)}")
    check_hashes.describe_episodes(res["episodes"])
    live.verdict("hash-recipe", "REPRODUCED" if good / checks > 0.97 else "NOT REPRODUCED",
                 f"{good} of {checks} checks verified with the recipe in orderbook.py", paths)
    announced_later = (st["book hashes verified with next trade price"]
                       + st["change groups verified with next trade price"])
    live.verdict("trade-before-announcement",
                 "REPRODUCED" if announced_later else "NOT REPRODUCED",
                 f"{announced_later} checks verified only with the next announced trade price",
                 paths)
    van_hollen = {"82322606787689507186452368934268895797822450917805855938369533831277560432502",
                  "89468624821153339755038928248216667629141563933414663720232714629209306424979"}
    eps = [ep for ep in res["episodes"] if ep["asset"] in van_hollen and ep["failed"] >= 4]
    live.verdict("omitted-change", "REPRODUCED" if len(eps) == 2 else "NOT REPRODUCED",
                 f"{len(eps)} Van Hollen tokens failed four or more consecutive checks", paths)


ANALYSES = {
    "repro_sdk silent-reconnect": repro_sdk.analyze_silent_reconnect,
    "repro_sdk drops-events": repro_sdk.analyze_drops_events,
    "repro_sdk drops-events-live": repro_sdk.analyze_drops_events_live,
    "repro_stream idle-close": repro_stream.analyze_idle_close,
    "repro_stream no-replay": repro_stream.analyze_no_replay,
    "repro_stream slow-consumer": repro_stream.analyze_slow_consumer,
    "repro_stream duplicates": repro_stream.analyze_duplicates,
    "repro_settlement settled-subscription": repro_settlement.analyze_settled_subscription,
    "repro_settlement settlement": lambda p: repro_settlement.analyze_settlement(p, None),
    "check_hashes replay": check_hash_recipe,
}

# (what it shows, analysis, files, expected verdicts, text the report must
# contain, text it must not contain)
CHECKS = [
    ("the SDK reconnects silently and loses the gap (investigation run)",
     "repro_sdk silent-reconnect", ["sdk-silent-reconnect--sdk-run.jsonl.gz"],
     {"silent-reconnect": "REPRODUCED"},
     ["2 source book states never delivered", "106 source book states",
      "10 source book states", "480 source book states"], []),
    ("the SDK reconnects silently and loses the gap (busy market)",
     "repro_sdk silent-reconnect", ["sdk-silent-reconnect--busy.jsonl.gz"],
     {"silent-reconnect": "REPRODUCED"}, ["596 source book states"], []),
    ("a live SDK subscription delivers 24 of 704 new_market events",
     "repro_sdk drops-events-live", ["sdk-drops-events--live.jsonl.gz"],
     {"drops-events-live": "REPRODUCED"},
     ["0 reconnect(s)", "delivered by the SDK: 24; never delivered: 680",
      "logged 680 dropped event(s) at DEBUG and counted 680", "handle.dropped = 0"], []),
    ("the SDK's parser rejects new_market events",
     "repro_sdk drops-events", ["sdk-drops-events--long-run.jsonl.gz"],
     {"drops-events": "REPRODUCED"}, ["new_market events on field game_start_time"], []),
    ("nothing is replayed after a reconnect",
     "repro_stream no-replay", ["stream-no-replay--repro.jsonl"],
     {"no-replay": "REPRODUCED"}, ["8 of the gap's 10 book states"], []),
    ("an idle connection is closed without a close frame (investigation)",
     "repro_stream idle-close", ["stream-idle-close--no-ping.jsonl",
                                 "stream-idle-close--ping.jsonl"],
     {"idle-close": "REPRODUCED"}, ["after 125.2 s"], []),
    ("an idle connection is closed without a close frame (reproduction)",
     "repro_stream idle-close", ["stream-idle-close--repro.jsonl"],
     {"idle-close": "REPRODUCED"}, ["after 125.1 s"], []),
    ("the server drops a slow consumer without a close frame",
     "repro_stream slow-consumer", ["stream-slow-consumer--settle-1.jsonl.gz"],
     {"slow-consumer": "REPRODUCED"},
     ["closed by the server without a close frame", "slowest PONG 9.47 s"], []),
    ("the server closes slow consumers with 1013",
     "repro_stream slow-consumer", ["stream-slow-consumer--settle-2.jsonl.gz"],
     {"slow-consumer": "REPRODUCED"},
     ["server sent close 1013 'slow consumer: send buffer full'"], []),
    ("a PONG waits 11.4 s behind data before a 1013 close",
     "repro_stream slow-consumer", ["stream-slow-consumer--repro.jsonl.gz"],
     {"slow-consumer": "REPRODUCED"}, ["slowest PONG 11.39 s"], []),
    ("messages repeat and arrive out of order (busy market)",
     "repro_stream duplicates", ["stream-duplicates--repro.jsonl.gz"],
     {"duplicates": "REPRODUCED", "out-of-order": "REPRODUCED"},
     ["38 messages arrived again", "45 events arrived after"], []),
    ("messages repeat and arrive out of order (60-minute run)",
     "repro_stream duplicates", ["stream-duplicates--long-run.jsonl"],
     {"duplicates": "REPRODUCED", "out-of-order": "REPRODUCED"},
     ["9 messages arrived again", "18 events arrived after"], []),
    ("a change stamped before an opening book arrives after it",
     "repro_stream duplicates", ["stream-late-change--settle-2.jsonl.gz"],
     {"out-of-order": "REPRODUCED"}, ["2 of them between book and price_change"], []),
    ("the hash recipe; a trade in the hash before it is announced; an omitted change",
     "check_hashes replay", ["hash--probe.jsonl.gz"],
     {"hash-recipe": "REPRODUCED", "trade-before-announcement": "REPRODUCED",
      "omitted-change": "REPRODUCED"}, [], []),
    ("a settlement goes unannounced as its connection drops",
     "repro_settlement settlement", ["settlement--settle-2.jsonl.gz"],
     {"settlement": "REPRODUCED"},
     ["btc-updown-15m-1791124200 emptied its book and settled",
      "288 events stamped after the end", "market_resolved at 14:37:28.448"], []),
    ("the server closes a connection once all its markets have settled",
     "repro_settlement settlement", ["settlement--all-resolved-1.jsonl.gz"],
     {"settlement": "NOT REPRODUCED"},
     ["server sent close 1000 'all subscribed assets resolved'"], []),
    ("the all-resolved close again, and market lookup's lag",
     "repro_settlement settlement", ["settlement--all-resolved-2.jsonl.gz"],
     {"settlement": "NOT REPRODUCED"},
     ["server sent close 1000 'all subscribed assets resolved'", "51 s after market_resolved"],
     []),
    ("the connection stays open while another market is unresolved",
     "repro_settlement settlement", ["settlement--others-open.jsonl.gz"],
     {"settlement": "NOT REPRODUCED"}, ["market_resolved at 15:57:00.944"],
     ["all subscribed assets resolved"]),
    ("settled tokens are left out of a subscription without an error",
     "repro_settlement settled-subscription",
     ["settled-subscription--mixed.jsonl", "settled-subscription--alone.jsonl"],
     {"settled-subscription": "REPRODUCED"}, [], []),
    ("settled tokens are left out, and REST rejects their books",
     "repro_settlement settled-subscription", ["settled-subscription--repro.jsonl"],
     {"settled-subscription": "REPRODUCED"},
     ["rejected: No orderbook exists for the requested token id"], []),
]


def verify_files() -> list[str]:
    problems = []
    for m in json.loads((EVIDENCE / "manifest.json").read_text()):
        path = EVIDENCE / m["file"]
        if not path.exists():
            problems.append(f"{m['file']}: missing")
        elif hashlib.sha256(path.read_bytes()).hexdigest() != m["sha256"]:
            problems.append(f"{m['file']}: does not match its SHA-256 in manifest.json")
    return problems


def main() -> None:
    verbose = "-v" in sys.argv
    problems = verify_files()
    for p in problems:
        print(f"FAIL {p}")
    failed = len(problems)
    print(f"{len(json.loads((EVIDENCE / 'manifest.json').read_text())) - len(problems)}"
          " files match manifest.json")
    for what, analysis, files, verdicts, must, must_not in CHECKS:
        live.VERDICTS.clear()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            ANALYSES[analysis]([str(EVIDENCE / f) for f in files])
        report = out.getvalue()
        got = dict(live.VERDICTS)
        wrong = [f"{k}: expected {v}, got {got.get(k)}" for k, v in verdicts.items()
                 if got.get(k) != v]
        wrong += [f"missing {t!r}" for t in must if t not in report]
        wrong += [f"unexpected {t!r}" for t in must_not if t in report]
        status = "FAIL" if wrong else "ok  "
        failed += bool(wrong)
        print(f"{status} {what}")
        for w in wrong:
            print(f"       {w}")
        if wrong or verbose:
            print("       " + report.strip().replace("\n", "\n       "))
    passed = len(CHECKS) - (failed - len(problems))
    print(f"{passed} of {len(CHECKS)} checks pass"
          + (f"; {len(problems)} evidence files do not match the manifest" if problems else ""))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
