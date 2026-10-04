# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Cut the evidence set in spikes/evidence/ from the full captures.

The full captures are hundreds of megabytes and stay out of git. This script
keeps, from each one, only the records that demonstrate a behavior in the
findings' catalog, and writes them to spikes/evidence/ with manifest.json.
The manifest records each source capture's size and SHA-256, what was kept,
and what each check in check_evidence.py must report for the excerpt.

The cut is deterministic: the same captures give byte-identical excerpts.
Larger excerpts are gzip-compressed with a fixed timestamp. Cookies the
websockets debug log recorded from Cloudflare's handshake are dropped, and
the script refuses to write any excerpt that still contains one.

Run it where the captures are kept:

    uv run spikes/extract_evidence.py
"""

import gzip
import hashlib
import io
import json
from pathlib import Path

HERE = Path(__file__).parent
CAPTURES = HERE / "captures"
EVIDENCE = HERE / "evidence"
COMPRESS_OVER = 100_000  # bytes


def read(path: Path) -> list[dict]:
    with open(path) as f:
        return [json.loads(line) for line in f]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def scrub(records: list[dict]) -> list[dict]:
    kept = [r for r in records
            if not (r["kind"] == "sdk_ws" and "cookie" in r["msg"].lower())]
    for r in kept:
        text = json.dumps(r).lower()
        if "set-cookie" in text or "__cf_bm" in text:
            raise SystemExit(f"a cookie survived scrubbing: {str(r)[:120]}")
    return kept


def events(r: dict) -> list[dict]:
    if r.get("kind") not in ("recv", "ref_recv") or r["text"] == "PONG":
        return []
    d = json.loads(r["text"])
    return d if isinstance(d, list) else [d]


def is_ping(r: dict) -> bool:
    return r.get("text") in ("PING", "PONG") or str(r.get("msg", "")).startswith(
        ("> TEXT 'PING'", "< TEXT 'PONG'"))


# --- Selections ----------------------------------------------------------------


def whole(records: list[dict]) -> list[dict]:
    return records


def fault_windows(records, faults=None, before=5.0, after=3.0):
    """SDK fault runs: every control and log record, the chosen faults, and SDK
    and reference events from `before` s ahead of each fault until `after` s
    past the SDK's next resubscription."""
    chosen = [r for r in records if r["kind"] == "fault" and not r["action"].endswith("end")
              and (faults is None or r["action"] in faults)]
    resubs = [r["elapsed"] for r in records if r["kind"] == "sdk_ws"
              and r["msg"].startswith("> TEXT '{\"type\":\"market\"")]
    windows = []
    for f in chosen:
        nxt = next(t for t in resubs if t > f["elapsed"])
        windows.append((f["elapsed"] - before, nxt + after))
    keep = []
    for r in records:
        k = r["kind"]
        if k in ("sdk_event", "ref_recv"):
            if any(a <= r["elapsed"] <= b for a, b in windows):
                keep.append(r)
        elif k == "fault":
            if r in chosen:
                keep.append(r)
        elif k in ("sdk_ws", "control") and is_ping(r):
            continue
        else:
            keep.append(r)
    return keep


def new_market_frames(records, every=10):
    """Every `every`-th frame carrying a new_market event, with the connection
    records."""
    keep, n = [], 0
    for r in records:
        if r["kind"] in ("recv", "ref_recv"):
            if any(e.get("event_type") == "new_market" for e in events(r)):
                if n % every == 0:
                    keep.append(r)
                n += 1
        elif r["kind"] != "control" and not is_ping(r):
            keep.append(r)
    return keep


def side_by_side_new_markets(records):
    """A drops-events-live run: the reference connection's new_market frames,
    the SDK's new_market deliveries, its drop log and counters, and the
    connection records."""
    keep = []
    for r in records:
        k = r["kind"]
        if k == "recv":
            if r.get("conn") == "reference" and any(
                    e.get("event_type") == "new_market" for e in events(r)):
                keep.append(r)
        elif k == "sdk_event":
            if r["type"] == "new_market":
                keep.append(r)
        elif not is_ping(r):
            keep.append(r)
    return keep


def connection_tails(records, tail=1.0):
    """Every non-data record (including PING and PONG, for their timing), and
    the data frames from the last `tail` s before each connection closed."""
    closes = {r.get("conn"): r["t"] for r in records if r["kind"] == "close"}
    keep = []
    for r in records:
        if r["kind"] == "recv" and r["text"] != "PONG":
            c = closes.get(r.get("conn"))
            if c is not None and c - tail <= r["t"] <= c:
                keep.append(r)
        else:
            keep.append(r)
    return keep


def repeats_and_regressions(records):
    """Both copies of every repeated message, and both events of every
    timestamp regression per token, by the rules repro_stream.py duplicates
    uses; plus the connection records."""
    marked: set[int] = set()
    seen: dict[tuple, int] = {}
    last: dict[tuple, tuple[int, int]] = {}
    for i, r in enumerate(records):
        for e in events(r):
            et = e.get("event_type")
            if et in ("new_market", "market_resolved"):
                continue
            body = dict(e)
            if et == "price_change":
                body["price_changes"] = sorted(json.dumps(c, sort_keys=True)
                                               for c in e["price_changes"])
            key = (r.get("conn"), json.dumps(body, sort_keys=True))
            if key in seen:
                marked.update((seen[key], i))
            seen[key] = i
            if not e.get("timestamp"):
                continue
            ts = int(e["timestamp"])
            tokens = ([c["asset_id"] for c in e["price_changes"]] if et == "price_change"
                      else [e.get("asset_id")])
            for tok in tokens:
                k = (r.get("conn"), tok)
                prev = last.get(k)
                if prev and ts < prev[0]:
                    marked.update((prev[1], i))
                else:
                    last[k] = (ts, i)
    return [r for i, r in enumerate(records)
            if i in marked or (r["kind"] not in ("recv", "control") and not is_ping(r))]


def opening_seconds(conn: str, secs: float):
    """Everything a connection received in its first `secs` s."""
    def select(records):
        opened = next(r["t"] for r in records if r["kind"] == "open" and r.get("conn") == conn)
        return [r for r in records
                if (r.get("conn") == conn and r["kind"] == "recv"
                    and r["t"] <= opened + secs and r["text"] != "PONG")
                or (r["kind"] in ("environment", "note", "open") and r.get("conn") in (conn, None))]
    return select


def settlement_frames(markets: list[dict] | None = None):
    """For each market: its frames stamped at or after its end date, its
    market_resolved, and each connection's opening frame, with every
    connection record. `markets` adds a note naming the markets, for
    captures that do not carry one."""
    def select(records):
        known = markets
        if known is None:
            known = next(r["markets"] for r in records if r["kind"] == "note" and "markets" in r)
        ends = {m["condition_id"]: _epoch(m["end_date"]) for m in known}
        first_seen: set = set()
        keep = []
        if markets is not None:
            keep.append({"t": records[0]["t"], "kind": "note", "markets": markets,
                         "added_by": "extract_evidence.py, from select_markets.py output"})
        for r in records:
            if r["kind"] in ("recv", "ref_recv") and r["text"] != "PONG":
                evs = events(r)
                opening = r.get("conn") not in first_seen
                first_seen.add(r.get("conn"))
                if opening or any(
                        e.get("market") in ends and (
                            e.get("event_type") == "market_resolved"
                            or int(e.get("timestamp") or 0) / 1000 >= ends[e["market"]])
                        for e in evs):
                    keep.append(r)
            elif r["kind"] in ("recv", "control") or is_ping(r):
                continue
            else:
                keep.append(r)
        return keep
    return select


def opening_frames(records):
    """Each connection's records other than data, and its first frame."""
    first_seen: set = set()
    keep = []
    for r in records:
        if r["kind"] == "recv" and r["text"] != "PONG":
            if r.get("conn") not in first_seen:
                first_seen.add(r.get("conn"))
                keep.append(r)
        elif not is_ping(r) and r["kind"] != "control":
            keep.append(r)
    return keep


def _epoch(iso: str) -> float:
    from datetime import datetime
    return datetime.fromisoformat(iso).timestamp()


# --- The evidence set ------------------------------------------------------------


def markets_from_select(path: Path, slugs: list[str]) -> list[dict]:
    out = []
    for line in open(path):
        m = json.loads(line)
        if m["slug"] in slugs:
            out.append({k: m[k] for k in ("slug", "question", "condition_id", "token_ids",
                                          "end_date")})
    return out


def repro(name: str) -> str:
    return next(p.name for p in sorted((CAPTURES / "repro").glob(f"{name}-*.jsonl"))
                if not p.name.endswith(".markets.jsonl"))


def plan() -> list[dict]:
    settle_markets = markets_from_select(
        CAPTURES / "markets-1.jsonl", ["btc-updown-5m-1791124200", "btc-updown-15m-1791124200"])
    return [
        {"file": "sdk-silent-reconnect--sdk-run", "source": "sdk-reconnect.jsonl",
         "behavior": "The SDK's stream reconnects and resubscribes without telling its consumer",
         "select": fault_windows, "kept": "all control, log, and proxy records; SDK and"
         " reference events from 5 s before each fault to 3 s after the SDK resubscribed"},
        {"file": "sdk-silent-reconnect--busy", "source": "repro/silent-reconnect-20261004T154747Z.jsonl",
         "behavior": "The SDK's stream reconnects and resubscribes without telling its consumer",
         "select": lambda rs: fault_windows(rs, faults={"kill"}, before=2.0, after=2.0),
         "kept": "the abort only: control and log records, and SDK and reference events from"
         " 2 s before it to 2 s after the SDK resubscribed"},
        {"file": "sdk-drops-events--live", "source": "repro/drops-events-live-20261004T184444Z.jsonl",
         "behavior": "The SDK drops events its parser rejects, logging only at DEBUG",
         "select": side_by_side_new_markets, "kept": "the reference connection's new_market"
         " frames, the SDK's new_market deliveries, its drop log and counters, and the"
         " connection records, from a 10-minute run with the SDK live beside the reference"},
        {"file": "sdk-drops-events--long-run", "source": "long-run.jsonl",
         "behavior": "The SDK drops events its parser rejects, logging only at DEBUG",
         "select": new_market_frames, "kept": "every tenth new_market frame of the 60-minute"
         " run, and the connection records"},
        {"file": "stream-no-replay--repro", "source": "repro/" + repro("no-replay"),
         "behavior": "Nothing is replayed after a reconnect",
         "select": whole, "kept": "the whole capture"},
        {"file": "stream-idle-close--no-ping", "source": "hb-idle.jsonl",
         "behavior": "A connection with no traffic is closed after about 125 s, without a"
         " close frame", "select": whole, "kept": "the whole capture"},
        {"file": "stream-idle-close--ping", "source": "hb-idle-ping.jsonl",
         "behavior": "A connection with no traffic is closed after about 125 s, without a"
         " close frame", "select": whole, "kept": "the whole capture"},
        {"file": "stream-idle-close--repro", "source": "repro/" + repro("idle-close"),
         "behavior": "A connection with no traffic is closed after about 125 s, without a"
         " close frame", "select": whole, "kept": "the whole capture"},
        {"file": "stream-slow-consumer--settle-1", "source": "settle-watch.jsonl",
         "behavior": "The server ends connections it considers slow consumers",
         "select": connection_tails, "kept": "every record but data frames, and data frames"
         " from the last second before each connection closed"},
        {"file": "stream-slow-consumer--settle-2", "source": "settle-watch-2.jsonl",
         "behavior": "The server ends connections it considers slow consumers",
         "select": connection_tails, "kept": "every record but data frames, and data frames"
         " from the last second before each connection closed"},
        {"file": "stream-slow-consumer--repro", "source": "repro/" + repro("slow-consumer"),
         "behavior": "The server ends connections it considers slow consumers",
         "select": connection_tails, "kept": "every record but data frames, and data frames"
         " from the last second before each connection closed"},
        {"file": "stream-duplicates--repro", "source": "repro/" + repro("duplicates"),
         "behavior": "Messages arrive more than once; a token's events arrive out of"
         " timestamp order", "select": repeats_and_regressions,
         "kept": "both copies of each repeated message and both events of each timestamp"
         " regression, with the connection records"},
        {"file": "stream-duplicates--long-run", "source": "long-run.jsonl",
         "behavior": "Messages arrive more than once; a token's events arrive out of"
         " timestamp order", "select": repeats_and_regressions,
         "kept": "both copies of each repeated message and both events of each timestamp"
         " regression, with the connection records"},
        {"file": "stream-late-change--settle-2", "source": "settle-watch-2.jsonl",
         "behavior": "A change stamped before an opening book can arrive after it",
         "select": opening_seconds("settle2", 1.0),
         "kept": "everything the first connection received in its first second"},
        {"file": "hash--probe", "source": "probe-long.jsonl",
         "behavior": "The order-book hash is a reproducible SHA-1 of the book; a trade enters"
         " it before it is announced; the stream can omit a change",
         "select": whole, "kept": "the whole 90-second capture of eight election markets"},
        {"file": "settlement--settle-2", "source": "settle-watch-2.jsonl",
         "behavior": "A settlement can go unannounced",
         "select": settlement_frames(settle_markets),
         "kept": "each market's frames from its end date on, market_resolved, each"
         " connection's opening frame, and the connection records; a note naming the"
         " markets is added from select_markets.py output"},
        {"file": "settlement--all-resolved-1", "source": "repro/" + repro("settlement"),
         "behavior": "When every market on a connection has settled, the server closes it",
         "select": settlement_frames(), "kept": "the market's frames from its end date on,"
         " market_resolved, each connection's opening frame, and the connection records"},
        {"file": "settlement--all-resolved-2", "source": "repro/settlement-20261004T160013Z.jsonl",
         "behavior": "When every market on a connection has settled, the server closes it",
         "select": settlement_frames(), "kept": "the market's frames from its end date on,"
         " market_resolved, each connection's opening frame, the connection records, and the"
         " market-lookup polls"},
        {"file": "settlement--others-open", "source": "repro/" + repro("slow-consumer"),
         "behavior": "When every market on a connection has settled, the server closes it",
         "select": settlement_frames(), "kept": "each market's frames from its end date on,"
         " market_resolved, each connection's opening frame, and the connection records"},
        {"file": "settled-subscription--mixed", "source": "mixed-sub.jsonl",
         "behavior": "Settled tokens are silently left out of a subscription",
         "select": opening_frames, "kept": "the connection records and the opening frame"},
        {"file": "settled-subscription--alone", "source": "settled-sub.jsonl",
         "behavior": "Settled tokens are silently left out of a subscription",
         "select": opening_frames, "kept": "the connection records and the opening frame"},
        {"file": "settled-subscription--repro", "source": "repro/settled-subscription-20261004T160139Z.jsonl",
         "behavior": "Settled tokens are silently left out of a subscription; REST returns"
         " 404", "select": opening_frames, "kept": "the connection records, each connection's"
         " opening frame, and the REST results"},
    ]


def encode(records: list[dict]) -> bytes:
    return "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records).encode()


def main() -> None:
    EVIDENCE.mkdir(exist_ok=True)
    manifest = []
    for item in plan():
        source = CAPTURES / item["source"]
        records = scrub(item["select"](read(source)))
        data = encode(records)
        name = item["file"] + ".jsonl"
        if len(data) > COMPRESS_OVER:
            buf = io.BytesIO()
            with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0) as gz:
                gz.write(data)
            data, name = buf.getvalue(), name + ".gz"
        (EVIDENCE / name).write_bytes(data)
        manifest.append({"file": name, "behavior": item["behavior"],
                         "source": item["source"], "source_bytes": source.stat().st_size,
                         "source_sha256": sha256(source), "kept": item["kept"],
                         "records": len(records), "bytes": len(data),
                         "sha256": hashlib.sha256(data).hexdigest()})
        print(f"{name:45} {len(records):6} records {len(data):9,} bytes  from {item['source']}")
    (EVIDENCE / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    total = sum(m["bytes"] for m in manifest)
    print(f"{len(manifest)} files, {total:,} bytes")


if __name__ == "__main__":
    main()
