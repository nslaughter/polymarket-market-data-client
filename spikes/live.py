# /// script
# requires-python = ">=3.12"
# dependencies = ["polymarket-client==0.12.0", "websockets==15.0.1"]
# ///
"""Helpers shared by the repro_*.py scripts.

Imported by them; not run on its own. Provides market selection through the
SDK, raw market connections that record every frame, and the capture files
the reproductions write and re-read.
"""

import asyncio
import gzip
import json
import platform
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from importlib import metadata
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CAPTURES = Path(__file__).parent / "captures" / "repro"
PACKAGES = ("polymarket-client", "websockets", "pydantic", "pydantic-core", "httpx")
# Handshake headers that identify what answered: the service has no version.
SERVER_HEADERS = ("date", "server", "cf-ray")


def environment() -> dict:
    """What a result was checked against. The SDK and libraries have versions;
    the service does not, so its results are tied to the time and the
    handshake headers recorded with each connection."""
    try:
        commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                                text=True, cwd=Path(__file__).parent).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--", "."],
                               capture_output=True, text=True,
                               cwd=Path(__file__).parent).stdout.strip()
    except OSError:
        commit, dirty = "", ""
    versions = {}
    for name in PACKAGES:
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = None
    return {"checked_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "script": Path(sys.argv[0]).name, "argv": sys.argv[1:],
            "python": platform.python_version(), "platform": platform.platform(),
            "packages": versions, "endpoint": URL,
            "repo_commit": commit + ("+changes" if dirty else "")}


def describe_environment(env: dict | None) -> str:
    if not env:
        return "versions not recorded in this capture; see docs/source-behavior.md"
    pk = env["packages"]
    return (f"checked {env['checked_at']} with polymarket-client {pk['polymarket-client']},"
            f" websockets {pk['websockets']}, pydantic {pk['pydantic']}, Python"
            f" {env['python']}, repo {env['repo_commit'] or 'unknown'}")


class Recorder:
    """Append records to a new capture file named after the reproduction."""

    def __init__(self, name: str) -> None:
        CAPTURES.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.path = CAPTURES / f"{name}-{stamp}.jsonl"
        self._file = self.path.open("a", buffering=1)
        self.start = time.monotonic()
        self.write("environment", **environment())

    def write(self, kind: str, **fields) -> None:
        now = time.monotonic()
        rec = {"t": time.time(), "mono": now, "elapsed": round(now - self.start, 6),
               "kind": kind, **fields}
        self._file.write(json.dumps(rec, default=str) + "\n")


def load(paths: list[str]) -> list[dict]:
    """Read captures, plain or gzip-compressed (.gz)."""
    records = []
    for p in paths:
        with (gzip.open(p, "rt") if str(p).endswith(".gz") else open(p)) as f:
            records.extend(json.loads(line) for line in f)
    return records


def frames(records: list[dict], conn: str | None = None):
    """Yield (record, event) for each market event received, optionally on one
    connection. Handles capture.py, sdk_reconnect.py, and repro captures."""
    for r in records:
        if r.get("kind") not in ("recv", "ref_recv") or r["text"] == "PONG":
            continue
        if conn is not None and r.get("conn") != conn:
            continue
        d = json.loads(r["text"])
        for e in d if isinstance(d, list) else [d]:
            yield r, e


def utc(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%H:%M:%S.%f")[:-3]


# --- Markets ---------------------------------------------------------------


def describe_market(m) -> dict:
    return {"slug": m.slug, "question": m.question, "condition_id": m.condition_id,
            "token_ids": [m.outcomes.yes.token_id, m.outcomes.no.token_id],
            "end_date": m.state.end_date.isoformat() if m.state.end_date else None}


async def select(kind: str, *, prefix: str = "btc-updown-5m-",
                 min_ahead: timedelta = timedelta(0)) -> dict:
    """Pick one market of a kind:

    long     the open election market ending 30+ days out with most 24-hour volume
    ending   the open market starting with prefix that ends soonest, at least
             min_ahead from now
    settled  the closed market starting with prefix that ended most recently
    """
    from polymarket import AsyncPublicClient

    now = datetime.now(UTC)
    async with AsyncPublicClient() as client:
        if kind == "long":
            pages = client.list_markets(closed=False, end_date_min=now + timedelta(days=30),
                                        order="volume24hr", ascending=False, page_size=100)
            keep = (lambda m: m.state.accepting_orders
                    and any(w in (m.slug or "") for w in ("election", "nomination")))
        elif kind == "ending":
            pages = client.list_markets(closed=False, end_date_min=now + min_ahead,
                                        end_date_max=now + timedelta(hours=2),
                                        order="endDate", ascending=True, page_size=100)
            keep = lambda m: (m.slug or "").startswith(prefix)  # noqa: E731
        elif kind == "settled":
            pages = client.list_markets(closed=True, end_date_min=now - timedelta(hours=2),
                                        end_date_max=now, order="endDate", ascending=False,
                                        page_size=100)
            keep = lambda m: (m.slug or "").startswith(prefix)  # noqa: E731
        else:
            raise ValueError(kind)
        async for m in pages.iter_items():
            if keep(m):
                return describe_market(m)
    raise SystemExit(f"no {kind} market found")


# --- Raw market connections --------------------------------------------------


class MarketSocket:
    """One market connection that records what it sends and receives.

    Records: open, sent, recv, close (with the close frames each way, if any).
    `closed` is set when the connection ends for any reason.
    """

    def __init__(self, rec: Recorder, label: str, assets: list[str], *,
                 custom: bool = True, ping: float | None = 10.0,
                 on_text=None, logger=None) -> None:
        self.rec, self.label, self.assets = rec, label, assets
        self.custom, self.ping, self.on_text = custom, ping, on_text
        self.logger = logger  # websockets' logger; default "websockets.client"
        self.closed = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._ws = None

    async def open(self) -> "MarketSocket":
        extra = {"logger": self.logger} if self.logger else {}
        self._ws = await connect(URL, ping_interval=None, max_size=None, **extra)
        headers = self._ws.response.headers
        self.rec.write("open", conn=self.label,
                       server={h: headers.get(h) for h in SERVER_HEADERS})
        frame = json.dumps({"type": "market", "assets_ids": self.assets,
                            "custom_feature_enabled": self.custom})
        await self._ws.send(frame)
        self.rec.write("sent", conn=self.label, text=frame)
        self._tasks.append(asyncio.create_task(self._read()))
        if self.ping:
            self._tasks.append(asyncio.create_task(self._ping()))
        return self

    async def _ping(self) -> None:
        while True:
            await asyncio.sleep(self.ping)
            try:
                await self._ws.send("PING")
            except ConnectionClosed:
                return
            self.rec.write("sent", conn=self.label, text="PING")

    async def _read(self) -> None:
        try:
            async for msg in self._ws:
                self.rec.write("recv", conn=self.label, text=msg)
                if self.on_text:
                    self.on_text(msg)
        except ConnectionClosed:
            pass
        finally:
            p = self._ws.protocol
            self.rec.write("close", conn=self.label,
                           rcvd=None if p.close_rcvd is None
                           else {"code": p.close_rcvd.code, "reason": p.close_rcvd.reason},
                           sent=None if p.close_sent is None
                           else {"code": p.close_sent.code, "reason": p.close_sent.reason},
                           rcvd_then_sent=p.close_rcvd_then_sent)
            self.closed.set()

    async def close(self) -> None:
        for t in self._tasks[1:]:
            t.cancel()
        if self._ws is not None:
            await self._ws.close()
        if self._tasks:
            await asyncio.gather(self._tasks[0], return_exceptions=True)


def how_closed(records: list[dict], conn: str) -> str:
    """Say how a connection ended, from capture.py or MarketSocket records."""
    ends = [r for r in records if r.get("conn") == conn and r["kind"] == "end"]
    closes = [r for r in records if r.get("conn") == conn and r["kind"] == "close"]
    if not closes:
        return "still open when the capture ended"
    c = closes[-1]
    if c.get("rcvd") and c.get("rcvd_then_sent"):
        reason = c["rcvd"]["reason"]
        return f"server sent close {c['rcvd']['code']}" + (f" {reason!r}" if reason else "")
    if ends or c.get("sent"):
        return "closed by the client"
    return "closed by the server without a close frame"


VERDICTS: list[tuple[str, str]] = []  # every verdict printed, for check_evidence.py


def verdict(name: str, outcome: str, why: str, path: Path | str | list) -> None:
    VERDICTS.append((name, outcome))
    paths = path if isinstance(path, list) else [path]
    print(f"\n{name}: {outcome}\n  {why}")
    for p in paths:
        env = next((r for r in load([str(p)]) if r["kind"] == "environment"), None)
        server = next((r["server"] for r in load([str(p)]) if r.get("server")), None)
        print(f"  capture: {p}")
        print(f"    {describe_environment(env)}")
        if server:
            print(f"    server: {', '.join(f'{k} {v}' for k, v in server.items() if v)}")
