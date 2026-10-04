# /// script
# requires-python = ">=3.12"
# dependencies = ["polymarket-client==0.12.0", "websockets==15.0.1"]
# ///
"""Force disconnects under the SDK's market stream and record what it does.

The SDK is pointed at a local TCP proxy through its environment config
(polymarket._internal.environment, a private module; the public Environment
type has no way to set the URL). The proxy rewrites the Host header, opens
TLS to the real market WebSocket, and forwards server frames whole, so it can
inject a close frame between frames. It runs this fault schedule, in seconds
after the SDK subscribes:

  --kill-at      abort both TCP connections without a close frame
  --stall-at     stop forwarding in both directions for --stall-for seconds,
                 keeping the TCP connections open
  --close-at     send the SDK a server close frame (1001) and close TCP
  --outage-at    stop accepting connections and abort the open one for
                 --outage-for seconds

A direct reference connection to the same markets runs alongside, so the
events the source sent while the SDK was disconnected can be compared with
what the SDK delivered after it reconnected.

Everything goes to one JSON Lines file: proxy actions, SDK log records, the
websockets library's log for the SDK's socket (sent frames and control
frames), SDK events, and the reference connection's raw frames. Run with
--summarize FILE to print the timeline and, for each fault, what the
reference connection received that the SDK never delivered.
"""

import argparse
import asyncio
import json
import logging
import ssl
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from websockets.asyncio.client import connect

UPSTREAM_HOST = "ws-subscriptions-clob.polymarket.com"
UPSTREAM_URL = f"wss://{UPSTREAM_HOST}/ws/market"


class Recorder:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("a", buffering=1)
        self.start = time.monotonic()

    def write(self, kind: str, **fields) -> None:
        now = time.monotonic()
        rec = {"t": time.time(), "elapsed": round(now - self.start, 6), "kind": kind, **fields}
        self._file.write(json.dumps(rec, default=str) + "\n")


class LogToRecorder(logging.Handler):
    def __init__(self, rec: Recorder, kind: str, keep=lambda msg: True) -> None:
        super().__init__(logging.DEBUG)
        self._rec, self._kind, self._keep = rec, kind, keep

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        if self._keep(msg):
            self._rec.write(self._kind, logger=record.name, level=record.levelname, msg=msg)


def keep_ws_line(msg: str) -> bool:
    # Sent text frames show the subscription frames; received text is too
    # much to log and is visible as SDK events instead.
    if msg.startswith("< TEXT"):
        return False
    return msg.startswith(("> ", "< ", "= connection", "x ", "! "))


# --- TCP proxy -------------------------------------------------------------


class FrameSplitter:
    """Split the server-to-client byte stream into the HTTP response and
    whole WebSocket frames, so a close frame can be injected between frames."""

    def __init__(self) -> None:
        self._buf = b""
        self._in_http = True

    def feed(self, data: bytes) -> list[bytes]:
        self._buf += data
        out = []
        if self._in_http:
            end = self._buf.find(b"\r\n\r\n")
            if end < 0:
                return out
            out.append(self._buf[: end + 4])
            self._buf = self._buf[end + 4 :]
            self._in_http = False
        while len(self._buf) >= 2:
            b1 = self._buf[1]
            n = b1 & 0x7F
            head = 2
            if n == 126:
                if len(self._buf) < 4:
                    break
                n, head = int.from_bytes(self._buf[2:4]), 4
            elif n == 127:
                if len(self._buf) < 10:
                    break
                n, head = int.from_bytes(self._buf[2:10]), 10
            if b1 & 0x80:
                head += 4
            if len(self._buf) < head + n:
                break
            out.append(self._buf[: head + n])
            self._buf = self._buf[head + n :]
        return out


class ProxiedConnection:
    def __init__(self, proxy: "Proxy", cid: int, creader, cwriter) -> None:
        self.proxy, self.cid = proxy, cid
        self.creader, self.cwriter = creader, cwriter
        self.ureader = self.uwriter = None
        self.flowing = asyncio.Event()
        self.flowing.set()
        self.client_lock = asyncio.Lock()
        self.tasks: list[asyncio.Task] = []

    async def run(self) -> None:
        rec = self.proxy.rec
        try:
            self.ureader, self.uwriter = await asyncio.open_connection(
                UPSTREAM_HOST, 443, ssl=ssl.create_default_context(),
                server_hostname=UPSTREAM_HOST)
        except OSError as exc:
            rec.write("proxy", action="upstream connect failed", conn=self.cid, error=repr(exc))
            self.cwriter.transport.abort()
            return
        rec.write("proxy", action="upstream connected", conn=self.cid)
        self.tasks = [asyncio.create_task(self._client_to_upstream()),
                      asyncio.create_task(self._upstream_to_client())]
        await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)
        self.abort()
        rec.write("proxy", action="connection ended", conn=self.cid)

    async def _client_to_upstream(self) -> None:
        head = b""
        while b"\r\n\r\n" not in head:
            data = await self.creader.read(65536)
            if not data:
                return
            head += data
        lines = head.split(b"\r\n")
        lines = [b"Host: " + UPSTREAM_HOST.encode() if ln.lower().startswith(b"host:") else ln
                 for ln in lines]
        self.uwriter.write(b"\r\n".join(lines))
        while True:
            data = await self.creader.read(65536)
            if not data:
                return
            await self.flowing.wait()
            self.uwriter.write(data)
            await self.uwriter.drain()

    async def _upstream_to_client(self) -> None:
        splitter = FrameSplitter()
        while True:
            data = await self.ureader.read(65536)
            if not data:
                return
            for chunk in splitter.feed(data):
                await self.flowing.wait()
                async with self.client_lock:
                    self.cwriter.write(chunk)
                    await self.cwriter.drain()

    def abort(self) -> None:
        for w in (self.cwriter, self.uwriter):
            if w is not None:
                w.transport.abort()

    async def send_close(self, code: int, reason: str) -> None:
        payload = code.to_bytes(2) + reason.encode()
        async with self.client_lock:
            self.cwriter.write(bytes([0x88, len(payload)]) + payload)
            await self.cwriter.drain()
            self.flowing.clear()  # forward nothing after the close frame
        await asyncio.sleep(2)  # let the client answer before TCP closes
        self.abort()


class Proxy:
    def __init__(self, rec: Recorder) -> None:
        self.rec = rec
        self.port = 0
        self.server = None
        self.conns: dict[int, ProxiedConnection] = {}
        self._next = 0

    async def listen(self) -> None:
        self.server = await asyncio.start_server(self._accept, "127.0.0.1", self.port,
                                                 reuse_address=True)
        self.port = self.server.sockets[0].getsockname()[1]
        self.rec.write("proxy", action="listening", port=self.port)

    def stop_listening(self) -> None:
        self.server.close()
        self.rec.write("proxy", action="stopped listening")

    async def _accept(self, creader, cwriter) -> None:
        self._next += 1
        conn = ProxiedConnection(self, self._next, creader, cwriter)
        self.conns[conn.cid] = conn
        self.rec.write("proxy", action="accepted", conn=conn.cid)
        try:
            await conn.run()
        finally:
            self.conns.pop(conn.cid, None)

    def live(self) -> list[ProxiedConnection]:
        return list(self.conns.values())


async def fault_schedule(proxy: Proxy, args, t0: float) -> None:
    async def at(offset: float) -> None:
        await asyncio.sleep(max(0.0, t0 + offset - time.monotonic()))

    rec = proxy.rec
    await at(args.kill_at)
    rec.write("fault", action="kill", conns=[c.cid for c in proxy.live()])
    for c in proxy.live():
        c.abort()

    await at(args.stall_at)
    stalled = proxy.live()
    rec.write("fault", action="stall start", conns=[c.cid for c in stalled])
    for c in stalled:
        c.flowing.clear()
    await asyncio.sleep(args.stall_for)
    rec.write("fault", action="stall end", conns=[c.cid for c in stalled])
    for c in stalled:
        c.flowing.set()

    await at(args.close_at)
    rec.write("fault", action="close frame 1001", conns=[c.cid for c in proxy.live()])
    await asyncio.gather(*(c.send_close(1001, "going away") for c in proxy.live()))

    await at(args.outage_at)
    rec.write("fault", action="outage start", conns=[c.cid for c in proxy.live()])
    proxy.stop_listening()
    for c in proxy.live():
        c.abort()
    await asyncio.sleep(args.outage_for)
    await proxy.listen()
    rec.write("fault", action="outage end")


# --- SDK and reference connection -----------------------------------------


def ms(dt: datetime | None) -> int | None:
    return None if dt is None else round(dt.timestamp() * 1000)


def describe(event) -> dict:
    p = event.payload
    d = {"type": event.type}
    if event.type == "price_change":
        d["ts"] = ms(p.timestamp)
        d["changes"] = [[c.asset_id, str(c.price), c.side, str(c.size), c.hash]
                        for c in p.price_changes]
    elif event.type == "book":
        d.update(asset=p.asset_id, ts=ms(p.timestamp), hash=p.hash,
                 bids=len(p.bids), asks=len(p.asks))
    elif event.type in ("market_resolved", "new_market"):
        d.update(market=p.market, ts=ms(p.timestamp))
    else:
        d.update(asset=getattr(p, "asset_id", None), ts=ms(getattr(p, "timestamp", None)))
    return d


async def reference(rec: Recorder, assets: list[str], stop: asyncio.Event) -> None:
    logger = logging.getLogger("reference.websockets")
    async with connect(UPSTREAM_URL, ping_interval=None, max_size=None, logger=logger) as ws:
        await ws.send(json.dumps({"type": "market", "assets_ids": assets,
                                  "custom_feature_enabled": True}))
        rec.write("ref", event="subscribed")

        async def ping():
            while True:
                await asyncio.sleep(10)
                await ws.send("PING")

        pinger = asyncio.create_task(ping())
        reader = asyncio.create_task(read(ws, rec))
        await stop.wait()
        pinger.cancel()
        reader.cancel()


async def read(ws, rec: Recorder) -> None:
    async for msg in ws:
        if msg != "PONG":
            rec.write("ref_recv", text=msg)


async def run(args) -> None:
    from polymarket import AsyncPublicClient
    from polymarket._internal.environment import PRODUCTION_CONFIG, create_environment
    from polymarket.streams import MarketSpec

    rec = Recorder(Path(args.out))
    sdk_logger = logging.getLogger("spike.sdk")
    sdk_logger.setLevel(logging.DEBUG)
    sdk_logger.addHandler(LogToRecorder(rec, "sdk_log"))
    sdk_logger.propagate = False
    ws_logger = logging.getLogger("websockets.client")
    ws_logger.setLevel(logging.DEBUG)
    ws_logger.addHandler(LogToRecorder(rec, "sdk_ws", keep_ws_line))
    ws_logger.propagate = False

    proxy = Proxy(rec)
    await proxy.listen()
    url = f"ws://127.0.0.1:{proxy.port}/ws/market"
    env = create_environment(name="spike-proxy",
                             config=replace(PRODUCTION_CONFIG, clob_market_ws_url=url))
    assets = []
    for line in Path(args.markets).read_text().splitlines():
        m = json.loads(line)
        if m["slug"] in args.slug:
            assets.extend(m["token_ids"])
    rec.write("note", sdk="polymarket-client 0.12.0", url=url, assets=assets,
              schedule={k: getattr(args, k) for k in
                        ("kill_at", "stall_at", "stall_for", "close_at", "outage_at",
                         "outage_for", "duration")})

    stop = asyncio.Event()
    ref = asyncio.create_task(reference(rec, assets, stop))
    await asyncio.sleep(2)  # let the reference subscribe first
    async with AsyncPublicClient(env, logger=sdk_logger) as client:
        handle = await client.subscribe(MarketSpec(token_ids=assets, custom_feature_enabled=True))
        t0 = time.monotonic()
        rec.write("sdk", event="subscribed")
        faults = asyncio.create_task(fault_schedule(proxy, args, t0))
        try:
            async with asyncio.timeout(args.duration):
                async for event in handle:
                    rec.write("sdk_event", **describe(event))
        except TimeoutError:
            pass
        rec.write("sdk", event="done", dropped=handle.dropped)
        faults.cancel()
        await handle.close()
    stop.set()
    await ref


# --- Summary ---------------------------------------------------------------


def summarize(path: str) -> None:
    recs = [json.loads(line) for line in open(path)]
    print("== Timeline (proxy faults, SDK logs, SDK socket lifecycle, sent frames)")
    for r in recs:
        k = r["kind"]
        if k in ("fault", "proxy", "sdk", "note"):
            rest = {x: y for x, y in r.items() if x not in ("t", "elapsed", "kind")}
            print(f"{r['elapsed']:9.3f} {k:8} {json.dumps(rest)}")
        elif k == "sdk_log":
            print(f"{r['elapsed']:9.3f} sdk_log  {r['level']} {r['msg']}")
        elif k == "sdk_ws":
            msg = r["msg"]
            if msg.startswith(("> TEXT 'PING'", "< TEXT 'PONG'", "> PING", "< PONG")):
                continue
            print(f"{r['elapsed']:9.3f} sdk_ws   {msg[:160]}")

    sdk = [r for r in recs if r["kind"] == "sdk_event" and r["type"] != "new_market"]
    ref = []
    ref_states = {}  # (token, hash) -> (elapsed, event type, source timestamp)
    for r in recs:
        if r["kind"] != "ref_recv":
            continue
        d = json.loads(r["text"])
        for e in d if isinstance(d, list) else [d]:
            et = e.get("event_type")
            if et == "new_market":
                continue
            ref.append((r["elapsed"], e))
            if et == "price_change":
                for c in e["price_changes"]:
                    ref_states.setdefault((c["asset_id"], c["hash"]),
                                          (r["elapsed"], et, e["timestamp"]))
            elif et == "book":
                ref_states.setdefault((e["asset_id"], e["hash"]),
                                      (r["elapsed"], et, e["timestamp"]))
    sdk_hashes = set()
    for r in sdk:
        if r["type"] == "price_change":
            sdk_hashes.update((c[0], c[4]) for c in r["changes"])
        elif r["type"] == "book":
            sdk_hashes.add((r["asset"], r["hash"]))
    print("\n== SDK delivery around each fault")
    faults = [r for r in recs if r["kind"] == "fault" and r["action"] != "stall end"
              and r["action"] != "outage end"]
    for f in faults:
        before = [r for r in sdk if r["elapsed"] < f["elapsed"]]
        after = [r for r in sdk if r["elapsed"] > f["elapsed"]]
        if not before or not after:
            continue
        a, b = before[-1], after[0]
        inside = [e for el, e in ref if a["elapsed"] < el < b["elapsed"]]
        missed = 0
        for e in inside:
            if e["event_type"] == "price_change":
                missed += sum((c["asset_id"], c["hash"]) not in sdk_hashes
                              for c in e["price_changes"])
            elif e["event_type"] == "book":
                missed += (e["asset_id"], e["hash"]) not in sdk_hashes
        print(f"  {f['action']} at {f['elapsed']:.3f}: last SDK event {a['elapsed']:.3f},"
              f" next {b['elapsed']:.3f} ({b['elapsed'] - a['elapsed']:.1f} s, a {b['type']});"
              f" the reference connection received {len(inside)} events meanwhile,"
              f" {missed} book states of which the SDK never delivered")

    print("\n== Books the SDK delivered, matched by hash to the reference connection")
    for r in sdk:
        if r["type"] == "book":
            m = ref_states.get((r["asset"], r["hash"]))
            where = (f"reference reached it at {m[0]:.3f} via {m[1]} ts {m[2]}" if m
                     else "no matching reference state")
            print(f"  {r['elapsed']:9.3f} token {r['asset'][:12]}… ts {r['ts']}: {where}")
    end = [r for r in recs if r["kind"] == "sdk" and r.get("event") == "done"]
    if end:
        print(f"\nSDK handle dropped count at end: {end[0]['dropped']}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--markets", help="JSON Lines file from select_markets.py")
    p.add_argument("--slug", action="append", help="market slug to subscribe")
    p.add_argument("--kill-at", type=float, default=60)
    p.add_argument("--stall-at", type=float, default=120)
    p.add_argument("--stall-for", type=float, default=75)
    p.add_argument("--close-at", type=float, default=240)
    p.add_argument("--outage-at", type=float, default=300)
    p.add_argument("--outage-for", type=float, default=60)
    p.add_argument("--duration", type=float, default=420)
    p.add_argument("--out")
    p.add_argument("--summarize", metavar="FILE")
    args = p.parse_args()
    if args.summarize:
        summarize(args.summarize)
    else:
        if not (args.markets and args.slug and args.out):
            p.error("--markets, --slug and --out are required to run")
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
