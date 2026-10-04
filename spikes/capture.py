# /// script
# requires-python = ">=3.12"
# dependencies = ["websockets==15.0.1"]
# ///
"""Capture the raw Polymarket market stream to a JSON Lines file.

Opens a connection to the market WebSocket, sends a `market` subscription
frame with custom_feature_enabled set (unless --no-custom-feature), and
records every frame in both directions with its wall-clock and monotonic
receipt time. When a connection closes, its close code and reason are
recorded; with --reconnect a new connection is opened and subscribed after
one second, until --duration.

The application heartbeat (the text frame PING) is sent every
--ping-interval seconds and can be stopped after --ping-for seconds, so the
server's response to a missing heartbeat can be observed. WebSocket protocol
pings from this client are off unless --protocol-ping is given, so the two
mechanisms can be told apart. Control frames the server sends (protocol
pings, close frames) are recorded from the websockets library's debug log.

Record kinds: environment (versions and repo commit), note, open (with the
server's handshake headers), sent, recv, control, close, end.
"""

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

import live

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class Recorder:
    def __init__(self, path: Path, label: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("a", buffering=1)
        self.label = label
        self.start = time.monotonic()

    def write(self, kind: str, **fields) -> None:
        now = time.monotonic()
        rec = {"t": time.time(), "mono": now, "elapsed": round(now - self.start, 6),
               "conn": self.label, "kind": kind, **fields}
        self._file.write(json.dumps(rec) + "\n")


class ControlFrameLog(logging.Handler):
    """Keep the websockets debug lines that describe control frames."""

    def __init__(self, rec: Recorder) -> None:
        super().__init__(logging.DEBUG)
        self._rec = rec

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        if msg[:2] in ("< ", "> ") and msg[2:].split(" ", 1)[0] in ("PING", "PONG", "CLOSE"):
            self._rec.write("control", line=msg)
        elif msg.startswith(("= connection", "x ", "! ")):
            self._rec.write("control", line=msg)


def load_assets(args) -> list[str]:
    assets = list(args.asset or [])
    if args.markets:
        for line in Path(args.markets).read_text().splitlines():
            m = json.loads(line)
            if args.group and m["group"] not in args.group:
                continue
            if args.slug and m["slug"] not in args.slug:
                continue
            assets.extend(m["token_ids"])
    if not assets:
        raise SystemExit("no token IDs selected")
    return assets


async def heartbeat(ws, rec: Recorder, interval: float, ping_for: float | None) -> None:
    started = time.monotonic()
    while True:
        await asyncio.sleep(interval)
        if ping_for is not None and time.monotonic() - started >= ping_for:
            rec.write("note", text="application PING stopped")
            return
        await ws.send("PING")
        rec.write("sent", text="PING")


async def run(args) -> None:
    rec = Recorder(Path(args.out), args.label)
    rec.write("environment", **live.environment())
    logger = logging.getLogger("websockets.client")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(ControlFrameLog(rec))
    logger.propagate = False

    assets = load_assets(args)
    frame = {"type": "market", "assets_ids": assets,
             "custom_feature_enabled": not args.no_custom_feature}
    ping_kwargs = {} if args.protocol_ping else {"ping_interval": None}
    rec.write("note", text="connecting", url=args.url, protocol_ping=args.protocol_ping,
              ping_interval=args.ping_interval, ping_for=args.ping_for)
    deadline = time.monotonic() + args.duration
    attempt = 1
    while not await session(args, rec, logger, frame, deadline, ping_kwargs):
        if not args.reconnect:
            return
        attempt += 1
        rec.label = f"{args.label}.{attempt}"
        await asyncio.sleep(1)


async def session(args, rec, logger, frame, deadline, ping_kwargs) -> bool:
    """Run one connection; return True once the duration is reached."""
    async with connect(args.url, max_size=None, logger=logger, **ping_kwargs) as ws:
        rec.write("open", server={h: ws.response.headers.get(h)
                                  for h in live.SERVER_HEADERS})
        text = json.dumps(frame)
        await ws.send(text)
        rec.write("sent", text=text)
        hb = None
        if args.ping_interval > 0:
            hb = asyncio.create_task(heartbeat(ws, rec, args.ping_interval, args.ping_for))
        done = False
        try:
            async with asyncio.timeout_at(asyncio.get_running_loop().time()
                                          + deadline - time.monotonic()):
                async for msg in ws:
                    rec.write("recv", text=msg if isinstance(msg, str) else msg.hex(),
                              binary=not isinstance(msg, str))
        except TimeoutError:
            rec.write("end", reason="duration reached")
            done = True
        except ConnectionClosed:
            pass
        finally:
            if hb:
                hb.cancel()
    rcvd, sent = ws.protocol.close_rcvd, ws.protocol.close_sent
    rec.write("close",
              rcvd=None if rcvd is None else {"code": rcvd.code, "reason": rcvd.reason},
              sent=None if sent is None else {"code": sent.code, "reason": sent.reason},
              rcvd_then_sent=ws.protocol.close_rcvd_then_sent)
    return done or time.monotonic() >= deadline


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--asset", action="append", help="token ID; repeatable")
    p.add_argument("--markets", help="JSON Lines file from select_markets.py")
    p.add_argument("--group", action="append", help="market group to take from --markets")
    p.add_argument("--slug", action="append", help="market slug to take from --markets")
    p.add_argument("--duration", type=float, default=300, help="seconds")
    p.add_argument("--ping-interval", type=float, default=10, help="0 sends no PING")
    p.add_argument("--ping-for", type=float, help="stop sending PING after this many seconds")
    p.add_argument("--protocol-ping", action="store_true",
                   help="let websockets send protocol pings (default: off)")
    p.add_argument("--no-custom-feature", action="store_true",
                   help="subscribe without custom_feature_enabled, so no lifecycle"
                        " events or new_market broadcasts arrive")
    p.add_argument("--reconnect", action="store_true",
                   help="open a new connection, labelled LABEL.N, when one closes")
    p.add_argument("--url", default=URL)
    p.add_argument("--label", default="c1")
    p.add_argument("--out", required=True)
    asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    main()
