# Source-behavior spikes

Scripts for the source investigation recorded in
[`docs/source-behavior.md`](../docs/source-behavior.md). They are not part of
the package or its checks. Each declares its dependencies inline (PEP 723)
and runs with `uv run`; `orderbook.py` is a helper the others import.

Captures go to `spikes/captures/`, which git ignores. Markets settle, so the
commands below need a fresh `markets.jsonl` and current slugs to rerun.

```sh
# Candidate markets: long-dated, short-dated, and recently settled.
uv run spikes/select_markets.py > spikes/captures/markets.jsonl

# Raw stream (probe, long, settle, heartbeat, idle, settled, and mixed runs).
uv run spikes/capture.py --markets spikes/captures/markets.jsonl --group long \
    --duration 90 --out spikes/captures/probe-long.jsonl
uv run spikes/capture.py --markets spikes/captures/markets.jsonl \
    --slug <market> --slug <market> --slug <market> \
    --duration 3600 --out spikes/captures/long-run.jsonl
uv run spikes/capture.py --markets spikes/captures/markets.jsonl \
    --slug <short-dated market> --duration 2700 --reconnect \
    --out spikes/captures/settle-watch.jsonl
uv run spikes/capture.py ... --ping-interval 0 --duration 600     # no PING
uv run spikes/capture.py ... --ping-for 60 --duration 600         # PING stops
uv run spikes/capture.py ... --no-custom-feature --ping-interval 0  # idle

# SDK under forced faults, through a local proxy.
uv run spikes/sdk_reconnect.py --markets spikes/captures/markets.jsonl \
    --slug <market> --slug <market> --out spikes/captures/sdk-reconnect.jsonl
uv run spikes/sdk_reconnect.py ... --faults withhold-pong \
    --withhold-pong-at 30 --withhold-pong-for 60 --duration 150
uv run spikes/sdk_reconnect.py --summarize spikes/captures/sdk-reconnect.jsonl

# REST snapshots joined to the stream.
uv run spikes/snapshot_join.py --markets spikes/captures/markets.jsonl \
    --slug <market> --slug <market> --slug <market> \
    --out spikes/captures/snapshot-join.jsonl
uv run spikes/snapshot_join.py --summarize spikes/captures/snapshot-join.jsonl

# Reports on captures.
uv run spikes/check_order.py spikes/captures/long-run.jsonl
uv run spikes/check_hashes.py spikes/captures/long-run.jsonl \
    --any-trade-price --drop-sample 20
```
