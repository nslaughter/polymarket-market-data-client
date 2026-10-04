# Source-behavior spikes

Scripts for the source investigation recorded in
[`docs/source-behavior.md`](../docs/source-behavior.md). They are not part of
the package or its checks. Each declares its dependencies inline (PEP 723)
and runs with `uv run`; `orderbook.py` and `live.py` are helpers the others
import.

## Reproducing the behavior the documentation does not state

Each `repro_*.py` subcommand checks one behavior from the catalog in
[`docs/source-behavior.md`](../docs/source-behavior.md#behavior-the-documentation-and-sdk-do-not-state).
Run without options, it picks current markets, runs live, saves a capture
under `spikes/captures/repro/`, and prints a verdict: REPRODUCED, NOT
REPRODUCED, or INCONCLUSIVE, with the reason. With `--capture FILE`
(repeatable), it analyzes recorded captures instead, including the ones the
findings came from.

```sh
uv run spikes/repro_sdk.py silent-reconnect           # about 2 minutes
uv run spikes/repro_sdk.py drops-events               # 3 minutes
uv run spikes/repro_stream.py idle-close              # up to 4.5 minutes
uv run spikes/repro_stream.py no-replay               # 1 minute
uv run spikes/repro_stream.py duplicates              # 5 minutes; also checks order
uv run spikes/repro_stream.py slow-consumer           # 10 minutes; depends on activity
uv run spikes/repro_settlement.py settled-subscription  # 30 seconds
uv run spikes/repro_settlement.py settlement          # until the next 5-minute
                                                      # market settles and lookup
                                                      # shows it; about 8 minutes

# Re-analyze the captures the findings came from, where they are kept:
uv run spikes/repro_sdk.py silent-reconnect --capture spikes/captures/sdk-reconnect.jsonl
uv run spikes/repro_settlement.py settlement \
    --capture spikes/captures/settle-watch-2.jsonl --markets spikes/captures/markets-1.jsonl
```

## The investigation's runs

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
