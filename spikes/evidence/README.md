# Evidence

Excerpts from the captures behind
[`docs/source-behavior.md`](../../docs/source-behavior.md). They keep, from
each full capture, the records that demonstrate a behavior in the findings'
[catalog](../../docs/source-behavior.md#behavior-the-documentation-and-sdk-do-not-state).
The full captures are hundreds of megabytes and stay out of git.

The excerpts record `polymarket-client` 0.12.0 and the market WebSocket as
of October 4, 2026, so they keep showing that behavior after the SDK or the
service changes. Whether a behavior persists is for the `repro_*.py` scripts,
run live.

## Checking them

```sh
uv run spikes/check_evidence.py       # add -v to print every report
```

[`check_evidence.py`](../check_evidence.py) runs offline. It first checks
every file against the SHA-256 in [`manifest.json`](manifest.json). Then it
runs the reproduction scripts' own analyses over the excerpts, and compares
each verdict and key figure with what the findings report. It exits 1 if
anything differs.

## How they were cut

[`extract_evidence.py`](../extract_evidence.py) cut them from the captures,
deterministically: rerunning it on the same captures gives byte-identical
files. `manifest.json` records, for each excerpt, its source capture's size
and SHA-256, what was kept, and the excerpt's own SHA-256. With the full
captures, anyone can confirm an excerpt came from them by rerunning the
script. Cookies that the websockets debug log recorded from Cloudflare's
handshake were dropped, and the script refuses to write an excerpt that
still contains one.

Some excerpts keep less than the analysis saw on the full capture, which
changes a few printed figures but not the verdicts:

- `sdk-drops-events--long-run` keeps every tenth `new_market` frame. Its
  parser rejects 355 of 369, about the 96% of the full hour's 3,595 of 3,754.
- `stream-slow-consumer--*` keeps PING and PONG timing in full, but data
  frames only from the last second before each close.
- `settlement--*` keeps each market's frames from its end date on, so
  trading before the end date is left out.
- `settled-subscription--*` keeps each connection's opening frame and drops
  later market data, none of which concerned the settled tokens.

## Files

| File | Behavior | Source capture | Records | Bytes |
| --- | --- | --- | ---: | ---: |
| `sdk-silent-reconnect--sdk-run.jsonl.gz` | The SDK's stream reconnects and resubscribes without telling its consumer | `sdk-reconnect.jsonl` | 730 | 45,077 |
| `sdk-silent-reconnect--busy.jsonl.gz` | The SDK's stream reconnects and resubscribes without telling its consumer | `repro/silent-reconnect-20261004T154747Z.jsonl` | 2,691 | 162,067 |
| `sdk-drops-events--long-run.jsonl.gz` | The SDK drops events its parser rejects, logging only at DEBUG | `long-run.jsonl` | 374 | 124,536 |
| `stream-no-replay--repro.jsonl` | Nothing is replayed after a reconnect | `repro/no-replay-20261004T154157Z.jsonl` | 44 | 62,592 |
| `stream-idle-close--no-ping.jsonl` | A connection with no traffic is closed after about 125 s, without a close frame | `hb-idle.jsonl` | 9 | 1,475 |
| `stream-idle-close--ping.jsonl` | A connection with no traffic is closed after about 125 s, without a close frame | `hb-idle-ping.jsonl` | 71 | 9,314 |
| `stream-idle-close--repro.jsonl` | A connection with no traffic is closed after about 125 s, without a close frame | `repro/idle-close-20261004T154157Z.jsonl` | 37 | 5,047 |
| `stream-slow-consumer--settle-1.jsonl.gz` | The server ends connections it considers slow consumers | `settle-watch.jsonl` | 1,293 | 109,712 |
| `stream-slow-consumer--settle-2.jsonl.gz` | The server ends connections it considers slow consumers | `settle-watch-2.jsonl` | 6,145 | 458,576 |
| `stream-slow-consumer--repro.jsonl.gz` | The server ends connections it considers slow consumers | `repro/slow-consumer-20261004T154944Z.jsonl` | 960 | 75,634 |
| `stream-duplicates--repro.jsonl.gz` | Messages arrive more than once; a token's events arrive out of timestamp order | `repro/duplicates-20261004T154159Z.jsonl` | 134 | 12,567 |
| `stream-duplicates--long-run.jsonl` | Messages arrive more than once; a token's events arrive out of timestamp order | `long-run.jsonl` | 42 | 72,416 |
| `stream-late-change--settle-2.jsonl.gz` | A change stamped before an opening book can arrive after it | `settle-watch-2.jsonl` | 907 | 78,236 |
| `hash--probe.jsonl.gz` | The order-book hash is a reproducible SHA-1 of the book; a trade enters it before it is announced; the stream can omit a change | `probe-long.jsonl` | 472 | 68,478 |
| `settlement--settle-2.jsonl.gz` | A settlement can go unannounced | `settle-watch-2.jsonl` | 499 | 40,844 |
| `settlement--all-resolved-1.jsonl.gz` | When every market on a connection has settled, the server closes it | `repro/settlement-20261004T154157Z.jsonl` | 371 | 23,422 |
| `settlement--all-resolved-2.jsonl.gz` | When every market on a connection has settled, the server closes it | `repro/settlement-20261004T160013Z.jsonl` | 363 | 25,734 |
| `settlement--others-open.jsonl.gz` | When every market on a connection has settled, the server closes it | `repro/slow-consumer-20261004T154944Z.jsonl` | 359 | 26,865 |
| `settled-subscription--mixed.jsonl` | Settled tokens are silently left out of a subscription | `mixed-sub.jsonl` | 6 | 17,130 |
| `settled-subscription--alone.jsonl` | Settled tokens are silently left out of a subscription | `settled-sub.jsonl` | 6 | 1,133 |
| `settled-subscription--repro.jsonl` | Settled tokens are silently left out of a subscription; REST returns 404 | `repro/settled-subscription-20261004T160139Z.jsonl` | 12 | 19,945 |

The data is Polymarket's public market data, cut to what each behavior
needs.
