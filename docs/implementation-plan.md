# Implementation plan

This plan divides the client's implementation into pull requests that can be
reviewed one at a time. Each names what it builds, what it leaves out, and
the checks that prove it done. The specifications are
[`spec/client.md`](../spec/client.md) and
[`spec/conformance.md`](../spec/conformance.md), at draft 0.1.0. Read
[`AGENTS.md`](../AGENTS.md) before starting any of them.

Steps 1 to 4 build the parts that need no network: the package, decoding,
the state machine, and the conformance harness. Steps 5 to 10 build the
client against the scripted server, one area of the contract at a time.
Steps 11 to 13 add the example, the live run, and the release.

## Progress

Each step's pull request changes its own row: it sets **Status** to `Done`,
and after the pull request is opened, a follow-up commit on the same branch
fills in **Pull request**. A row reads `Done` on `main` only once its pull
request is merged. A step whose status is `Needs operator decision` cannot
start until the operator records the decision and changes the status here.
The contract's design decisions, D1 to D8, are all
[owner specifications](../spec/client.md#owner-specifications), decided on
2026-10-05; the **Owner specifications** column names those each step
follows. Steps 12 and 13 still wait on the operator: step 12 for the live
run's period and markets, and step 13 for the release's version and tag
scheme.

| Step | Owner specifications | Status | Pull request |
| --- | --- | --- | --- |
| 1. Create the package, records, and configuration | D5, D8 | Not started | |
| 2. Decode frames | D8 | Not started | |
| 3. Keep books and token states | | Not started | |
| 4. Build the conformance harness | | Not started | |
| 5. Connect, subscribe, and deliver records | | Not started | |
| 6. Detect interruptions and recover | D1, D2 | Not started | |
| 7. Settle markets through the stream and lookup | D6 | Not started | |
| 8. Apply subscription changes | D7 | Not started | |
| 9. Bound the consumer handoff | D3 | Not started | |
| 10. Verify order-book hashes | D4 | Not started | |
| 11. Add the research example and check the built wheel | | Not started | |
| 12. Record a limited live run | live-run period and markets | Needs operator decision | |
| 13. Release a tagged wheel | release name | Needs operator decision | |

If the operator changes an owner specification, the scenarios marked
`owner-spec` for it may need to change. That change is a new version of the
specification, made before the step that depends on it, never inside an
implementation pull request.

## How a step turns on its scenarios

The conformance harness (step 4) runs the scenarios listed in
`tests/conformance/enabled.txt`, one name per line. Each step adds the names
it must pass. A scenario that is not listed has not been turned on yet; no
other way of skipping or relaxing a scenario is allowed. By the end of step
10 every scenario in `spec/conformance.md` is listed, and the harness checks
that.

## Package layout

A pull request may refine this layout if it explains why. Module names use
the import name D5 specifies, `polymarket_market_data`.

| Path | Contents |
| --- | --- |
| `src/polymarket_market_data/__init__.py` | The public names, and nothing else. |
| `_records.py` | Record dataclasses and enums. |
| `_config.py` | `ClientConfig`, `ReconnectPolicy`, and validation. |
| `_errors.py` | The exceptions. |
| `_decode.py` | Frame decoding, repeat detection, and affected tokens. Pure: no I/O. |
| `_book.py` | The order book: apply, replace, compare. Pure. |
| `_state.py` | Token states, capture gaps, and record order, driven by inputs. Pure. |
| `_hash.py` | The hash recipe and verification (step 10). Pure. |
| `_connection.py` | The socket, subscription frame, heartbeat, reader, and reconnection. |
| `_client.py` | `MarketDataClient`: the desired set, the queue, statistics, and the tasks. |
| `_lookup.py` | `MarketLookup` and the default lookup (D6). |
| `tests/` | Unit tests. |
| `tests/conformance/` | The scenario parser, the scripted server and lookup, the runner, and `enabled.txt`. It reads `spec/conformance.md` directly. |
| `examples/` | The research example (step 11). |

## Steps

### 1. Create the package, records, and configuration

Follows D5: CPython 3.12 and later, the distribution
`polymarket-market-data-client`, and the import `polymarket_market_data`.
Follows D8 for the types.

- A `pyproject.toml` for the distribution, a `src/` layout with `py.typed`,
  the runtime dependencies on `websockets` and on `pydantic` v2 (D8), and
  the optional extra D6 specifies, which installs `polymarket-client`
  0.12.0 for the default lookup. The ranges of `websockets` and `pydantic`
  must overlap the SDK's, `websockets` from 13 to below 16 and `pydantic`
  from 2 to below 3, so that the extra installs beside them.
- Development tools: `ruff` for formatting and linting, `mypy` in strict
  mode, and `pytest`. Commands as in [`AGENTS.md`](../AGENTS.md#commands).
- A CI workflow that, on each supported Python version, checks formatting,
  lints, type-checks, runs the tests, builds the wheel, installs it in a
  clean virtual environment, and imports the package from it.
- Every record, enum, exception, `ClientConfig`, and `ReconnectPolicy` the
  contract names, with their fields and types: records, `Market`, and the
  other public types as frozen dataclasses with slots, and `ClientConfig`
  and `ReconnectPolicy` as frozen Pydantic models (D8). Configuration
  validation raises `ConfigError` as the contract says, with Pydantic's
  validation error as its cause.
- Every default as the contract's
  [Configuration](../spec/client.md#configuration) table gives it,
  including those D1 to D4 and D6 specify.
- Tests: each invalid configuration value is refused; records are frozen;
  every public name is importable from the top level; every record type
  reads back equal through `TypeAdapter`'s `dump_json` and `validate_json`,
  except a payload's decimals, which come back as strings (D8). The cases
  include an `UndecodableFrame` for the binary frame `00ff`, an
  `UnknownEvent`, and a `NewMarketEvent` whose payload holds a decimal.

Out of scope: any behavior. `MarketDataClient` may exist only as a stub
that raises `NotImplementedError`.

### 2. Decode frames

- In `_decode.py`, turn one received frame into decoded events, an
  `UnknownEvent`, or `UndecodableFrame`s, exactly as
  [Decoding](../spec/client.md#decoding) says, with one Pydantic model per
  event type (D8): `json.loads` with `parse_float=Decimal` and
  `parse_constant=Decimal`, then each object validated by its model, never
  `model_validate_json`; the required and optional fields per type, finite
  decimals only, `side` values, integer timestamps, and the affected tokens
  of an undecodable event, named from the validation error's location. The
  decoder maps each model to its public record.
- Repeat detection over a window, comparing content with `price_change`
  entries as a multiset.
- Tests build frames with the conformance notation's shapes, written out as
  JSON in the tests. Cover every row of the decoding table, every event
  type, `NaN` and `Infinity` refused as strings and as literals, numbers
  sent as JSON numbers, one with more digits than a float holds and
  `1e400` among them, a `new_market` with a string `game_start_time`, and
  repeats inside and outside the window. Frames holding an unpaired
  surrogate escape, in a value of an unknown event and in a member name,
  each become an `invalid_json` record that reads back equal through
  `TypeAdapter`'s `dump_json` and `validate_json`.

Out of scope: books, states, and I/O.

### 3. Keep books and token states

- In `_book.py`, a book that takes a `book` event, applies entries as the
  [recovery contract](../spec/client.md#recovery-contract) says, and
  compares itself with a new book.
- In `_state.py`, the token state machine of
  [the contract](../spec/client.md#per-token-state-machine), driven by
  inputs (frames sent and received, interruptions, timeouts, lookup answers,
  application changes) and producing records in the contract's
  [order](../spec/client.md#record-order). It opens and ends capture gaps.
  Hash checks (T7, T9) take their result as an input, so step 10 can supply
  it.
- Tests drive each transition, T1 to T18, and each "changes nothing" case,
  and check the records and their order.

Out of scope: I/O, timers, and hash computation. Timers are inputs here.

### 4. Build the conformance harness

- In `tests/conformance/`, implement
  [`spec/conformance.md`](../spec/conformance.md) completely: the block
  parser and the `start` macro; the scripted server, on its own event loop,
  with every server step, the reference books, and the frame notation with
  its hashes; the scripted lookup; the runner with every runner and
  expectation step, matching, step times and `within` windows, the
  `Decimal` type check, shutdown checks, and failure reports.
- The harness computes hashes with its own implementation of the recipe,
  separate from the client's, and checks it against the four vectors in
  `spec/conformance.md` and the one in `spec/client.md`.
- A pytest entry point runs each scenario in `enabled.txt` as its own test.
  `enabled.txt` starts empty.
- Tests: every block in `spec/conformance.md` parses, and every label it
  uses is defined; each frame notation expands to the documented members in
  order; each server step behaves as documented against a plain
  `websockets` client; the matcher's rules, one test per rule.

Out of scope: the client. No scenario runs yet.

### 5. Connect, subscribe, and deliver records

- `MarketDataClient` with the desired set, `records()`, `backlog`, and
  `stats()`; the `async with` lifecycle; one connection with the
  subscription frame, the heartbeat's `PING`s, and a reader that never
  waits for the consumer; decoding, books, and states from steps 2 and 3;
  the queue; shutdown and cancellation as the contract says.
- Until step 9, a full queue ends the client with an error; no scenario of
  this step fills the profile's `queue_size` of 1000. Until step 6, a
  connection that ends unexpectedly does the same. Both are stopgaps that
  later steps replace, never silent losses.
- Turn on: `initial-books`, `market-event-types`, `mid-connection-book`,
  `out-of-order-types`, `repeated-messages`, `change-before-book`,
  `change-before-any-book`, `unknown-event-type`, `malformed-frames`,
  `invalid-known-event`, `new-market-filtered`, `new-market-delivered`,
  `quiet-subscription-pings`, `exit-while-connected`, `read-timeout`.

### 6. Detect interruptions and recover

Follows D1 and D2: the `PONG` timeout and the reconnect bounds.

- Detecting every interruption cause except `consumer_overflow` and
  `subscription_change`; the `PONG` timeout; reconnection with backoff,
  jitter, `connect_timeout`, and both bounds; capture gaps; `failed` and
  `RecoveryFailed`.
- Unit tests: with `jitter` on, every `retry_in` lies in [0, the delay
  for its attempt), and the values vary; with it off, each equals that
  delay. No scenario can show a random delay. Frames that arrive while
  the client closes a connection after a `pong_timeout` are neither
  delivered nor applied, and are counted; a scenario cannot time them.
- Turn on: `drop-without-close`, `close-frames`, `close-slow-consumer`,
  `pong-withheld`, `pong-late-within-timeout`, `startup-retry`,
  `connect-timeout`, `reconnect-refused-then-accepted`,
  `recovery-exhausted-attempts`, `recovery-exhausted-time`,
  `recovery-exhausted-no-frame`, `cancel-during-recovery`.

### 7. Settle markets through the stream and lookup

Follows D6: how the pinned SDK serves lookup and settlement confirmation.

- `MarketLookup`, the default lookup D6 specifies, and `resolve` with its
  errors; `book_timeout`; settlement by `market_resolved`, by the
  all-resolved close, and by lookup, with its polling and timeout; `ended`
  and `idle`; and `subscribe` on an idle client, which connects it, as
  `resolve-by-slug` needs. Changes to a running connection are step 8.
- The default lookup is tested with the SDK's HTTP layer replaced by
  recorded synthetic responses, never against the live service.
- Turn on: `resolve-by-slug`, `settle-announced-others-open`,
  `settle-all-resolved-close`, `settle-all-resolved-close-unannounced`,
  `settle-unannounced-drop`, `settled-at-subscription`,
  `settled-with-active`, `unknown-market`, `settlement-unconfirmed`,
  `settlement-without-slug`, `settle-lookup-after-late-book`.

### 8. Apply subscription changes

Follows D7: how subscription changes are applied.

- `subscribe` and `unsubscribe` while running, as D7 specifies, including the
  `subscription_change` interruption, which is never a failed attempt,
  changes made during recovery, and a market added again after it was
  removed.
- Turn on: `subscribe-while-connected`, `subscribe-before-first-frame`,
  `subscribe-during-outage`, `unsubscribe`, `unsubscribe-all-then-subscribe`,
  `resubscribe-removed`.

### 9. Bound the consumer handoff

Follows D3: the queue size and the response at the limit.

- `queue_size`, checked once per frame, `Backlog` records, and the
  `disconnect` response D3 specifies, including resuming afterwards;
  holding back reconnection while too many status records are waiting.
- Turn on: `consumer-stops-reading`, `frame-larger-than-queue`,
  `status-records-bounded`, `consumer-pause-outlasts-recovery-time`.

### 10. Verify order-book hashes

Follows D4: verification with the recipe, and its burst rule.

- `_hash.py` with the recipe, the trade-price retries, when a burst is
  checked, with `burst_quiet`'s timer as an input, the divergence rule, and
  the parameters fetched through lookup and retried every
  `settlement_poll_interval` after a failure, feeding T7 and T9 in
  `_state.py`. Statistics for checks.
- A unit test that the constructor raises `ConfigError` when `verify_hash`
  is on and no lookup is available. No scenario can show this: the harness
  always passes its scripted lookup.
- Turn on: `hash-checks-pass`, `hash-burst-across-frames`,
  `hash-single-failure`, `hash-divergence`, `hash-book-fails-own-check`,
  `hash-trade-before-announcement`, `hash-check-predates-undecodable`,
  `hash-parameters-retried`. The harness now checks that every scenario is
  enabled.

### 11. Add the research example and check the built wheel

- `examples/research.py`: chooses active markets through lookup when it runs,
  so it keeps working as markets settle, subscribes, and writes every record
  as one JSON line, an inspectable recovery timeline. A controlled mode
  runs the same code against the scripted server and a built-in scenario,
  with a dropped connection and a settlement, so it needs no access to the
  live service. The live mode needs the SDK extra, which provides the
  lookup that market selection and, with `verify_hash` on, hash checks
  need.
- The README gains an installation quickstart and the example's commands.
- CI builds the wheel, installs it in a clean virtual environment on each
  supported Python version, runs the whole conformance suite against the
  installed wheel, and runs the controlled example.

### 12. Record a limited live run

Needs the operator to agree the run's period and its markets, or how the
example chooses them. Only this step contacts the live service.

- Run the example against the live service for a limited period chosen
  with the operator, with the client's configuration recorded.
- Add `docs/live-run.md`: the client version and commit, the configuration,
  the markets and the observation period, every interruption and settlement
  observed with its records, and the source behavior left unresolved,
  carrying forward the open questions of `spec/client.md` and
  `docs/source-behavior.md`. It claims nothing the records do not show, and
  it states the source coverage limits: the markets, period, and versions
  the run covers.
- Raw output stays out of git, as the investigation's captures did.

### 13. Release a tagged wheel

Needs the operator to choose the first version and the tag scheme.

- A release workflow, triggered by a version tag, that builds the wheel,
  runs the full suite against it on each supported Python version, and
  attaches it to a GitHub release. The operator pushes the tag; the pull
  request adds the workflow only.
- Update the README's status line.
- Done when the workflow passes on the pull request, without publishing.
