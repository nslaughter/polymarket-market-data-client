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

Steps 14 to 17 come after the release, so the client ships first. They add
examples that show the client as a library to build on: an alert that acts
correctly on state it knows is incomplete, a seeded synthetic streamer, a
dashboard, and, if the owner chooses, a way for applications to test
their own recovery handling. Every example has a controlled mode that CI
runs against the scripted server or the streamer, each with its scripted
lookup; none needs the live service to be checked, and these steps never
contact it, since only step 12 does. The plan leaves out notebooks, whose
output would hold Polymarket's data; storage, which is the pipeline's;
integrations with web frameworks beyond the dashboard's relay; anything
that places orders; and replacing settled markets while running, which
edges toward the continuous operation the README leaves out of scope.

## Progress

Each step's pull request changes its own row: it sets **Status** to `Done`,
and after the pull request is opened, a follow-up commit on the same branch
fills in **Pull request**. A row reads `Done` on `main` only once its pull
request is merged. A step whose status is `Needs owner decision` cannot
start until the owner records the decision and changes the status here.
The contract's design decisions, D1 to D8, are all
[owner specifications](../spec/client.md#owner-specifications), decided on
2026-10-05; the **Owner specifications** column names those each step
follows. Step 12 still waits on the owner for the live run's period, to be
agreed once step 11 is merged. Step 17 waits on the owner to decide whether
the scripted server becomes a supported way to test applications, a
decision the owner deferred on 2026-10-08 until step 16 is merged.

| Step | Owner specifications | Status | Pull request |
| --- | --- | --- | --- |
| 1. Create the package, records, and configuration | D5, D8 | Done | [#6](https://github.com/nslaughter/polymarket-market-data-client/pull/6) |
| 2. Decode frames | D8 | Done | [#8](https://github.com/nslaughter/polymarket-market-data-client/pull/8) |
| 3. Keep books and token states | | Done | [#10](https://github.com/nslaughter/polymarket-market-data-client/pull/10) |
| 4. Build the conformance harness | | Done | [#11](https://github.com/nslaughter/polymarket-market-data-client/pull/11) |
| 5. Connect, subscribe, and deliver records | | Done | [#15](https://github.com/nslaughter/polymarket-market-data-client/pull/15) |
| 6. Detect interruptions and recover | D1, D2 | Done | [#18](https://github.com/nslaughter/polymarket-market-data-client/pull/18) |
| 7. Settle markets through the stream and lookup | D6 | Done | [#20](https://github.com/nslaughter/polymarket-market-data-client/pull/20) |
| 8. Apply subscription changes | D7 | Done | |
| 9. Bound the consumer handoff | D3 | Not started | |
| 10. Verify order-book hashes | D4 | Not started | |
| 11. Add the research example and check the built wheel | | Not started | |
| 12. Record a limited live run | live-run period and markets | Needs owner decision | |
| 13. Release a tagged wheel | release name | Not started | |
| 14. Add the price-alert example | | Not started | |
| 15. Stream synthetic markets | | Not started | |
| 16. Add the dashboard example | | Not started | |
| 17. Offer the scripted server for testing applications | public testing module | Needs owner decision | |

If the owner changes an owner specification, the scenarios marked
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
| `_client.py` | `MarketDataClient`: the desired set, statistics, and the tasks; it owns the queue. |
| `_queue.py` | The consumer handoff: the queue, its limit, and the records iterator. No I/O. Apart from `_client.py` because `_connection.py` feeds it too. |
| `_lookup.py` | `MarketLookup` and the default lookup (D6). |
| `tests/` | Unit tests. |
| `tests/conformance/` | The scenario parser, the scripted server and lookup, the runner, and `enabled.txt`. It reads `spec/conformance.md` directly. |
| `tests/streamer/` | The seeded synthetic streamer and its soak tests (step 15), built on the scripted server. |
| `examples/` | The research example and the quickstart (step 11), the price alert (step 14), and the dashboard (step 16). |

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
  and `ReconnectPolicy` as frozen Pydantic models (D8). Their constructors
  raise `ConfigError` for an invalid value, as the contract's
  [Configuration](../spec/client.md#configuration) says, with Pydantic's
  validation error as its cause.
- Every default as the contract's
  [Configuration](../spec/client.md#configuration) table gives it,
  including those D1 to D4 and D6 specify.
- Tests: each invalid configuration value, a nested policy's included, is
  refused with `ConfigError`, never with Pydantic's `ValidationError`;
  records are frozen;
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
  uses is defined; a quoted value with spaces, such as
  `close_reason="going away"`, is one word whose value is read as a JSON
  string; each frame notation expands to the documented members in
  order; each server step behaves as documented against a plain
  `websockets` client; the matcher's rules, one test per rule.

Out of scope: the client. No scenario runs yet.

### 5. Connect, subscribe, and deliver records

- `MarketDataClient` with the desired set, `records()`, `backlog`, and
  `stats()`; the `async with` lifecycle; one connection with the
  subscription frame, the heartbeat's `PING`s, and a reader that never
  waits for the consumer; decoding, books, and states from steps 2 and 3;
  the queue; shutdown and cancellation as the contract says.
- A unit test that `MarketDataClient` refuses, with `ConfigError`, a
  configuration made invalid by `model_copy(update=…)`, which skips
  validation. No scenario can show this: the runner builds every
  configuration by its constructor.
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
- A unit test that `resolve` raises `ClientStateError` when no lookup is
  available, with `verify_hash` off and no lookup passed or installed. No
  scenario can show this: the harness always passes its scripted lookup.
- Turn on: `resolve-by-slug`, `settle-announced-others-open`,
  `settle-all-resolved-close`, `settle-all-resolved-close-unannounced`,
  `settle-unannounced-drop`, `settled-at-subscription`,
  `settled-with-active`, `unknown-market`, `settlement-unconfirmed`,
  `settlement-confirmation-across-reconnect`, `settlement-without-slug`,
  `settle-lookup-after-late-book`.

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
  `_state.py`. Statistics for checks, and `rest_book_not_found` for the
  `None` answers of `book_parameters` (D6), with a unit test that such an
  answer is counted and settles nothing.
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
- `examples/quickstart.py`: the shortest useful program. It resolves the
  market whose slug it is given, subscribes to it, and handles each record
  type with `match`, as the contract's
  [Public interface](../spec/client.md#public-interface) shows. It leaves
  the `async with` block, and so ends, when the client reports `idle`,
  which follows its market's settlement
  ([Connection states](../spec/client.md#connection-states)). Its `main`
  takes the configuration and the lookup, defaulting to the client's own.
- A test runs the quickstart with `url` set to the scripted server and
  with the scripted lookup. `url` alone is not enough: `resolve` and the
  hash checks, on by default (D4), call the lookup, and the default one
  calls the live service (D6). The server plays `A`'s opening books, a
  `price_change`, and `market_resolved`, and the test checks that the
  program prints each record and returns.
- The README gains an installation quickstart, the quickstart program, and
  the examples' commands. A test checks that the README's copy of the
  program matches `examples/quickstart.py`.
- CI builds the wheel, installs it in a clean virtual environment on each
  supported Python version, runs the whole conformance suite against the
  installed wheel, and runs the controlled example.

### 12. Record a limited live run

Needs the owner to agree the run's period, once step 11 is merged. The
owner decided on 2026-10-08 that the run watches the markets the research
example chooses when it runs, rather than a list fixed in advance. Only
this step contacts the live service.

- Run the example against the live service for a limited period chosen
  with the owner, with the client's configuration recorded.
- Add `docs/live-run.md`: the client version and commit, the configuration,
  the markets and the observation period, every interruption and settlement
  observed with its records, and the source behavior left unresolved,
  carrying forward the open questions of `spec/client.md` and
  `docs/source-behavior.md`. It claims nothing the records do not show, and
  it states the source coverage limits: the markets, period, and versions
  the run covers.
- Raw output stays out of git, as the investigation's captures did.

### 13. Release a tagged wheel

The owner decided on 2026-10-08 that the first release is version `0.1.0`,
tagged `v0.1.0`: a release's tag is `v` and its version.

- Set the package's version to `0.1.0`.
- A release workflow, triggered by a version tag, that builds the wheel,
  runs the full suite against it on each supported Python version, and
  attaches it to a GitHub release. The owner pushes the tag; the pull
  request adds the workflow only.
- Update the README's status line.
- Done when the workflow passes on the pull request, without publishing.

### 14. Add the price-alert example

An application that acts correctly on state it knows is incomplete, which
is what the client is for.

- `examples/alert.py`: watches tokens for their midpoint crossing a
  threshold. It keeps each token's book from the records, as the
  [recovery contract](../spec/client.md#recovery-contract) describes, and
  computes the midpoint in `Decimal` from that book's best bid and best
  ask. A book with no bid or no ask has no midpoint, and the alert skips
  it. The stream emptied each book before the settlements it announced,
  and its entries then gave a best bid of 0 and a best ask of 1
  ([findings §6](source-behavior.md#6-settlement)): that is no quote, not
  a midpoint of 0.5. The owner decided on 2026-10-08 that the alert then
  compares the next midpoint with the last one it had, rather than
  starting again when both sides return.
- A midpoint equal to the threshold counts as above it, so every midpoint
  lies on one side: a rising midpoint crosses when it reaches the
  threshold, and a falling one when it goes below it. The owner decided
  this on 2026-10-08, so a midpoint that touches the threshold from below
  and falls back gives two alerts. It compares each
  midpoint it has while a token is `ready` with the last one it had while
  the token was `ready`. If they lie on opposite sides of the threshold
  and the token stayed `ready` between them, it prints an alert, stamped
  with the `received_at` of the record that moved the midpoint. If the
  token left `ready` between them, it reports a crossing at an unknown
  time, giving both midpoints and when each arrived. It gives no time for
  the crossing, and no bounds from a `CaptureGap`: the token may have been
  `uncertain` before the gap opened, and events outside a gap may be
  missing too ([Recovery contract](../spec/client.md#recovery-contract)).
- While a token is `synchronizing` or `uncertain`, the alert prints the
  `reason` its `TokenStateChange` gives, and no alert, then or later. It
  compares at the token's `ready` record, which comes after the `book`
  that ends a gap ([Record order](../spec/client.md#record-order), rule
  3), so it reports each crossing once.
- When a market settles, it stops watching its tokens and reports the
  winner if known.
- Unit tests of the comparison, with the token `ready` throughout: a
  midpoint that rises to the threshold and then past it gives one alert,
  at the record that reached the threshold, and one that falls to the
  threshold and then below it gives one alert, at the record that went
  below it.
- A controlled mode runs it against the scripted server and the scripted
  lookup, in a built-in scenario. The midpoint crosses the threshold once
  while the token is `ready`; once while it is `uncertain` after an
  undecodable frame, a period that ends with no capture gap; and once
  across a capture gap that opened while the token was already
  `uncertain`. Then the book empties, and the market settles. CI runs it
  and checks that it prints one alert, two crossings at an unknown time,
  nothing for the emptied book, and the winner.
- The README lists the example and its commands.

Out of scope: placing orders, sending alerts anywhere but standard output,
and any advice on what to do with one.

### 15. Stream synthetic markets

A seeded streamer that makes the scripted server look like a live market:
for people watching the examples, for anything published about them, and
for soak tests. It is synthetic and says so, and it claims nothing about the
live source beyond [`docs/source-behavior.md`](source-behavior.md).

- `tests/streamer/`: drives the scripted server and its `Source` (step 4),
  so frames and hashes come from the harness's one implementation, never a
  copy. A seed fixes the source's events, counted in steps of the walk
  rather than in wall-clock time:
  - for each synthetic market, a bounded random walk of its books: prices
    stay strictly between 0 and 1 and move in tick-size steps, a market's
    two tokens mirror each other, and every update carries its hash;
  - settlement as the investigation saw it
    ([findings §6](source-behavior.md#6-settlement)): the book empties,
    `market_resolved` names the winner, and the server closes with
    `1000 all subscribed assets resolved` once no unresolved market is
    left on the connection. At some seeded settlements the connection
    drops without a close frame after the book empties and before any
    announcement, as it did once in the findings; the owner decided on
    2026-10-08 to include these unannounced settlements. A later connection
    sends no book for a settled market;
  - interruptions at seeded points: a drop without a close frame, a close
    frame, a withheld `PONG`, and a slow-consumer close;
  - a load setting that raises the message rate until `PONG` queues behind
    market data ([findings §2](source-behavior.md#2-heartbeat)) and a slow
    consumer's queue fills.
- The frames a client receives also follow its own timing: the `PONG`s
  answer its `PING`s, and the time it takes to reconnect decides which
  steps it misses, and so the books it opens with. So the same seed gives
  the same events but not the same frames, and a test checks the events.
- The streamer has its own scripted lookup, which answers for its markets:
  `open`, with the book parameters the hashes use, and then `closed`, with
  the winner, once the streamer has settled the market. Every client run
  against the streamer uses it: in the soak tests, in the examples'
  controlled modes, and in the relay (step 16). So none falls back to the
  default lookup, which calls the live service, and a settlement the
  stream does not announce is confirmed
  ([Settlement](../spec/client.md#settlement)).
- A pace setting: real time, for people watching, or as fast as possible,
  for CI.
- Soak tests: for a fixed set of seeds, each run bounded to a few seconds,
  run the client against the streamer and check rules the contract states
  for every record sequence, each test citing its rule: the
  [record order](../spec/client.md#record-order); a token interrupted
  reaching `ready` again only through a `book` on a later connection
  ([Per-token state machine](../spec/client.md#per-token-state-machine));
  every price and size a `Decimal` ([Values](../spec/client.md#values));
  and no task outliving the `async with` block
  ([Cancellation and shutdown](../spec/client.md#cancellation-and-shutdown)).
  They add to the conformance scenarios and replace none.
- The examples' controlled modes may run against the streamer as well as
  their built-in scenarios.

Out of scope: realistic price models, any change to the client or to
`spec/`, and new conformance scenarios.

### 16. Add the dashboard example

A browser view built on the client. It draws what only this client knows:
where each token's book was uncertain, and where a capture gap leaves the
record incomplete.

- `examples/dashboard/server.py`: an asyncio program that runs the client,
  relays its records to the browser as JSON written with
  `TypeAdapter(...).dump_json` (D8), over a WebSocket or server-sent
  events, and serves the built page. It has three sources: the live
  service, which needs the SDK extra; the streamer (step 15), run in the
  same process, with the streamer's lookup passed to the client; and a
  JSON-lines timeline written by the research example (step 11), replayed.
  The relay's own code starts no threads. The streamer's scripted server
  keeps the thread it runs on
  ([The scripted server](../spec/conformance.md#the-scripted-server)).
  The owner decided on 2026-10-08 to run the streamer in the relay's
  process, rather than as a separate process the relay connects to by
  `url`. Decimals stay strings until the page draws them. An example-only
  dependency is allowed if the pull request says why.
- `examples/dashboard/web/`: TypeScript, built with Vite, with no UI
  framework and one small charting library, such as uPlot or Observable
  Plot; the pull request names its choice and why. For each token, it
  shows the midpoint and spread over time, shaded wherever the token was
  not `ready` and with capture gaps marked; connection and recovery events
  on the same time axis; settlement and the winner; and the backlog.
- Polymarket's terms give no right to redistribute its data. Every
  screenshot or recording committed or published comes from the streamer,
  and nothing in the repository deploys the dashboard where others can see
  live data.
- CI, on one Node version, installs from the lockfile, type-checks, runs
  the page's unit tests of the code that turns records into chart state,
  and builds the page. It also runs the relay against a short seeded
  streamer run and checks that a client of the relay receives every record
  in order.
- `AGENTS.md` gains the dashboard's commands. The README gains a screenshot
  made from the streamer, and the commands.

Out of scope: hosting, accounts, storage, which is the pipeline's, orders,
and any UI framework.

### 17. Offer the scripted server for testing applications

Needs the owner to decide whether the scripted server and the streamer
become a supported way for applications to test their own handling of
interruptions and settlements. The owner deferred this decision on
2026-10-08 until step 16 is merged. Today they are test code in `tests/`,
outside the wheel and the contract. The options:

- a public module in the distribution, such as
  `polymarket_market_data.testing`, which adds it to the
  [public interface](../spec/client.md#public-interface) and makes its API
  a compatibility commitment;
- a separate distribution, released with the client;
- neither: the README explains how to run an application against them from
  a checkout.

The first two need a new version of `spec/client.md` before this step
starts. Whichever is chosen, this step adds an example of an application's
own tests: a consumer checked against a dropped connection, a withheld
`PONG`, and a settlement.
