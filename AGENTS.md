# Working in this repository

This repository holds a read-only Python client for Polymarket's market
WebSocket, the specification it implements, and the source investigation
the specification rests on. The specifications come first; the code
implements them.

## Read these first

| Document | Governs |
| --- | --- |
| [`spec/client.md`](spec/client.md) | What the client does: its interface, records, connection, per-token state machine, recovery contract, settlement, decoding, consumer handoff, decisions, and open questions. |
| [`spec/conformance.md`](spec/conformance.md) | The scripted server's scenarios and the records a correct client delivers. The conformance harness reads the scenarios from this file. |
| [`docs/implementation-plan.md`](docs/implementation-plan.md) | The order of pull requests and what each must deliver. |
| [`docs/source-behavior.md`](docs/source-behavior.md) | The evidence: how the live stream and the SDK behaved, for the markets, versions, and periods recorded. |

The README describes the project for people; it is not a specification.

## Rules

- **Do not change `spec/` in an implementation pull request.** If the code
  disagrees with a scenario, investigate the code first. If you conclude
  that a specification is wrong, ambiguous, or contradicts itself, or
  contradicts `docs/source-behavior.md`, stop and report it, quoting the
  passages, instead of choosing an interpretation or editing an expectation
  to match the code.
- **Do not change the evidence.** `docs/source-behavior.md`, `spikes/`, and
  `spikes/evidence/` record what was observed on October 4, 2026. Code and
  documents must not claim more about the source or the SDK than the
  findings support. If you observe new source behavior, report it; do not
  build it into the client on your own.
- **Follow the plan.** Do one pull request from the implementation plan at a
  time, in order, within its stated scope, as described in
  [Doing the next item](#doing-the-next-item). Note anything you deferred in
  the pull request description.
- **Follow the owner specifications.** D1 to D8 in `spec/client.md` are
  owner specifications, decided by the operator on 2026-10-05. Implement
  them as written, and don't reopen one or depart from it on your own; if
  one seems wrong, stop and report it, as for any specification. Do not
  start a step whose status is `Needs operator decision`.
- **Done means verified.** A pull request is done when formatting, lint,
  type checks, and tests pass, and every scenario in
  `tests/conformance/enabled.txt`, including the ones the step adds, passes
  against the client. Report results as they are; never skip, relax, or
  mark as expected to fail a check or a scenario to make it pass. A
  scenario is turned on only by listing it in `enabled.txt`.
- **No live service in checks.** Tests and CI talk only to the scripted
  server and scripted lookup on localhost. Only plan step 12 contacts the
  live service, for the period the operator agrees.
- **Nothing outside this repository.** Do not open issues, post comments, or
  send anything to other projects, the SDK's included, unless the operator
  asks.

## Implementation guidance

- `asyncio` only, no threads. The socket is the `websockets` asyncio client,
  opened with `ping_interval=None` so the application heartbeat is the only
  liveness check, and `max_size` set from `max_message_bytes`.
- Every task the client starts belongs to a `TaskGroup` or equivalent the
  client owns and awaits on shutdown. No fire-and-forget
  `asyncio.create_task`; no task outlives the `async with` block.
- The task that reads the socket never awaits the consumer's queue. Use
  non-blocking puts and apply the overflow response instead.
- A price, size, tick size, spread, or fee never passes through `float`:
  parse frames with `json.loads(text, parse_float=Decimal,
  parse_constant=Decimal)`, so that no value becomes a `float`, then
  validate each object with its Pydantic model (D8). Never validate the
  frame's text with `model_validate_json`, which keeps only a float's
  precision. Refuse non-finite values.
- Pydantic validates input; dataclasses carry output. See
  [Pydantic and dataclasses](#pydantic-and-dataclasses).
- Deadlines use the event loop's monotonic clock; `at` and `received_at`
  use `datetime.now(UTC)`.
- Records are produced in one place, in the contract's
  [record order](spec/client.md#record-order), so the sequence is
  deterministic.
- Keep `_decode.py`, `_book.py`, `_state.py`, and `_hash.py` free of I/O and
  timers, so their rules can be tested directly.
- Do not use the SDK's streams. The pinned SDK is used only behind
  `MarketLookup`, as D6 specifies.
- Do not use the captures or the excerpts in `spikes/evidence/` as test
  fixtures. They are Polymarket's data, and whether its terms allow
  republishing it has not been checked. Build synthetic frames as
  `spec/conformance.md` does. Nothing in `spikes/` is imported by the package
  or its tests.
- Logs supplement records and never replace them: anything the consumer
  needs to act on is a record.

### Pydantic and dataclasses

D8 settles where each is used. Keep to it:

| What | Built as | Why |
| --- | --- | --- |
| `ClientConfig` and `ReconnectPolicy` | Frozen Pydantic models | They validate the caller's input. A validation error is raised as `ConfigError`, with Pydantic's error as its `__cause__`. |
| Wire models, one per event type | Pydantic models, private to `_decode.py` | They validate the source's input, declaring only the fields the decoder uses and ignoring the rest. A validation error becomes an `UndecodableFrame`, never an exception out of the reader. |
| Records, `Market`, `ClientStats`, and every other public type | Frozen dataclasses with slots | They carry data already validated. The public API stays free of Pydantic's version, and records take positional `match` patterns. |
| Records as JSON, in the example, tests, or a consumer | `TypeAdapter(<type>).dump_json`, `validate_json`, and `json_schema` | Serialization needs no record to be a model. |

- The decoder copies the fields it uses from a wire model into its record,
  and sets the client's own fields, such as `received_at`, `connection`,
  `repeat`, and `held_book_matched`, itself. A wire model never fills a
  record directly, so a field the source adds can never overwrite one the
  client sets.
- No public signature, record field, or exception exposes a wire model or
  a Pydantic type. `ConfigError` keeps Pydantic's error only as its cause.
- Don't make a record a model, add `model_dump`-style methods to it, or
  subclass `BaseModel` outside configuration and `_decode.py`. Changing
  that is a new version of D8, which is the operator's.
- Keep the wire models lenient, unlike the SDK's, whose strict validation
  dropped most `new_market` events
  ([findings §1](docs/source-behavior.md#1-reconnection-and-subscription-restoration-in-the-sdk)).

## Commands

Step 1 sets these up; until then they do not run.

| Task | Command |
| --- | --- |
| Install the development environment | `uv sync` |
| Format check | `uv run ruff format --check .` |
| Lint | `uv run ruff check .` |
| Type check | `uv run mypy` |
| Unit tests and enabled scenarios | `uv run pytest` |
| One scenario | `uv run pytest tests/conformance -k <scenario-name>` |
| Build the wheel | `uv build` |

## Doing the next item

When asked to do the next item:

1. Update `main` (`git checkout main && git pull --ff-only`) and read the
   **Progress** table in
   [`docs/implementation-plan.md`](docs/implementation-plan.md). The next
   item is the first step whose status is not `Done`.
2. Stop and report instead of starting if any of these holds:
   - `gh pr list --state open` shows a pull request for that step; report its
     state, because the operator reviews and merges it;
   - the step's status is `Needs operator decision`; name the decision.
3. Create a branch named `step-<N>-<short-slug>`, such as
   `step-2-decode-frames`, and implement the step within its scope.
4. Run every check the step lists. If a check fails because a specification
   seems wrong or ambiguous, stop and report it as the [Rules](#rules)
   require; do not open a pull request built on a guess.
5. In the same branch, set the step's status to `Done` in the Progress table.
6. Push, and open a pull request as described below. Then add its number to
   the step's row in a follow-up commit on the same branch.
7. Do not merge. Report the pull request's link, the checks you ran and their
   results, and anything deferred.

## Commits and pull requests

Write commit subjects as short imperative sentences in sentence case, such as
"Decode price changes into records", with a body that says what changed and
why. Commit each logical change separately.

The repository allows squash merges only, so the pull request's description
becomes the commit on `main`. Keep it current as commits are added. It names
its step in the implementation plan, lists the scenarios it turns on and the
checks run with their results, and lists anything deferred and any
specification question raised.
