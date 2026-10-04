# Polymarket market-data client

A reusable, read-only Python client demonstration for consuming Polymarket
market data, restoring subscriptions and state after a disconnect, and making
uncertain application state visible to the consumer.

I'm [Nathan Slaughter](https://nathanslaughter.com/). I build integrations and
data systems with attention to how they behave in operation. My work includes
observability integrations, financial systems, and operating data pipelines.
This project will apply that experience to a focused third-party API workflow.

**Status:** Project brief. This repository currently contains this README.
A short investigation of source behavior comes first; the client, package
interface, fixtures, and runnable examples follow it. Nothing described here
has been implemented or tested yet. The client will be written in Python. This
is an independent, read-only demonstration. It is not affiliated with or
endorsed by Polymarket, and it is not client work.

## What this project demonstrates

This is the supporting example for my API integration development work: client
code that completes one application workflow against a third-party API, with
its recovery behavior tested and documented. It is meant to show:

- **WebSocket handling in its own code.** The connection lifecycle, the
  application-level heartbeat, subscription frames, reconnection, close
  handling, and cancellation are implemented and tested here, not delegated to
  a library.
- **Connection states an application can act on.** An open connection,
  restored subscriptions, usable application state, and a settled market are
  distinct, observable states.
- **Recovery with explicit uncertainty.** After an interruption, the client
  reconnects within a bounded retry policy, restores its subscriptions, and
  marks affected state uncertain until the agreed recovery conditions are met.
  Intervals whose completeness is unknown remain visible.
- **A bounded handoff to the consumer.** Buffering toward the application has
  a limit, backlog is reported, and reaching the limit has a defined outcome.
  Events are not dropped silently.
- **Evidence for diagnosis.** Unfamiliar or undecodable events are kept,
  and records carry the identities and timestamps the consumer needs.
- **Failure behavior anyone can exercise.** A scripted local WebSocket server
  reproduces disconnects, a missing heartbeat reply, and other failures without
  access to the live service.

The client is the first of two stages. The companion
[polymarket-data-pipeline](https://github.com/nslaughter/polymarket-data-pipeline)
will consume a tagged client release and add durable storage.

## Reopening a connection is only part of restoring usable data

Consider a research application following a selected set of markets. After a
connection drops, it needs to restore its subscriptions and establish which
state it can use. It also needs to know what the interruption leaves unknown.

The client manages the market WebSocket connection itself. It connects to
`wss://ws-subscriptions-clob.polymarket.com/ws/market` and subscribes by
sending a `market` frame with the selected token IDs and
`custom_feature_enabled` set, which adds market lifecycle events such as
`market_resolved`
([Polymarket real-time data](https://docs.polymarket.com/market-data/realtime-data),
checked October 4, 2026). Owning the connection is deliberate: the client must
mark state uncertain the moment a connection drops, and a library that
reconnects silently would hide that event.

Polymarket also publishes an official Python SDK,
[`polymarket-client`](https://pypi.org/project/polymarket-client/), with an
asynchronous client for the same stream
([Python SDK](https://docs.polymarket.com/getting-started/python), checked
October 4, 2026). This project pins a version of it for market lookup and any
REST snapshots the recovery procedure needs. The investigation below checked
version 0.12.0. Its stream does reconnect and resend its subscriptions after a
dropped, stalled, or closed connection, but it reports none of this to the
consumer. Its subscription handle yields only market events, with nothing to
mark a disconnect or the events lost during one. Only failed reconnect
attempts and its own heartbeat timeout reach its logger. It also drops events
it cannot parse, logging them only at debug level
([findings, question 1](docs/source-behavior.md#1-reconnection-and-subscription-restoration-in-the-sdk)).
So the client owns the connection, as the paragraph above requires. The scope
covers market selection, event handling, the connection lifecycle, recovery,
and the handoff into the consuming application.

The market WebSocket uses an application-level heartbeat: the client sends the
text frame `PING` every 10 seconds, and the server replies with `PONG`
([Polymarket real-time data](https://docs.polymarket.com/market-data/realtime-data),
checked October 4, 2026). The documentation does not say what the server does
when the heartbeat stops. In the investigation, the server kept connections
open without `PING` while market data flowed. It closed one with no traffic in
either direction after about 125 seconds, without a close frame, and a `PING`
every 10 seconds prevented that. `PONG` came back in about 0.14 seconds, but
it is queued behind market data and took 9.5 seconds under heavy load
([findings, question 2](docs/source-behavior.md#2-heartbeat)). So the client
sends `PING` even on quiet subscriptions. How long it waits for `PONG` before
treating the connection as interrupted remains a design choice. That timeout
must allow for queued data, and the client documents it.

## How the client will recover and report its state

1. Resolve the selected markets to the identifiers the source requires, and
   record the desired subscriptions independently of any connection.
2. Connect and send the subscription frame. Take each token's initial book
   from the `book` event the stream sends on subscribing, then apply the
   token's later `price_change` entries in arrival order. Report the token
   ready once its book is in place. In the investigation, that `book` matched
   the source's current book every time it was checked. Each token's book
   changes arrived in timestamp order, apart from two that trailed an opening
   `book` by 1 ms. Some messages arrived twice, and other event types
   sometimes arrived out of timestamp order
   ([findings, questions 3 and 5](docs/source-behavior.md#3-event-order-and-replay)).
3. When a close frame, a dropped connection, or a missing `PONG` reveals an
   interruption, mark the affected state uncertain and record the last
   confirmed activity and the time the interruption was detected. The server
   also ends connections it considers slow consumers, sometimes without a
   close frame. Within a connection, an order-book hash that keeps disagreeing
   with the client's own book marks that token's book uncertain as well
   ([findings, questions 2 and 4](docs/source-behavior.md#4-revealing-a-missed-event)).
4. Reconnect with exponential backoff, jitter, and a bounded number of
   attempts, all cancellable. Resend the subscription frames from the desired
   set. The source does not replay missed events. Instead the stream sends a
   fresh `book` for each token, which replaces the client's book before
   updates are used again
   ([findings, question 3](docs/source-behavior.md#3-event-order-and-replay)).
5. Report restored current state separately from the capture interval whose
   completeness remains unknown, including when recovery fails.

Steps 2 to 4 depend on source behavior the documentation does not answer:
snapshots, event ordering, detecting missed events, replay, and the heartbeat.
They follow the investigation's findings in
[`docs/source-behavior.md`](docs/source-behavior.md), which hold for the
markets and periods recorded there. The handoff between a snapshot and the
stream was consistent every time it was checked. That held both for the
stream's own `book` and for a REST snapshot joined to the stream by its
order-book hash
([findings, question 5](docs/source-behavior.md#5-joining-a-snapshot-to-the-stream)).
The hash is undocumented, so the client treats a mismatch as evidence that a
book has diverged, not as a condition for readiness. A fresh view of the
market restores current state; it does not reconstruct every change that
occurred during a disconnect.

Every market eventually settles; Polymarket calls this resolution. When a
subscribed market settles, the client reports it as settled, not ready, and
removes it from the desired subscriptions so a reconnect does not resubscribe
it. How reliably the stream announces a settlement is one of the
investigation's questions below.

## Source behavior is checked before recovery is specified

Before the client is built, a short investigation against the live service
answers the questions the recovery design depends on:

- Whether the official SDK's stream reconnects and restores subscriptions on
  its own, and whether it reports doing so.
- What the server does when `PING` frames stop, and how promptly `PONG`
  arrives, which together inform the client's `PONG` timeout.
- Whether events arrive in a consistent order, and whether the source can
  replay events missed during a disconnect.
- Whether the stream carries anything, such as sequence numbers or order-book
  hashes, that reveals a missed event.
- Whether a snapshot can be joined to the stream's updates without losing or
  repeating any.
- What the stream does when a subscribed market settles, and what a
  subscription to an already-settled market returns.

The investigation runs in this repository. Its scripts live in `spikes/` and
are not part of the package or its checks. Findings are recorded in
`docs/source-behavior.md` with the SDK version, the date checked, the markets
observed, and the observation period, and this README cites them where it
relies on source behavior. Questions the investigation cannot answer remain
open there and carry into the live run's unresolved source behavior.

## What the application receives

Records reach the consumer with their source identities, source timestamps
where provided, receipt times, and the connection they arrived on. Raw payloads
are available for diagnosing decoding failures and unfamiliar events. State
changes such as uncertain, recovering, ready, and settled arrive alongside the
data, so the application can decide what to show or do during an interruption
or after a market settles. The client's responsibility ends at this handoff;
durable storage belongs to the pipeline.

## A slow consumer needs an explicit outcome

If the application stops keeping up, the client bounds its memory, reports the
backlog, and applies a configured response at the limit. Backpressure cannot
make the upstream service retain events. If the source cannot replay,
disconnecting to protect memory creates a capture gap, and the client records
it as one.

## Design choices for Python

- **Asynchronous, on `asyncio` and the `websockets` library.** One task reads
  frames, one sends the heartbeat, and events reach the application through an
  async iterator backed by a bounded queue, with a configured response when
  the queue fills.
- **Frames decoded defensively.** Each frame is parsed into a typed event.
  A frame the client cannot decode, or an event type it doesn't recognize, is
  kept with its raw payload and reported, not allowed to stop the connection.
- **Cancellation from the standard library.** Cancelling the consuming task,
  or wrapping it in `asyncio.timeout()`, stops reconnection attempts and closes
  the connection. No background task outlives the client's `async with` block.
- **Exact values.** Prices and sizes are decoded straight to `Decimal` and
  never pass through `float`.
- **Typed records and states.** Market events, connection-state changes, and
  capture gaps are distinct types, so the application can tell data from
  status with an ordinary `match` statement.
- **A package the pipeline can pin.** Releases are tagged wheels. CI installs
  the built wheel in a clean virtual environment and runs the fixture checks
  on each supported Python version.

## Scope

In scope: a limited, explicit set of markets and event types; Python;
read-only market data; the market WebSocket connection; and recovery, consumer
buffering, and the application handoff. Recovery includes replaying missed
events from the source, if the investigation finds that the source can.

Outside this demonstration: order execution and trading, durable storage and
batch delivery (handled by the pipeline), reconstructing missed events by any
other means, and continuous operation.

## The demonstration is complete when

- Source-behavior findings, including the questions left open, are recorded
  in `docs/source-behavior.md`, and this README cites them where it relies on
  source behavior.
- The built wheel installs in a clean virtual environment and the documented
  research example runs.
- Initial and recovered states from the scripted WebSocket server match
  independently prepared expectations.
- Checks against that server exercise a dropped connection, a close frame, a
  withheld `PONG`, subscription restoration and changes, a market that
  settles, unknown and malformed frames, and a consumer that stops reading.
  They assert when the client becomes uncertain, when it may report readiness
  again, and when it reports a market settled.
- A limited live run is recorded separately, with its client version,
  configuration, observation period, interruptions, and unresolved source
  behavior.

## What the repository will contain

- This README, explaining the application problem, and an installation
  quickstart.
- A runnable market-data example that chooses active markets when it runs,
  so it keeps working as markets settle.
- The client interface and its documented recovery contract.
- A scripted local WebSocket server and deterministic fixtures with
  independently prepared expected states.
- Automated checks in CI on each supported Python version, and a tagged
  release with a built wheel.
- An inspectable recovery timeline and documented source coverage limits.
- Source-behavior findings in `docs/source-behavior.md`, with the SDK version,
  dates checked, markets observed, and observation periods. The investigation
  scripts in `spikes/` are kept out of the package and its checks.

A reader should be able to run the controlled example without access to the
live service.

## What the results will and will not establish

The fixture can establish missing-message counts because its emitted sequence
is known. A live run needs source evidence to support the same claim. Local
receipt counters describe the client's own order; they cannot prove that the
source delivered every event. Live findings, whether from the live run or the
source investigation, apply only to the markets, versions, and observation
periods recorded.

## Related projects and writing

- [polymarket-data-pipeline](https://github.com/nslaughter/polymarket-data-pipeline):
  durable storage built on a tagged release of this client.
- *How to keep a Polymarket market-data integration running through
  disconnects*: an article on this client's recovery design, in preparation.
  I'll link it here when it is published.

## Work with me on a difficult API integration

I build client packages and services around the workflows a team needs from
an external API. Delivery can include application integration, recovery checks,
documentation, deployment guidance, and ongoing maintenance as the source
changes.

[Discuss an API integration](https://www.linkedin.com/in/nathan-slaughter) with
the API, your runtime, and the integration behavior your application needs.
