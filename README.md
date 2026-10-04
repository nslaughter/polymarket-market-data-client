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
  restored subscriptions, and usable application state are distinct,
  observable states.
- **Recovery with explicit uncertainty.** After an interruption, the client
  reconnects within a bounded retry policy, restores its subscriptions, and
  marks affected state uncertain until the agreed recovery conditions are met.
  Intervals whose completeness is unknown remain visible.
- **A bounded handoff to the consumer.** Buffering toward the application has
  a limit, backlog is reported, and reaching the limit has a defined outcome.
  Events are not dropped silently.
- **Evidence for investigation.** Unfamiliar or undecodable events are kept,
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
sending a `market` frame with the selected token IDs
([Polymarket real-time data](https://docs.polymarket.com/market-data/realtime-data),
checked October 4, 2026). Owning the connection is deliberate: the client must
mark state uncertain the moment a connection drops, and a library that
reconnects silently would hide that event.

Polymarket also publishes an official Python SDK,
[`polymarket-client`](https://pypi.org/project/polymarket-client/), with an
asynchronous client for the same stream
([Python SDK](https://docs.polymarket.com/getting-started/python), checked
October 4, 2026). This project pins a version of it for market lookup and any
REST snapshots the recovery procedure needs. Whether the SDK's own stream
reconnects and restores subscriptions, and whether it reports doing so, is
checked in the source investigation below, so the choice to own the connection
rests on evidence. The scope covers market selection, event handling, the
connection lifecycle, recovery, and the handoff into the consuming application.

The market WebSocket uses an application-level heartbeat: the client sends the
text frame `PING` every 10 seconds, and the server replies with `PONG`
([Polymarket real-time data](https://docs.polymarket.com/market-data/realtime-data),
checked October 4, 2026). The documentation does not say what the server does
when the heartbeat stops, so how long the client waits for `PONG` before
treating the connection as interrupted is a design choice. The source
investigation below informs that timeout, and the client documents it.

## How the client will recover and report its state

1. Resolve the selected markets to the identifiers the source requires, and
   record the desired subscriptions independently of any connection.
2. Connect and send the subscription frame. Establish initial application
   state using the source's verified snapshot and update behavior before
   reporting readiness.
3. When a close frame, a dropped connection, or a missing `PONG` reveals an
   interruption, mark
   the affected state uncertain and record the last confirmed activity and the
   time the interruption was detected.
4. Reconnect with exponential backoff, jitter, and a bounded number of
   attempts, all cancellable. Resend the subscription frames from the desired
   set, and follow the verified synchronization procedure before using updates
   again.
5. Report restored current state separately from the capture interval whose
   completeness remains unknown, including when recovery fails.

Steps 2 to 4 depend on source behavior the documentation does not settle:
snapshots, event ordering, detecting missed events, replay, and the heartbeat.
They are specified after the investigation below. If a consistent handoff
between a snapshot and the stream cannot be established, the example will
narrow its claim and expose the uncertainty. A fresh view of the market
restores current state; it does not reconstruct every change that occurred
during a disconnect.

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

The investigation runs in this repository. Its scripts live in `spikes/` and
are not part of the package or its checks. Findings are recorded in
`docs/source-behavior.md` with the SDK version, the date checked, the markets
observed, and the observation period, and this README cites them where it
relies on source behavior. Questions the investigation cannot settle remain
open there and carry into the live run's unresolved source behavior.

## What the application receives

Records reach the consumer with their source identities, source timestamps
where provided, receipt times, and the connection they arrived on. Raw payloads
are available for investigating decoding failures and unfamiliar events. State
changes such as uncertain, recovering, and ready arrive alongside the data, so
the application can decide what to show or do during an interruption. The
client's responsibility ends at this handoff; durable storage belongs to the
pipeline.

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
buffering, and the application handoff.

Outside this demonstration: order execution and trading, durable storage and
batch delivery (handled by the pipeline), historical reconstruction of missed
events, and continuous operation.

## The demonstration is complete when

- Source-behavior findings, including the questions left open, are recorded
  in `docs/source-behavior.md`, and this README cites them where it relies on
  source behavior.
- The built wheel installs in a clean virtual environment and the documented
  research example runs.
- Initial and recovered states from the scripted WebSocket server match
  independently prepared expectations.
- Checks against that server exercise a dropped connection, a close frame, a
  withheld `PONG`, subscription restoration and changes, unknown and malformed
  frames, and a consumer that stops reading. They assert when the client
  becomes uncertain and when it may report readiness again.
- A limited live run is recorded separately, with its client version,
  configuration, observation period, interruptions, and unresolved source
  behavior.

## What the repository will contain

- This README, explaining the application problem, and an installation
  quickstart.
- A runnable market-data example.
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
