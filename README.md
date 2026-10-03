# Polymarket market-data client

A reusable, read-only client demonstration for consuming Polymarket market
data, restoring subscriptions and state after a disconnect, and making
uncertain application state visible to the consumer.

I'm [Nathan Slaughter](https://nathanslaughter.com/). I build integrations and
data systems with attention to how they behave in operation. My work includes
observability integrations, financial systems, and operating data pipelines.
This project will apply that experience to a focused third-party API workflow.

**Status:** Project brief. This repository currently contains this README.
The client, package interface, fixtures, and runnable examples are planned;
nothing described here has been implemented or tested yet. This is an
independent, read-only demonstration. It is not affiliated with or endorsed by
Polymarket, and it is not client work.

## What this project demonstrates

This is the supporting example for my API integration development work: client
code that completes one application workflow against a third-party API, with
its recovery behavior tested and documented. It is meant to show:

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
- **Failure behavior anyone can exercise.** A deterministic source fixture
  reproduces disconnects and other failures without access to the live service.

The client is the first of two stages. The companion
[polymarket-data-pipeline](https://github.com/nslaughter/polymarket-data-pipeline)
will consume a tagged client release and add durable storage.

## Reopening a connection is only part of restoring usable data

Consider a research application following a selected set of markets. After a
connection drops, it needs to restore its subscriptions and establish which
state it can use. It also needs to know what the interruption leaves unknown.

The first implementation will inspect the current source contract and
Polymarket's official client before choosing one language and an interface.
It will pin a client version, build on that client where it fits, and add the
behavior it lacks. The scope covers market selection, event handling,
connection lifecycle, recovery, and the handoff into the consuming
application.

The market WebSocket uses an application-level heartbeat: the client sends the
text frame `PING` every 10 seconds, and the server replies with `PONG`
([Polymarket real-time data](https://docs.polymarket.com/market-data/realtime-data),
checked October 3, 2026). The documentation does not say what the server does
when the heartbeat stops, so how long the client waits for `PONG` before
treating the connection as interrupted is a design choice. That timeout will
be documented and checked against the live service.

## How the client will recover and report its state

1. Resolve the selected markets to the identifiers the source requires, and
   record the desired subscriptions independently of any connection.
2. Connect and subscribe. Establish initial application state using the
   source's verified snapshot and update behavior before reporting readiness.
3. When a closed connection or a missing `PONG` reveals an interruption, mark
   the affected state uncertain and record the last confirmed activity and the
   time the interruption was detected.
4. Reconnect under a bounded retry policy that supports cancellation, restore
   the subscriptions, and follow the verified synchronization procedure before
   using updates again.
5. Report restored current state separately from the capture interval whose
   completeness remains unknown, including when recovery fails.

Source replay, event ordering, and snapshot behavior need investigation before
step 4 can be specified. If a consistent handoff between a snapshot and the
stream cannot be established, the example will narrow its claim and expose the
uncertainty. A fresh view of the market restores current state; it does not
reconstruct every change that occurred during a disconnect.

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

## Scope

In scope: a limited, explicit set of markets and event types; one language;
read-only market data; and the connection lifecycle, recovery, consumer
buffering, and application handoff.

Outside this demonstration: order execution and trading, durable storage and
batch delivery (handled by the pipeline), historical reconstruction of missed
events, and continuous operation.

## The demonstration is complete when

- The packaged client installs in a clean environment and the documented
  research example runs.
- Initial and recovered states from a deterministic source fixture match
  independently prepared expectations.
- Checks exercise disconnection, a missing `PONG`, subscription
  restoration and changes, unfamiliar events, and a slow consumer. They assert
  when the client becomes uncertain and when it may report readiness again.
- A limited live run is recorded separately, with its client version,
  configuration, observation period, interruptions, and unresolved source
  behavior.

## What the repository will contain

- This README, explaining the application problem, and an installation
  quickstart.
- A runnable market-data example.
- The client interface and its documented recovery contract.
- Deterministic fixtures with independently prepared expected states.
- Automated checks in CI and a tagged package release.
- An inspectable recovery timeline and documented source coverage limits.

A reader should be able to run the controlled example without access to the
live service.

## What the results will and will not establish

The fixture can establish missing-message counts because its emitted sequence
is known. A live run needs source evidence to support the same claim. Local
receipt counters describe the client's own order; they cannot prove that the
source delivered every event. Live findings apply only to the markets, client
version, and observation period recorded.

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
