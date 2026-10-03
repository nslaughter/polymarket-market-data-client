# Polymarket market-data client

A reusable client integration demonstration for consuming Polymarket market
data, managing connection recovery, and making uncertain application state
visible to the consumer.

I'm [Nathan Slaughter](https://nathanslaughter.com/). I build integrations and
data systems with attention to how they behave in operation. My work includes
observability integrations, financial systems, and operating data pipelines.
This project will apply that experience to a focused third-party API workflow.

**Status:** Project brief. This repository currently contains this README.
The client, package interface, fixtures, and runnable examples are planned.
This is an independent read-only demonstration project.

## Reopening a connection is only part of restoring usable data

Consider a research application following a selected set of markets. After a
connection drops, it needs to restore its subscriptions and establish which
state it can use. It also needs to know what the interruption leaves unknown.

The first implementation will inspect the current source contract and official
client before choosing a language and interface. Its scope will cover market
selection, event handling, connection lifecycle, recovery, and the handoff into
the consuming application. Order execution is outside this demonstration.

## How the client will recover and report its state

1. Record the desired subscriptions and establish initial application state
   using the source's verified synchronization behavior.
2. Deliver events with the identities and timestamps needed by the consumer.
3. Mark affected state uncertain when an interruption is detected, reconnect
   under a bounded retry policy, and restore subscriptions.
4. Report readiness only after the agreed recovery conditions are satisfied,
   while retaining evidence of any unresolved capture interval.

Source replay, event ordering, and snapshot behavior need investigation before
the recovery algorithm can be specified. A fresh view of the market does not
by itself reconstruct every change that occurred during a disconnect.

## The consumer should be able to exercise the failure behavior

The planned package will include a research example and a deterministic source
fixture with independently prepared expected states. Checks will exercise
connection loss, subscription restoration, unfamiliar events, and a consumer
that stops keeping up. A limited live run will document observations separately
from the controlled fixture results.

The companion
[polymarket-data-pipeline](https://github.com/nslaughter/polymarket-data-pipeline)
will consume a tagged client release and add durable storage. Keeping the
client usable on its own makes the application handoff explicit and gives
other consumers a smaller integration to adopt.

## Work with me on a difficult API integration

I build client packages and services around the workflows a team needs from
an external API. Delivery can include application integration, recovery checks,
documentation, deployment guidance, and ongoing maintenance as the source
changes.

[Discuss an API integration](https://nathanslaughter.com/) with the API,
your runtime, and the integration behavior your application needs.
