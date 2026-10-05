# Client contract: Polymarket market-data client

**Status:** Draft 0.1.0, for the operator's review; not tagged. Seven
decisions, D1 to D7, are the operator's to make. Each is set out under
[Decisions awaiting the operator](#decisions-awaiting-the-operator) with its
options, its evidence, and a recommended default. Where this document needs
a value for one of them, it gives the recommended default and names the
decision; none of those values is settled. None of the client exists yet.

This document governs the client's code: its public interface, the records
it delivers, the per-token state machine, the recovery contract, and the
handoff to the consuming application. The [README](../README.md) describes
the project for people; it is not a specification. The
[conformance scenarios](conformance.md) give the scripted server's frames
and the records a correct client delivers in response, and the
[implementation plan](../docs/implementation-plan.md) orders the work. If
this document and a conformance scenario disagree, report the disagreement
instead of choosing one.

## Evidence

The design rests on the source investigation in
[`docs/source-behavior.md`](../docs/source-behavior.md). It checked
`polymarket-client` 0.12.0 and the market WebSocket on October 4, 2026, and
each finding holds only for the markets, versions, and periods it records
([Versions]). Every statement here about the source or the SDK cites the
section it rests on and says what kind of evidence it is:

| Label | Meaning |
| --- | --- |
| Observed | Seen in the investigation's runs, as the cited section reports. |
| SDK source | Read in the 0.12.0 wheel's source, as the cited section reports. |
| Documented | Stated by Polymarket's documentation as the README cites it, and used in every run that did not say otherwise ([How it ran]). |
| Inferred | Concluded from what was observed, but not seen directly. |
| Read for this document | Not in the findings: read or measured while writing this document, as listed below. |

These facts were read or measured for this document and are not in the
findings. Each says so where it appears:

- The event field names, the order of members in captured frames, and the
  newline after an empty opening `[]`, read from the excerpts in
  [`spikes/evidence/`](../spikes/evidence). The findings quote whole frames
  only for `market_resolved` and the hash input ([§4], [§6]).
- How long opening frames took (0.13 to 0.52 s on 24 connections), the
  largest frame (68,573 bytes), and that no `price_change` entry arrived
  before its token's opening `book` (none of 5,434 entries on the 10
  connections whose excerpts keep the opening frame), measured from the
  same excerpts. The excerpts keep only parts of the captures, so these are
  samples, not totals.
- That a burst's entries, which share a hash, came in separate frames: of
  20,038 `price_change` entries in those excerpts, repeats aside and with
  excerpts cut from the same capture merged, 1,793 from 7 captures carried
  the hash of their token's previous entry, which had come in an earlier
  frame, at most 0.27 s before (D4).
- That no frame other than JSON and `PONG` appears in those excerpts.
- From the 0.12.0 wheel ([Versions] gives its hash): its metadata requires
  Python 3.11 or later and `websockets` from 13 to below 16, and lists
  `eth-abi`, `eth-account`, `httpx`, `pydantic`, and more; and its
  `market_protocol.py` sends `subscribe` and `unsubscribe` operation frames
  on an open connection.
- The names of the SDK methods the investigation's scripts called:
  `AsyncPublicClient`, `list_markets`, `get_market`, and `get_order_book`.

The findings' open questions stay open. The client is designed to behave
sensibly whichever way they resolve, and no conformance scenario assumes an
answer to one.

## Scope

In scope:

- the read-only connection to the market WebSocket, owned by the client:
  connecting, subscribing, the application heartbeat, detecting
  interruptions, reconnecting within bounds, and closing;
- a desired set of markets held apart from any connection;
- decoding market events into typed records with exact values;
- a per-token order book and state, and the recovery contract that says
  when a token's state can be used;
- settlement, from the stream and through market lookup;
- the bounded handoff of records to the consuming application;
- cancellation and errors.

Out of scope: trading and any authenticated channel; durable storage, which
belongs to the
[pipeline](https://github.com/nslaughter/polymarket-data-pipeline);
reconstructing missed events by any means; the user channel and the other
real-time feeds; continuous operation.

## Concepts

- A **market** is a Polymarket market, identified by its condition ID (the
  `market` field of its events). It has one token per outcome.
- A **token** is one outcome's asset ID (`asset_id`). It is the unit of
  subscription and of the order book. Token IDs are decimal strings of up to
  78 digits in the findings ([Markets]); the client treats them as opaque
  strings.
- The **desired set** is the markets the application wants, in the order it
  added them. The client builds every subscription frame from it, and it
  survives interruptions. A market leaves it when the application removes
  it or when it settles, and the application can add it again.
- A **connection** is one WebSocket connection to the market endpoint. Each
  connection the client opens gets the next **generation**, starting at 1.
- A token's **book** is the client's copy of its order book: a size for each
  price on each side.
- A **capture gap** is an interval in which the client may have missed a
  token's events.
- A market is **settled** once it has resolved. In every settlement
  observed, the stream sent nothing more for it ([§6]); whether
  `market_resolved` is ever repeated or late is open.

## Public interface

The names in this section use the recommended package and import names,
which are D5's to settle. All public names are importable from the package's
top level.

```python
import asyncio

from polymarket_market_data import (
    BookEvent, CaptureGap, MarketDataClient, PriceChangeEvent, TokenState,
    TokenStateChange,
)


async def main() -> None:
    async with MarketDataClient() as client:
        client.subscribe(await client.resolve("some-market-slug"))
        async for record in client.records():
            match record:
                case TokenStateChange(state=TokenState.READY):
                    ...  # the token's book can be used
                case TokenStateChange(state=TokenState.SETTLED):
                    ...  # nothing more will arrive for it
                case TokenStateChange():
                    ...  # not usable for now; record.reason says why
                case BookEvent() | PriceChangeEvent():
                    ...
                case CaptureGap():
                    ...  # events in this interval may be missing


asyncio.run(main())
```

| Member | Behavior |
| --- | --- |
| `MarketDataClient(config=ClientConfig(), *, markets=(), lookup=None)` | Validates the configuration and records the initial desired set. Does no I/O. Raises `ConfigError` for an invalid value. `lookup` replaces the default [market lookup](#market-lookup). |
| `async with client` | Starts the client. It connects once the desired set is non-empty. Leaving the block, by any path, shuts it down ([Cancellation and shutdown](#cancellation-and-shutdown)). A client can be entered once. |
| `client.subscribe(*markets: Market)` | Adds markets to the end of the desired set. A market already in it is left where it is. A market that was removed or settled can be added again; its tokens [start over](#adding-a-market-again). Allowed before and inside the block. |
| `client.unsubscribe(*condition_ids: str)` | Removes markets from the desired set. An ID not in it is ignored. |
| `client.desired` | The desired set: a tuple of `Market`, in order. |
| `client.records()` | The async iterator of [records](#records). It can be called once. |
| `client.backlog` | The number of market-event records waiting for the consumer. |
| `client.stats()` | A snapshot of the client's [counters](#statistics). |
| `await client.resolve(slug)` | Looks a market up by slug and returns its `Market`. Raises `MarketNotFound` if the lookup finds none, and `LookupFailed` if the lookup raises or times out. |

`Market` is a frozen dataclass: `condition_id: str`, `token_ids:
tuple[str, ...]` in outcome order, and `slug: str | None`. A token may
belong to only one market in the desired set; `subscribe` raises
`ValueError` otherwise.

`subscribe` and `unsubscribe` change only the desired set and return at
once. The connection task applies the change, and the records show its
effect. Calling either after the client has shut down raises
`ClientStateError`.

### Errors

| Exception | Raised by | When |
| --- | --- | --- |
| `ClientError` | | Base class of the exceptions below. |
| `ConfigError(ClientError, ValueError)` | the constructor | A configuration value is invalid. |
| `ClientStateError(ClientError, RuntimeError)` | any member | The client is used in a way its lifecycle does not allow: `records()` called twice, the block entered twice, or a change after shutdown. |
| `MarketNotFound(ClientError, LookupError)` | `resolve` | The lookup found no market for the slug. |
| `LookupFailed(ClientError)` | `resolve` | The lookup raised or exceeded `lookup_timeout`. Its `__cause__` is the lookup's exception, or the `TimeoutError`. |
| `RecoveryFailed(ClientError)` | the iterator | Reconnection exhausted its bounds (D2). It is raised after the records that report the failure ([Reconnecting](#reconnecting)). |
| `ConsumerTooSlow(ClientError)` | the iterator | Only if D3 settles on failing at the limit. |

A lookup that fails while the client confirms a settlement or fetches hash
inputs, an undecodable frame, and a lost connection are not exceptions. The
lookup failure is counted (`lookup_failures`), and if lookup never confirms
the settlement the tokens become `settlement_unconfirmed` (T13); the others
are reported in records. Only `resolve`, which the application awaits
itself, raises a lookup failure. A defect in the client's own code
ends the client: the exception is raised once, chained as the `__cause__`
of a `ClientError`, by the iterator if a consumer is reading and otherwise
when the block is left. The client never stops silently. The SDK's stream
did: its source shows reconnection stopping after any error other than its
own `TransportError`, with the handle left open and silent (SDK source,
[§1]).

### Cancellation and shutdown

- Leaving the `async with` block shuts the client down: normally, by an
  exception, or by cancellation, including cancellation of the task that
  holds the block and an expired `asyncio.timeout()` around it. Shutdown
  cancels every task the client started, closes an open connection (code
  1000, reason `client exit`, waiting at most `close_timeout` for the
  closing handshake), and awaits those tasks. No task the client started
  outlives the block.
- Cancelling a task while it awaits the next record, or an expired
  `asyncio.timeout()` around that await, raises in that task only. The
  client keeps running, and the record it was waiting for is not lost: the
  next read returns it.
- In the usual pattern, the task that reads the records also holds the
  block. Cancelling it, or an expired `asyncio.timeout()` around it, then
  does both: the read raises, the exception leaves the block, and the
  client shuts down.
- Once the client has shut down, the iterator ends (`StopAsyncIteration`)
  after any exception it owes the consumer. Records still queued are
  discarded.
- Cancellation stops a pending connection attempt or backoff at once.

## Records

Every record is a frozen dataclass with slots. Event records carry what the
source sent; status records carry what the client concluded. A consumer
tells them apart with an ordinary `match` statement.

### Fields of every event record

These fields are on every record in the next table.

| Field | Type | Meaning |
| --- | --- | --- |
| `event_type` | `str` | The source's `event_type`. |
| `market` | `str` | The condition ID, from the event's `market`. |
| `source_timestamp_ms` | `int` | The event's `timestamp`: milliseconds since the Unix epoch, exactly as sent. |
| `received_at` | `datetime` | When the frame arrived: UTC wall-clock time, timezone-aware. |
| `connection` | `int` | The generation of the connection it arrived on. |
| `frame` | `int` | The frame's number on that connection: 1 for the first frame after the subscription frame, counting every text and binary frame except `PONG`. |
| `index` | `int` | Its position in the frame: the array index, or 0 for a frame holding one object. |
| `repeat` | `bool` | The client judged it a [repeat](#repeated-messages) of an earlier message. |
| `raw` | `str \| None` | The frame's text, if `keep_raw` is set. |

A `book` event's timestamp is the time of the book's last change, not of the
snapshot: an opening book's can be well before the subscription (Observed,
[§3]). The client does not judge freshness by it. Source timestamps and
arrival order disagree for events of different types (Observed, [§3]), so
the client keeps both and reorders nothing.

### Event records

| Record | Fields beyond the common ones |
| --- | --- |
| `BookEvent` | `asset_id: str`; `hash: str`; `bids`, `asks: tuple[Level, ...]`, in the order sent; `tick_size`, `last_trade_price: Decimal \| None`, present only on a subscription's opening books (Observed, [§4]); `opening: bool`, true when the book arrived in the connection's first frame; `held_book_matched: bool \| None` (below). |
| `PriceChangeEvent` | `changes: tuple[PriceChange, ...]`, in the order sent. |
| `BestBidAskEvent` | `asset_id`; `best_bid`, `best_ask`, `spread: Decimal \| None`. |
| `LastTradePriceEvent` | `asset_id`; `price`, `size: Decimal`; `side: Side`; `fee_rate_bps: Decimal \| None`; `transaction_hash: str \| None`. |
| `TickSizeChangeEvent` | `asset_id`; `old_tick_size`, `new_tick_size: Decimal`. |
| `MarketResolvedEvent` | `id: str \| None`; `assets_ids: tuple[str, ...]`; `winning_asset_id: str`; `winning_outcome: str \| None`; `tags: tuple[str, ...]`. |
| `NewMarketEvent` | `id: str`; `condition_id: str \| None`, from `condition_id` or else `market`; `slug: str \| None`; `question: str \| None`; `assets_ids: tuple[str, ...]`, empty if absent; `payload: Mapping[str, Any]`, the whole event, read-only. Delivered only if `new_market` is `deliver`. The common field `market` is `""` when the event has none. |

`Level` holds `price` and `size`, both `Decimal`. `PriceChange` holds
`asset_id`, `side: Side` (`BUY` or `SELL`), `price`, `size`, `hash`,
`best_bid` and `best_ask` (`Decimal | None`), and two flags the client sets:

- `applied: bool`: the client applied the entry to its book. It is false
  for an entry in a repeat, for a token outside the desired set, and for a
  token with no book on this connection.
- `before_book: bool`: the entry is stamped earlier than the token's
  opening book on this connection but arrived after it
  ([below](#changes-stamped-before-the-opening-book)).

`held_book_matched` compares a new `book` with the book the client held for
that token just before it, level for level: `True` if every price and size
on both sides is equal, `False` if not, `None` if the client held no book.
After a reconnect, `False` shows that the book changed during the gap. `True`
does not show that nothing was missed: a change that a later change
overwrote leaves no trace (Observed, [§4]).

### Records the client cannot decode

| Record | Fields |
| --- | --- |
| `UnknownEvent` | `event_type: str \| None`; `payload: Mapping[str, Any]`; `raw: str`; `received_at`, `connection`, `frame`, `index`. A JSON object whose `event_type` the client does not know, or that has none. |
| `UndecodableFrame` | `reason`: `invalid_json`, `not_object`, `binary`, or `invalid_event`; `event_type: str \| None`; `error: str`; `raw: str \| bytes`; `received_at`, `connection`, `frame`; `index: int \| None`, the item's position if the frame was a JSON array, otherwise `None`; `affected: tuple[str, ...]`, the tokens it made uncertain. |

[Decoding](#decoding) says which applies and what each does to token states.

### Status records

| Record | Fields |
| --- | --- |
| `TokenStateChange` | `token_id`; `market`; `state: TokenState`; `previous: TokenState \| None`; `reason: str`; `at: datetime`; `connection: int \| None`; `last_confirmed_at: datetime \| None`, set when the reason is `interrupted`; `winning_asset_id: str \| None`, set when settled and known. |
| `ConnectionStateChange` | `state: ConnectionState`; `at`; `connection: int \| None`, the generation for `open`, `subscribed`, `interrupted`, `ended`, and `idle` after a connection, otherwise `None`; `attempt: int \| None`, set for `connecting` and `recovering`; `reason: str \| None`; `detail: str \| None`; `close_code: int \| None`; `close_reason: str \| None`; `retry_in: float \| None`; `last_confirmed_at: datetime \| None`. |
| `CaptureGap` | `token_id`; `market`; `cause: str`; `close_code: int \| None`; `close_reason: str \| None`; `last_confirmed_at: datetime`; `detected_at: datetime`; `resumed_at: datetime \| None`; `end: str`; `connection_before: int`; `connection_after: int \| None`; `held_book_matched: bool \| None`; `discarded: int \| None`; `at`. |
| `Backlog` | `queued: int`; `limit: int`; `rising: bool`; `at`. |

A `TokenStateChange` is emitted whenever a token's state or its reason
changes. `at` on a status record is when the client reached the conclusion.
The vocabularies for `reason`, `cause`, and `end` are fixed by the tables in
this document; a client must not add values without a new version of it.

### Values

Prices, sizes, tick sizes, spreads, and fee rates are `Decimal`, built from
the source's strings. Frames are parsed with `json.loads(text,
parse_float=Decimal)`, so a number sent as a JSON number never passes
through `float` either. A value that is not a finite decimal makes its event
undecodable. `Decimal` keeps the source's digits, so `str(level.price)`
returns the text that was sent. Timestamps are integers of milliseconds,
also exactly as sent.

### Record order

Records reach the consumer in the order the client produces them, by these
rules, so that every conformance scenario has one correct sequence:

1. A frame's events are handled in their order in the frame. Each event's
   record comes first, then the status records it causes.
2. When one cause affects several tokens, their records follow the desired
   set's order: markets in the order added, and each market's tokens in
   `token_ids` order.
3. When a token becomes ready on a book that ends a capture gap, the order
   is `BookEvent`, `CaptureGap`, `TokenStateChange`. When a gap ends with
   settlement or removal, the `CaptureGap` comes just before the token's
   `TokenStateChange`.
4. A connection-level cause emits its `ConnectionStateChange` before the
   token records it causes, and any further connection record after them:
   for example `interrupted`, then each token's `uncertain`, then
   `recovering`. The one exception is `failed`, which comes after the
   `CaptureGap`s that recovery's failure ends.
5. A `Backlog` record with `rising=True` comes after every record caused by
   the event whose record brought the count to the warning level.

The [state machine](#per-token-state-machine) and
[connection](#the-connection) sections give each sequence in full.

## The connection

### Connecting and subscribing

The client connects to `url` with the `websockets` library, its own
protocol-level keepalive turned off (`ping_interval=None`), as the
investigation's own sockets did ([How it ran]). The application heartbeat
below is the client's only liveness check. No run recorded a protocol ping
from the server (Observed, [§2]); if one arrives, the library answers it.

Once the handshake completes, the client sends one subscription frame built
from the desired set at that moment:

```json
{
  "type": "market",
  "assets_ids": ["<token>", "..."],
  "custom_feature_enabled": true
}
```

`assets_ids` lists every token of every desired market, in desired-set
order. `custom_feature_enabled` is always true, because it adds the
lifecycle events, `market_resolved` among them (Documented, as the README
cites; set in every settlement run, [§6]). The frame shape is the one the
runs used ([How it ran]).

The source does not acknowledge a subscription. Its first frame is an array
of `book` events, one for each subscribed token that is still trading, and
`[]` when there is none (Observed, [§5], [§6]). A settled or otherwise
inactive token is left out without an error (Observed, [§6]).

A connection that ends before its subscription frame is sent counts as a
failed attempt, not an interruption. One that ends after it but before
delivering a frame is both: an interruption, since its tokens were
`synchronizing` on it, and a failed attempt
([Reconnecting](#reconnecting)).

### Heartbeat

Once the subscription frame is sent, the client sends the text frame `PING`
every `ping_interval` seconds (10 by default), the first one
`ping_interval` after the subscription frame, whatever other traffic there
is. The server answers each with `PONG` (Documented, as the
README cites; [How it ran]). A quiet connection needs this: one with no
traffic in either direction was closed after about 125 s without a close
frame, four times in four, and a `PING` every 10 s prevented it (Observed,
[§2]). Connections that stopped sending `PING` stayed open for 10 minutes
while data flowed (Observed, [§2]); how long beyond that is open. The client
sends it regardless, as its liveness check.

Each `PONG` answers the oldest unanswered `PING`; `PONG` carries no
identifier. A `PONG` with no `PING` outstanding is counted and ignored.

`PONG` is sent in order with market data, so it waits behind any backlog:
its median was 0.14 s, but it took 9.5 s and 11.4 s on busy markets before
the server ended those connections (Observed, [§2]). The client reads frames
continuously and never waits for the consumer, so its own reading never
delays a `PONG`.

### Detecting an interruption

An interruption is an end of a subscribed connection that may lose events
for a token still in the desired set. The client detects one when:

| Cause (`reason`) | How it shows | Evidence |
| --- | --- | --- |
| `close_frame` | The server sends a close frame with any code and reason except `1000` with reason `all subscribed assets resolved`. This includes `1013 slow consumer: send buffer full`, `1001`, `1011`, and `1000` with any other reason. | 1013 Observed on busy markets, though not in a 10-minute review run ([§2], [Review runs]) |
| `dropped` | The connection ends without a close frame: a reset, an end of file, or a protocol error. | Observed for idle connections and slow consumers ([§2]) and at a settlement ([§6]) |
| `pong_timeout` | A `PING` has gone unanswered for `pong_timeout` seconds (D1). The client then closes the connection (code 1000, reason `pong timeout`). | Choice; [§2] bounds it |
| `consumer_overflow` | The consumer fell behind and D3's response is to disconnect. The client closes (1000, `client backlog`). | Choice |
| `subscription_change` | Applying a change to the desired set needs a new connection, as D7's recommended default does. The client closes (1000, `subscription change`). | Choice |

The close frame `1000 all subscribed assets resolved` is not an
interruption; it settles every token on the connection
([Settlement](#settlement)). Nor is the end of a connection with no desired
token left on it: the client emits one `idle` record for it, however it
ended.

On an interruption the client records:

1. `ConnectionStateChange(interrupted)` with the cause as `reason`, the
   close code and reason if a close frame came, and `last_confirmed_at`,
   the receipt time of the last frame of any kind on the connection. A
   `PONG` confirms the data before it, since it is queued behind that data.
2. A `TokenStateChange` to `uncertain`, reason `interrupted`, for each
   desired token on the connection that is `synchronizing`, `ready`, or
   `uncertain`, carrying the same `last_confirmed_at`.
3. `ConnectionStateChange(recovering)` for the next attempt, unless no
   desired token remains or the bounds are exhausted. It is attempt 1,
   unless the connection ended before delivering a frame; then it is the
   attempt after the one that opened that connection.

A capture gap opens for each of those tokens that holds a book, starting at
`last_confirmed_at` and detected at the interruption record's `at`. A token
whose gap is already open, because it never got a fresh book after an
earlier interruption, keeps that gap.

From the `interrupted` record on, nothing more from that connection is
delivered or applied. When the client closes the connection itself, after
`pong_timeout`, `consumer_overflow`, or `subscription_change`, frames can
still arrive before the close completes: the stream may be sending data,
and `PONG` waits behind it (Observed, [§2]). Those frames are counted
(`frames_after_interruption`) and discarded, so `last_confirmed_at` stays
the end of what the client applied.

### Reconnecting

The client reconnects with exponential backoff, within the bounds D2 sets.
With the recommended defaults, the delay before attempt *k* is
`min(max_delay, base_delay × 2^(k − 1))`, multiplied by a uniform random
factor in [0, 1) when `jitter` is on. Each attempt has `connect_timeout` to
complete its handshake. The first attempt when the client starts, or when a
market is added to an empty desired set, starts at once; later attempts
follow the delays.

Before waiting for an attempt, the client fails instead if `max_attempts`
attempts in a row have failed, or if the wait would end more than
`max_recovery_time` after the interruption that began the recovery (at
startup, after the first attempt began). Time spent waiting for the
consumer (`waiting_for_consumer`) does not count toward it, since that wait
says nothing about the server. An attempt under way is bounded by
`connect_timeout`, not cut short.

The client also waits for the consumer before an attempt while `queue_size`
or more status records are waiting for it, which keeps them bounded
([Consumer handoff](#consumer-handoff)). It then emits `recovering` with
reason `waiting_for_consumer` in place of the record it would otherwise
emit, and starts the attempt, with no delay, once the consumer's reading
leaves fewer than `queue_size` waiting.

| Record | When |
| --- | --- |
| `recovering`, `attempt=k`, `retry_in`, `reason=backoff` | Before the delay for attempt *k*. After a failed attempt, `detail` says how it failed. |
| `recovering`, `attempt=1`, `reason=subscription_change`, `retry_in=0` | Before reconnecting to apply a change (D7). |
| `recovering`, `attempt=k`, `reason=waiting_for_consumer`, `retry_in=None` | After a `consumer_overflow` interruption, with `attempt=1`, until the queue drains (D3); or before attempt *k* while `queue_size` or more status records are waiting. |
| `connecting`, `attempt=k` | When attempt *k* starts. |
| `open`, `connection=g` | When the handshake completes. |
| `subscribed`, `connection=g` | When the subscription frame has been sent. The tokens' `synchronizing` records follow. |
| `failed`, `reason=max_attempts` or `max_recovery_time` | When the bounds are exhausted. |

Attempt numbers and the recovery clock start again when a new connection
delivers its first frame after subscribing. A connection that ends without
one, before or after its subscription frame, is a failed attempt and leaves
them running: the next attempt's number follows on from it, and the time
bound still runs from the interruption that began the recovery. So an
endpoint that accepts connections and drops them, at once or after the
subscription frame, still exhausts the bounds.

When the bounds are exhausted the client emits, in order, after the
`interrupted` and `uncertain` records if a subscribed connection's end
exhausted them, a `CaptureGap` with `end` `recovery_failed` and
`resumed_at` `None` for each token with an open gap, then `failed`, with no
`recovering` record for the attempt it will not make. The iterator then
raises `RecoveryFailed`, and the client stays shut down until the block is
left. Tokens keep their last state, `uncertain`.

The SDK, by contrast, retries without limit and reports neither the
disconnect nor the reconnect to its consumer (SDK source and Observed,
[§1]). That is why the client owns the connection.

### Connection states

| State | Meaning |
| --- | --- |
| `connecting` | An attempt is under way. |
| `open` | The handshake completed; `connection` is the new generation. |
| `subscribed` | The subscription frame was sent on it: subscriptions are restored. |
| `interrupted` | The connection ended in a way that may have lost events. |
| `recovering` | Waiting before the next attempt. |
| `ended` | The server closed with `1000 all subscribed assets resolved` while tokens on the connection were not yet settled. Their `settled` records follow, then `idle` if no desired market remains, or else `connecting` (attempt 1, no delay) for desired markets that were not on the connection. |
| `idle` | No connection is needed: the desired set has no unsettled market. Reason `no_subscriptions`. The client connects again when a market is added. |
| `failed` | Reconnection exhausted its bounds. Terminal. |

When the desired set becomes empty while a connection is open, the client
closes it (1000, `no subscriptions`) and emits `idle`, unless the server's
close comes first; either way one `idle` record follows, with `connection`
set to the generation that ended. When the desired set becomes empty during
recovery, the client stops and emits `idle`. A client entered with an empty
desired set emits nothing and opens no connection until a market is added.

A change to the desired set made while no connection is subscribed, during
backoff or an attempt, takes effect in the next subscription frame and
causes no extra reconnect.

## Per-token state machine

### States

| State | Meaning | Usable |
| --- | --- | --- |
| `synchronizing` | Subscribed on the current connection and waiting for the token's opening `book`. | No |
| `ready` | The client holds a book taken from the stream on the current connection and has applied every later change in arrival order. | Yes |
| `uncertain` | The book may be out of date or wrong, or there is none when there should be. `reason` says why. | No |
| `settled` | The market has resolved. Terminal: the market has left the desired set, unless the application adds it again. | Final |
| `removed` | The application removed the market. Terminal, unless the application adds it again. | No |

Only `ready` means the token's book and the events after it can be used as
current. `ready` does not mean the source sent every event: no source signal
can show that ([§4]).

### Transitions

Each row is one trigger. "Records" lists what the client emits for it, in
order. Rows T14, T16, and T18 also emit the connection records described
under [The connection](#the-connection).

| # | From | Trigger | To (`reason`) | Records |
| --- | --- | --- | --- | --- |
| T1 | none, `removed`, or `settled` | A subscription frame naming the token is sent. | `synchronizing` (`subscribed`) | `TokenStateChange` |
| T2 | `uncertain` | A subscription frame naming the token is sent on a new connection. | `synchronizing` (`subscribed`) | `TokenStateChange`; an open gap stays open |
| T3 | `synchronizing` | A `book` for the token arrives on the current connection. | `ready` (`book`) | `BookEvent`; `CaptureGap` if one is open; `TokenStateChange` |
| T4 | `synchronizing` | `book_timeout` passes after the subscription frame with no `book` for it. | `uncertain` (`no_book`) | `TokenStateChange`; [settlement confirmation](#settlement) starts |
| T5 | `ready` | A later `book` for the token. | `ready` (reason unchanged) | `BookEvent`; the book is replaced |
| T6 | `ready` | A `price_change` entry for the token. | `ready` (reason unchanged) | `PriceChangeEvent`, entry applied |
| T7 | `ready` | Hash verification reports divergence (D4). | `uncertain` (`hash_mismatch`) | the event; `TokenStateChange` |
| T8 | `ready` | An undecodable frame or event that may affect the token ([Decoding](#decoding)). | `uncertain` (`undecodable`) | `UndecodableFrame`; `TokenStateChange` |
| T9 | `uncertain` (`hash_mismatch` or `undecodable`) | A hash check verifies (D4). | `ready` (`hash_verified`) | the event; `TokenStateChange` |
| T10 | `uncertain` (any reason but `interrupted`) | A `book` for the token. | `ready` (`book`) | `BookEvent`; `CaptureGap` if one is open; `TokenStateChange` |
| T11 | any but `settled` or `removed` | Market lookup, confirming a settlement, shows the market closed (D6). | `settled` (`lookup_closed`) | per token: `CaptureGap` if open; `TokenStateChange` |
| T12 | `uncertain` (`no_book`) | Market lookup finds no such market. | `uncertain` (`unknown_market`) | `TokenStateChange` |
| T13 | `uncertain` (`no_book`) | `settlement_confirm_timeout` passes without lookup showing the market closed. | `uncertain` (`settlement_unconfirmed`) | `TokenStateChange` |
| T14 | `synchronizing`, `ready`, or `uncertain`, on the connection | The connection is interrupted. | `uncertain` (`interrupted`) | `TokenStateChange`, after `interrupted`; a gap opens if the token holds a book |
| T15 | any but `settled` or `removed` | `market_resolved` for its market. | `settled` (`market_resolved`) | `MarketResolvedEvent`; then, per token, `CaptureGap` if open and `TokenStateChange` |
| T16 | any but `settled` or `removed`, on the connection | The server closes with `1000 all subscribed assets resolved`. | `settled` (`all_resolved_close`) | after `ended`, per token: `CaptureGap` if open; `TokenStateChange` |
| T17 | any but `settled` or `removed` | The application removes its market. | `removed` (`removed`) | `CaptureGap` if open; `TokenStateChange` |
| T18 | `uncertain` (`interrupted`) | Reconnection exhausts its bounds. | `uncertain` | `CaptureGap`, end `recovery_failed`, before `failed` |

T5 and T6 keep the token's reason, which says why it last became `ready`,
so they emit no `TokenStateChange`.

These change nothing about a token's state:

- a `price_change` entry for a token in `synchronizing`, or in `uncertain`
  for `no_book`: no book to apply it to, so it is delivered with
  `applied=False`. The findings do not report this; in the excerpts,
  measured for this document, no entry came before its token's opening
  `book`;
- a `price_change` entry for a token in `uncertain` for `hash_mismatch` or
  `undecodable`: applied, so that a later check can verify the book;
- a repeated message, an event out of timestamp order, and an entry stamped
  before the opening book ([Event handling](#event-handling));
- a book emptied of every level: the stream did this before each
  settlement it announced (Observed, [§6]), but an empty book is still a
  book, and settlement is confirmed only as described below;
- `best_bid_ask`, `last_trade_price`, `tick_size_change`, unknown events,
  and a late `PONG` within `pong_timeout`.

### Adding a market again

A market the application adds again after it was removed or settled starts
over. Its tokens hold no book and no open gap from before. They stay
`removed` or `settled`, and their events are discarded as
[outside the desired set](#events-outside-the-desired-set), until a
subscription frame names them; then T1 makes them `synchronizing`, with
`previous` set to `removed` or `settled`. They take no part in an
interruption before then. With D7's recommended default, the addition
reconnects at once, as any addition does.

### Catalogued behavior

Each behavior in the findings'
[catalog][catalog] and the client's response. The [conformance coverage
table](conformance.md#coverage) names the scenarios that check each.

| Behavior ([catalog]) | Client response |
| --- | --- |
| The SDK's stream reconnects and resubscribes without telling its consumer ([§1]) | The client owns the connection and reports every interruption, recovery, and gap. The SDK's stream is not used. |
| The SDK drops events its parser rejects ([§1]) | The client's own decoder accepts what the stream sends, `new_market`'s string `game_start_time` included, and keeps what it cannot decode ([Decoding](#decoding)). |
| The SDK stops reconnecting after other errors ([§1], source only) | Any failure of an attempt is retried within D2's bounds; exhaustion is reported and raised. |
| Nothing is replayed after a reconnect ([§3]) | The fresh opening `book` replaces the held book; a `CaptureGap` reports the interval ([Recovery contract](#recovery-contract)). |
| A connection with no traffic is closed after about 125 s without a close frame ([§2]) | `PING` every `ping_interval` on every connection; any drop is an interruption. |
| Slow consumers are closed with 1013 or no close frame; `PONG` waits behind data ([§2]) | Both are interruptions; `pong_timeout` (D1) allows for queued data. |
| Messages arrive more than once ([§3]) | Flagged `repeat`, delivered, not applied. |
| A token's events arrive out of timestamp order across types ([§3]) | Delivered in arrival order with source timestamps; nothing is reordered or rejected. |
| A change stamped before an opening `book` can arrive after it ([§3]) | Applied in arrival order and flagged `before_book`. |
| Opening `book` timestamps are the book's last change ([§3]) | Kept as sent; never used to judge freshness. |
| A trade's price enters the hash before it is announced, and a tick-size change seemed to, once ([§4]) | If D4 adopts verification: a failed check is retried with a searched trade price, and only persistent failure is divergence. |
| The hash recipe is undocumented; on busy markets its trade price does not follow announced trades ([§4]) | D4 decides whether and how to verify. |
| The stream can omit a change ([§4]) | With verification, the token becomes `uncertain` until a check verifies or a book replaces it. Without it, the next `book` reports `held_book_matched=False`. |
| The server closes with `1000 all subscribed assets resolved` when every market on the connection has settled ([§6]) | Settlement, not an interruption: no reconnect for those tokens. |
| A settlement can go unannounced ([§6]) | Settlement is also found from a missing book confirmed by lookup. |
| Settled tokens are silently left out of a subscription; REST returns 404 ([§6]) | `book_timeout`, then `no_book`, then confirmation by lookup (D6). |
| `new_market` arrives in bulk for every new market ([§6]) | Dropped and counted by default; delivered only if configured. |

## Recovery contract

This is how a token's book is built, kept, and restored, and what a capture
gap reports. A consumer that keeps its own book from the records gets the
client's book by following the same rules.

1. **The initial book is the stream's.** A token's book is the `book` event
   the stream sends on subscribing. In runs sdk and sdk-pong, on the Vance
   and Harris markets, all 34 `book` events the SDK received matched by
   hash and timestamp a state a reference connection reached, and 31 were
   its latest state (Observed, [§5]). No REST snapshot is taken. A REST
   snapshot could be joined to the stream by hash (Observed, 456 of 456,
   [§5]), but the stream's own book needs no join.
2. **Changes are applied in arrival order.** Each `price_change` entry sets
   the size at its price on one side: `BUY` on the bids, `SELL` on the asks.
   A size of 0 removes the level. The investigation's replays applied
   entries this way and verified the result against the source's hashes
   (Observed, [§4]; [`spikes/orderbook.py`](../spikes/orderbook.py)). The
   client never reorders entries by timestamp: a token's book changes
   arrived in timestamp order except twice, at connection start (Observed,
   [§3]), and the client handles those as below.
3. **A later book replaces the held one.** The stream sometimes sends a
   `book` mid-connection, after trades (Observed, [§4], [§5]). The client
   replaces its book with it and reports `held_book_matched`.
4. **After a reconnect, the fresh book replaces the held one before any
   change is applied.** The source replays nothing; the reopened connection
   starts with current books (Observed, [§3]). The client keeps the old book
   while the token is `uncertain`, so it can compare, and applies no entry
   for the token until the fresh `book` arrives.
5. **A capture gap is reported when it ends.** A gap opens when an
   interruption finds a token holding a book, and one `CaptureGap` is
   emitted when it ends:

   | Field | Meaning |
   | --- | --- |
   | `cause` | The interruption's reason: `close_frame`, `dropped`, `pong_timeout`, `consumer_overflow`, or `subscription_change`; or `records_discarded` if D3's response is to drop records. |
   | `close_code`, `close_reason` | The close frame, if one came. |
   | `last_confirmed_at` | The last frame received on the interrupted connection. |
   | `detected_at` | When the client detected the interruption. |
   | `resumed_at` | When the fresh `book` arrived; `None` if capture never resumed. |
   | `end` | `book`, `settled`, `removed`, or `recovery_failed`. |
   | `connection_before`, `connection_after` | The interrupted generation, and the one whose book ended the gap (`None` if none). |
   | `held_book_matched` | As on the fresh `BookEvent`; `None` if the gap did not end with a book. |
   | `discarded` | The number of records the client itself discarded, when it knows it exactly (`records_discarded` only); otherwise `None`. |

   The count of events the source sent during a gap is unknown, and the
   gap never claims one: the stream has no sequence numbers, and the hash
   cannot count missed events (Observed, [§4]). Events in the interval may
   be missing; events outside it may still have been missed without trace.

### Changes stamped before the opening book

Twice, in the first second of a connection, a `price_change` entry arrived
just after an opening `book` stamped 1 ms later. Applying it failed one hash
check, and the next check verified (Observed, [§3]). The client cannot tell
whether the book already included it. It applies the entry in arrival order,
as every other, and sets `before_book`. A `price_change` entry sets an
absolute size, so applying a change the book already holds leaves the book
as it was unless a later change moved the same level; in both observed
cases the book verified at the next check.

## Settlement

When a subscribed market settled, the stream emptied its book, sent
`market_resolved` naming the winning token two to two and a half minutes
after the end date, and then sent nothing more for it. When it was the last
unresolved market on the connection, the server also closed the connection
with `1000 all subscribed assets resolved` (Observed, [§6]). Neither signal
is reliable on its own, so the client uses three:

| How the settlement shows | Seen | The client |
| --- | --- | --- |
| `market_resolved` for a desired market | 6 of 7 settlements ([§6]) | T15: the market's tokens become `settled`, with the winner. |
| Close `1000 all subscribed assets resolved` | every settlement that left no unresolved market, three times with the frame, once probably without it ([§6]) | T16: every unsettled token on the connection becomes `settled`, winner unknown unless announced. |
| A token gets no `book` after subscribing | every subscription to a settled market, including a resubscription ([§6]) | T4, then lookup: T11 settles the market's tokens once lookup shows the market closed. |

The third path covers the settlement the stream did not announce: there the
connection dropped between the emptied book and any announcement, and the
reconnection got `[]` and nothing more (Observed, once, [§6]). It also
covers an application subscribing to a market that has already settled.

A missing book is not settlement on its own. An unknown token would also get
no book, according to the findings, though no run subscribed one (Inferred,
[§6]). So the client confirms through [market lookup](#market-lookup), once
per market: it asks at once when a token of the market reaches T4, then every
`settlement_poll_interval` seconds, until lookup shows the market closed
(T11), finds no market (T12), or `settlement_confirm_timeout` passes (T13).
T11 settles every token of the market that is not already `settled` or
`removed`, whatever its state, since the market then leaves the desired
set. T12 and T13 apply only to the market's tokens that are still
`uncertain` with `no_book`. Lookup lags the stream: the market lookup the
investigation used first showed settled markets as closed 51 s, 186 s, and
about three minutes after `market_resolved`, and its `closedTime` does not
say when a client could first see that (Observed, [§6]). A `book` that
arrives meanwhile makes the token `ready` (T10), and lookup goes on; if it
then shows the market closed, T11 settles that token too. A token left
`uncertain` by T12 or T13 is subscribed again on the next connection.

A settled market leaves the desired set at once, so no later subscription
frame names it. Events for it that still arrive, such as a repeated
`market_resolved`, are discarded and counted
([Events outside the desired set](#events-outside-the-desired-set)).

Only automatically resolved crypto markets were seen settling ([§6]).
Markets resolved through a UMA proposal may settle differently; the client
relies on none of the timings above, only on the three signals.

## Event handling

### Decoding

Each text frame other than `PONG` is parsed as JSON. Each object in it, or
the object it is, is decoded by its `event_type`:

| Input | Result | Effect on token states |
| --- | --- | --- |
| A known `event_type` whose fields decode | Its typed record | As the state machine says |
| A known `event_type` with a missing or invalid field the client uses | `UndecodableFrame`, `invalid_event` | For `book`, `price_change`, and `tick_size_change`: T8 for each `ready` desired token the event names. If any part of it names no token the client can read, such as a `price_change` entry without a readable `asset_id`, T8 also for each `ready` token of the market it names, or, if it names no market the client can read, for every `ready` token on the connection. For other types: none. |
| An object with an unknown or missing `event_type` | `UnknownEvent` | None |
| Text that is not JSON | `UndecodableFrame`, `invalid_json` | T8 for every `ready` token on the connection |
| JSON that is not an object or an array of objects | `UndecodableFrame`, `not_object`, per item | T8 for every `ready` token on the connection |
| A binary frame | `UndecodableFrame`, `binary` | T8 for every `ready` token on the connection |

A frame the client cannot attribute could have carried a change to any
token, so every book on the connection is in doubt. Likewise, a part of an
event the client cannot attribute to a token could have changed any token
of the event's market, or, with no readable market, any token on the
connection. A `ready` token made `uncertain` this way becomes `ready` again
with its next `book` (T10) or a verifying hash check (T9). Nothing
undecodable stops the connection.

The fields the client uses, by event type:

| `event_type` | Required | Optional |
| --- | --- | --- |
| `book` | `market`, `asset_id`, `timestamp`, `hash`, `bids`, `asks` (lists of `price` and `size`) | `tick_size`, `last_trade_price` |
| `price_change` | `market`, `timestamp`, `price_changes`: each with `asset_id`, `price`, `size`, `side`, `hash` | each entry's `best_bid`, `best_ask` |
| `best_bid_ask` | `market`, `asset_id`, `timestamp`, `best_bid`, `best_ask` | `spread` |
| `last_trade_price` | `market`, `asset_id`, `timestamp`, `price`, `size`, `side` | `fee_rate_bps`, `transaction_hash` |
| `tick_size_change` | `market`, `asset_id`, `timestamp`, `old_tick_size`, `new_tick_size` | |
| `market_resolved` | `market`, `timestamp`, `assets_ids`, `winning_asset_id` | `id`, `winning_outcome`, `tags` |
| `new_market` | `id`, `timestamp` | everything else, kept in `payload` uninterpreted |

The field names and shapes are those of the captured frames, read from the
committed excerpts for this document; the findings quote whole frames only
for `market_resolved` ([§6]). Unknown fields are ignored. `side` is `BUY` or
`SELL`. `new_market` events carry `game_start_time` as a string such as
`'2026-10-04 14:25:00+00'`, which the SDK's validator rejects (Observed,
[§1]); the client keeps it in `payload` without interpreting it.

`tick_size_change` updates the token's tick size and `last_trade_price`
updates its market's announced trade price; the hash check (D4) uses both.

### Repeated messages

Some messages arrive twice, in separate frames, with the same entries down
to the hashes, sometimes listed in a different order: up to 172 in 45
minutes on busy markets, up to 120 ms apart (Observed, [§3],
[Reproduction runs]). The client treats an event as a repeat when an event
with the same content arrived on the same connection within `repeat_window`
seconds before it. Content is compared as the event's JSON value with each
`price_change` entry list taken as a multiset.

A repeat is delivered with `repeat=True`, so that counts stay visible, and
is not applied. Applying a repeated entry would be harmless only if nothing
had changed its level in between. A repeat changes no state, and a repeated
`market_resolved` is discarded with its market already gone from the
desired set.

### Arrival order

Events of different types for one token sometimes arrive out of timestamp
order: a `best_bid_ask` or `last_trade_price` can trail or lead the book, by
up to 0.621 s on busy markets (Observed, [§3]). The client delivers events
in arrival order and applies book changes in arrival order. It does not
treat a timestamp running backwards as an error.

### Events outside the desired set

An event that names only tokens outside the desired set, or a market outside
it, is discarded and counted as such, before repeat detection, so it does not
count as a repeat. This covers removed and settled markets and
any `market_resolved` for a market not subscribed; no run received one of
those ([§6]). It also covers a market added again, until a subscription
frame names it ([Adding a market again](#adding-a-market-again)).
`new_market` events name new markets, not subscribed ones, and
follow `new_market` below. `UnknownEvent` and `UndecodableFrame` are always
delivered.

### `new_market`

With `custom_feature_enabled` set, `new_market` arrives for every new market
on Polymarket, in bursts: 3,689 in an hour, 566 in one minute (Observed,
[§6]). With `new_market` set to `drop`, the default, the client decodes and
counts them and does not deliver them, so they never fill the consumer's
queue. With `deliver`, each becomes a `NewMarketEvent`.

## Consumer handoff

Records pass to the consumer through one bounded FIFO queue:

- Market-event records, including `UnknownEvent` and `UndecodableFrame`,
  count against `queue_size`.
- The limit is checked once per frame. A frame reaches the limit when its
  first market-event record finds `queue_size` or more waiting; otherwise
  every record from it is queued, even past `queue_size`. A frame is never
  split, and one with more events than `queue_size`, such as an opening
  frame for more tokens than that, still gets through once the backlog is
  below the limit. The queue holds at most `queue_size − 1` market-event
  records plus one frame's.
- Status records never count against it and are never dropped. They are
  bounded another way: while `queue_size` or more status records are
  waiting, the client starts no connection attempt
  ([Reconnecting](#reconnecting)). Status records come with market-event
  records, which the limit bounds; from connecting and interruptions, a
  few per token for each connection; or from lookup and the application's
  own changes. So an endpoint that keeps accepting connections and dropping
  them cannot grow the queue without limit while the consumer is not
  reading.
- The task that reads the socket never waits for the consumer. It must keep
  reading `PONG` and close frames, and a reader blocked on a full queue
  would turn a slow consumer into a heartbeat timeout or a server close
  (Observed: the server closes connections whose send buffer fills, [§2]).
- `client.backlog` gives the current count at any time. In the stream, a
  `Backlog` record with `rising=True` follows the records caused by the
  event whose record brings the count to `backlog_warning × queue_size`,
  rounded up ([Record order](#record-order), rule 5), and one with
  `rising=False` is appended when the consumer's reading brings it back
  below that. Only one `rising=True` record is outstanding at a time. A
  `Backlog` record shows where in the stream the backlog crossed the level;
  it reaches the consumer only after the records ahead of it.

What happens at the limit is D3's to settle. Backpressure cannot make the
source retain events: the server ends a connection whose send buffer fills,
and nothing is replayed (Observed, [§2], [§3]). So every response at the
limit loses events, and each must report the loss as a capture gap.

With D3's recommended `disconnect`, a frame that reaches the limit starts
the `consumer_overflow` interruption. That frame is neither delivered nor
applied, nor is anything after it on that connection
([Detecting an interruption](#detecting-an-interruption)). The
interruption's `last_confirmed_at` is the receipt time of the last frame
whose records were queued, so the gap includes the frame that overflowed.
The client reconnects, with no delay, once the consumer's reading brings
the count to `resume_below × queue_size`, rounded down, or lower. However
long that takes, it does not count toward `max_recovery_time`. When one
read both brings the count below the warning level and lets the client
resume, the `Backlog` record comes before `connecting`.

## Market lookup

The client needs a market lookup to resolve a slug to its condition ID and
tokens, to confirm settlement, and, when `verify_hash` is set, to fetch the
two hash inputs the stream does not carry. With `verify_hash` off, the
client never calls `book_parameters`. How the pinned SDK provides it is
D6's to settle. Whatever D6 decides, the client calls lookup through this
interface, so tests replace it with a scripted one:

```python
class MarketLookup(Protocol):
    async def market(self, *, slug: str | None = None,
                     condition_id: str | None = None) -> MarketInfo | None: ...
    async def book_parameters(self, token_id: str) -> BookParameters | None: ...
```

`MarketInfo` holds `condition_id`, `slug`, `question`, `token_ids` and
`outcomes` in the same order, `closed: bool`, `end_date`, and, when known,
`winning_asset_id` and `resolution_status`. `BookParameters` holds
`min_order_size: Decimal` and `neg_risk: bool`. `None` means not found.

If no lookup is available, as when D6 makes the SDK an optional extra that
is not installed, `resolve` raises `ClientStateError`, and a token reaching
`no_book` moves straight to `settlement_unconfirmed` (T13).

Lookup calls run outside the reading task, each limited to `lookup_timeout`.
An exception or a timeout counts as a failed call: it is counted, and for
settlement confirmation it is treated as "still open" until
`settlement_confirm_timeout`. Failures of the calls the client makes on its
own are never raised to the consumer. A failure of the call `resolve`
makes is raised to its caller as `LookupFailed`.

## Order-book hash

Whether the client verifies the hash, and how, is D4's to settle. This
section records what the findings established, which any verification must
follow.

The hash on a `book` event and on each `price_change` entry is the SHA-1 of
the token's book written as compact JSON, in the REST book's key order, with
`hash` set to the empty string (Observed, reproduced on every snapshot and
book checked, [§4]):

```text
{"market":…,"asset_id":…,"timestamp":…,"hash":"","bids":[…],"asks":[…],"min_order_size":…,"tick_size":…,"neg_risk":…,"last_trade_price":…}
```

- Bids are in ascending price order, asks in descending, each level
  `{"price":…,"size":…}` with the source's strings ([§4]).
- `timestamp` is the event's timestamp ([§4]).
- `min_order_size` and `neg_risk` are only in the REST book; they are fixed
  per market ([§4]).
- `tick_size` and `last_trade_price` come with a subscription's opening
  books but not later ones ([§4]). The investigation's helper writes
  `last_trade_price` with three decimals and takes it per market
  ([`spikes/orderbook.py`](../spikes/orderbook.py)).
- Consecutive entries for a token in one burst share a hash, that of the
  book after the last of them, so a check applies once per burst ([§4]).
  In the excerpts, measured for this document, a burst's entries came in
  separate frames, so a live client cannot tell when one has ended; D4
  leaves that open.
- A trade's price enters the hash shortly before the stream announces it.
  On busy markets the price in the hash did not follow announced trades at
  all and was found only by trying the 1,001 prices on the 0.001 grid; a
  match still confirms every level ([§4]). The one tick-size change
  observed seemed to enter the hash the same way; the findings leave that
  open ([§4]).
- Occasionally an entry's hash already includes the token's next change; a
  single failed check that passes after the next change is not a
  divergence ([§4]).

Test vector, computed with the investigation's helper: synthetic market
`A` of the [conformance scenarios](conformance.md#synthetic-markets), token
`A1`, at timestamp `1791199970000`, with bids 0.47 × 250 and 0.48 × 100,
asks 0.53 × 300 and 0.52 × 120, `min_order_size` `"5"`, `tick_size`
`"0.01"`, `neg_risk` false, and `last_trade_price` `"0.500"`:

```text
{"market":"0x00000000000000000000000000000000000000000000000000000000000000a1","asset_id":"10000000000000000000000000000000000000000000000000000000000000000000000000011","timestamp":"1791199970000","hash":"","bids":[{"price":"0.47","size":"250"},{"price":"0.48","size":"100"}],"asks":[{"price":"0.53","size":"300"},{"price":"0.52","size":"120"}],"min_order_size":"5","tick_size":"0.01","neg_risk":false,"last_trade_price":"0.500"}
```

hashes to `b17e93a1f6202e13d8e0dd3aeb958b4a882dca3c`.

What verification can show: a book that has diverged from the source's,
usually at the token's next change. In the probe it exposed an order the
stream never announced (Observed, [§4]). What it cannot show: how many
events were missed, or a missed change that a later one overwrote ([§4]).
The recipe is undocumented and could change without notice, so the client
treats a mismatch as evidence of divergence, never as a condition for
readiness ([§4]; README).

## Statistics

`client.stats()` returns a frozen `ClientStats` snapshot of counters,
cumulative since the client started. It has at least these fields; the
conformance scenarios check them by name:

| Field | Type | Counts |
| --- | --- | --- |
| `frames` | `int` | Frames received after subscription frames, except `PONG` |
| `frames_after_interruption` | `int` | Frames received on a connection after its interruption, discarded |
| `events` | `Mapping[str, int]` | Decoded events, by `event_type` |
| `repeats` | `int` | Events judged repeats |
| `unknown` | `int` | `UnknownEvent` records |
| `undecodable` | `Mapping[str, int]` | `UndecodableFrame` records, by `reason` |
| `new_market_dropped` | `int` | `new_market` events not delivered |
| `discarded_outside` | `int` | Events discarded as outside the desired set |
| `pongs`, `pongs_unsolicited` | `int` | `PONG`s received, and those with no `PING` outstanding |
| `pong_delay_last`, `pong_delay_max` | `float` | Seconds from a `PING` to its `PONG` |
| `connections` | `int` | Connections opened |
| `interruptions` | `Mapping[str, int]` | Interruptions, by cause |
| `lookups`, `lookup_failures` | `int` | Lookup calls, and those that raised or timed out |
| `hash_verified`, `hash_retried`, `hash_failed` | `int` | Hash checks that verified at once, verified after a retry, and failed (D4) |

The statistics are for diagnosis and the live run; the records remain the
contract.

## Configuration

`ClientConfig` is a frozen dataclass. Values marked with a decision are its
recommended defaults, not settled.

| Field | Default | Meaning |
| --- | --- | --- |
| `url` | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | The market endpoint ([How it ran]). |
| `ping_interval` | `10.0` | Seconds between `PING`s (Documented, as the README cites). |
| `pong_timeout` | `20.0` (D1) | Seconds a `PING` may go unanswered before the connection counts as interrupted. |
| `connect_timeout` | `10.0` | Seconds for one attempt's handshake. |
| `close_timeout` | `1.0` | Seconds the client waits for a closing handshake it started. |
| `reconnect` | `ReconnectPolicy()` (D2) | `base_delay` 0.5, `max_delay` 30.0, `max_attempts` 10, `max_recovery_time` 300.0, `jitter` true. |
| `book_timeout` | `10.0` | Seconds after the subscription frame before a token without a `book` becomes `uncertain` (`no_book`). |
| `repeat_window` | `2.0` | Seconds within which an identical event counts as a repeat. |
| `queue_size` | `10000` (D3) | Market-event records waiting at which the next frame reaches the limit ([Consumer handoff](#consumer-handoff)). |
| `backlog_warning` | `0.5` (D3) | Fraction of `queue_size` at which `Backlog` records are emitted. |
| `overflow` | `"disconnect"` (D3) | The response at the limit. |
| `resume_below` | `0.1` (D3) | With `disconnect`, the fraction of `queue_size` the backlog must fall to before reconnecting. |
| `verify_hash` | `True` (D4) | Whether to verify order-book hashes. |
| `hash_grace` | `2.0` (D4) | Seconds a mismatch must persist before it counts as divergence. |
| `settlement_poll_interval` | `15.0` (D6) | Seconds between settlement lookups. |
| `settlement_confirm_timeout` | `300.0` (D6) | Seconds after `no_book` to keep looking. |
| `lookup_timeout` | `10.0` (D6) | Seconds for one lookup call. |
| `new_market` | `"drop"` | `drop` or `deliver`. |
| `keep_raw` | `False` | Attach each frame's text to its event records. |
| `max_message_bytes` | `16777216` | The largest frame the client accepts. |

Every duration must be positive, `queue_size` at least 1, and each fraction
greater than 0 and at most 1, with `resume_below` below `backlog_warning`;
otherwise the constructor raises `ConfigError`.

## Decisions

These are proposed by this draft and take effect when the operator approves
it. Each can be revisited in a later version.

1. **The client owns the connection and does not use the SDK's stream.** The
   SDK's stream hides disconnects, reconnects, and the events it drops
   (Observed and SDK source, [§1]); the client must report all three.
2. **One connection carries the whole desired set.** It is simplest, it is
   what the SDK does (SDK source, [§1]), and it makes the all-resolved
   close mean what it says for the client's whole subscription.
3. **`custom_feature_enabled` is always set.** It adds `market_resolved`
   (Documented, as the README cites), and every settlement run set it
   ([§6]). The `new_market` traffic it brings is filtered (decision 15).
4. **The application heartbeat is the only liveness check.** The protocol
   keepalive is off, as in the investigation's sockets, so one documented
   timeout governs ([How it ran]).
5. **Only `ready` is usable, and `uncertain` always carries a reason.** A
   consumer that needs only usable data checks one state; one that needs to
   act on the cause reads the reason.
6. **The stream's opening book is the initial state; no REST snapshot.** It
   matched a state of the source each time it was checked, in 34 checks on
   two markets ([§5]), and it saves a dependency on REST rate limits, which
   are unknown ([§5]).
7. **Book changes are applied in arrival order, never by timestamp.** Book
   changes for a token arrived in timestamp order apart from two entries at
   connection start ([§3]), and the README specifies arrival order.
8. **Repeats are delivered, flagged, and not applied.** Dropping them would
   hide them; applying them could undo a later change at the same level.
9. **An entry stamped before the opening book is applied and flagged.** The
   findings cannot say whether the book includes it, and both observed cases
   verified at the next check after applying it ([§3]).
10. **An emptied book does not change state.** It preceded every announced
    settlement by under half a second ([§6]), so a separate state would
    add little, and an empty book is valid.
11. **The all-resolved close settles; every other close interrupts.** The
    close reason is explicit ([§6]). A close with any other code or reason,
    including `1000` with another reason, is treated as possible loss. If
    the reason text changes, the client reconnects, finds no books, and
    settles through lookup, so the cost is a delay: `book_timeout`, then
    lookup's lag, which was 51 s to about three minutes ([§6]).
12. **A missing book is confirmed through lookup before settling.** An
    unknown token would look the same, the findings say, though no run
    subscribed one (Inferred, [§6]).
13. **An undecodable frame makes the books it may affect uncertain.** A
    frame the client could not read may have changed a book; saying so is
    the point of the uncertain state. The client does not reconnect to
    force a fresh book; a later book or a verifying check restores the
    token.
14. **Unknown event types are delivered and change nothing.** New lifecycle
    events can appear with the custom feature; they are kept, as the README
    requires, without guessing their effect.
15. **`new_market` is dropped and counted by default.** Its bursts would
    fill the queue with events about markets the application did not ask
    for ([§6]).
16. **Events outside the desired set are discarded and counted.** They
    belong to markets the application removed or that settled.
17. **A capture gap is one record, emitted when it ends.** It then carries
    both ends, how it ended, and whether the book changed. While it is open,
    the token's `uncertain` state with `last_confirmed_at` reports it.
18. **Every `book` is compared with the held book.** It is cheap, needs
    nothing undocumented, and shows a change during a gap, whatever D4
    decides.
19. **Record order is fixed.** One correct sequence per scenario lets the
    conformance scenarios compare records in order.
20. **Status records bypass the queue limit; the reader never blocks.** A
    state change or gap must never be lost, and a reader that stops reading
    loses the connection ([§2]). Status records are bounded instead by
    holding back reconnection while too many are waiting.
21. **Values are `Decimal` and timestamps integer milliseconds, as sent.**
    The README requires exact values.
22. **Client-initiated closes use code 1000 with a reason naming the
    cause.** The scripted server checks the reason; the source's response to
    the code is not known to matter.
23. **`book_timeout` defaults to 10 s.** The findings do not report how long
    opening frames take. Measured for this document from the committed
    evidence, they arrived 0.13 to 0.52 s after the subscription frame on
    the 24 connections whose excerpts keep them. Ten seconds leaves room for
    a slow path without leaving a settled token unreported for long.
24. **`repeat_window` defaults to 2 s.** Repeats arrived at most 120 ms
    apart (Observed, [Reproduction runs]); a repeat's content includes its
    timestamp and hashes, so a wider window risks little.
25. **`max_message_bytes` defaults to 16 MiB.** The `websockets` default of
    1 MiB would close the connection on a large opening frame and repeat on
    every reconnect. The largest frame in the committed evidence, measured
    for this document, is 68,573 bytes: the probe's opening frame for 16
    tokens (about 4 KB per token).
26. **Attempt counting restarts only when a connection delivers a frame.**
    A connection that ends before its first frame is a failed attempt,
    whether or not the subscription frame was sent, so an endpoint that
    accepts and drops connections still exhausts the bounds.

## Decisions awaiting the operator

Each needs the operator's decision. The recommended default is what this
document and the scenarios use until then; the scenarios that depend on a
decision say so.

### D1. `PONG` timeout

**Needs operator decision.**

| Option | For | Against |
| --- | --- | --- |
| 30 s, as the SDK's watchdog | Never misreads the slowest observed `PONG` as a dead connection | A stalled connection is noticed 30 to 40 s late |
| 20 s | Nearly twice the slowest observed `PONG`; notices a stall in 20 to 30 s | |
| 15 s | Faster | Little margin over 11.4 s |
| Extend the timeout while data keeps arriving, up to a cap | Separates a backlog from a dead connection | More logic; a backlog that keeps growing ends with a server close anyway |

Evidence: `PONG` took a median of 0.14 s; 1.7 s at most over an hour on the
election markets, 3.5 s over 45 minutes on busy markets, and 9.5 s and
11.4 s on busy markets just before the server ended those connections
(Observed, [§2]). A review run's connection survived a 7.05 s backlog with
a slowest `PONG` of 6.42 s (Observed, [Review runs]). The SDK uses 30 s,
checked every 5 s (SDK source, [§1]); in its run, the `websockets`
keepalive caught a stall after 20.5 s, before the watchdog (Observed,
[§1]). How much backlog the server allows before closing a slow consumer is
open ([§2]).

**Recommended default: 20 s, fixed, measured from the oldest unanswered
`PING`.** It is more than 8 s above the slowest `PONG` observed, and the two
connections whose slowest `PONG` exceeded 6.5 s were then ended by the
server itself ([§2]).

### D2. Reconnect bounds

**Needs operator decision.**

| Option | For | Against |
| --- | --- | --- |
| Unlimited attempts, as the SDK | Never gives up | The README requires bounds; a consumer could wait forever |
| A number of attempts | Simple | Total time depends on the backoff schedule |
| A total time | Bounds the wait directly | A burst of fast failures could use many attempts |
| Both, whichever comes first | Bounds both | Two numbers to choose |

Backoff: base delay, factor, cap, and jitter.

Evidence: the SDK draws each delay uniformly between zero and 0.25 s ×
2^attempt, capped at 30 s, without limit (SDK source, [§1]). It reconnected
0.5 s after an abort; through a 60-second outage it made nine failed
attempts, and its next came 21.3 s after the proxy returned (Observed,
[§1]). On busy markets one 45-minute run lost its connection to `1013`
three times and once more without a close frame (Observed, [§2]). Whether
the server penalizes quick reconnects is not known. The README describes "a
bounded number of attempts", which the time-only option would not meet.

**Recommended default: both bounds, 10 consecutive failed attempts or 300 s
since the interruption; delay `min(30, 0.5 × 2^(k−1))` s with full jitter;
counting restarts when a connection delivers a frame (decision 26).**

### D3. Queue size and the response at the limit

**Needs operator decision.**

| Response | What happens | For | Against |
| --- | --- | --- | --- |
| `disconnect` | The client closes the connection (`consumer_overflow`), waits for the backlog to fall to `resume_below`, and reconnects. Tokens are `uncertain` meanwhile; gaps end with fresh books. | Reuses the recovery path; the consumer gets fresh books; memory is bounded | Every token loses events, not only the busy one |
| `drop` | Market-event records are discarded while the queue is full and counted per token; a `CaptureGap` with cause `records_discarded` and the exact `discarded` count follows when space returns. Token states do not change. | The connection stays up; the loss is counted exactly | A consumer keeping its own book cannot repair it without a fresh `book` |
| `fail` | The iterator raises `ConsumerTooSlow` and the client shuts down. | Nothing is lost silently, and the application decides | The application must restart the client |
| block | The reader stops reading when the queue is full. | No client-side loss | Rejected: `PONG` and close frames stop being read, and the server closes slow consumers anyway ([§2]) |

Evidence: busy markets produced up to about 840 frames a second (Observed,
[§2]). The server ended one connection after its receipt lag reached 11.7 s
and another after a `PONG` took 11.4 s, while a review run's connection
survived a 7.05 s backlog (Observed, [§2], [Review runs]). `new_market`
bursts of 566 a minute are filtered by default (decision 15).

**Recommended default: `disconnect`, `queue_size` 10,000, `backlog_warning`
0.5, `resume_below` 0.1.** At the peak frame rate, 10,000 records is about
12 s, if each frame yields about one record, as a `price_change` frame does
(an estimate, not an observation). That is close to the backlogs at which
the server itself ended connections,
and the README describes the client disconnecting to protect memory.

### D4. Hash verification

**Needs operator decision.**

| Option | For | Against |
| --- | --- | --- |
| No verification | Nothing undocumented; no REST call | A diverged book stays `ready` until the next `book` |
| Compare levels only when the stream sends a `book` | Cheap; documented fields only; the client does this anyway (decision 18) | Detects divergence only when a `book` happens to arrive |
| Verify with the recipe; persistent mismatch makes the token `uncertain` (T7, T9) | Catches divergence at the next change, as the README describes | Undocumented recipe; two REST fields per token; a trade-price search |
| Verify, but report only in statistics | Evidence without state changes | Divergence never reaches the consumer's state |

Evidence ([§4]): the recipe reproduced every REST snapshot and stream book
checked. Over an hour on election markets, 21,136 checks: 20,726 verified
at once, 322 with the next announced trade price, 13 only after a
trade-price search, 64 one change late, and 11 failed singly, each followed
within 48 ms by a check that verified. Over 45 minutes on busy markets,
444,131 checks with 52 failures in 17 episodes, all but two clearing within
0.75 s; those two were cut off by a settlement. The one real divergence, in
the probe, failed four consecutive checks on each token over about 4 s.
Checks need `min_order_size` and `neg_risk` from REST, and on busy markets a
search over 1,001 trade prices. Whether every market uses this recipe is
open. Measured for this document from the committed excerpts, which keep
only parts of the captures, with excerpts cut from the same capture merged,
a burst's entries arrive in separate frames: of 20,038 `price_change`
entries, repeats aside, 1,793 from 7 captures carried the same hash as
their token's previous entry, which had come in an earlier frame, 1,791 of
them with the same timestamp. They followed it by at most 0.27 s, and 143
came after other frames in between.

**Recommended default: verify, with these rules.** Fetch `min_order_size`
and `neg_risk` through lookup when a token enters the desired set; until they
arrive, or if they cannot be had, do not check that token. Check each `book`
event's own hash when it arrives, and each burst after its last entry, using
the event's timestamp. On failure, retry with the market's current announced
trade price, then with every price on the 0.001 grid; a check that verifies
after a retry counts as verified. Treat failures as divergence only when at
least two checks have failed, with no verifying check between them, and the
first and the latest arrived at least `hash_grace` (2 s) apart; then T7. A
verifying check restores the token (T9). If a `book` fails its own check,
stop checking that token until a later `book` verifies, and count it, since
the recipe or its inputs, not the source's book, are then wrong.

**Open within this default: when a burst has ended.** A live client cannot
tell a burst's last entry when it arrives. The investigation's replay
checked each run of entries sharing a hash once the token's next entry,
with another hash, or its next `book` had arrived (`check_hashes.py`),
which a replay of a finished capture can always do. The ways to end a
burst live:

| Option | For | Against |
| --- | --- | --- |
| Check when the token's next entry carries another hash or its next `book` arrives, or after a quiet period with no entry for it | One check per burst, as in the replay, so its counts carry over | A new setting; detection waits for the quiet period, which must exceed the 0.27 s above |
| Check at the end of each frame, and withdraw a failed check when the token's next entry carries the same hash | No new setting; nothing waits | Every frame of a burst but its last fails a check and runs a trade-price search: about one entry in eleven in the excerpts |
| Check only when the token's next entry carries another hash or its next `book` arrives | No new setting; the replay's rule exactly | Divergence shows one change later, and a token's last burst before it goes quiet is not checked |

Until the operator decides, the `pending D4` scenarios hold under the first
option with a quiet period of at most 0.3 s, and under the second. Under
the third, `hash-divergence` needs one more change before its token becomes
`uncertain`.

### D5. Supported Python versions, and the package and import names

**Needs operator decision.**

Python: the SDK 0.12.0 requires Python 3.11 or later. That is from its
wheel's metadata, read for this document; the findings record only that
they ran on CPython 3.12.13 ([Versions]). The design needs 3.11 at least,
for `asyncio.timeout()` and `TaskGroup`.

| Option | For | Against |
| --- | --- | --- |
| 3.11 and later | Matches the SDK's floor | Checked against nothing the findings ran |
| 3.12 and later | Matches the findings' interpreter | Drops 3.11 users |
| 3.13 and later | Fewer versions in CI | Drops more |

Names: the SDK's import package is `polymarket` (SDK source, [§1]), so the
client must not use it.

| Option | Distribution | Import |
| --- | --- | --- |
| Match the repository | `polymarket-market-data-client` | `polymarket_market_data` |
| Shorter | `pm-market-data` | `pm_market_data` |

**Recommended default: CPython 3.12 and later, with CI on each supported
release from 3.12 on; distribution `polymarket-market-data-client`, import
`polymarket_market_data`.** Releases are wheels attached to tagged GitHub
releases, as the README describes. If the package is ever published to a
public index under a name containing "polymarket", consider whether the name
reads as affiliated with Polymarket.

### D6. How the pinned SDK serves market lookup and settlement confirmation

**Needs operator decision.**

| Option | For | Against |
| --- | --- | --- |
| The SDK's `AsyncPublicClient` behind `MarketLookup`: `get_market(slug=…)` for resolution and settlement, `get_order_book(token_id=…)` for the hash inputs | What the investigation's scripts used: `get_market` in `repro_settlement.py`, `get_order_book` in `check_hashes.py` and `snapshot_join.py`; one pinned dependency | Its wheel metadata lists `eth-abi`, `eth-account`, `httpx`, `pydantic`, and more; whether it looks markets up by condition ID was not checked |
| As above, but confirm settlement by the REST `404` for the token's book | One call | Only settled tokens were seen to return it; an unknown token might too ([§6]) |
| No SDK: call the REST APIs directly | Fewer dependencies | The README commits to pinning the SDK for lookup |

And where the SDK sits: a required dependency, or an optional extra with
the default lookup, leaving the core client with `websockets` alone.

Evidence: market lookup showed a settled market closed 51 s, 186 s, and
about three minutes after `market_resolved` (Observed, [§6]). The REST book
for a settled token returns HTTP 404 with "No orderbook exists for the
requested token id" (Observed, [§6]). The SDK's own stream is not used
(decision 1). The pin is `polymarket-client==0.12.0`, the version the
findings checked ([Versions]). Its wheel metadata, read for this document,
requires `websockets` from 13 to below 16, which bounds the client's own
`websockets`.

**Recommended default: the SDK behind `MarketLookup`, as the investigation
used it, as an optional extra that provides the default lookup;
confirmation by `get_market(slug=…)` showing the market closed, polled every
15 s for up to 300 s after `no_book`.** A market added without a slug cannot
be confirmed this way and becomes `settlement_unconfirmed`. The 404 is
counted in statistics, not trusted alone.

### D7. Applying subscription changes

**Needs operator decision.** This was found while writing the spec: the
findings do not cover changing a subscription on an open connection.

| Option | For | Against |
| --- | --- | --- |
| Reconnect with the new desired set | Uses only the subscription frame the findings verified | Every change interrupts every token: a short gap for all |
| Send a change frame on the open connection | No gap for unchanged tokens | The server's response is unverified |
| Open a new connection for added markets; stop delivering removed ones and leave them off the next subscription frame | Verified frames; no gap for unchanged tokens | Several connections, each with its own heartbeat and close handling |

The SDK 0.12.0 sends `{"operation": "subscribe", "assets_ids": […],
"custom_feature_enabled": …}` and `{"operation": "unsubscribe",
"assets_ids": […]}` frames on an open connection (`market_protocol.py`, read
for this document; not in the findings). Whether the server then sends books
for added tokens, in what shape, or rejects anything was not checked.

**Recommended default: reconnect with the new desired set for additions
(`subscription_change`, no backoff), and apply removals without
reconnecting: a removed market's tokens become `removed` at once, its events
are discarded, and the next subscription frame leaves it out.** Revisit
after a live check of the change frames. The scenarios for subscription
changes are written for this default.

## Open questions

From the findings ([Open questions][findings-open]), unresolved here:

- Whether the 125-second close of an idle connection comes from the market
  socket or from Cloudflare, and how long a connection without `PING` stays
  open beyond 10 minutes.
- How much backlog the server allows before it closes a connection as a slow
  consumer, and whether that depends on the client or the path.
- Whether the market channel accepts an undocumented replay or resume field.
- Whether every market uses the hash recipe found here, and how the trade
  price inside the hash is chosen on busy markets.
- How REST and the stream line up on busier markets, and REST rate limits.
- How settlement looks for markets resolved through a UMA proposal; whether
  `market_resolved` is ever sent after a reconnect or more than once; and
  whether the all-resolved close can lose `market_resolved`, as it seems to
  have once.
- How the SDK behaves with several subscriptions, subscription changes during
  a fault, and TLS failures on its own connection.

From the sections of the findings, not in their consolidated list:

- Whether `market_resolved` is ever sent late ([§6]).
- Whether a tick-size change enters the hash before it is announced, as the
  one observed seemed to ([§4]).
- What happens when a REST snapshot's hash never appears in the stream; it
  did not occur ([§5]). The client takes no REST snapshot (decision 6).
- How the SDK behaves over longer periods ([§1]).

Raised by this document:

- How the server handles a subscription change on an open connection (D7).
- Whether an unknown token, like a settled one, gets no `book` and a REST
  404 (decision 12, D6).
- Whether market lookup's `closed` means settled for a market resolved
  through a UMA proposal, or only that trading stopped.
- Whether the server limits the tokens in one subscription frame or the
  connections per client. The probe subscribed 16 tokens on one connection
  ([Runs]).
- Whether the source ever sends text other than JSON and `PONG`. None
  appears in the committed evidence, checked for this document.
- Whether an opening frame is ever split across frames, or delayed beyond
  `book_timeout` under load.
- Whether a `price_change` can arrive for a token before its opening `book`.
  The findings do not say; none did in the excerpts measured for this
  document.
- Whether the server penalizes quick reconnects (D2). D7's recommended
  default reconnects at once on every addition.
- Whether the decoder's required fields hold for every event the source
  sends. They were read from the captured frames, not from the findings.

[§1]: ../docs/source-behavior.md#1-reconnection-and-subscription-restoration-in-the-sdk
[§2]: ../docs/source-behavior.md#2-heartbeat
[§3]: ../docs/source-behavior.md#3-event-order-and-replay
[§4]: ../docs/source-behavior.md#4-revealing-a-missed-event
[§5]: ../docs/source-behavior.md#5-joining-a-snapshot-to-the-stream
[§6]: ../docs/source-behavior.md#6-settlement
[catalog]: ../docs/source-behavior.md#behavior-the-documentation-and-sdk-do-not-state
[How it ran]: ../docs/source-behavior.md#how-the-investigation-ran
[Versions]: ../docs/source-behavior.md#versions
[Markets]: ../docs/source-behavior.md#markets
[Runs]: ../docs/source-behavior.md#runs
[Reproduction runs]: ../docs/source-behavior.md#reproduction-runs
[Review runs]: ../docs/source-behavior.md#review-runs
[findings-open]: ../docs/source-behavior.md#open-questions
