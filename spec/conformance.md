# Conformance scenarios

**Status:** Draft 0.1.0, for the operator's review, with the
[client contract](client.md); not tagged. Scenarios that depend on a
decision awaiting the operator say so with a `pending` line.

These scenarios check a client against a scripted local WebSocket server.
Each gives the frames and closes the server sends, the steps the test takes
on the client, and the records the client must deliver, in order, with the
times by which it must deliver them. They were written from the client
contract and the source findings, not by running a client. A client that
disagrees with one needs investigation, not a changed expectation. If the
investigation finds an error in a scenario, correct it in a new version of
this document.

The runner reads the scenarios from this file: every fenced code block whose
info string is `scenario` is one scenario, written in the notation below.
Nothing else here is executed.

## Synthetic data

The scenarios use synthetic markets and frames shaped like the ones the
investigation captured: the same event types, field names, key order,
string-encoded numbers, and millisecond timestamps, and the same opening
array and `[]` frame. The shapes were read from the committed excerpts for
this document; the findings quote whole frames only for `market_resolved`
([§6]). The scenarios do not reuse the captured data. The excerpts in
[`spikes/evidence/`](../spikes/evidence) are Polymarket's market data, and
whether its terms allow republishing it has not been checked.

### Synthetic markets

`T0` is `1791200000000`. A time written `t=<offset>` is `T0` plus that many
milliseconds.

| Market | Condition ID | Tokens | Outcomes | Slug |
| --- | --- | --- | --- | --- |
| `A` | `0x00000000000000000000000000000000000000000000000000000000000000a1` | `A1` `10000000000000000000000000000000000000000000000000000000000000000000000000011`<br>`A2` `10000000000000000000000000000000000000000000000000000000000000000000000000012` | Yes, No | `synthetic-a` |
| `B` | `0x00000000000000000000000000000000000000000000000000000000000000b2` | `B1` `10000000000000000000000000000000000000000000000000000000000000000000000000021`<br>`B2` `10000000000000000000000000000000000000000000000000000000000000000000000000022` | Yes, No | `synthetic-b` |
| `S` | `0x000000000000000000000000000000000000000000000000000000000000005e` | `S1` `10000000000000000000000000000000000000000000000000000000000000000000000000031`<br>`S2` `10000000000000000000000000000000000000000000000000000000000000000000000000032` | Up, Down | `synthetic-s` |
| `U` | `0x000000000000000000000000000000000000000000000000000000000000000e` | `U1` `10000000000000000000000000000000000000000000000000000000000000000000000000041`<br>`U2` `10000000000000000000000000000000000000000000000000000000000000000000000000042` | Yes, No | `synthetic-u` |

`A` and `B` are trading. Each has `min_order_size` `"5"`, `neg_risk` false,
and tick size `"0.01"`; the last trade price is `"0.500"` for `A` and
`"0.400"` for `B`. `S` has settled, with `S2` winning, and the server sends
no book for it. `U` is unknown to market lookup, and the server sends no book
for it either. A scenario passes a market to the client as
`Market(condition_id, token_ids, slug)` from this table.

Standard books, each last changed at `t=-30000`, so that opening books carry
old timestamps as real ones do ([§3]):

| Token | Bids | Asks | Hash of the opening book |
| --- | --- | --- | --- |
| `A1` | 0.48 × 100, 0.47 × 250 | 0.52 × 120, 0.53 × 300 | `b17e93a1f6202e13d8e0dd3aeb958b4a882dca3c` |
| `A2` | 0.48 × 120, 0.47 × 300 | 0.52 × 100, 0.53 × 250 | `84549e080ed1722e15d1891c5ee759b77760e125` |
| `B1` | 0.39 × 80, 0.38 × 200 | 0.41 × 90, 0.42 × 150 | `07ce21178f36559de10e79c515e4e07832dcea28` |
| `B2` | 0.59 × 90, 0.58 × 150 | 0.61 × 80, 0.62 × 200 | `b17904e76ff27a2131684e3764670c829d4c3e18` |

The hashes follow the recipe in the
[client contract](client.md#order-book-hash) and were computed with the
investigation's helper, [`spikes/orderbook.py`](../spikes/orderbook.py). The
runner's tests check its own hashing against them.

## How a scenario runs

### The runner

For each scenario the runner:

1. starts the scripted server on a local port;
2. builds a `ClientConfig` from the [profile](#profile) and the scenario's
   `config` lines, with `url` pointing at the server;
3. builds a scripted market lookup from the scenario's `lookup` lines;
4. constructs the client with the scenario's `markets` and that lookup, and
   enters its `async with` block in a separate host task;
5. runs the scenario's steps in order, in its own task. It reads records
   from `client.records()` only at expectation steps, one record each, so
   records wait in the client's queue between them;
6. when the steps run out, checks that no record is waiting
   (`client.backlog` is 0 and no status record is queued), then leaves the
   block as `exit` does, if the scenario has not already.

A step that waits fails after 5 s unless it says otherwise. The first failing
step ends the scenario.

### The scripted server

The server accepts WebSocket connections on its port. It runs on its own
event loop in a separate thread, so its tasks never mix with the client's.
It numbers the connections it accepts 1, 2, and so on; the client's
generations must agree, and the records say so. One connection is open at a
time, the current one.

Without a step telling it otherwise, the server:

- holds each connection attempt until the script reaches `accept` or a
  refusal;
- answers each `PING` from the client with `PONG` at once (`pong auto`);
- records every frame the client sends. A text frame other than `PING`
  that no `recv-subscribe` step consumes fails the scenario. Close frames are
  checked only by `expect-client-close`;
- keeps a reference book for each token of `A` and `B`, starting from the
  standard books. The reference is the source's own state: the frames below
  are built from it, and the steps that change the book change it.

### The scripted lookup

The lookup implements `MarketLookup` for the synthetic markets. Unless a
`lookup` line or step says otherwise, `A` and `B` answer `open`, `S`
answers `closed winner=S2`, and `U` answers `missing`.

| Answer | `market(...)` returns | `book_parameters(token)` returns |
| --- | --- | --- |
| `open` | `MarketInfo` with `closed` false | `min_order_size` 5, `neg_risk` false |
| `closed winner=<T>` | `MarketInfo` with `closed` true and `winning_asset_id` `<T>` | `None` |
| `missing` | `None` | `None` |
| `error` | raises `RuntimeError` | raises `RuntimeError` |

It finds a market by slug or by condition ID, and counts every call.

### Profile

The client configuration every scenario starts from. The values are short so
that scenarios run in seconds; they are not recommended defaults.

| Field | Value |
| --- | --- |
| `url` | the scripted server |
| `ping_interval` | `0.5` |
| `pong_timeout` | `2.0` |
| `connect_timeout` | `1.0` |
| `close_timeout` | `0.5` |
| `reconnect` | `base_delay` 0.1, `max_delay` 0.4, `max_attempts` 3, `max_recovery_time` 10.0, `jitter` false |
| `book_timeout` | `1.0` |
| `repeat_window` | `1.0` |
| `queue_size` | `1000` |
| `backlog_warning` | `0.5` |
| `overflow` | `disconnect` |
| `resume_below` | `0.1` |
| `verify_hash` | `false` |
| `hash_grace` | `0.5` |
| `settlement_poll_interval` | `0.5` |
| `settlement_confirm_timeout` | `3.0` |
| `lookup_timeout` | `1.0` |
| `new_market` | `drop` |
| `keep_raw` | `false` |

With these values, the delay before reconnection attempt 1 is 0.1 s, before
attempt 2 is 0.2 s, and before attempts 3 and later is 0.4 s.

## Notation

### Lines

- A line whose first non-space character is `#` is a comment. Blank lines
  are ignored.
- A line that begins with a space continues the line above; the two are
  joined with one space.
- Words are separated by spaces. A word that starts with `"` is a JSON
  string literal and may contain spaces.
- A step may begin with a label, `<label>:`, made of lowercase letters,
  digits, and hyphens. Labels name times for `within`.

### Header

The block begins with these lines, before any step.

| Line | Meaning |
| --- | --- |
| `scenario <name>` | Required, first. The name matches the scenario's heading. |
| `markets <M> ...` | The desired set passed to the constructor, in order. Without it, the set is empty. |
| `config <field>=<value> ...` | Overrides of the profile. `reconnect.<field>` sets a field of the reconnect policy. |
| `lookup <M> <answer>` | The lookup's initial answer for `M`. |
| `pending <decision> ...` | The decisions in [the client contract](client.md#decisions-awaiting-the-operator) the scenario depends on. Informational. |

### Server steps

| Step | Effect |
| --- | --- |
| `accept` | Wait for the client's next connection attempt and complete its handshake. It becomes the current connection. |
| `refuse <n>` | Answer the next `n` attempts with HTTP 503 instead of a handshake, and wait until all `n` have arrived. |
| `refuse-all` | Answer every attempt with HTTP 503 until the next `accept`. Does not wait. |
| `recv-subscribe <T> ...` | Wait for the next text frame other than `PING`. It must be a JSON object with exactly three members: `type` `"market"`, `assets_ids` listing exactly these tokens in this order, and `custom_feature_enabled` `true`. |
| `recv-ping` | Wait for the next `PING` the server receives after this step starts. |
| `send <frame>` | Send a frame written in the [frame notation](#frame-notation). |
| `send-text <text>` | Send the rest of the line as one text frame, after replacing references: `${A}` with a market's condition ID, `${A1}` with a token's ID, and `${t:<offset>}` with the decimal digits of `T0` plus the offset. |
| `send-binary <hex>` | Send these bytes as a binary frame. |
| `send-again <label> [reversed]` | Send the text of the frame sent by the step with that label again. With `reversed`, a `price_change`'s entries are listed in reverse order. The reference books do not change. |
| `pong auto`, `pong off`, `pong hold` | Answer each `PING` at once; never answer; or keep the answers owed. The setting lasts, across connections, until changed. |
| `release-pongs` | Send every `PONG` owed, in order. |
| `silent <T> t=<offset> <side>:<price>:<size>` | Change the reference book as a `price_change` entry would, without sending anything: a change the stream omits. |
| `trade <M> <price>` | Set the trade price that enters `M`'s hashes, without sending anything. |
| `idle-close <seconds>` | From now on, end the current connection without a close frame if no frame arrives from the client for this long. |
| `close <code> [<reason>]` | Send a close frame and complete the closing handshake. If the client has already started closing, complete its handshake instead. |
| `drop` | End the current connection's TCP connection without a close frame. |
| `expect-client-close <code> <reason>` | Wait until the client has sent a close frame on the current connection, and check its code and reason. |
| `expect-no-connect <seconds>` | Fail if a connection attempt arrives within this many seconds. |

### Runner steps

| Step | Effect |
| --- | --- |
| `wait <seconds>` | Sleep. |
| `subscribe <M>` | Call `client.subscribe` with market `M`. |
| `unsubscribe <M>` | Call `client.unsubscribe` with `M`'s condition ID. |
| `resolve <slug>` | Call `client.resolve(slug)`; the result must equal the synthetic market with that slug. |
| `lookup <M> <answer>` | Change the lookup's answer for `M` from now on. |
| `exit` | Leave the client's block normally, then check shutdown. |
| `cancel` | Cancel the host task, then check shutdown. |
| `read-timeout <seconds>` | Await the next record inside `asyncio.timeout(seconds)`; it must raise `TimeoutError`. |

A runner step that calls the client may end with `raises <Exception>`: the
call must raise that exception.

Checking shutdown means: the host task has finished; every task the client
started has finished, judged by comparing `asyncio.all_tasks()` on the
runner's loop, leaving out the runner's own host and step tasks, with the
set taken before the client was entered; and the client's iterator, once
read to its end, raises `StopAsyncIteration`.

### Expectation steps

| Step | Passes when |
| --- | --- |
| `expect <record>` | The next record matches. |
| `expect-nothing <seconds>` | No record arrives in this many seconds. |
| `expect-error <Exception>` | The next read raises this exception. |
| `expect-end` | The next read raises `StopAsyncIteration`. |
| `expect-backlog <n>` | `client.backlog` is `n`. |
| `expect-stats <counter><op><value> ...` | Each named counter of `client.stats()` compares as stated: `=`, `>=`, or `<=`. A counter that is a mapping is named with a dot, as `undecodable.binary`; named alone, it means the sum of its values. |

### Record notation

| Notation | Matches |
| --- | --- |
| `conn <state>` | `ConnectionStateChange` with that state |
| `token <T> <state>` | `TokenStateChange` for that token and state |
| `gap <T>` | `CaptureGap` for that token |
| `backlog` | `Backlog` |
| `book <T>`, `best_bid_ask <T>`, `last_trade_price <T>`, `tick_size_change <T>` | That event record for that token |
| `price_change <M>`, `market_resolved <M>` | That event record for that market |
| `new_market` | `NewMarketEvent` |
| `unknown` | `UnknownEvent` |
| `undecodable` | `UndecodableFrame` |

Any number of `<field>=<value>` words follow. The record must have each
named field with a matching value; fields not named are not compared. A
word that names a synthetic market or token stands for its ID. How the
value is compared depends on the field:

- `none` matches `None`, and `set` matches anything but `None`, in any
  field.
- A `bool` field matches `true` or `false`.
- An `int` field matches the number as an integer.
- A `float` field matches a number within 0.001 of it.
- A `Decimal` field matches when its `str()` is exactly the word, so
  `0.500` does not match `Decimal("0.5")`.
- A `str` field matches the word, or the JSON string literal, exactly.
- An enum field matches the member's name in lower case, such as `ready`
  or `buy`.
- A tuple of IDs matches a comma-separated list of names in that order;
  `()` matches an empty tuple.
- `t=<offset>` matches `source_timestamp_ms` equal to `T0` plus the offset.
- `resumed=true` and `resumed=false` match a `CaptureGap` whose
  `resumed_at` is or is not set.
- On a `price_change`, `applied` and `before_book` take one value for every
  entry, or a comma-separated list with one value per entry, in order.
- `payload.<key>` matches that key of the record's `payload`, as a JSON
  value.

Every `Decimal` field of every event record the runner reads must be a
`decimal.Decimal`, never a `float`, whether the step names it or not.

`within <lo>..<hi> of <label>` may end an `expect` or `recv-` step. The time
of the matched record (`at` on a status record, `received_at` on an event
record), or the time the `recv-` step's frame arrived, minus the time of the
labelled step, must lie between `lo` − 0.05 and `hi` + 0.25 seconds.

A step's time is when it took effect:

- for `send`, `send-text`, `send-binary`, `send-again`, `close`, and
  `release-pongs`, when the server sent the frame, before any handshake
  that follows;
- for `drop`, when the server ended the TCP connection;
- for `refuse`, when it sent the last of its HTTP 503 responses;
- for `accept`, `recv-subscribe`, `recv-ping`, and `expect-client-close`,
  when what the step waited for arrived;
- for `pong`, `silent`, `trade`, `idle-close`, and the runner steps, when
  the step returned;
- for a labelled `expect` step, its record's time.

### Frame notation

`send` writes frames as below. Every frame is compact JSON (no spaces),
with members in the order shown, which is the order of the captured frames
as the excerpts keep them.
Prices and sizes are strings as written; timestamps are decimal strings of
milliseconds.

| Notation | Frame |
| --- | --- |
| `opening` | The text `[]` followed by a newline. A subscription to settled tokens returned `[]` ([§6]); the newline is as the excerpts keep it. |
| `opening <item> ...` | A JSON array with one opening book per item. An item is a token name, for its current reference book, or `(book <T> t=<offset> bids=<levels> asks=<levels>)`, which first replaces the reference book. |
| `book <T>` | One book object from the current reference book. With `t=`, `bids=`, and `asks=`, it first replaces the reference book. |
| `pc <M> t=<offset> <entry> ...` | One `price_change` object. Each entry is `<T>:<BUY or SELL>:<price>:<size>`. |
| `bba <T> t=<offset>` | One `best_bid_ask` object from the reference book. |
| `ltp <T> t=<offset> price=<p> size=<s> side=<BUY or SELL>` | One `last_trade_price` object. |
| `tsc <T> t=<offset> new=<tick>` | One `tick_size_change` object. |
| `resolved <M> t=<offset> winner=<T>` | One `market_resolved` object. |
| `new-market <n> t=<offset>` | One `new_market` object for synthetic new market `n`. |

`<levels>` is `<price>:<size>` pairs separated by commas, or `-` for none.
`hash=bad` after a `book` or `pc` replaces every hash in the frame with 40
zeros.

The objects, with `<…>` filled from the notation and the reference:

| Event | Members, in order |
| --- | --- |
| opening book | `market`, `asset_id`, `timestamp`, `hash`, `bids`, `asks`, `tick_size`, `event_type` `"book"`, `last_trade_price` |
| later book | `market`, `asset_id`, `bids`, `asks`, `hash`, `timestamp`, `event_type` `"book"` |
| `price_change` | `market`, `price_changes` (each: `asset_id`, `price`, `size`, `side`, `hash`, `best_bid`, `best_ask`), `timestamp`, `event_type` |
| `best_bid_ask` | `market`, `asset_id`, `best_bid`, `best_ask`, `spread`, `timestamp`, `event_type` |
| `last_trade_price` | `market`, `asset_id`, `price`, `size`, `fee_rate_bps` `"0"`, `side`, `timestamp`, `event_type`, `transaction_hash` |
| `tick_size_change` | `market`, `asset_id`, `old_tick_size`, `new_tick_size`, `timestamp`, `event_type` |
| `market_resolved` | `id`, `market`, `assets_ids`, `winning_asset_id`, `winning_outcome`, `event_message` `null`, `timestamp`, `event_type`, `tags` `[]` |
| `new_market` | `id`, `question`, `market`, `slug`, `description`, `assets_ids`, `outcomes`, `event_message` `null`, `timestamp`, `event_type`, `tags` `[]`, `condition_id`, `active` `true`, `clob_token_ids`, `game_start_time`, `order_price_min_tick_size` `"0.01"` |

Filled in this way:

- A book's levels: bids in ascending price order and asks in descending, as
  in the hash input ([§4]) and the captured books. Its `timestamp` is the
  reference book's last change. An opening book's `tick_size` is the
  token's tick size and its `last_trade_price` is the market's trade price
  with three decimals.
- A `book`'s `hash`, and each `price_change` entry's, follow the
  [recipe](client.md#order-book-hash) over the reference book, with
  `min_order_size` `"5"`, `neg_risk` false, the token's tick size, and the
  market's trade price set by `trade` or the last `ltp`, written with three
  decimals, as the opening books carry it. A `price_change`
  applies its entries to the reference books in order; each entry then
  carries the hash, best bid, and best ask of its token's book after all of
  the message's entries for that token, with the message's timestamp, as
  the source does ([§4]). An empty side's best price is `"0"` for bids and
  `"1"` for asks ([§6]).
- `best_bid_ask` gives the reference book's best prices and their
  difference as `spread`.
- `ltp` sets the market's trade price. Its `transaction_hash` is `0x` and 64
  hexadecimal digits of the count of `ltp` frames sent so far in the
  scenario.
- `tsc` gives the token's tick size as `old_tick_size` and then sets it.
- `market_resolved`: `id` is `9000000` plus the count of `resolved` frames
  sent so far, `assets_ids` the market's two tokens, and `winning_outcome`
  the winner's outcome.
- `new_market` `n`: `id` `"<9100000 + n>"`; `question` `"Synthetic new
  market <n>?"`; `market` and `condition_id` `0x` and 64 hexadecimal digits
  of `0xf00 + n`; `slug` `"synthetic-new-<n>"`; `description`
  `"Synthetic."`; `assets_ids` and `clob_token_ids` two tokens, `2`
  followed by the 76-digit zero-padded decimal of `10n + 1` and of
  `10n + 2`; `outcomes` `["Yes","No"]`; `game_start_time`
  `"2026-10-11 14:00:00+00"`, a string as the source sends it ([§1]).

A reference book's last change is the latest `t` of the steps that changed
it. `opening`, `book`, `pc`, and `silent` change it; `send-again` does not.

### The `start` macro

Most scenarios begin with the same connection. `start <M> ...` stands for
these steps, where `<tokens>` is every token of the named markets, in order.
It is used only when those markets are the whole desired set and no
connection has been opened yet:

```text
expect conn connecting attempt=1
accept
recv-subscribe <tokens>
expect conn open connection=1
expect conn subscribed connection=1
expect token <T> synchronizing previous=none reason=subscribed   # each token
send opening <tokens>
expect book <T> opening=true connection=1 held_book_matched=none # then
expect token <T> ready previous=synchronizing reason=book        # each token
```

The last two lines alternate, token by token.

## Coverage

### The README's completion criteria

| Criterion | Scenarios |
| --- | --- |
| Initial states match independently prepared expectations | `initial-books`, `market-event-types`, `resolve-by-slug` |
| Recovered states match them | `drop-without-close`, `close-frames`, `close-slow-consumer`, `pong-withheld`, `reconnect-refused-then-accepted` |
| A dropped connection | `drop-without-close` |
| A close frame | `close-frames`, `close-slow-consumer` |
| A withheld `PONG` | `pong-withheld`; and a late one that is not a failure, `pong-late-within-timeout` |
| Subscription restoration | `drop-without-close`, `settled-with-active`, `settle-announced-others-open`, `unsubscribe` |
| Subscription changes | `subscribe-while-connected`, `subscribe-during-outage`, `unsubscribe`, `unsubscribe-all-then-subscribe`, `resubscribe-removed` |
| A market that settles | `settle-announced-others-open`, `settle-all-resolved-close`, `settle-all-resolved-close-unannounced`, `settle-unannounced-drop`, `settled-at-subscription`, `settled-with-active`, `settlement-unconfirmed`, `unknown-market`, `settle-lookup-after-late-book` |
| Unknown and malformed frames | `unknown-event-type`, `malformed-frames`, `invalid-known-event` |
| A consumer that stops reading | `consumer-stops-reading`, `frame-larger-than-queue`, `status-records-bounded`, `consumer-pause-outlasts-recovery-time` |
| When the client becomes uncertain, may report readiness again, and reports a market settled | the `within` windows in `drop-without-close`, `pong-withheld`, `settle-unannounced-drop`, `settled-at-subscription`, and `hash-divergence` |
| A bounded retry policy (README step 4) | `startup-retry`, `reconnect-refused-then-accepted`, `recovery-exhausted-attempts`, `recovery-exhausted-time`, `recovery-exhausted-no-frame` |
| Cancellation (README design choices) | `exit-while-connected`, `cancel-during-recovery`, `read-timeout` |

### The findings' catalog

| Behavior ([catalog]) | Scenarios |
| --- | --- |
| The SDK's stream reconnects and resubscribes without telling its consumer | every scenario with an interruption: each asserts the records an interruption produces |
| The SDK drops events its parser rejects | `new-market-delivered`, `invalid-known-event` |
| The SDK stops reconnecting after an error other than its own | `recovery-exhausted-attempts`, `recovery-exhausted-time`, `recovery-exhausted-no-frame` |
| Nothing is replayed after a reconnect | `drop-without-close`, `consumer-stops-reading` |
| A connection with no traffic is closed after about 125 s, without a close frame | `quiet-subscription-pings`, `drop-without-close` |
| The server ends slow consumers, with 1013 or no close frame; `PONG` waits behind data | `close-slow-consumer`, `pong-late-within-timeout`, `drop-without-close` |
| Messages arrive more than once | `repeated-messages` |
| A token's events arrive out of timestamp order across types | `out-of-order-types` |
| A change stamped before an opening `book` can arrive after it | `change-before-book` |
| Opening `book` timestamps are the book's last change | `initial-books` |
| A trade's price enters the hash before it is announced, and a tick-size change seemed to | `hash-trade-before-announcement`, `hash-single-failure` |
| The hash is undocumented; on busy markets its trade price does not follow trades | `hash-checks-pass`, `hash-trade-before-announcement` |
| The stream can omit a change | `hash-divergence`, `mid-connection-book` |
| The all-resolved close when every market has settled | `settle-all-resolved-close`, `settle-all-resolved-close-unannounced` |
| A settlement can go unannounced | `settle-unannounced-drop`, `settle-all-resolved-close-unannounced` |
| Settled tokens are silently left out of a subscription | `settled-at-subscription`, `settled-with-active`, `unknown-market` |
| `new_market` arrives in bulk for every new market | `new-market-filtered`, `new-market-delivered` |

## Scenarios

### Initial state and market events

#### `initial-books`

The first connection: the subscription frame lists the desired set in order,
tokens wait for their books, and each becomes ready on the stream's opening
`book`, which carries the book's last-change time, not the snapshot time
([§3], [§5]). Rests on the subscription frame of [How it ran].

```scenario
scenario initial-books
markets A B

expect conn connecting attempt=1 connection=none
accept
recv-subscribe A1 A2 B1 B2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing previous=none reason=subscribed connection=1
expect token A2 synchronizing previous=none reason=subscribed
expect token B1 synchronizing previous=none reason=subscribed
expect token B2 synchronizing previous=none reason=subscribed
expect-nothing 0.3
o: send opening A1 A2 B1 B2
expect book A1 opening=true connection=1 frame=1 index=0 t=-30000
  held_book_matched=none tick_size=0.01 last_trade_price=0.500
  within 0..0.3 of o
expect token A1 ready previous=synchronizing reason=book connection=1
expect book A2 opening=true frame=1 index=1
expect token A2 ready
expect book B1 opening=true frame=1 index=2 last_trade_price=0.400
expect token B1 ready
expect book B2 opening=true frame=1 index=3
expect token B2 ready
expect-nothing 1.0
expect-stats frames=1 connections=1
```

#### `resolve-by-slug`

Resolving a market through lookup, then subscribing it to a client that
started with an empty desired set, which needs no connection until then.

```scenario
scenario resolve-by-slug

expect-no-connect 0.5
expect-nothing 0.1
resolve synthetic-a
resolve synthetic-u raises MarketNotFound
subscribe A
start A
```

#### `market-event-types`

Each other event type becomes its record, with `Decimal` values and the
source's identities, and changes no state ([§3], [§4]).

```scenario
scenario market-event-types
markets A

start A
send pc A t=100 A1:BUY:0.49:50 A1:SELL:0.51:75
expect price_change A t=100 connection=1 frame=2 applied=true
  before_book=false repeat=false
send bba A1 t=100
expect best_bid_ask A1 t=100 best_bid=0.49 best_ask=0.51 spread=0.02
send ltp A1 t=150 price=0.51 size=10 side=BUY
expect last_trade_price A1 t=150 price=0.51 size=10 side=buy
  fee_rate_bps=0
send tsc A1 t=200 new=0.001
expect tick_size_change A1 t=200 old_tick_size=0.01 new_tick_size=0.001
expect-nothing 0.5
expect-stats repeats=0 undecodable=0 unknown=0 events.price_change=1
```

#### `mid-connection-book`

A `book` mid-connection, as the stream sends after trades, replaces the held
book and reports whether it matched; one that reveals a change the stream
omitted does not change the token's state when hashes are not verified
([§4], [§5]).

```scenario
scenario mid-connection-book
markets A

start A
send pc A t=100 A1:BUY:0.49:50 A2:SELL:0.51:50
expect price_change A t=100 applied=true
send book A1
expect book A1 opening=false t=100 held_book_matched=true
silent A1 t=200 BUY:0.46:500
send book A1
expect book A1 opening=false t=200 held_book_matched=false
send pc A t=300 A1:BUY:0.49:0
expect price_change A t=300 applied=true
send book A1
expect book A1 t=300 held_book_matched=true
expect-nothing 0.5
```

#### `out-of-order-types`

Events of different types arrive out of timestamp order; the client delivers
them in arrival order, keeps their timestamps, and still applies book
changes in arrival order ([§3]).

```scenario
scenario out-of-order-types
markets A

start A
send pc A t=200 A1:BUY:0.49:50
expect price_change A t=200 applied=true
send bba A1 t=150
expect best_bid_ask A1 t=150 best_bid=0.49
send ltp A1 t=120 price=0.52 size=5 side=BUY
expect last_trade_price A1 t=120
send pc A t=210 A1:BUY:0.49:60
expect price_change A t=210 applied=true
send book A1
expect book A1 t=210 held_book_matched=true
expect-nothing 0.5
```

#### `repeated-messages`

A message that arrives again, with its entries reordered, is delivered as a
repeat and not applied, so the later change at the same level stands. One
outside `repeat_window` is not a repeat ([§3]).

```scenario
scenario repeated-messages
markets A

start A
x: send pc A t=100 A1:BUY:0.49:50 A2:SELL:0.51:50
expect price_change A t=100 repeat=false applied=true
send pc A t=105 A1:BUY:0.49:70
expect price_change A t=105 repeat=false applied=true
send-again x reversed
expect price_change A t=100 repeat=true applied=false
b: send bba A1 t=110
expect best_bid_ask A1 t=110 repeat=false
send-again b
expect best_bid_ask A1 t=110 repeat=true
send book A1
expect book A1 t=105 held_book_matched=true
wait 1.2
send-again b
expect best_bid_ask A1 t=110 repeat=false
expect-nothing 0.3
expect-stats repeats=2
```

#### `change-before-book`

An entry stamped 1 ms before the token's opening book arrives after it. It
is applied in arrival order and flagged, and the token stays ready ([§3]).

```scenario
scenario change-before-book
markets A

start A
send pc A t=-30001 A1:BUY:0.48:100
expect price_change A t=-30001 applied=true before_book=true
send pc A t=50 A1:BUY:0.47:200
expect price_change A t=50 applied=true before_book=false
send book A1
expect book A1 t=50 held_book_matched=true
expect-nothing 0.5
```

#### `change-before-any-book`

An entry for a token that has no book on the connection yet is delivered and
not applied. The findings do not report one, and none appears in the
excerpts measured for the contract.

```scenario
scenario change-before-any-book
markets A

expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
send pc A t=-29000 A1:BUY:0.49:50
expect price_change A t=-29000 applied=false
send opening A1 A2
expect book A1 t=-29000 held_book_matched=none
expect token A1 ready
expect book A2 t=-30000
expect token A2 ready
expect-nothing 0.5
```

### Interruption and recovery

#### `drop-without-close`

The connection ends without a close frame, as idle connections, slow
consumers, and one settlement did ([§2], [§6]). The client marks the tokens
uncertain at once with the last confirmed activity, reconnects, restores the
subscription from the desired set, and replaces each book with the fresh
one. Nothing is replayed: a change during the gap shows only as a book that
differs from the held one ([§3]).

```scenario
scenario drop-without-close
markets A

start A
send pc A t=100 A1:BUY:0.49:50
expect price_change A t=100 applied=true
d: drop
expect conn interrupted reason=dropped connection=1 close_code=none
  last_confirmed_at=set within 0..0.5 of d
expect token A1 uncertain previous=ready reason=interrupted connection=1
  last_confirmed_at=set
expect token A2 uncertain previous=ready reason=interrupted
expect conn recovering attempt=1 retry_in=0.1 reason=backoff
silent A1 t=2000 BUY:0.46:500
expect conn connecting attempt=1 within 0.1..0.6 of d
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain reason=subscribed
  connection=2
expect token A2 synchronizing previous=uncertain reason=subscribed
o: send opening A1 A2
expect book A1 opening=true connection=2 frame=1 t=2000
  held_book_matched=false
expect gap A1 cause=dropped end=book connection_before=1
  connection_after=2 resumed=true held_book_matched=false
  last_confirmed_at=set detected_at=set discarded=none
expect token A1 ready previous=synchronizing reason=book connection=2
  within 0..0.3 of o
expect book A2 opening=true t=-30000 held_book_matched=true
expect gap A2 cause=dropped end=book resumed=true held_book_matched=true
expect token A2 ready
send pc A t=2100 A1:BUY:0.49:0
expect price_change A t=2100 connection=2 frame=2 applied=true
send book A1
expect book A1 t=2100 held_book_matched=true
expect-nothing 0.5
expect-stats connections=2 interruptions.dropped=1
```

#### `close-frames`

A close frame is an interruption whatever its code, including `1000` with a
reason other than the all-resolved one ([§6]). Attempt numbers start again
after a connection delivers its first frame (client decision 26).

```scenario
scenario close-frames
markets A

start A
c1: close 1001 "going away"
expect conn interrupted reason=close_frame connection=1 close_code=1001
  close_reason="going away" within 0..0.5 of c1
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 retry_in=0.1
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
send opening A1 A2
expect book A1 held_book_matched=true
expect gap A1 cause=close_frame close_code=1001 close_reason="going away"
  end=book connection_before=1 connection_after=2
expect token A1 ready
expect book A2
expect gap A2 cause=close_frame close_code=1001 end=book
expect token A2 ready
c2: close 1000 ""
expect conn interrupted reason=close_frame connection=2 close_code=1000
  close_reason="" within 0..0.5 of c2
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 retry_in=0.1
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=3
expect conn subscribed connection=3
expect token A1 synchronizing
expect token A2 synchronizing
send opening A1 A2
expect book A1
expect gap A1 cause=close_frame close_code=1000 connection_before=2
  connection_after=3
expect token A1 ready
expect book A2
expect gap A2 cause=close_frame close_code=1000
expect token A2 ready
expect-stats interruptions.close_frame=2
```

#### `close-slow-consumer`

`PONG` is delayed behind data and then the server ends the connection as a
slow consumer, as on busy markets ([§2]). The delay alone is not an
interruption; the `1013` close is.

```scenario
scenario close-slow-consumer
markets A

start A
pong hold
send pc A t=100 A1:BUY:0.49:10
send pc A t=110 A1:BUY:0.49:20
send pc A t=120 A1:BUY:0.49:30
wait 1.7
release-pongs
c: close 1013 "slow consumer: send buffer full"
pong auto
expect price_change A t=100
expect price_change A t=110
expect price_change A t=120
expect conn interrupted reason=close_frame connection=1 close_code=1013
  close_reason="slow consumer: send buffer full" within 0..0.5 of c
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 retry_in=0.1
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing
expect token A2 synchronizing
send opening A1 A2
expect book A1 held_book_matched=true
expect gap A1 cause=close_frame close_code=1013 end=book
expect token A1 ready
expect book A2
expect gap A2 cause=close_frame close_code=1013 end=book
expect token A2 ready
expect-stats pong_delay_max>=1.0
```

#### `startup-retry`

The first attempt starts at once; a refused one is retried after the
backoff, and no token has a state until it is subscribed.

```scenario
scenario startup-retry
markets A

expect conn connecting attempt=1
r: refuse 1
expect conn recovering attempt=2 retry_in=0.2 reason=backoff detail=set
  within 0..0.5 of r
expect conn connecting attempt=2 within 0.2..0.5 of r
accept
recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing previous=none
expect token A2 synchronizing previous=none
send opening A1 A2
expect book A1
expect token A1 ready
expect book A2
expect token A2 ready
```

#### `reconnect-refused-then-accepted`

Refused attempts back off exponentially, and the attempt that reaches the
bound still counts: with `max_attempts` 3, two failures leave a third
attempt.

```scenario
scenario reconnect-refused-then-accepted
markets A
pending D2

start A
d: drop
expect conn interrupted reason=dropped
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 retry_in=0.1
expect conn connecting attempt=1
r1: refuse 1
expect conn recovering attempt=2 retry_in=0.2 detail=set
expect conn connecting attempt=2 within 0.2..0.5 of r1
r2: refuse 1
expect conn recovering attempt=3 retry_in=0.4 detail=set
expect conn connecting attempt=3 within 0.4..0.7 of r2
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing
expect token A2 synchronizing
send opening A1 A2
expect book A1
expect gap A1 cause=dropped end=book connection_before=1
  connection_after=2
expect token A1 ready
expect book A2
expect gap A2 cause=dropped end=book
expect token A2 ready
```

#### `recovery-exhausted-attempts`

When `max_attempts` attempts in a row fail, the client reports the gaps that
never resumed, reports the failure, and raises it to the consumer. It
neither retries forever nor goes silent, as the SDK can ([§1]).

```scenario
scenario recovery-exhausted-attempts
markets A
pending D2

start A
drop
expect conn interrupted reason=dropped
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 retry_in=0.1
expect conn connecting attempt=1
refuse 1
expect conn recovering attempt=2 retry_in=0.2
expect conn connecting attempt=2
refuse 1
expect conn recovering attempt=3 retry_in=0.4
expect conn connecting attempt=3
f: refuse 1
expect gap A1 cause=dropped end=recovery_failed resumed=false
  connection_before=1 connection_after=none held_book_matched=none
expect gap A2 cause=dropped end=recovery_failed resumed=false
expect conn failed reason=max_attempts within 0..0.5 of f
expect-error RecoveryFailed
expect-end
expect-no-connect 1.0
```

#### `recovery-exhausted-time`

The time bound: before waiting for an attempt, the client fails if the wait
would end more than `max_recovery_time` after the interruption.
Attempts start 0.1, 0.3, 0.7, and 1.1 s after it; the next would start at
1.5 s, after the 1.3 s bound.

```scenario
scenario recovery-exhausted-time
markets A
config reconnect.max_attempts=100 reconnect.max_recovery_time=1.3
pending D2

start A
refuse-all
d: drop
expect conn interrupted reason=dropped
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 retry_in=0.1
expect conn connecting attempt=1
expect conn recovering attempt=2 retry_in=0.2
expect conn connecting attempt=2
expect conn recovering attempt=3 retry_in=0.4
expect conn connecting attempt=3
expect conn recovering attempt=4 retry_in=0.4
expect conn connecting attempt=4
expect gap A1 end=recovery_failed resumed=false
expect gap A2 end=recovery_failed resumed=false
expect conn failed reason=max_recovery_time within 1.0..1.4 of d
expect-error RecoveryFailed
expect-end
```

#### `recovery-exhausted-no-frame`

The server accepts each attempt and takes the subscription, then drops the
connection before sending anything. Each such connection is an
interruption, since its tokens were `synchronizing`, and a failed attempt:
attempt numbers follow on, and the third failure exhausts the bounds
(client decision 26).

```scenario
scenario recovery-exhausted-no-frame
markets A
pending D2

start A
drop
expect conn interrupted reason=dropped connection=1
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 retry_in=0.1
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
drop
expect conn interrupted reason=dropped connection=2
expect token A1 uncertain previous=synchronizing reason=interrupted
expect token A2 uncertain previous=synchronizing reason=interrupted
expect conn recovering attempt=2 retry_in=0.2
expect conn connecting attempt=2
accept
recv-subscribe A1 A2
expect conn open connection=3
expect conn subscribed connection=3
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
drop
expect conn interrupted reason=dropped connection=3
expect token A1 uncertain previous=synchronizing reason=interrupted
expect token A2 uncertain previous=synchronizing reason=interrupted
expect conn recovering attempt=3 retry_in=0.4
expect conn connecting attempt=3
accept
recv-subscribe A1 A2
expect conn open connection=4
expect conn subscribed connection=4
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
f: drop
expect conn interrupted reason=dropped connection=4
expect token A1 uncertain previous=synchronizing reason=interrupted
expect token A2 uncertain previous=synchronizing reason=interrupted
expect gap A1 cause=dropped end=recovery_failed resumed=false
  connection_before=1 connection_after=none
expect gap A2 cause=dropped end=recovery_failed resumed=false
expect conn failed reason=max_attempts within 0..0.5 of f
expect-error RecoveryFailed
expect-end
expect-no-connect 1.0
```

### Heartbeat

#### `pong-withheld`

The server stops answering `PING`. Once the oldest unanswered `PING` has
waited `pong_timeout`, the connection counts as interrupted and the client
closes it ([§2]; D1 sets the timeout). The first unanswered `PING` goes out
within `ping_interval` of the server's change, so the interruption comes 2.0
to 2.5 s after it.

```scenario
scenario pong-withheld
markets A

start A
o: pong off
expect conn interrupted reason=pong_timeout connection=1
  last_confirmed_at=set within 2.0..2.5 of o
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 retry_in=0.1
expect-client-close 1000 "pong timeout"
pong auto
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing
expect token A2 synchronizing
send opening A1 A2
expect book A1
expect gap A1 cause=pong_timeout end=book
expect token A1 ready
expect book A2
expect gap A2 cause=pong_timeout end=book
expect token A2 ready
expect-stats interruptions.pong_timeout=1
```

#### `pong-late-within-timeout`

`PONG` queued behind data arrives late but within `pong_timeout`: not an
interruption ([§2]).

```scenario
scenario pong-late-within-timeout
markets A

start A
pong hold
send pc A t=100 A1:BUY:0.49:10
send pc A t=110 A1:BUY:0.49:20
send pc A t=120 A1:BUY:0.49:30
wait 1.6
release-pongs
pong auto
expect price_change A t=100
expect price_change A t=110
expect price_change A t=120
expect-nothing 2.5
expect-stats pong_delay_max>=1.0 interruptions=0
```

#### `quiet-subscription-pings`

On a connection with no market data, the client keeps sending `PING`, so a
server that closes idle connections never closes it ([§2]).

```scenario
scenario quiet-subscription-pings
markets A

start A
idle-close 1.5
p1: recv-ping
p2: recv-ping within 0.4..0.6 of p1
recv-ping within 0.4..0.6 of p2
expect-nothing 3.0
expect-stats interruptions=0
```

### Settlement

#### `settle-announced-others-open`

A market settles while another stays open: its book is emptied,
`market_resolved` names the winner, and nothing more comes for it ([§6]).
The settled market leaves the desired set, a repeated announcement is
discarded, and a later reconnect does not resubscribe it.

```scenario
scenario settle-announced-others-open
markets A B

start A B
send pc A t=1000 A1:BUY:0.48:0 A1:BUY:0.47:0 A1:SELL:0.52:0
  A1:SELL:0.53:0 A2:BUY:0.48:0 A2:BUY:0.47:0 A2:SELL:0.52:0
  A2:SELL:0.53:0
expect price_change A t=1000 applied=true
expect-nothing 0.3
r: send resolved A t=1001 winner=A2
expect market_resolved A t=1001 winning_asset_id=A2 assets_ids=A1,A2
  winning_outcome=No
expect token A1 settled previous=ready reason=market_resolved
  winning_asset_id=A2 within 0..0.3 of r
expect token A2 settled previous=ready reason=market_resolved
  winning_asset_id=A2
send-again r
expect-nothing 0.5
expect-stats discarded_outside=1
send pc B t=1100 B1:BUY:0.39:90
expect price_change B t=1100 applied=true
drop
expect conn interrupted reason=dropped connection=1
expect token B1 uncertain reason=interrupted
expect token B2 uncertain reason=interrupted
expect conn recovering attempt=1
expect conn connecting attempt=1
accept
recv-subscribe B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token B1 synchronizing
expect token B2 synchronizing
send opening B1 B2
expect book B1
expect gap B1 cause=dropped end=book
expect token B1 ready
expect book B2
expect gap B2 cause=dropped end=book
expect token B2 ready
```

#### `settle-all-resolved-close`

The last market on the connection settles with an announcement, and the
server closes with `1000 all subscribed assets resolved` ([§6]). Nothing is
left to subscribe, so the client goes idle and does not reconnect.

```scenario
scenario settle-all-resolved-close
markets A

start A
send pc A t=1000 A1:BUY:0.48:0 A1:BUY:0.47:0 A1:SELL:0.52:0
  A1:SELL:0.53:0 A2:BUY:0.48:0 A2:BUY:0.47:0 A2:SELL:0.52:0
  A2:SELL:0.53:0
expect price_change A t=1000
send resolved A t=1001 winner=A1
close 1000 "all subscribed assets resolved"
expect market_resolved A winning_asset_id=A1
expect token A1 settled reason=market_resolved winning_asset_id=A1
expect token A2 settled reason=market_resolved winning_asset_id=A1
expect conn idle reason=no_subscriptions connection=1
expect-no-connect 1.0
expect-nothing 0.3
expect-stats interruptions=0
```

#### `settle-all-resolved-close-unannounced`

The all-resolved close arrives without `market_resolved`. It settles every
token on the connection, winner unknown, and is not an interruption. This
case was not observed: once, a connection ended at that moment with neither
the close frame nor the announcement, and whether the close can arrive
without the announcement is open ([§6]). The scenario guards against it;
the observed case is `settle-unannounced-drop`.

```scenario
scenario settle-all-resolved-close-unannounced
markets A

start A
send pc A t=1000 A1:BUY:0.48:0 A1:BUY:0.47:0 A1:SELL:0.52:0
  A1:SELL:0.53:0 A2:BUY:0.48:0 A2:BUY:0.47:0 A2:SELL:0.52:0
  A2:SELL:0.53:0
expect price_change A t=1000
c: close 1000 "all subscribed assets resolved"
expect conn ended connection=1 close_code=1000
  close_reason="all subscribed assets resolved" within 0..0.5 of c
expect token A1 settled previous=ready reason=all_resolved_close
  winning_asset_id=none
expect token A2 settled previous=ready reason=all_resolved_close
expect conn idle reason=no_subscriptions connection=1
expect-no-connect 1.0
expect-stats interruptions=0
```

#### `settle-unannounced-drop`

The settlement the stream did not announce: the book is emptied, the
connection drops without a close frame, and the reconnection gets `[]` and
nothing more ([§6]). The token gets no book; lookup at first still shows the
market open, as lookup lags the stream ([§6]); once it shows it closed, the
tokens settle and their gaps end without resuming.

```scenario
scenario settle-unannounced-drop
markets A

start A
send pc A t=1000 A1:BUY:0.48:0 A1:BUY:0.47:0 A1:SELL:0.52:0
  A1:SELL:0.53:0 A2:BUY:0.48:0 A2:BUY:0.47:0 A2:SELL:0.52:0
  A2:SELL:0.53:0
expect price_change A t=1000
drop
expect conn interrupted reason=dropped connection=1
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1 retry_in=0.1
expect conn connecting attempt=1
accept
s: recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
send opening
expect token A1 uncertain previous=synchronizing reason=no_book
  connection=2 within 0.9..1.3 of s
expect token A2 uncertain previous=synchronizing reason=no_book
expect-nothing 1.0
l: lookup A closed winner=A2
expect gap A1 cause=dropped end=settled resumed=false connection_before=1
  connection_after=none held_book_matched=none within 0..0.6 of l
expect token A1 settled previous=uncertain reason=lookup_closed
  winning_asset_id=A2
expect gap A2 cause=dropped end=settled resumed=false
expect token A2 settled previous=uncertain reason=lookup_closed
expect conn idle reason=no_subscriptions connection=2
expect-client-close 1000 "no subscriptions"
expect-no-connect 1.0
expect-stats lookups>=3
```

#### `settled-at-subscription`

Subscribing to a market that has already settled returns `[]`, no book, and
no error ([§6]). After `book_timeout` the client confirms the settlement
through lookup.

```scenario
scenario settled-at-subscription
markets S

expect conn connecting attempt=1
accept
s: recv-subscribe S1 S2
expect conn open connection=1
expect conn subscribed connection=1
expect token S1 synchronizing previous=none
expect token S2 synchronizing previous=none
send opening
n: expect token S1 uncertain previous=synchronizing reason=no_book
  within 0.9..1.3 of s
expect token S2 uncertain reason=no_book
expect token S1 settled previous=uncertain reason=lookup_closed
  winning_asset_id=S2 within 0..0.3 of n
expect token S2 settled reason=lookup_closed winning_asset_id=S2
expect conn idle reason=no_subscriptions connection=1
expect-client-close 1000 "no subscriptions"
expect-no-connect 1.0
```

#### `settled-with-active`

A settled market subscribed with an open one is left out of the opening
frame without an error ([§6]). The open market is unaffected; the settled
one is confirmed through lookup and is not resubscribed after a reconnect.

```scenario
scenario settled-with-active
markets A S

expect conn connecting attempt=1
accept
s: recv-subscribe A1 A2 S1 S2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
expect token S1 synchronizing
expect token S2 synchronizing
send opening A1 A2
expect book A1 frame=1 index=0
expect token A1 ready
expect book A2 frame=1 index=1
expect token A2 ready
expect token S1 uncertain reason=no_book within 0.9..1.3 of s
expect token S2 uncertain reason=no_book
expect token S1 settled reason=lookup_closed winning_asset_id=S2
expect token S2 settled reason=lookup_closed
expect-nothing 0.5
drop
expect conn interrupted reason=dropped connection=1
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing
expect token A2 synchronizing
send opening A1 A2
expect book A1
expect gap A1 cause=dropped end=book
expect token A1 ready
expect book A2
expect gap A2 cause=dropped end=book
expect token A2 ready
```

#### `unknown-market`

A token that gets no book and that lookup does not know stays uncertain,
reported as unknown, and lookup is not repeated. The findings say that an
unknown token would get no book, as a settled one does, but no run
subscribed one (Inferred, [§6]).

```scenario
scenario unknown-market
markets U

expect conn connecting attempt=1
accept
s: recv-subscribe U1 U2
expect conn open connection=1
expect conn subscribed connection=1
expect token U1 synchronizing
expect token U2 synchronizing
send opening
expect token U1 uncertain reason=no_book within 0.9..1.3 of s
expect token U2 uncertain reason=no_book
expect token U1 uncertain previous=uncertain reason=unknown_market
expect token U2 uncertain previous=uncertain reason=unknown_market
expect-nothing 1.0
expect-stats lookups=1
```

#### `settlement-unconfirmed`

A token gets no book while lookup first fails and then keeps showing the
market open. After `settlement_confirm_timeout` the client stops looking and
reports the settlement unconfirmed; it never settles a token on a missing
book alone.

```scenario
scenario settlement-unconfirmed
markets A
lookup A error

expect conn connecting attempt=1
accept
s: recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
send opening
n: expect token A1 uncertain reason=no_book within 0.9..1.3 of s
expect token A2 uncertain reason=no_book
wait 1.0
lookup A open
expect token A1 uncertain previous=uncertain
  reason=settlement_unconfirmed within 2.9..3.3 of n
expect token A2 uncertain previous=uncertain
  reason=settlement_unconfirmed
expect-nothing 1.0
expect-stats lookup_failures>=2
```

#### `settle-lookup-after-late-book`

One token's book arrives after `book_timeout`, while lookup is still
confirming the settlement; the other token's never does. Once lookup shows
the market closed, both tokens settle, the ready one included, since the
market leaves the desired set.

```scenario
scenario settle-lookup-after-late-book
markets A
pending D6

expect conn connecting attempt=1
accept
s: recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
send opening
expect token A1 uncertain previous=synchronizing reason=no_book
  within 0.9..1.3 of s
expect token A2 uncertain previous=synchronizing reason=no_book
send book A1
expect book A1 opening=false held_book_matched=none
expect token A1 ready previous=uncertain reason=book
expect-nothing 0.6
l: lookup A closed winner=A1
expect token A1 settled previous=ready reason=lookup_closed
  winning_asset_id=A1 within 0..0.6 of l
expect token A2 settled previous=uncertain reason=lookup_closed
  winning_asset_id=A1
expect conn idle reason=no_subscriptions connection=1
expect-client-close 1000 "no subscriptions"
expect-no-connect 1.0
```

### Subscription changes

These follow D7's recommended default: an addition reconnects with the new
desired set, and a removal takes effect without reconnecting.

#### `subscribe-while-connected`

```scenario
scenario subscribe-while-connected
markets A
pending D7

start A
add: subscribe B
expect conn interrupted reason=subscription_change connection=1
  within 0..0.5 of add
expect token A1 uncertain previous=ready reason=interrupted
expect token A2 uncertain previous=ready reason=interrupted
expect conn recovering attempt=1 reason=subscription_change retry_in=0
expect-client-close 1000 "subscription change"
expect conn connecting attempt=1
accept
recv-subscribe A1 A2 B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
expect token B1 synchronizing previous=none
expect token B2 synchronizing previous=none
send opening A1 A2 B1 B2
expect book A1 held_book_matched=true
expect gap A1 cause=subscription_change end=book held_book_matched=true
expect token A1 ready
expect book A2
expect gap A2 cause=subscription_change end=book
expect token A2 ready
expect book B1 held_book_matched=none
expect token B1 ready
expect book B2
expect token B2 ready
```

#### `subscribe-during-outage`

A market added while no connection is subscribed joins the next subscription
frame, without an extra reconnect.

```scenario
scenario subscribe-during-outage
markets A
pending D7

start A
drop
expect conn interrupted reason=dropped
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1
subscribe B
expect conn connecting attempt=1
accept
recv-subscribe A1 A2 B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
expect token B1 synchronizing previous=none
expect token B2 synchronizing previous=none
send opening A1 A2 B1 B2
expect book A1
expect gap A1 cause=dropped end=book
expect token A1 ready
expect book A2
expect gap A2 cause=dropped end=book
expect token A2 ready
expect book B1
expect token B1 ready
expect book B2
expect token B2 ready
expect-stats connections=2
```

#### `unsubscribe`

A removed market's tokens end at once; its events are discarded, the other
market is unaffected, and the next subscription frame leaves it out.

```scenario
scenario unsubscribe
markets A B
pending D7

start A B
u: unsubscribe A
expect token A1 removed previous=ready reason=removed within 0..0.3 of u
expect token A2 removed previous=ready reason=removed
send pc A t=100 A1:BUY:0.49:10
send pc B t=100 B1:BUY:0.39:90
expect price_change B t=100 applied=true
expect-nothing 0.3
expect-stats discarded_outside=1
drop
expect conn interrupted reason=dropped connection=1
expect token B1 uncertain reason=interrupted
expect token B2 uncertain reason=interrupted
expect conn recovering attempt=1
expect conn connecting attempt=1
accept
recv-subscribe B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token B1 synchronizing
expect token B2 synchronizing
send opening B1 B2
expect book B1
expect gap B1 cause=dropped end=book
expect token B1 ready
expect book B2
expect gap B2 cause=dropped end=book
expect token B2 ready
```

#### `unsubscribe-all-then-subscribe`

Removing every market closes the connection and leaves the client idle; the
desired set outlives the connection, and adding a market connects again.

```scenario
scenario unsubscribe-all-then-subscribe
markets A
pending D7

start A
unsubscribe A
expect token A1 removed reason=removed
expect token A2 removed reason=removed
expect conn idle reason=no_subscriptions connection=1
expect-client-close 1000 "no subscriptions"
expect-no-connect 0.5
subscribe B
expect conn connecting attempt=1
accept
recv-subscribe B1 B2
expect conn open connection=2
expect conn subscribed connection=2
expect token B1 synchronizing previous=none
expect token B2 synchronizing previous=none
send opening B1 B2
expect book B1
expect token B1 ready
expect book B2
expect token B2 ready
```

#### `resubscribe-removed`

A market removed and then added again starts over: it joins the end of the
desired set, its tokens take no part in the interruption the addition
causes, and they become `synchronizing` from `removed` and get a book with
nothing held to compare it with.

```scenario
scenario resubscribe-removed
markets A B
pending D7

start A B
unsubscribe A
expect token A1 removed previous=ready reason=removed
expect token A2 removed previous=ready reason=removed
add: subscribe A
expect conn interrupted reason=subscription_change connection=1
  within 0..0.5 of add
expect token B1 uncertain previous=ready reason=interrupted
expect token B2 uncertain previous=ready reason=interrupted
expect conn recovering attempt=1 reason=subscription_change retry_in=0
expect-client-close 1000 "subscription change"
expect conn connecting attempt=1
accept
recv-subscribe B1 B2 A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token B1 synchronizing previous=uncertain
expect token B2 synchronizing previous=uncertain
expect token A1 synchronizing previous=removed reason=subscribed
  connection=2
expect token A2 synchronizing previous=removed reason=subscribed
send opening B1 B2 A1 A2
expect book B1 held_book_matched=true
expect gap B1 cause=subscription_change end=book
expect token B1 ready
expect book B2 held_book_matched=true
expect gap B2 cause=subscription_change end=book
expect token B2 ready
expect book A1 opening=true held_book_matched=none
expect token A1 ready previous=synchronizing reason=book
expect book A2 held_book_matched=none
expect token A2 ready previous=synchronizing reason=book
expect-nothing 0.5
expect-stats connections=2
```

### Decoding

#### `unknown-event-type`

An object with an unknown or missing `event_type` is delivered as
`UnknownEvent` and changes nothing; the connection continues.

```scenario
scenario unknown-event-type
markets A

start A
send-text {"market":"${A}","asset_id":"${A1}","timestamp":"${t:100}",
  "event_type":"something_new"}
expect unknown event_type=something_new connection=1 frame=2 index=0
send-text {"market":"${A}","note":"no event type"}
expect unknown event_type=none
send pc A t=200 A1:BUY:0.49:10
expect price_change A t=200 applied=true
expect-nothing 0.5
expect-stats unknown=2 interruptions=0
```

#### `malformed-frames`

Frames that cannot be decoded are kept with their raw payload and reported.
One that cannot be attributed to a token puts every ready book on the
connection in doubt; a later book restores each. Nothing undecodable stops
the connection.

```scenario
scenario malformed-frames
markets A B

start A B
send-text not json
expect undecodable reason=invalid_json index=none raw="not json"
  affected=A1,A2,B1,B2
expect token A1 uncertain previous=ready reason=undecodable
expect token A2 uncertain previous=ready reason=undecodable
expect token B1 uncertain previous=ready reason=undecodable
expect token B2 uncertain previous=ready reason=undecodable
send book A1
expect book A1 held_book_matched=true
expect token A1 ready previous=uncertain reason=book
send-binary 00ff
expect undecodable reason=binary affected=A1
expect token A1 uncertain reason=undecodable
send-text 42
expect undecodable reason=not_object index=none affected=()
send-text [7]
expect undecodable reason=not_object index=0 affected=()
send pc A t=300 A1:BUY:0.49:10
expect price_change A t=300 applied=true
send book A1
expect book A1 t=300 held_book_matched=true
expect token A1 ready
send book A2
expect book A2
expect token A2 ready
send book B1
expect book B1
expect token B1 ready
send book B2
expect book B2
expect token B2 ready
expect-nothing 0.5
expect-stats undecodable.invalid_json=1 undecodable.binary=1
  undecodable.not_object=2 interruptions=0
```

#### `invalid-known-event`

An event of a known type with a field the client cannot use is undecodable.
It makes uncertain the tokens it names. If any part of it names no token
the client can read, it also makes uncertain its market's tokens, or, with
no readable market either, every token on the connection. Only the types
that change a book do this.

```scenario
scenario invalid-known-event
markets A B

start A B
send-text {"market":"${A}","asset_id":"${A1}","timestamp":"${t:100}",
  "hash":"0000000000000000000000000000000000000000",
  "bids":[{"price":"abc","size":"1"}],"asks":[],"event_type":"book"}
expect undecodable reason=invalid_event event_type=book index=none
  affected=A1
expect token A1 uncertain previous=ready reason=undecodable
send-text {"market":"${A}","price_changes":[{"asset_id":"${A2}",
  "price":"0.49","size":"NaN","side":"BUY",
  "hash":"0000000000000000000000000000000000000000"}],
  "timestamp":"${t:110}","event_type":"price_change"}
expect undecodable reason=invalid_event event_type=price_change
  affected=A2
expect token A2 uncertain previous=ready reason=undecodable
send-text {"market":"${B}","asset_id":"${B1}","best_bid":"x",
  "best_ask":"0.41","spread":"0.02","timestamp":"${t:120}",
  "event_type":"best_bid_ask"}
expect undecodable reason=invalid_event event_type=best_bid_ask
  affected=()
send-text {"market":"${B}","price_changes":[{"price":"0.39","size":"1",
  "side":"BUY","hash":"0000000000000000000000000000000000000000"}],
  "timestamp":"${t:130}","event_type":"price_change"}
expect undecodable reason=invalid_event event_type=price_change
  affected=B1,B2
expect token B1 uncertain previous=ready reason=undecodable
expect token B2 uncertain previous=ready reason=undecodable
send book A1
expect book A1 held_book_matched=true
expect token A1 ready previous=uncertain reason=book
send book A2
expect book A2 held_book_matched=true
expect token A2 ready previous=uncertain reason=book
send-text {"market":"${A}","price_changes":[{"asset_id":"${A1}",
  "price":"0.49","size":"10","side":"BUY",
  "hash":"0000000000000000000000000000000000000000"},
  {"price":"0.51","size":"10","side":"SELL",
  "hash":"0000000000000000000000000000000000000000"}],
  "timestamp":"${t:140}","event_type":"price_change"}
expect undecodable reason=invalid_event event_type=price_change
  affected=A1,A2
expect token A1 uncertain previous=ready reason=undecodable
expect token A2 uncertain previous=ready reason=undecodable
send book B1
expect book B1 held_book_matched=true
expect token B1 ready previous=uncertain reason=book
send-text {"price_changes":[{"price":"0.39","size":"1","side":"BUY",
  "hash":"0000000000000000000000000000000000000000"}],
  "timestamp":"${t:150}","event_type":"price_change"}
expect undecodable reason=invalid_event event_type=price_change
  affected=B1
expect token B1 uncertain previous=ready reason=undecodable
expect-nothing 0.5
expect-stats undecodable.invalid_event=6 interruptions=0
```

#### `new-market-filtered`

`new_market` events arrive for markets nobody subscribed, in bulk ([§6]). By
default they are decoded, counted, and not delivered. The string
`game_start_time` the SDK rejects ([§1]) is no obstacle.

```scenario
scenario new-market-filtered
markets A

start A
send new-market 1 t=100
send new-market 2 t=101
send new-market 3 t=102
send pc A t=110 A1:BUY:0.49:10
expect price_change A t=110
expect-nothing 0.5
expect-stats new_market_dropped=3 undecodable=0
```

#### `new-market-delivered`

With `new_market` set to `deliver`, each becomes a `NewMarketEvent`, with
`game_start_time` kept as the source sent it ([§1]).

```scenario
scenario new-market-delivered
markets A
config new_market=deliver

start A
send new-market 1 t=100
expect new_market id=9100001 slug=synthetic-new-1 t=100 connection=1
  payload.game_start_time="2026-10-11 14:00:00+00"
expect-nothing 0.5
expect-stats undecodable=0
```

### Consumer handoff

#### `consumer-stops-reading`

The consumer stops reading. The client reports the backlog, and at the limit
it closes the connection, waits for the consumer to drain the queue, and
reconnects; the overflowing change is part of the gap, so the fresh book
differs from the held one ([§2], [§3]). The queue holds 4 market records, a
`Backlog` record marks 3, so the two opening books stay below it, and the
client resumes at 1.

```scenario
scenario consumer-stops-reading
markets A
config queue_size=4 backlog_warning=0.75 resume_below=0.25
pending D3

start A
send pc A t=100 A1:BUY:0.49:10
send pc A t=101 A1:BUY:0.49:11
send pc A t=102 A1:BUY:0.49:12
send pc A t=103 A1:BUY:0.49:13
send pc A t=104 A1:BUY:0.49:14
expect-client-close 1000 "client backlog"
expect-backlog 4
expect price_change A t=100
expect price_change A t=101
expect price_change A t=102
expect backlog queued=3 limit=4 rising=true
expect price_change A t=103
expect conn interrupted reason=consumer_overflow connection=1
expect token A1 uncertain previous=ready reason=interrupted
expect token A2 uncertain previous=ready reason=interrupted
expect conn recovering attempt=1 reason=waiting_for_consumer
  retry_in=none
expect backlog queued=2 limit=4 rising=false
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
send opening A1 A2
expect book A1 t=104 held_book_matched=false
expect gap A1 cause=consumer_overflow end=book held_book_matched=false
expect token A1 ready
expect book A2 held_book_matched=true
expect gap A2 cause=consumer_overflow end=book held_book_matched=true
expect token A2 ready
expect-backlog 0
```

#### `frame-larger-than-queue`

The limit is checked once per frame, and a frame is never split. With a
queue of one record, the two-book opening frame is delivered whole, both at
the start and after an overflow, so the client can always make progress.

```scenario
scenario frame-larger-than-queue
markets A
config queue_size=1 backlog_warning=1 resume_below=0.5
pending D3

expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
send opening A1 A2
expect book A1 opening=true
expect token A1 ready
expect backlog queued=1 limit=1 rising=true
expect book A2 opening=true
expect token A2 ready
expect backlog queued=0 limit=1 rising=false
send pc A t=100 A1:BUY:0.49:10
send pc A t=101 A1:BUY:0.49:11
expect-client-close 1000 "client backlog"
expect price_change A t=100
expect backlog queued=1 limit=1 rising=true
expect conn interrupted reason=consumer_overflow connection=1
expect token A1 uncertain previous=ready reason=interrupted
expect token A2 uncertain previous=ready reason=interrupted
expect conn recovering attempt=1 reason=waiting_for_consumer
  retry_in=none
expect backlog queued=0 limit=1 rising=false
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
send opening A1 A2
expect book A1 t=101 held_book_matched=false
expect gap A1 cause=consumer_overflow end=book held_book_matched=false
expect token A1 ready
expect backlog queued=1 limit=1 rising=true
expect book A2 held_book_matched=true
expect gap A2 cause=consumer_overflow end=book held_book_matched=true
expect token A2 ready
expect backlog queued=0 limit=1 rising=false
expect-backlog 0
expect-stats interruptions.consumer_overflow=1
```

#### `status-records-bounded`

The consumer is not reading, and the server accepts connections, sends
`[]`, and drops them. Each cycle adds status records and no market-event
record, so the queue limit never applies. Once `queue_size` status records
are waiting, the client starts no attempt until the consumer reads.

```scenario
scenario status-records-bounded
markets A
config queue_size=10
pending D2

accept
recv-subscribe A1 A2
send opening
drop
accept
recv-subscribe A1 A2
send opening
drop
expect-no-connect 1.0
expect conn connecting attempt=1
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing previous=none
expect token A2 synchronizing previous=none
expect conn interrupted reason=dropped connection=1
expect token A1 uncertain previous=synchronizing reason=interrupted
expect token A2 uncertain previous=synchronizing reason=interrupted
expect conn recovering attempt=1 retry_in=0.1 reason=backoff
expect conn connecting attempt=1
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
expect conn interrupted reason=dropped connection=2
expect token A1 uncertain previous=synchronizing reason=interrupted
expect token A2 uncertain previous=synchronizing reason=interrupted
expect conn recovering attempt=1 reason=waiting_for_consumer
  retry_in=none
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=3
expect conn subscribed connection=3
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
send opening A1 A2
expect book A1 held_book_matched=none
expect token A1 ready previous=synchronizing reason=book
expect book A2 held_book_matched=none
expect token A2 ready
expect-stats connections=3 interruptions.dropped=2
```

#### `consumer-pause-outlasts-recovery-time`

The consumer stops reading for longer than `max_recovery_time`. Time spent
waiting for the consumer does not count toward that bound, so once the
backlog drains the client reconnects instead of failing.

```scenario
scenario consumer-pause-outlasts-recovery-time
markets A
config queue_size=4 backlog_warning=0.75 resume_below=0.25
  reconnect.max_recovery_time=0.5
pending D2 D3

start A
send pc A t=100 A1:BUY:0.49:10
send pc A t=101 A1:BUY:0.49:11
send pc A t=102 A1:BUY:0.49:12
send pc A t=103 A1:BUY:0.49:13
send pc A t=104 A1:BUY:0.49:14
expect-client-close 1000 "client backlog"
wait 1.0
expect price_change A t=100
expect price_change A t=101
expect price_change A t=102
expect backlog queued=3 limit=4 rising=true
expect price_change A t=103
expect conn interrupted reason=consumer_overflow connection=1
expect token A1 uncertain previous=ready reason=interrupted
expect token A2 uncertain previous=ready reason=interrupted
expect conn recovering attempt=1 reason=waiting_for_consumer
  retry_in=none
expect backlog queued=2 limit=4 rising=false
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=2
expect conn subscribed connection=2
expect token A1 synchronizing previous=uncertain
expect token A2 synchronizing previous=uncertain
send opening A1 A2
expect book A1 t=104 held_book_matched=false
expect gap A1 cause=consumer_overflow end=book
expect token A1 ready
expect book A2 held_book_matched=true
expect gap A2 cause=consumer_overflow end=book
expect token A2 ready
expect-backlog 0
expect-stats interruptions.consumer_overflow=1
```

### Shutdown and cancellation

#### `exit-while-connected`

Leaving the block closes the connection, ends every task the client
started, and ends the iterator. The client cannot be used afterwards.

```scenario
scenario exit-while-connected
markets A

start A
exit
expect-client-close 1000 "client exit"
expect-end
expect-no-connect 0.5
subscribe B raises ClientStateError
```

#### `cancel-during-recovery`

Cancelling the task that holds the block during backoff stops reconnection
at once.

```scenario
scenario cancel-during-recovery
markets A

start A
refuse-all
drop
expect conn interrupted reason=dropped
expect token A1 uncertain reason=interrupted
expect token A2 uncertain reason=interrupted
expect conn recovering attempt=1
expect conn connecting attempt=1
expect conn recovering attempt=2
cancel
expect-end
expect-no-connect 1.0
```

#### `read-timeout`

A read that times out raises in the reader only. The client keeps running
and loses no record.

```scenario
scenario read-timeout
markets A

start A
read-timeout 0.3
send pc A t=100 A1:BUY:0.49:10
expect price_change A t=100 applied=true
expect-stats interruptions=0
```

### Hash verification

These follow D4's recommended default and run with `verify_hash` on.

#### `hash-checks-pass`

Checks of correct hashes verify silently, once per burst ([§4]).

```scenario
scenario hash-checks-pass
markets A
config verify_hash=true
pending D4

start A
send pc A t=100 A1:BUY:0.49:50 A2:SELL:0.51:50
expect price_change A t=100 applied=true
send pc A t=110 A1:BUY:0.49:60 A1:BUY:0.47:0
expect price_change A t=110 applied=true
send book A1
expect book A1 held_book_matched=true
expect-nothing 0.5
expect-stats hash_verified>=3 hash_failed=0
```

#### `hash-single-failure`

A single failed check followed by one that verifies is not a divergence,
as the source's own hashes are sometimes briefly ahead ([§4]).

```scenario
scenario hash-single-failure
markets A
config verify_hash=true
pending D4

start A
send pc A t=100 A1:BUY:0.49:50 hash=bad
expect price_change A t=100 applied=true
send pc A t=110 A1:BUY:0.49:60
expect price_change A t=110 applied=true
expect-nothing 1.0
expect-stats hash_failed=1
```

#### `hash-divergence`

The stream omits a change, as it did once in the probe ([§4]). Checks fail
from then on; once they have failed across `hash_grace` and at least two
checks, the token becomes uncertain. Changes are still applied, and the next
book restores it.

```scenario
scenario hash-divergence
markets A
config verify_hash=true hash_grace=0.5
pending D4

start A
silent A1 t=50 BUY:0.45:30
send pc A t=100 A1:BUY:0.49:50
expect price_change A t=100 applied=true
wait 0.3
send pc A t=200 A1:BUY:0.49:60
expect price_change A t=200 applied=true
wait 0.4
g: send pc A t=300 A1:BUY:0.49:70
expect price_change A t=300 applied=true
expect token A1 uncertain previous=ready reason=hash_mismatch
  within 0..0.3 of g
send pc A t=400 A1:BUY:0.49:80
expect price_change A t=400 applied=true
send book A1
expect book A1 t=400 held_book_matched=false
expect token A1 ready previous=uncertain reason=book
send pc A t=500 A1:BUY:0.49:90
expect price_change A t=500 applied=true
expect-nothing 1.0
```

#### `hash-trade-before-announcement`

A trade's price enters the hash before `last_trade_price` announces it
([§4]). The client finds it by search, so the checks verify after a retry
and nothing fails. Once the trade is announced, checks verify at once.

```scenario
scenario hash-trade-before-announcement
markets A
config verify_hash=true
pending D4

start A
trade A 0.530
send pc A t=100 A1:BUY:0.49:50
expect price_change A t=100 applied=true
send pc A t=110 A1:BUY:0.49:55
expect price_change A t=110 applied=true
send ltp A1 t=120 price=0.53 size=5 side=BUY
expect last_trade_price A1 t=120 price=0.53
send pc A t=130 A1:BUY:0.49:60
expect price_change A t=130 applied=true
expect-nothing 1.0
expect-stats hash_failed=0 hash_retried>=1
```

## Failure reports

A runner reports, for each failing scenario, its name, the failing step's
line in this file, and the expected and actual values at the first
difference, or the step that timed out. For a failed `expect`, it also prints
the record it read.

[§1]: ../docs/source-behavior.md#1-reconnection-and-subscription-restoration-in-the-sdk
[§2]: ../docs/source-behavior.md#2-heartbeat
[§3]: ../docs/source-behavior.md#3-event-order-and-replay
[§4]: ../docs/source-behavior.md#4-revealing-a-missed-event
[§5]: ../docs/source-behavior.md#5-joining-a-snapshot-to-the-stream
[§6]: ../docs/source-behavior.md#6-settlement
[catalog]: ../docs/source-behavior.md#behavior-the-documentation-and-sdk-do-not-state
[How it ran]: ../docs/source-behavior.md#how-the-investigation-ran
