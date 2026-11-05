# Live run

The limited live run that
[plan step 12](implementation-plan.md#12-record-a-limited-live-run) calls
for: the research example, run against Polymarket's market WebSocket for 60
minutes on November 2, 2026, with the client's default configuration. This
document records what the client's records and statistics show for the
markets, period, and versions below, and nothing beyond them. What the run
could not answer is under
[Source behavior left unresolved](#source-behavior-left-unresolved), and
what it covers under [Source coverage limits](#source-coverage-limits).

## How it ran

- **Date and period:** November 2, 2026. Times are UTC. The example started
  at 21:21:48, chose and resolved its markets, and subscribed at
  21:21:56.450. Its 60 minutes ended the run at 22:21:56. The last record
  arrived at 22:21:52.463.
- **Command:** `uv run --extra sdk python examples/research.py --duration
  3600 --output <timeline>`, with the example's other options at their
  defaults.
- **Client:** `polymarket-market-data-client` 0.0.0.dev0, at commit
  `3af1b8592496e12a0f0e41a1dd1cd29f4e51a2ad`, installed from the checkout
  with `uv sync --extra sdk`.
- **Versions:** CPython 3.12.13 managed by uv 0.12.23, on macOS 26.6.2
  (arm64); `websockets` 15.0.1 and `pydantic` 2.13.5; and, from the sdk
  extra, `polymarket-client` 0.12.0 with `httpx` 0.28.1, as `uv.lock` pins
  them at that commit. These are the versions the investigation recorded
  ([findings, Versions](source-behavior.md#versions)).
- **Endpoint and lookup:** the default `url`,
  `wss://ws-subscriptions-clob.polymarket.com/ws/market`, and the default
  lookup, which the sdk extra provides (D6). The example listed open
  markets with the SDK's `list_markets`, as
  [plan step 11](implementation-plan.md#11-add-the-research-example-and-check-the-built-wheel)
  describes.
- **Configuration:** `ClientConfig()`, every field at its default
  ([Configuration](../spec/client.md#configuration)), as the timeline's
  first line records. Among them: `PING` every 10 s with a `pong_timeout` of
  20 s, a `queue_size` of 10,000, hash verification on, `new_market` events
  dropped, and no raw frames kept.
- **Raw output:** the timeline, 211,451 lines and 181,180,089 bytes,
  SHA-256 `e8dc2c089919c679bf2716446dd8b1c01acea50e7eb279b9bd015c064d56d741`.
  It is Polymarket's data, so, like the investigation's captures, it stays
  on the machine that made it and is not in git. The example wrote nothing
  to standard error, and the client logs nothing.
- **How the figures were taken:** from the timeline's records and its last
  line, the client's [statistics](../spec/client.md#statistics) at the end.
  Where a figure needs the books, they were rebuilt from the timeline's
  `book` events and applied `price_change` entries, as the
  [recovery contract](../spec/client.md#recovery-contract) describes. No
  figure here quotes a price, a size, or a hash.

## Markets

The example chose these when it ran, by the rule the owner decided on
2026-11-01: the two open markets ending at least 30 days out with the most
24-hour volume, and the two `btc-updown-5m-` markets ending soonest.

| Chosen as | Slug | Condition ID | Token IDs |
| --- | --- | --- | --- |
| most 24-hour volume | `will-nikki-haley-win-the-2028-us-presidential-election` | `0xe0173375b7eaccb836f7b92d28a9a2d1cc69e54e5a65fae89a5d7e49aadd332e` | `14253956839582698877884742930225331487593177635330318923853046874994118249367`, `84386913213192310444216977895083273572419886997719576824320630372812149173320` |
| most 24-hour volume | `will-tim-scott-win-the-2028-us-presidential-election` | `0xe4caa69a6fcc3ec5656a32d2e822e8cbdc6416178eaeb87e2e2006391c62e9ec` | `24908228760416922213612434617001183346078469158297998009944252494961344688838`, `11859092901364772208765529642173057538268245606218298881278208061029457183888` |
| ending soonest | `btc-updown-5m-1793654400` | `0x80f06c6ad6d8d14c666b38600efc0397686c3fa036f1b8b266df48f540f8cc32` | `42289448598343475128507427612176703025170126859064645469448219466984924620263`, `63083213416555930373978216337815618461378757150156922474371125069859905748500` |
| ending soonest | `btc-updown-5m-1793654700` | `0x6158f88929d34beab6d168e7f45766eb79b65fd601e256d0c037c8c1f69fab64` | `52647215793584650173079895301958323085623270869222983058899864647455643773250`, `69595738768202793987275780408638445905063995022706578468779616645452377435454` |

A `btc-updown-5m-` slug ended in the Unix time its five-minute window began
for the markets in the findings ([findings, Markets](source-behavior.md#markets)).
Read the same way, these two windows ended at 21:25:00 and 21:30:00. The
lookup returns no end date, so these times come from the slugs alone.

The Bitcoin markets carried 209,668 of the run's 211,426 event records, and
the election markets 1,758:

| Market | `book` | `price_change` | `best_bid_ask` | `last_trade_price` | `tick_size_change` | `market_resolved` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `will-nikki-haley-…` | 4 | 1,092 | 2 | 1 | 0 | 0 |
| `will-tim-scott-…` | 2 | 657 | 0 | 0 | 0 | 0 |
| `btc-updown-5m-1793654400` | 1,750 | 75,644 | 2,094 | 874 | 4 | 1 |
| `btc-updown-5m-1793654700` | 1,946 | 123,818 | 2,562 | 970 | 4 | 1 |

## What the records show

### The connection

One connection, generation 1, carried the whole run:

| Record | `at` |
| --- | --- |
| `ConnectionStateChange`, `connecting`, attempt 1 | 21:21:55.787 |
| `ConnectionStateChange`, `open`, connection 1 | 21:21:56.450 |
| `ConnectionStateChange`, `subscribed`, connection 1, then a `TokenStateChange` to `synchronizing` (`subscribed`) for each of the eight tokens | 21:21:56.450 |

All eight opening books came in the connection's first frame, received at
21:21:56.688, 0.24 s after the subscription frame, and each token became
`ready` (`book`).

No interruption occurred. The timeline has no `interrupted`, `recovering`,
or `CaptureGap` record, and the statistics count 1 connection, no
interruption, and no frame after one. The client sent `PING` every 10 s and
received 359 `PONG`s, none unsolicited. The slowest came 0.860 s after its
`PING`, and the last 0.125 s after; the statistics keep no median. No
`Backlog` record was emitted, so the records waiting for the consumer never
reached the warning level, half of `queue_size`.

The connection was still open when the 60 minutes ended. The election
markets were still subscribed and unsettled, so the client did not report
`idle`, and the server did not close the connection. The example then stopped reading records and
wrote the statistics, taken before the client closed the connection on
leaving its `async with` block.

### Settlements

Both Bitcoin markets settled during the run, and the stream announced each.
Times are source timestamps:

| Market | Window ended | Stamped after the end | Book emptied | `market_resolved` | The connection |
| --- | --- | --- | --- | --- | --- |
| `btc-updown-5m-1793654400` | 21:25:00 | 555 `price_change` events; 5 trades, the last at 21:26:08.400; a `tick_size_change` for each token at 21:26:00.340 | 21:27:21.807 | 21:27:21.837, 142 s after the end; received 21:27:21.866 | stayed open |
| `btc-updown-5m-1793654700` | 21:30:00 | 565 `price_change` events; 8 trades, the last at 21:31:10.787; a `tick_size_change` for each token at 21:30:05.747 | 21:32:31.972 | 21:32:31.996, 152 s after the end; received 21:32:32.210 | stayed open |

"Book emptied" is the `price_change` event that removed the last levels of
both tokens' books. It arrived 20 ms before `market_resolved` for the first
market and 0.5 ms before it for the second. Each `tick_size_change` arrived
twice, and the client flagged the second as a repeat.

Each settlement's records, in order, at timeline lines 182,387 to 182,389
and 209,985 to 209,987:

1. `MarketResolvedEvent`, naming both tokens and the winning token:
   `63083213…` for the first market, `69595738…` for the second.
2. For each token, a `TokenStateChange` to `settled` (`market_resolved`),
   from `ready`, with the winning token (T15). No gap was open, so no
   `CaptureGap` came with it.

Nothing more arrived for either market after its `market_resolved`: no
later event names it, and the statistics count no event discarded as
outside the desired set. This is the pattern the findings describe: the
book emptied, `market_resolved` named the winner, and nothing followed
([findings, §6](source-behavior.md#6-settlement)). The findings' seven
settlements announced theirs 119 to 148 s after the end date; here it took
142 and 152 s. The client relies on no such timing
([Settlement](../spec/client.md#settlement)).

The two other settlement signals did not occur. No token went without its
`book`, so no settlement confirmation through lookup ran, and the server
never closed the connection, so there was no all-resolved close.

### Books and hash checks

Of the 3,702 `book` events, 8 opened the connection and 3,694 came during
it. 3,685 of those matched the book the client held, level for level. 9
did not, all on the Bitcoin markets: 2 on `btc-updown-5m-1793654400`, one
per token, received at 21:23:31.123; and 7 on `btc-updown-5m-1793654700`,
received between 21:27:07.356 and 21:27:37.036. Each replaced the held book
(T5). The records do not show why the held books differed.

The client made 390,248 hash checks (D4): 108,440 verified at once, 280,796
verified after a retry, and 1,012 failed. No token became `uncertain`
(`hash_mismatch`), so no run of failures met D4's rule for divergence: at
least two failed checks, none verifying between them, at least `hash_grace`
(2 s) apart. Most checks counted in `hash_retried`, as the owner's
2026-10-28 decision under
[D4](../spec/client.md#d4-hash-verification) expected on busy markets.

The statistics do not split the checks by token, market, or time, and the
client logs nothing. So the run cannot say which tokens the failures fell
on, whether they came in episodes, or whether any was a `book` failing its
own check, which under D4 stops checks of that token until a later `book`
verifies. The D4 evidence counted 52 failures in 444,131 checks, about
0.012%, over 45 minutes on busy markets. Here 0.26% failed. The records
cannot say whether the difference lies in the markets, the period, the
source, or the client's checks.

The 201,211 `price_change` events held 402,422 entries. Every entry was
applied but the 398 in the 199 events flagged as repeats, and none was
flagged `before_book`.

### The failed hash checks, replayed November 5, 2026

[Plan step 13](implementation-plan.md#13-investigate-the-live-runs-failed-hash-checks)
replayed the run's timeline, the one whose SHA-256 is given above, with
[`tools/replay_hash_checks.py`](../tools/replay_hash_checks.py), to find
which checks failed and what explains them. This section adds to the one
before it, which stays as it stands. Like the rest of the document, it gives
counts, times, and IDs only.

**How the replay ran.** The script fed the timeline's events, in order, to
the client's own `Verifier`, with each record's `received_at` in place of
the event loop's clock. It applied each entry the records say the client
applied, skipped the events the client flagged as repeats, and checked a
burst whose `burst_quiet` had passed before the next frame was read. The
package's code is unchanged since the run's commit, `3af1b85`, so the
verifier is the one the run used. The timeline does not hold
`min_order_size` and `neg_risk`, which the client fetched through lookup,
so the script inferred both for each of the eight tokens from its opening
book's hash. It ran under CPython 3.12.13 and uv 0.12.23, in about two and
a half minutes.

**The counts reproduce.** The replay counted 108,440 checks verified at
once, 280,796 verified after a retry, and 1,012 failed, as the client did,
with no difference in any. Its clock and its inferred inputs changed no
check's outcome. Four of its checks were of bursts still pending when the
timeline ends, which the client checked in the seconds it ran after the
last record.

**Where they fell.** Every failed check was on a Bitcoin market: 937 on
`btc-updown-5m-1793654700`, 470 for token `52647215…` and 467 for
`69595738…`, and 75 on `btc-updown-5m-1793654400`, 38 for `63083213…` and
37 for `42289448…`. They came between 21:22:01.024 and 21:30:05.772: 833
within their market's five-minute window, 175 before it, and 4 after it
ended. All but 5 checked a burst that the token's next entry ended; those 5
checked bursts that a book ended. No book failed its own check.

**Episodes.** The failed checks came in 56 episodes, each a token's failed
checks with no verifying check between them, as D4's rule for divergence
counts them. They began at 30 moments: 26 on both tokens of a market at
once, and 4 on one token. They held from 1 to 106 failed checks, and each
ended with a check that verified: 51 with a burst's check, 5 with a book's
own. The longest lasted 0.206 s from its first failed check to that check,
and the median 0.012 s. The nearest to D4's rule was 70 failed checks of
`52647215…` from 21:27:21.847, whose hashes arrived 0.204 s apart, against
`hash_grace`'s 2 s. None came nearer, so no token became `uncertain`, as the
records show.

**At the start of a second.** 537 of the failed checks, 53.1%, checked a
hash stamped in the first 100 ms of a second of the source's clock, against
41,121, 10.8%, of the 382,038 other burst checks of the same four tokens.
Books and trades came in the 2 s before a failed check less often than
before the others: a book before 81.0% of failed checks and 92.9% of the
others, a trade before 80.8% and 92.8%. A repeat came before 68.4% and
59.7%, and a `tick_size_change` before 0.4% and 0.1%.

**What explains them.** For each failed check, the script tried what the
timeline and the findings suggest, none of which the client tries, and put
the check down to the first of these that applies:

| Put down to | Failed checks |
| --- | ---: |
| Repeats the client did not apply | 380 |
| A hash ahead of its entries | 199 |
| A book stamped before the held book's last change | 132 |
| A book holding changes no entry had brought | 64 |
| The tick size before a change | 4 |
| Another event's timestamp | 4 |
| Nothing tried | 229 |

- **Repeats the client did not apply: 380.** The client flagged 199
  `price_change` events as repeats and applied none, as
  [Repeated messages](../spec/client.md#repeated-messages) says. Applying
  192 of them would have changed nothing. Each of the other 7, received at
  21:22:17.486, 21:22:39.829, 21:26:22.750, 21:26:43.851, 21:26:58.108,
  21:27:21.847, and 21:27:52.073, named both tokens of a Bitcoin market and
  arrived 0.2 to 1.1 ms after its first copy. Between the two copies came an
  event, stamped the same millisecond, that set the same level to another
  size. Five of the 7 began episodes, on both tokens, within 1 ms of their
  arrival: 380 failed checks in all, each of which verifies with the held
  book as it would have stood had the client applied the repeat. Replayed with the 7
  applied, 632 checks fail: those 380 pass, and the count for every other
  cause stays as it is. So the source's later hashes held the sizes the
  second copies set. The findings saw repeats as one message received twice
  ([findings, §3](source-behavior.md#3-event-order-and-replay)). Whether
  these second copies were that, or a second change with the same content,
  the timeline cannot show.
- **A hash ahead of its entries: 199.** Each verifies with the held book
  after the token's next 1 to 159 applied entries, with the checked hash's
  own timestamp. The last of those entries arrived at most 0.102 s after the
  check. 15 verify with the next entry alone, which the findings saw as one
  change late ([findings, §4](source-behavior.md#4-revealing-a-missed-event));
  the findings record no hash ahead by more than one change. All 199
  checked hashes stamped in the first 100 ms of a second.
- **A book stamped before the held book's last change: 132.** At
  21:27:07.355 and 21:27:07.357 a book arrived for each token of
  `btc-updown-5m-1793654700`, stamped 5 ms before the held book's last
  change, and replaced the held book, as rule 3 of the
  [recovery contract](../spec/client.md#recovery-contract) and T5 say. Each
  verified its own hash. Each differed from the held book at 2 levels, and
  an entry stamped after the book had last set both. Then 63 checks of each
  token failed, until entries set both levels again at 21:27:07.481; the
  next check, at 21:27:07.482, verified.
  At 21:27:37.008 the same happened to `52647215…`, with a book stamped 3 ms
  before the held book's last change and differing at 1 level: 6 checks
  failed until its next book, at 21:27:37.035. The recovery contract
  replaces the held book with any later book, and does not consider one
  stamped before changes already applied.
- **A book holding changes no entry had brought: 64.** At 21:23:31.122 and
  21:23:31.123 books for both tokens of `btc-updown-5m-1793654400` differed
  from the held book at 22 levels, and at 21:27:36.064 and 21:27:36.065
  books for both tokens of `btc-updown-5m-1793654700` at 3. Each was stamped
  at the held book's last change and verified its own hash. The failed
  checks began 34 and 41 ms before them, and their episodes ended with
  them. Entries next set every level that differed 0.53 s and 0.80 s after
  the books. Of the other 2 books that differed from the held
  book, each at 1 level, one is the book at 21:27:37.035 above, and the
  other, at 21:27:37.008 for `69595738…`, came with no failed check.
- **The tick size before a change: 4.** Bursts that arrived at 21:26:00.073
  for both tokens of `btc-updown-5m-1793654400`, and at 21:30:05.376 for both
  tokens of `btc-updown-5m-1793654700`, were still pending when the token's
  `tick_size_change` arrived, 0.290 s and 0.393 s later. The client checked
  them with the new tick size, and they verify with the old. The
  specification does not say which tick size checks a burst pending across
  a tick-size change. The client uses the tick size at the check, since the
  one change the findings observed seemed to enter the hash before its event
  ([findings, §4](source-behavior.md#4-revealing-a-missed-event)).
- **Another event's timestamp: 4.** Three bursts, checked at 21:23:26.610,
  21:24:01.049, and 21:26:48.890, spanned two events stamped 1 ms apart, and
  verify with the first event's timestamp. The client used the last's. The
  fourth, checked at 21:28:31.879, verifies with a timestamp 1 ms before its
  event's. D4 checks a burst with "the event's timestamp", and does not say
  which event's when a burst spans two.
- **Nothing tried: 229.** These made up the episodes at 21:23:51.019,
  21:26:01.031, and 21:27:30.032, each on both tokens of a market, and parts
  of those at 21:24:00.038, 21:24:21.031, and 21:26:06.023. Every one
  checked a hash stamped in the first 100 ms of a second. None verifies with
  the held book after any of the token's next entries within `burst_quiet`,
  with a skipped repeat applied, with the tick size before a change, or with
  the timestamp of another event or 1 ms off. Each of their episodes ended
  with a burst's check that verified, with no book replacing the held book.

**What the replay shows about the client.** The replay found no place where
the client's code departs from the specification. On its own, a replay with
the client's verifier could not show a fault in that verifier, but the
causes above were tested with the recipe alone. The client's held book
matched 3,685 of the 3,694 books sent during the connection, and the 9 that
differed are those above. Each failed check the replay explains comes from
the source's hashes or books holding changes whose entries came later, or
from a rule the specification sets, applied as written: a repeat is not
applied; a later book replaces the held one; and a burst is checked with the
book as it stood before the entry that ended it (D4), which fails a hash
that already holds later changes. For the tick size and the timestamp, the
specification does not say, and the client's choices are those above. The
step reported these to the owner, and changed nothing in the client or the
specification.

**Against the D4 evidence.** D4's evidence, 52 failures in 444,131 checks on
busy markets, came from the investigation's replay
([findings, §4](source-behavior.md#4-revealing-a-missed-event)). That
replay applied every entry, repeats included, and counted as verified a
check that verified after the token's next change. Counted more nearly that
way, with the 7 repeats applied and the 15 checks one change late verified, 617 of
this run's 390,262 checks fail: 0.16%, against 0.012% then. The timeline
cannot say whether that difference lies in the markets or the period.

**What would settle the rest.** The timeline holds one connection's records,
not its frames. Two connections recorded over the same period, as runs
hb-none and hb-stop were
([findings, §4](source-behavior.md#4-revealing-a-missed-event)), would
show whether the failures at the start of a second, the repeats, and the
early books came at the same moments on both, and so from the source rather
than from what one connection received. Keeping each frame's text
(`keep_raw`) would show whether a repeat's two copies are the same text.
Both need another run against the live service, which is the owner's to
decide.

### Decoding and other events

All 212,077 frames decoded: there was no `UndecodableFrame` and no
`UnknownEvent`. 658 `new_market` events arrived, and the client dropped and
counted them, as `new_market` `drop` says. 203 events were flagged as
repeats: 199 `price_change` and 4 `tick_size_change`.

### Records and statistics

The timeline's 211,451 lines are the `run` line, 211,449 records, and the
`stats` line:

| Record | Count |
| --- | ---: |
| `ConnectionStateChange` | 3 |
| `TokenStateChange` | 20 |
| `BookEvent` | 3,702 |
| `PriceChangeEvent` | 201,211 |
| `BestBidAskEvent` | 4,658 |
| `LastTradePriceEvent` | 1,845 |
| `TickSizeChangeEvent` | 8 |
| `MarketResolvedEvent` | 2 |

The statistics at the end:

| Field | Value |
| --- | --- |
| `frames` | 212,077 |
| `frames_after_interruption` | 0 |
| `events` | `book` 3,702; `price_change` 201,211; `best_bid_ask` 4,658; `last_trade_price` 1,845; `new_market` 658; `tick_size_change` 8; `market_resolved` 2 |
| `repeats` | 203 |
| `unknown` | 0 |
| `undecodable` | none |
| `new_market_dropped` | 658 |
| `discarded_outside` | 0 |
| `pongs`, `pongs_unsolicited` | 359, 0 |
| `pong_delay_last`, `pong_delay_max` | 0.125 s, 0.860 s |
| `connections` | 1 |
| `interruptions` | none |
| `lookups`, `lookup_failures` | 12, 0 |
| `rest_book_not_found` | 0 |
| `hash_verified`, `hash_retried`, `hash_failed` | 108,440; 280,796; 1,012 |

Twelve lookups is one `market` call for each of the four markets and one
`book_parameters` call for each of the eight tokens. None failed, and every
token had a REST book, so the client had the hash inputs for all eight.

## Source behavior left unresolved

Every open question of the specification
([Open questions](../spec/client.md#open-questions)) and of the findings
([findings, Open questions](source-behavior.md#open-questions)) remains
open. One hour on four markets cannot settle any of them. This run bears on
some, within the limits below.

Seen in this run, and still open beyond it:

- **Settlement.** Both settlements were announced, each by one
  `market_resolved`, 142 and 152 s after the end date, the second outside
  the findings' 119 to 148 s. Whether `market_resolved` is ever sent late,
  more than once, or after a reconnect stays open, as does how markets
  resolved through a UMA proposal settle; no such market settled.
- **The hash recipe and the trade price in it.** Each of the eight tokens
  had its hash inputs, and no token diverged, but 1,012 checks failed and
  the run cannot say on which tokens or why. Whether every market uses the
  recipe stays open. 72% of checks verified only after a retry, on the
  announced trade price at the time of the check or a searched one.
- **Book events that differ from the held book.** 9 books sent during the
  connection differed from the book the client held. Whether the stream
  omitted changes there is not shown.
- **What the source sends.** All 212,077 frames other than `PONG` were JSON
  the decoder accepted. Whether the source ever sends other text, and
  whether the decoder's required fields hold for every event, stay open
  beyond this run.
- **The opening frame.** All eight opening books came in one frame, 0.24 s
  after subscribing. Whether an opening frame is ever split, or delayed
  beyond `book_timeout` under load, stays open.
- **A change before its token's opening book.** None came. Whether one can
  stays open.
- **Tick-size changes and the hash.** Each Bitcoin market's two tokens got
  one tick-size change after the market's end date. Whether a tick-size change enters
  the hash before it is announced is not shown: the statistics do not tie
  checks to events.
- **Limits on subscriptions.** Eight tokens on one connection were accepted.
  Whether the server limits the tokens in a subscription frame or the
  connections per client stays open.
- **Backlog and slow consumers.** No slow-consumer close came, and the
  slowest `PONG` took 0.860 s. How much backlog the server allows stays
  open.

Not exercised by this run:

- the 125-second close of an idle connection, and how long a connection
  without `PING` stays open: the client sent `PING` throughout;
- an undocumented replay or resume field: none was tried;
- how REST and the stream line up, REST rate limits, and a REST snapshot
  whose hash never appears in the stream: the client took no REST
  snapshot, and made only the eight `book_parameters` calls for hash
  inputs;
- whether market lookup's `closed` means settled for a market resolved
  through a UMA proposal: no such market settled, and no settlement needed
  lookup;
- whether the all-resolved close can lose `market_resolved`: the server
  never closed the connection;
- how the server handles a subscription change on an open connection (D7):
  the run made none;
- whether an unknown token gets no `book` and a REST 404: none was
  subscribed;
- whether the server penalizes quick reconnects (D2): there was no
  reconnect;
- how the SDK behaves with several subscriptions, subscription changes
  during a fault, TLS failures, and over longer periods: the client does
  not use the SDK's streams, and the SDK only listed markets and served
  lookup.

## Source coverage limits

- **Markets:** the four above. Two 2028 presidential-election markets, with
  1,758 events between them in the hour, and two five-minute Bitcoin
  markets, which carried 99% of the events and settled automatically. No
  market resolved through a UMA proposal settled.
- **Period:** one hour, 21:21:56 to 22:21:52 UTC on November 2, 2026, on one
  connection from one machine. The service has no version; this is its
  behavior then.
- **Versions:** the client at commit `3af1b85`, `polymarket-client` 0.12.0
  for listing and lookup, `websockets` 15.0.1, and CPython 3.12.13, as
  [How it ran](#how-it-ran) lists.
- **Recovery:** no interruption occurred, so the run shows nothing of the
  client's reconnection, capture gaps, or restored subscriptions against the
  live service. Those are checked only against the scripted server
  ([`spec/conformance.md`](../spec/conformance.md)).
- **Completeness:** the records cannot show that the source delivered every
  event. The stream has no sequence numbers, and the hash shows that a book
  diverged, not how many events were missed
  ([findings, §4](source-behavior.md#4-revealing-a-missed-event)).
