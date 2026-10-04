# Source behavior

Findings from the source investigation the [README](../README.md) calls for:
how Polymarket's market WebSocket and the official Python SDK behave in the
situations the client's recovery design depends on. Each finding applies only
to the SDK version, markets, and observation periods recorded with it. A
question the evidence cannot answer is left open, and the open questions are
collected at the end.

## How the investigation ran

- **Date checked:** October 4, 2026. Times below are UTC.
- **Versions:** `polymarket-client` 0.12.0 and `websockets` 15.0.1, on
  CPython 3.12.13. The full list, and how to check a later release without
  losing these results, is under [Versions](#versions).
- **Endpoints:** the market WebSocket at
  `wss://ws-subscriptions-clob.polymarket.com/ws/market`, which answered
  through Cloudflare (its handshake response carries `Server: cloudflare`),
  and REST order books from `https://clob.polymarket.com/book` through the
  SDK.
- **Subscription:** `{"type": "market", "assets_ids": [...],
  "custom_feature_enabled": true}` unless a run says otherwise. Clients sent
  the text frame `PING` every 10 seconds unless a run withheld it. Scripts
  that open their own socket turned WebSocket protocol pings off.
- **Scripts:** in [`spikes/`](../spikes), each declaring its own
  dependencies and locked to exact versions. Raw captures stayed on the
  machine that ran them and are not in git. Excerpts below are quoted from
  them.

### Versions

Every finding holds for these versions only. A finding about the SDK may be
fixed in a later release, and the service may change without any version to
mark it. So the versions are recorded here, and pinned in the scripts, so
that the original behavior stays reproducible after a fix.

| Component | Version checked |
| --- | --- |
| Official Python SDK | `polymarket-client` 0.12.0, wheel `polymarket_client-0.12.0-py3-none-any.whl`, sha256 `b563c6f487f86c1f7ac459feb29b438d48c1df3261d75975249c02618d03fb48`. Source paths cited below are in this wheel. The [SDK changelog](https://docs.polymarket.com/changelog/sdks) listed Python releases only through 0.11.0 when read. |
| SDK dependencies that shape the findings | `websockets` 15.0.1 (whose keepalive caught the stall), `pydantic` 2.13.5 and `pydantic-core` 2.46.5 (whose validation rejects the `new_market` events), `httpx` 0.28.1 |
| Every other package | locked per script in `spikes/<script>.py.lock` |
| Python and platform | CPython 3.12.13 managed by uv 0.12.23, on macOS 26.6.2 (arm64) |
| Market WebSocket and REST APIs | No version. Checked October 4, 2026, 14:26 to 16:08 UTC. Handshakes were answered by Cloudflare (`Server: cloudflare`, rays ending `-DFW`). |
| Documentation | Read October 4, 2026 as Markdown (page URL plus `.md`). SHA-256 of what was read: [real-time data](https://docs.polymarket.com/market-data/realtime-data) `8ad4bd28afd777ebb9229afa8bfd859c8c9e5db6448b6f0f6e750f72adcb0b51`, [prices and order books](https://docs.polymarket.com/market-data/prices-order-books) `e7b41e330500084c013dc31e59d363a25f66b51fc9f3124a14a4889680662b1b`, [Python SDK](https://docs.polymarket.com/getting-started/python) `c5860a04c695fb915a7a1b736cfeae723c4e4b3ccd982946d62aa4caf2efe32b`, [SDK changelog](https://docs.polymarket.com/changelog/sdks) `3aca66c34314921115818c7ef7ff0e30048624cd08e64107ec5d255974a05929`, [resolution](https://docs.polymarket.com/concepts/resolution) `698262d533e21d42ea6192c5d09d421d81646397a82ffe96e88c8b9cd77931ac`. "Not documented" below means not in these pages as read. |

The scripts run under the lockfiles, so `uv run` rebuilds this environment
exactly, including after newer releases. Each reproduction's capture opens
with an `environment` record of the versions, the time, and the repository
commit. Each connection's `open` record carries the server's handshake
headers, and each verdict prints both. To check whether a later SDK release
fixes an SDK finding, re-pin a copy of the script, lock it, and run it beside
the original. The 0.12.0 result stays reproducible from the original script.
For the service, a later NOT REPRODUCED verdict is dated evidence of a
change, compared with the dates above.

### Markets

The long-dated markets were chosen by running
[`select_markets.py`](../spikes/select_markets.py) for open election
markets ending at least 30 days out, ordered by 24-hour volume, and then
recording all eight for 90 seconds. The three with the most events whose end
dates are months away were kept for questions 1 to 5. The probe's other five
markets appear in the hash evidence for question 4.

Kept for questions 1 to 5:

| Market | Condition ID | Token IDs | End date |
| --- | --- | --- | --- |
| Will JD Vance win the 2028 US Presidential Election? (`will-jd-vance-win-the-2028-us-presidential-election`) | `0x7ad403c3508f8e3912940fd1a913f227591145ca0614074208e0b962d5fcc422` | Yes `16040015440196279900485035793550429453516625694844857319147506590755961451627`<br>No `94476829201604408463453426454480212459887267917122244941405244686637914508323` | 2028-11-08 04:59 |
| Will Kamala Harris win the 2028 Democratic presidential nomination? (`will-kamala-harris-win-the-2028-democratic-presidential-nomination-641`) | `0x909659c9436228e2be56d5582ba6188166f2f8bf3c596335512c8f2e380d01ec` | Yes `38171024903091354977195167437673496224420673639143973968032407064172828043588`<br>No `26930844951471612827925270830087046333229637930899178134435055616497220846773` | 2028-11-08 04:59 |
| Will David Lisnard win the 2027 French presidential election? (`will-david-lisnard-win-the-2027-french-presidential-election`) | `0xce610556ec78cfd7894f4169d91fe70e31c2fb49f927fffd7fe7d06530f88d56` | Yes `55404192439775628931124671719520886657525387168536821457706619003199189099864`<br>No `79358475953126059185096320578578995575874288599466094920037110232928386404141` | 2027-04-19 03:59 |

Recorded only in the 90-second probe:

| Market | Condition ID | Token IDs | End date |
| --- | --- | --- | --- |
| Will Chris Van Hollen win the 2028 Democratic presidential nomination? | `0xe29ca104ce56feb4c093c133b5dfc0c379e505373b4412cfec54fe2220f20b68` | Yes `82322606787689507186452368934268895797822450917805855938369533831277560432502`<br>No `89468624821153339755038928248216667629141563933414663720232714629209306424979` | 2028-11-08 04:59 |
| Will Chelsea Clinton win the 2028 Democratic presidential nomination? | `0xf2e51acfbb6d0414dc2ace81b7dc2af7c165e443dcb91f6caa7aab6d6ab4f06d` | Yes `46211769122513729244602558513001215391004042100934296321661285791281233261657`<br>No `76057920052421891902791411567177996435483774677774664174053982044923692373687` | 2028-11-08 04:59 |
| Will Mitch Landrieu win the 2028 Democratic presidential nomination? | `0xd0e58ada317f9778a221592cfed03405b235b137c8dd916817a39b72bf6dc19a` | Yes `4865811310965824253043301476456494687696227904538198271117692275943047535567`<br>No `79794608519930936057256245365460158683416637197135162121080424897667989834195` | 2028-11-08 04:59 |
| Will Hunter Biden win the 2028 Democratic presidential nomination? (`will-person-a-win-…`) | `0x1945a8b23e313ed7423b6b6fd556f9ab5578900376b565a61dc480a5f4f35d21` | Yes `11343337042526652606304508556838778144915122118685013460142819817079620240439`<br>No `19117858613830240204442128472263675815111662346578092643025820800380629933012` | 2028-11-08 04:59 |
| Will the Republican Party control the House after the 2026 Midterm elections? | `0x4e4f77e7dbf4cab666e9a1943674d7ae66348e862df03ea6f44b11eb95731928` | Yes `65139230827417363158752884968303867495725894165574887635816574090175320800482`<br>No `17371217118862125782438074585166210555214661810823929795910191856905580975576` | 2026-11-04 04:59 |

For question 6, two short-dated markets expected to settle during the
session, and one that had already settled. These "Bitcoin Up or Down" markets
resolve automatically a few minutes after they end.

| Market | Condition ID | Token IDs | End date |
| --- | --- | --- | --- |
| Bitcoin Up or Down, 10:30–10:35 AM ET (`btc-updown-5m-1791124200`) | `0x0c1b1a29cf49e127499e878853e4edbe79b929ccaaf45e96345a11fe0c263426` | Up `100888370987976300727976104132259754468885708358624157065950665701873247248130`<br>Down `105432187511272365632213611061207512483403511195339600044714295797509063777660` | 2026-10-04 14:35 |
| Bitcoin Up or Down, 10:30–10:45 AM ET (`btc-updown-15m-1791124200`) | `0xea7102ce594e37c69fa3db1fd77e54d720a92ead7884d2490590b9b0dc6b0552` | Up `17490127828538307323126492080101474970420767947931945995259916583244906351364`<br>Down `64843467217026795898050085742798878160351925394144219954781536995022973234439` | 2026-10-04 14:45 |
| Already settled: Bitcoin Up or Down, 10:15–10:20 AM ET (`btc-updown-5m-1791123300`) | `0x63733a58101a126a30c8d19700ca7bfff78788059c0ce72c442dff281d26d519` | Up `6012180248743313116112314719986762687955919882234931815345456773884607889280`<br>Down `59124705498088345406614874304734362081361971958233729558690850455967428254529` | 2026-10-04 14:20 |

### Runs

| Run | Script and options | Markets | Period (UTC) |
| --- | --- | --- | --- |
| probe | `capture.py` | the eight election markets above | 14:26:03–14:27:34 (1.5 min) |
| long | `capture.py` | Vance, Harris, Lisnard | 14:27:57–15:27:57 (60 min) |
| settle-1 | `capture.py` | the 5- and 15-minute Bitcoin markets | 14:27:57–14:31:04 (3.1 min; ended by the server) |
| settle-2 | `capture.py --reconnect` | the 5- and 15-minute Bitcoin markets | 14:33:05–15:18:05 (45 min) |
| hb-none | `capture.py --ping-interval 0` | Vance | 14:28:05–14:38:06 (10 min) |
| hb-stop | `capture.py --ping-for 60` | Vance | 14:28:05–14:38:06 (10 min) |
| idle-1, idle-2 | `capture.py --no-custom-feature --ping-interval 0` | the settled Bitcoin market | 14:39:00–14:41:06 and 14:41:27–14:43:33 |
| idle-ping | `capture.py --no-custom-feature` | the settled Bitcoin market | 14:41:27–14:46:27 (5 min) |
| settled | `capture.py` | the settled Bitcoin market | 14:39:00–14:44:00 (5 min) |
| mixed | `capture.py` | Harris and the settled Bitcoin market | 14:48:26–14:48:56 (30 s) |
| sdk | `sdk_reconnect.py` | Vance, Harris | 14:33:22–14:40:24 (7 min) |
| sdk-pong | `sdk_reconnect.py --faults withhold-pong` | Vance, Harris | 14:50:08–14:52:38 (2.5 min) |
| join | `snapshot_join.py` | Vance, Harris, Lisnard | 14:43:20–14:48:22 (5 min) |

The numbers below come from the scripts' own reports:
[`check_order.py`](../spikes/check_order.py) for ordering and PONG timing,
[`check_hashes.py`](../spikes/check_hashes.py) for hashes,
`sdk_reconnect.py --summarize` and `snapshot_join.py --summarize` for their
runs.

## Answers at a glance

| Question | Answer for the observed markets and periods |
| --- | --- |
| 1. Does the SDK's stream reconnect and restore subscriptions, and report it? | It reconnects and resends its subscription on its own after every fault tried. It reports neither the disconnect nor the reconnect to the consumer; only failed attempts and its own heartbeat timeout reach its logger. |
| 2. What happens when PING stops, and how promptly does PONG arrive? | With market data flowing, nothing: connections without PING stayed open for 10 minutes. A connection with no traffic at all was closed after about 125 seconds without a close frame; PING every 10 seconds kept it open. PONG took a median of 0.14 seconds but waits behind queued data, up to 11.4 seconds under load. |
| 3. Is the order consistent, and can missed events be replayed? | Book changes for each token arrived in timestamp order, with two 1 ms exceptions at the start of a connection. Events of different types for one token sometimes did not, and some messages arrived twice. Nothing is replayed: after a reconnect the stream sends a fresh book, and the events in between are gone. |
| 4. Does anything reveal a missed event? | No sequence numbers. The order-book hash, an undocumented SHA-1 of the token's book, can be recomputed locally and reveals a book that has diverged from the source's. It cannot count missed events, and some of its inputs are not on the stream. |
| 5. Can a REST snapshot be joined to the stream without losing or repeating updates? | Yes, by hash: all 456 snapshots taken matched a stream state exactly. The stream's own book on subscribing also matched the source's state each time, so the client may not need REST for this. |
| 6. What happens when a subscribed market settles, or is already settled? | The book was emptied, then `market_resolved` named the winner, then nothing more. When no unresolved market was left on the connection, the server closed it (`1000 all subscribed assets resolved`); once, that close arrived without `market_resolved`. A settled market's subscription returns no book and no error. Partly open: only automatically resolved crypto markets were seen settling. |

## Behavior the documentation and SDK do not state

These are the behaviors found here that neither the
[real-time data documentation](https://docs.polymarket.com/market-data/realtime-data)
nor the SDK's documentation states, and that can leave a client's data
wrong or incomplete without any error. Each links to its evidence below. The
last column names the script that reproduces it: run live, it repeats the
check against current markets, and with `--capture` it re-analyzes a
recorded capture. Each prints a verdict. Behaviors seen only once, or only
in the SDK's source, say so.

| Behavior | What it does to a client's data | Seen | Reproduce |
| --- | --- | --- | --- |
| The SDK's stream reconnects and resubscribes without telling its consumer ([1](#1-reconnection-and-subscription-restoration-in-the-sdk)) | The consumer keeps reading after a gap with no sign that source events were lost, so it cannot mark its state uncertain | the abort, stall, and close in run sdk and both aborts in the reproductions; 2 to 596 book states lost per gap | `repro_sdk.py silent-reconnect` |
| The SDK drops events its parser rejects, logging only at DEBUG ([1](#1-reconnection-and-subscription-restoration-in-the-sdk)) | Events vanish: 3,595 of 3,754 `new_market` events in run long's hour (96%) carry a `game_start_time` string the SDK rejects | every capture with `new_market` events | `repro_sdk.py drops-events` |
| The SDK stops reconnecting after an error other than its `TransportError` ([1](#1-reconnection-and-subscription-restoration-in-the-sdk)) | The handle stays open and silent; a consumer waits forever | source only | none (source: `streams/reconnect.py`, `streams/clob/market.py`) |
| Nothing is replayed after a reconnect ([3](#3-event-order-and-replay)) | Changes made during a disconnect are gone; the stream sends only current books | every reconnect observed | `repro_stream.py no-replay` |
| A connection with no traffic is closed after about 125 s, without a close frame ([2](#2-heartbeat)) | A quiet subscription that skips `PING` loses its connection in a way that looks like a network failure | 3 of 3 idle runs | `repro_stream.py idle-close` |
| The server ends connections it considers slow consumers, with 1013 or no close frame, and `PONG` waits behind queued data ([2](#2-heartbeat)) | Busy subscriptions lose their connection and the events in the gap; a short `PONG` timeout misreads a backlog as a dead connection | 9 connections ended on busy Bitcoin markets; `PONG` up to 11.4 s | `repro_stream.py slow-consumer` |
| Messages arrive more than once ([3](#3-event-order-and-replay)) | Event counts and anything derived from them double-count | 60 in 3 min, 172 in 45 min, and 38 in 5 min on busy markets; 9 in run long's hour | `repro_stream.py duplicates` |
| A token's events arrive out of timestamp order across event types ([3](#3-event-order-and-replay)) | Sorting by source timestamp and arrival order disagree; `best_bid_ask` and `last_trade_price` can trail or lead the book | every busy capture; 18 in run long's hour | `repro_stream.py duplicates` |
| A change stamped before an opening `book` can arrive after it ([3](#3-event-order-and-replay)) | The client cannot tell whether the book already includes it; applying it failed one hash check | twice, at connection start | `repro_stream.py duplicates` (out-of-order verdict) |
| Opening `book` timestamps are the book's last change, not the snapshot time ([3](#3-event-order-and-replay)) | A freshness check on the timestamp misjudges how current the book is | every subscription | any capture |
| A trade's price and a tick-size change enter the book hash before the stream announces them ([4](#4-revealing-a-missed-event)) | A client checking hashes sees false mismatches unless it waits for the announcement | every capture with trades | `check_hashes.py` |
| The order-book hash is a reproducible SHA-1 of the book, but undocumented; on busy markets its trade price does not follow announced trades ([4](#4-revealing-a-missed-event)) | Without the recipe a client cannot check its book; with it, it still needs two REST-only fields and sometimes a search | all captures | `check_hashes.py --any-trade-price` |
| The stream can omit a change to the book ([4](#4-revealing-a-missed-event)) | The client's book silently differs from the source's until the level changes again | once, in the probe | `check_hashes.py` on a capture; cannot be forced |
| When every market on a connection has settled, the server closes it with `1000 all subscribed assets resolved` ([6](#6-settlement)) | A client that treats every close as an interruption reconnects to settled tokens and gets no books | all 3 settlements that left no unresolved market: twice with the close frame, once probably without it | `repro_settlement.py settlement` |
| A settlement can go unannounced ([6](#6-settlement)) | `market_resolved` is lost if the connection drops at that moment and is not sent again; the client keeps a settled market as ready | 1 of 5 settlements | `repro_settlement.py settlement` |
| Settled tokens are silently left out of a subscription; REST returns 404 for their books ([6](#6-settlement)) | The client waits for a book that never comes and cannot tell a settled token from an unknown one | every settled subscription | `repro_settlement.py settled-subscription` |
| `new_market` arrives for every new market whenever `custom_feature_enabled` is set ([6](#6-settlement)) | Up to about five a second that the client must filter out | every run with the flag | `repro_sdk.py drops-events` |

### Reproduction runs

The reproductions ran against current markets on October 4, 2026, between
15:41 and 16:08 UTC, after the investigation's own runs. Runs that
started before 16:01 predate the `environment` record in captures. uv's
cached environments for those scripts hold the same versions listed under
[Versions](#versions).

| Check | Period (UTC) | Markets | Verdict and what it saw |
| --- | --- | --- | --- |
| `repro_sdk.py silent-reconnect` | 15:47:47–15:49:40 | 15-minute Bitcoin ending 16:00 | REPRODUCED. After its connection was aborted, the SDK resubscribed in 0.5 s and logged nothing above DEBUG. 596 source book states from that gap never reached its consumer. Withheld `PONG`s were reported, with a WARNING, and 1,067 states were lost across that reconnect. An earlier run, 15:41:53–15:43:46, on the quieter Lisnard market, also reconnected silently, but nothing changed during its gap. |
| `repro_sdk.py drops-events` | 15:43:47–15:46:48 | Lisnard | REPRODUCED. The SDK's parser rejected 2 of 8 `new_market` events, on `game_start_time`. |
| `repro_stream.py idle-close` | 15:41:57–15:44:23 | 5-minute Bitcoin ended 15:35 (settled) | REPRODUCED. The quiet connection was closed without a close frame after 125.1 s; the one sending `PING` was not. |
| `repro_stream.py no-replay` | 15:41:57–15:42:59 | Lisnard | REPRODUCED. 8 of the 10 book states in a 15.4 s gap never arrived. The reopened connection began with books holding the latest state. |
| `repro_stream.py duplicates` | 15:41:59–15:47:00 | 15-minute Bitcoin ending 16:00 | REPRODUCED. 38 messages arrived a second time, up to 120 ms apart. 45 events arrived after a later-stamped event for the same token, none of them between `book` and `price_change`. |
| `repro_stream.py slow-consumer` | 15:49:44–15:59:47 | 15-minute Bitcoin ending 16:00, 5-minute ending 15:55 | REPRODUCED. The server closed the first connection after 206 s with `1013 slow consumer: send buffer full`; its slowest `PONG` took 11.4 s. The second ran 392 s until the script ended it. The `settlement` run below also lost three connections, without close frames, in its first two and a half minutes. |
| `repro_settlement.py settled-subscription` | 15:43:01–15:43:23 and 16:01:39–16:02:02 | 5-minute Bitcoin ended 15:40, then 15:55 (both settled), with Lisnard | REPRODUCED both times. Settled tokens were left out of the opening frame without an error, as `[]` when alone, and REST rejected their books. The second run's capture records its versions and the server's headers. |
| `repro_settlement.py settlement` | 15:41:57–15:47:07 | 5-minute Bitcoin ending 15:45 | NOT REPRODUCED: announced, then closed `1000 all subscribed assets resolved`. See [6](#6-settlement). |
| `repro_settlement.py settlement` | 16:00:12–16:07:50 | 5-minute Bitcoin ending 16:05 | NOT REPRODUCED: announced, then closed `1000 all subscribed assets resolved`. Gamma showed it closed 51 s later. |

Markets in these runs, besides Lisnard (listed above):

| Market | Condition ID | Token IDs | End date |
| --- | --- | --- | --- |
| Bitcoin Up or Down, 11:45 AM–12:00 PM ET (`btc-updown-15m-1791128700`) | `0xa44816409ffab71a4b1b97a4698c6a8a07096df79fc7478c9e93fe43b8dbf2df` | Up `70600805409937177295864275106224308283229243324762709520579752291314277221371`<br>Down `17027885903092230389687017952614785495343173441180876354284593341606000602266` | 2026-10-04 16:00 |
| Bitcoin Up or Down, 11:30–11:35 AM ET (`btc-updown-5m-1791127800`) | `0xb8bf724e28ba073aec2f767136ece99cba29ff1147eb481f26f46db6e37841eb` | Up `3356153585581581299384671177036309751587799874232375764857851138457443302582`<br>Down `53354206921733681090908778912489675736681464086698317103031479808844776750464` | 2026-10-04 15:35 |
| Bitcoin Up or Down, 11:35–11:40 AM ET (`btc-updown-5m-1791128100`) | `0xbb2692be2a4dfdf711a9b23bb202fde1f2df8cf92b2e0bbf42d3591f25dfdb90` | Up `69750102095438646456313142718268593528259383312765261569370182872581610352729`<br>Down `62134854186097270384205989574688195437556809883859836611127486634717024211348` | 2026-10-04 15:40 |
| Bitcoin Up or Down, 11:40–11:45 AM ET (`btc-updown-5m-1791128400`) | `0x73264056363c9bd503a8e5fcb9b5df2aba18a0fba6e6cbb6eb3aa625404bd7b9` | Up `72575087309303581923230069367247381191351895261274278778842742972833526755316`<br>Down `31927854566288319280601675148230350642150933835733407674785557193008540231119` | 2026-10-04 15:45 |
| Bitcoin Up or Down, 11:50–11:55 AM ET (`btc-updown-5m-1791129000`) | `0x8232df1e165b34828ef42befbebf4d20024b6475d06187b914849b9eaa60d2b0` | Up `16218391670914544698325664221894081973763748876477747482610137685375518815082`<br>Down `70315748122003180930189135738896142286865370061917538834567908956206819727751` | 2026-10-04 15:55 |
| Bitcoin Up or Down, 12:00–12:05 PM ET (`btc-updown-5m-1791129600`) | `0xe32b27cd816b40445f8f9cece2fd6fe7f1031c6b6be1eb4a0859814d9c8a40d2` | Up `35902896712260582114771873921200347198359549353294277159121615050932857714630`<br>Down `79181699931229917353627113086356511148325493966026070653706439205502792302912` | 2026-10-04 16:05 |

## 1. Reconnection and subscription restoration in the SDK

**Checked** October 4, 2026, with `polymarket-client` 0.12.0.
**Markets:** Vance and Harris. **Observed:** run sdk, 14:33:22–14:40:24
(7 min), and run sdk-pong, 14:50:08–14:52:38 (2.5 min). **Evidence:** the SDK
source and [`sdk_reconnect.py`](../spikes/sdk_reconnect.py).

### From reading the source

- `ClobMarketStreamManager` (`polymarket/_internal/streams/clob/market.py`)
  carries every market subscription on one socket. When the socket closes
  without the client asking, it schedules a reconnect through
  `ReconnectScheduler` (`polymarket/_internal/streams/reconnect.py`). Each
  delay is drawn uniformly between zero and 0.25 s × 2^attempt, capped at
  30 s (`polymarket/_internal/ws/backoff.py`). Attempts are not limited, and
  the count resets after a reconnect succeeds. The one exception is an
  attempt that fails with anything other than the SDK's `TransportError`
  (socket, timeout, and WebSocket errors become one). That is logged with a
  traceback and not retried, and the handle stays open with no further
  events. No run triggered it.
- After reconnecting, it sends one `market` frame built from all live
  subscriptions: the union of their token IDs, with `custom_feature_enabled`
  if any of them asked for it (`build_initial_frame` in `market_protocol.py`).
- Two mechanisms detect a dead connection. The SDK calls
  `websockets.connect` without ping settings, so that library's default
  keepalive applies: a protocol ping every 20 s, with 20 s to answer. The SDK's
  own `ClobWebSocketHeartbeat` sends `PING` every 10 s, and a watchdog
  checking every 5 s closes the socket once no `PONG` has arrived for 30 s.
- The handle returned by `subscribe()` yields market events only. It has no
  connection-state event, callback, or property. The only signals are records
  on the logger given to the client. A failed reconnect attempt is logged at
  INFO (`market stream reconnect failed: …; rescheduling`), and the watchdog's
  close at WARNING (`WebSocket heartbeat stale; closing`). Nothing is logged
  when the connection drops or when a reconnect succeeds.
- Events that fail validation are dropped with a DEBUG record and counted on the
  stream manager, which the public client does not expose. Frames that are not
  JSON are dropped without a record. The handle's queue holds 1,024 events and
  drops the oldest when full, counting the losses in `handle.dropped`.
- The socket URL comes from the environment configuration. Callers cannot
  build an `Environment`, so the spike pointed the SDK at its proxy through
  the private `polymarket._internal.environment.create_environment`.

### From running it

`sdk_reconnect.py` connected the SDK to a local TCP proxy that held TLS to
the real socket and could cut, stall, or close the connection. A direct
connection to the same four tokens ran alongside as a reference.

| Fault (seconds after subscribing) | SDK detected it | Reconnected and resubscribed | Logged above DEBUG |
| --- | --- | --- | --- |
| Both TCP connections aborted, no close frame (62.6) | at once | reconnect began 40 ms later; open and `market` frame resent at 63.1 | nothing |
| Both directions stalled, TCP left open (122.6) | after 20.5 s, by the websockets keepalive: `> CLOSE 1011 (internal error) keepalive ping timeout` | open and resent at 144.1 | nothing |
| Server close frame 1001 (242.6) | at once; it answered the close | open and resent at 244.8, once TCP closed | nothing |
| Connections refused for 60 s (302.6) | at once | nine attempts failed at 0.2 to 55.2 s; the next came 21.3 s after the proxy returned; open and resent at 384.4 | nine INFO records, one per failed attempt |
| Server `PONG` frames withheld, everything else forwarded (sdk-pong, 32.5) | 25.0 s later, about 35 s after the last `PONG`, by its own watchdog; it closed with 1000 | open and resent 0.7 s later; the watchdog fired again 30 s after that, since `PONG` was still withheld | `WARNING WebSocket heartbeat stale; closing`, each time |

Each resent frame named all four tokens with `custom_feature_enabled` set,
as in this excerpt from the websockets debug log:

```text
> TEXT '{"type":"market","assets_ids":["160400154401962..._feature_enabled":true}' [382 bytes]
```

After every reconnect, the SDK's next events were a fresh `book` for each
token. The events the reference connection received while the SDK was away
never reached the SDK's consumer. That was 2 book states across the abort, 106
across the stall, 10 across the close frame, and 480 across the outage. The
handle's `dropped` count stayed at zero, and nothing in the event stream
marked any of these gaps.

The SDK also dropped events it could not parse. In run sdk it logged
`dropped 1 malformed market event(s)` at DEBUG 41 times. Each was a
`new_market` event whose `game_start_time` is a string such as
`'2026-10-04 14:25:00+00'`, which the SDK's validator rejects as not epoch
milliseconds. Only 8 of the 49 `new_market` events it received reached the
consumer.

### Answer

The SDK's market stream reconnects and restores its subscriptions on its
own. The source shows the mechanism: unlimited jittered retries and a resent
`market` frame. Runs sdk and sdk-pong confirmed it after an abort, a stall,
a close frame, a refused-connection outage, and withheld `PONG`s.

It does not report doing so to the consumer. The source shows that the
handle has no way to: it carries market events only, and the SDK logs
nothing for a disconnect or a successful reconnect. The runs confirmed that
nothing in the event stream marked any gap, and that only failed attempts
(INFO) and the SDK's own watchdog (WARNING) reached its logger. One thing the
source alone would not settle is which mechanism catches a stall. In the run,
the websockets keepalive closed the stalled connection after 20.5 s, before
the SDK's 30-second watchdog could act.

An application that must mark state uncertain when a connection drops cannot
get that from this stream, which supports owning the connection.

Not tested: TLS failures on the SDK's own connection, since it spoke plain
WebSocket to the proxy. Also untested were the SDK with several
subscriptions or with subscription changes during a fault, and behavior over
longer periods.

## 2. Heartbeat

**Checked** October 4, 2026, with a raw socket (`websockets` 15.0.1).
**Markets:** Vance (runs hb-none and hb-stop), the
settled Bitcoin market (idle runs), Vance, Harris, and Lisnard (run long), the
5- and 15-minute Bitcoin markets (runs settle-1 and settle-2). **Observed:**
the periods in the run table. **Evidence:**
[`capture.py`](../spikes/capture.py) and `check_order.py`.

- **PING stopped while data flows.** Run hb-none sent no `PING` at all, and
  run hb-stop sent five and then stopped. Neither sent protocol pings. Both
  connections stayed open for the full 10 minutes and kept receiving market
  data, about 1,610 frames each. Each closed normally (1000) only when the
  script ended it. No run recorded a protocol ping from the server.
- **No traffic at all.** Runs idle-1 and idle-2 subscribed to the settled
  market without `custom_feature_enabled`, so after an opening `[]` frame
  nothing arrived. They sent no `PING`. Both connections were closed 125.2
  seconds after opening, without a close frame:

  ```text
  14:41:28.236 control = connection is OPEN
  14:43:33.403 control = connection is CLOSED
  14:43:33.403 control x closing TCP connection
  ```

  Run idle-ping was the same except for a `PING` every 10 seconds. It stayed
  open for its full 5 minutes. Whether the market socket or Cloudflare in
  front of it closed the idle connections is not visible from the client.
- **PONG timing.** Over run long's 60 minutes, all 359 `PING`s got a `PONG`.
  The median was 0.142 s, the 95th percentile 0.536 s, the 99th 1.259 s, and
  the maximum 1.687 s. Over run settle-2's 45 minutes on busier markets, 266
  `PONG`s took a median of 0.138 s, a 99th percentile of 1.306 s, and at most
  3.49 s. One `PING` went unanswered when the server ended its connection. In
  run idle-ping, 29 `PONG`s took a median of 0.135 s and at most 0.161 s.
- **PONG waits behind data.** Run settle-1, on the busy Bitcoin markets, saw
  receipt lag (receipt time minus the event's source timestamp) grow from
  about 0.1 s to 11.7 s over its last 40 seconds. Its PONGs grew with it:
  the last one before the server ended the connection took 9.47 s. In the
  slow-consumer reproduction, a `PONG` took 11.4 s before the server closed
  the connection as a slow consumer. The server sends `PONG` in order with
  market data, so a delayed `PONG` can mean a backlog rather than a dead
  connection.
- **The server ends connections it cannot keep up with.** In run settle-2,
  on the same markets, the server closed the connection three times with
  `1013 slow consumer: send buffer full`, and ended a fourth without a close
  frame. Run settle-1 also ended without a close frame, after the
  backlog above. In run settle-2 the capture process averaged about 6% of one
  CPU over its first five minutes, which included three of these closes, so
  the client's own processing does not look like the limit. These markets
  produced up to about 840 frames a second around 14:30, when both markets'
  trading windows began.

### Answer

The server did not react when `PING` stopped while market data was flowing,
over two 10-minute runs. A connection with no traffic in either direction
was closed after about 125 seconds without a close frame, twice, and a
`PING` every 10 seconds prevented that. `PONG` normally came back in about
0.14 seconds, but it is queued behind market data. Under heavy load it took
9.5 seconds, and 11.4 seconds in a reproduction, before the server dropped
the connection. So the PONG
timeout is still the client's choice, but this evidence bounds it: it must
allow for backlog delay, and the client must keep sending `PING` on quiet
subscriptions. The SDK's choice is 30 seconds.

Open: whether the 125-second close comes from the market socket or from
Cloudflare; how long a connection without `PING` stays open beyond 10
minutes; and how much backlog the server allows before it closes a
connection as a slow consumer.

## 3. Event order and replay

**Checked** October 4, 2026; raw socket with `websockets` 15.0.1, and the SDK
0.12.0 for replay. **Markets:** Vance, Harris, and Lisnard (run long); the 5-
and 15-minute Bitcoin markets (runs settle-1 and settle-2). **Observed:** run
long, 14:27:57–15:27:57 (60 min), and the other periods in the run table.
**Evidence:** `check_order.py`, `check_hashes.py`, and `sdk_reconnect.py
--summarize`.

### Order

Run long held one connection to Vance, Harris, and Lisnard for 60 minutes
without a disconnect. It received 14,444 frames, with 21,386 `price_change`
entries and 24 `book` events across the six tokens. A token's `book` and
`price_change` events always arrived in timestamp order. 18 times an event's
source timestamp was earlier than the previous event's for the same token, by
at most 58 ms, and every one involved a `best_bid_ask` or `last_trade_price`
event.

On the busy Bitcoin markets, run settle-1 counted 254 times in 3.1 minutes
when an event's source timestamp was earlier than the previous event's for
the same token. Most were 1 or 2 ms, the largest 0.621 s. All involved
different event types: a `best_bid_ask` followed by an earlier `price_change`
(146) or `book` (72), a `price_change` followed by an earlier
`last_trade_price` (22), and a few others. `price_change` entries for one
token never arrived out of timestamp order. Run settle-2, over 45 minutes on
the same markets, counted 1,372 such regressions, none larger than 25 ms, and
again none between `price_change` entries. Twice, though, in the first second
of a connection, a `price_change` entry arrived just after an opening `book`
timestamped 1 ms later. Applying it to that book failed one hash check, and
the next check verified.

Three more order effects matter to a client:

- **Repeated messages.** Run settle-1 received 60 `price_change` messages a
  second time, in separate frames up to 6 ms apart. Each repeat had the same
  entries, down to the hashes, though sometimes listed in a different order.
  Setting a level to the same size twice leaves the book unchanged, but a
  client counting events would count them twice. Run settle-2 received 172
  `price_change` messages a second time, along with 2 `best_bid_ask` and 2 of
  its 4 `tick_size_change` events, up to 18 ms apart. Run long received 9 in
  60 minutes.
- **Trades are announced after the book reflects them.** In the probe, a
  `book` event for the House-control market, received 8.119 s after
  subscribing, already reflected a trade at 0.93 in its hash. The
  `last_trade_price` event announcing that trade arrived at 8.614 s with a
  later timestamp.
- **A subscription's opening books carry old timestamps.** A `book` event's
  timestamp is the time of the book's last change, which can be well before
  the subscription: 33 s for Harris's tokens in run sdk. Across a connection,
  timestamps therefore run backwards at the start. That accounts for the
  largest regressions per connection that `check_order.py` reports. The rest
  are events for different tokens interleaving out of order by a few
  milliseconds.

### Replay

The documentation describes no way to request missed market events: no
sequence number, offset, or resume field on the subscription frame. The
`seq` fields it shows belong to the crypto and equity price feeds, not the
market channel. The SDK has no replay either. On reconnect it resends the
same `market` frame.

In run sdk, after each of the four forced disconnects, the stream started
again with a fresh `book` for each token. Each of those books matched, by
hash, a state the reference connection had reached. The changes in between
were never sent, as section 1 counts. The raw reconnects in run settle-2
began the same way, with an array of `book` events for the tokens whose
markets had not settled.

### Answer

For each token, the changes that alter the book arrived in timestamp order in
every run. The one exception was two entries that arrived just after an
opening `book` stamped 1 ms later. Events of different types for one token did
not always arrive in order, and some messages arrived twice. The source did
not replay events missed during a disconnect, documents no way to ask for
them, and the SDK does not try. After reconnecting, the stream supplies
current state, not the changes missed.

Open: whether the market channel accepts any undocumented replay or resume
field. None was tried.

## 4. Revealing a missed event

**Checked** October 4, 2026; raw socket with `websockets` 15.0.1; REST books
through the SDK 0.12.0. **Markets:** the eight election markets (probe), Vance,
Harris, and Lisnard (run long), the 5- and 15-minute Bitcoin markets (run
settle-1). **Observed:** the periods in the run table. **Evidence:**
[`orderbook.py`](../spikes/orderbook.py) and `check_hashes.py`.

No market event carries a sequence number, in the documentation or in
any frame captured. What the stream does carry is a `hash` on each `book`
event and on each entry of a `price_change` event. The REST book has one too,
documented only as a way to tell whether a book changed between reads.

### What the hash covers

The hash turned out to be reproducible. It is the SHA-1 of the token's book
written as compact JSON, in the REST response's key order, with `hash` set
to the empty string:

```text
{"market":…,"asset_id":…,"timestamp":…,"hash":"","bids":[…],"asks":[…],"min_order_size":…,"tick_size":…,"neg_risk":…,"last_trade_price":…}
```

Bids are in ascending price order and asks in descending. Every one of the
456 REST snapshots in run join recomputed this way. So did all 18 `book`
events in the probe, all 630 in run settle-1, and all 2,762 in run settle-2,
though on the Bitcoin markets only after the trade price was found by search,
as below. A
`price_change` entry's hash is the same function of the token's book after
the change, at the event's timestamp. This is not documented, so it could
change without notice.

Some inputs are not on the stream:

- `min_order_size` and `neg_risk` appear only in the REST book. They are fixed
  per market. For the settled market, which has no REST book, they were
  recovered by trying likely values against the first stream book's hash.
- `tick_size` and `last_trade_price` appear on a subscription's first `book`
  event but not on later ones.
- A trade's price enters the hash before `last_trade_price` announces it, as
  section 3 shows. On the election markets, checks that failed with the
  current price nearly always passed with the market's next announced price.
- On the busy Bitcoin markets, the price inside the hash did not follow the
  announced trades at all. It could be recovered only by trying all 1,001
  prices on the 0.001 grid. A match still confirms every level exactly, since
  only the price was guessed.

Two patterns in how hashes are assigned also matter:

- Consecutive entries for a token in one burst share a hash, the hash of the
  book after the last of them. So the check applies once per burst.
- Occasionally an entry's hash already includes the token's next change,
  which arrives a millisecond or two later with its own hash. A single failed
  check that passes after the next change is not a divergence.

### What it detected

Run long's replay made 21,136 checks, one per run of `price_change` entries
sharing a hash. 20,726 verified at once, 322 with the market's next announced
trade price, 13 only after searching for the trade price, and 64 one change
late. 11 failed, each a single check followed within 48 ms by one that
verified. All 24 `book` events verified, and the 18 sent after trades matched
the replayed book level for level. Nothing in the hour indicates a lost
change.

On the busy Bitcoin markets, run settle-2's replay made 444,131 checks over
45 minutes. 433,457 verified at once, 9,070 with the next announced trade
price, 1,544 after searching for the trade price, and 8 one change late. 52
failed, in 17 episodes. All but two cleared within 0.75 s, and those two were
cut off when the connection ended during the 15-minute market's settlement.
The 12 later `book` events that differed from the replayed book were the
fresh books after the run's three reconnects, one per token. Each reconnect
had lost changes, and comparing the fresh book with the one held showed it.

Runs hb-none and hb-stop were separate connections to the Vance market over
the same 10 minutes. Each had 2,759 checks that verified and 5 isolated ones
that failed, and the failures fell at the same moments on both connections,
322.1 s and 525.1 s after opening. Each was followed within 30 ms by a check
that verified. So these come from the source's hashes, not from anything one
connection lost. Run join's stream verified all 1,580 of its checks.

In the probe, the hash exposed a real difference between the stream and the
source. On the Van Hollen market, four consecutive checks on each token
failed, starting 4.67 s after subscribing. The failures stopped at 8.78 s,
when the stream removed two levels it had never added: a bid at 0.29 on the
No token and the matching ask at 0.71 on the Yes token. The source's book had
held an order the stream never announced, and the hash showed it. The probe
cannot tell whether the subscription book was missing the order or the
stream later omitted its arrival.

Skipping entries in a replay shows what the check can and cannot catch.
In run long, 20 entries spread across the hour were skipped, one per replay.
7 were detected, after a median of 0.43 s and at most 2.9 s. The other 13
were overwritten by the token's next change at the same level. Most of those
belonged to a quote at 0.01 and 0.99 on the Vance market that was resized
back and forth between 53,500 and 53,600. The probe (10 skips) and run hb-none
(20) followed the same pattern: 6 detected, 23 overwritten, and 1 with no
later change before the capture ended. Every skipped entry that left the book
different at the next check was caught. A detected divergence lasted until a
later change happened to set the affected level again or a fresh `book`
replaced the replayed one. In one replay that took 2,537 failed checks.

### Answer

The stream carries no sequence numbers. The order-book hash, recomputed from
a locally maintained book, reveals that the book has diverged from the
source's. That happens when a missed or misapplied change matters to the
book, usually at the next change on that token. It does not reveal every
missed event: a missed change that a later change overwrites leaves no trace.
It cannot count missed events, and it needs inputs the stream does not fully
carry: two fields from REST, and on some markets a trade price that has to be
searched for. Because the recipe is undocumented, the client can use it as
evidence of divergence but should not depend on it to be ready.

Open: whether every market uses this recipe (checked on eight election
markets and two Bitcoin markets); how the trade price inside the hash is
chosen on busy markets. One tick-size change was observed, from 0.01 to 0.001
on both tokens of the 15-minute Bitcoin market. The one check just before its
event failed, and checks after it verified with the new tick size, so the
tick size seems to enter the hash shortly before the event announces it, as
the trade price does.

## 5. Joining a snapshot to the stream

**Checked** October 4, 2026; raw socket with `websockets` 15.0.1; REST books
through the SDK 0.12.0. **Markets:** Vance, Harris, and Lisnard (run join);
Vance and Harris (runs sdk and sdk-pong). **Observed:** run join,
14:43:20–14:48:22 (5 min), plus runs sdk and sdk-pong. **Evidence:**
[`snapshot_join.py`](../spikes/snapshot_join.py) and `sdk_reconnect.py
--summarize`.

**The stream's own snapshot.** On subscribing, the stream sends a `book` for
each active token. In runs sdk and sdk-pong, all 34 `book` events the SDK
received matched, by hash and timestamp, a state the continuous reference
connection reached. 32 came on subscribing or resubscribing and 2 after
trades. 31 of them were the reference's latest state for that token when the
SDK received them; for the other three the reference had already moved on.
So the subscription book is the source's current book. A
client that takes it as the starting point and applies the token's later
entries in order needs no separate snapshot.

**REST snapshots.** Run join fetched each token's REST book every few
seconds while the stream ran: 456 snapshots of 6 tokens in 5 minutes, with a
mean request time of 149 ms. The REST `timestamp` is the time of the book's
last change, not of the request. For every snapshot:

- its hash recomputed from its contents (456 of 456);
- the stream had a state with the same hash for that token (456 of 456).
  The stream reached it before the request in 389 cases, during it in 33,
  and after the response in 34, between 258 ms (median) and 1,077 ms after;
- the levels of the replayed stream book at that state equalled the
  snapshot's (456 of 456).

Joining instead by timestamp, taking the stream state after the last entry
whose timestamp is at or before the snapshot's, matched in 455 of 456. In
4 cases several entries shared the snapshot's timestamp, which makes that
rule ambiguous.

### Answer

Yes, for these markets and this period. A REST snapshot joins to the stream
without losing or repeating any update when the client finds the stream
entry carrying the snapshot's hash and applies only the entries after it. If
the stream has not reached that state yet, the client holds its entries until
it does, which took up to about a second here. A join by timestamp nearly
always works, but shared timestamps make it ambiguous. The stream's own
`book` on subscribing gives the same result without REST.

Open: busier markets, where REST may lag or lead by more; the case where the
snapshot's hash never appears in the stream, which did not occur; REST rate
limits.

## 6. Settlement

**Checked** October 4, 2026; raw socket with `websockets` 15.0.1; REST through
`curl` and the SDK. **Markets:** the 5- and 15-minute Bitcoin markets ending
14:35 and 14:45, the Bitcoin market that ended at 14:20 and had already
settled, and, in the reproduction runs, the 5-minute Bitcoin market ending
15:45. **Observed:** run settle-2, 14:33:05–15:18:05 (45 min); runs settled,
idle-1, idle-2, idle-ping, and mixed; and the reproduction runs listed above.
**Evidence:** `capture.py` and
[`repro_settlement.py`](../spikes/repro_settlement.py).

### A subscribed market settles

The 5-minute market ended at 14:35:00 and kept trading past its end date:
288 of its events, including 4 trades, carry later timestamps. At
14:37:28.420 a burst of `price_change` entries set every level on both tokens
to size 0, leaving a best bid of 0 and a best ask of 1. At 14:37:28.448, 2.5
minutes after the end date, the stream sent:

```json
{"id":"5234673","market":"0x0c1b1a29cf49e127499e878853e4edbe79b929ccaaf45e96345a11fe0c263426","assets_ids":["100888370987976300727976104132259754468885708358624157065950665701873247248130","105432187511272365632213611061207512483403511195339600044714295797509063777660"],"winning_asset_id":"105432187511272365632213611061207512483403511195339600044714295797509063777660","winning_outcome":"Down","event_message":null,"timestamp":"1791124648409","event_type":"market_resolved","tags":["Crypto","Bitcoin","Crypto Prices","Recurring","Up or Down","Hide From New","5M"]}
```

Nothing more arrived for that market. The connection stayed open, and the
15-minute market's events continued on it.

The 15-minute market, which ended at 14:45:00, went the same way up to a
point. A trickle of changes followed its end date, and at 14:47:38.336 a
burst began setting its levels to 0, leaving a best bid of 0 and a best ask
of 1. At 14:47:38.852, 127 ms after the last entry received and before any
`market_resolved`, the connection was closed without a close frame. The
reconnection at 14:47:40 received `[]` and nothing for that market
afterwards. Gamma lists the market as closed at 14:46:28 and resolved "Down".
So in this run the only stream evidence of that settlement was the emptied
book and, after reconnecting, the missing one.

A third settlement, watched by `repro_settlement.py settlement`, explains the
disconnect. The 5-minute market ending 15:45:00 was the only market on its
connection. Its book was emptied at 15:47:01.600, and `market_resolved`
arrived at 15:47:01.835, 122 s after the end date. In the same instant the
server closed the connection:

```text
close received: 1000 'all subscribed assets resolved'
```

When the 15-minute market settled at 14:47:38, the 5-minute market had
already settled, so it too was the last unresolved market on its
connection. That connection ended 127 ms after its book was emptied, with
neither the close frame nor `market_resolved` arriving. Most likely the
server closed it for the same reason and the final frames were lost; the
capture cannot show which.

Gamma's record does not settle the question promptly either. Its
`closedTime` for the two earlier markets reads 14:36:26 and 14:46:28, a
minute or more before the stream's announcement. Yet after the third
market's `market_resolved` at 15:47:01, Gamma kept listing it as open, with
no resolution status, at 15:47:07 and again at 15:49:16. Polled every 15 s
from then, it first showed the market closed at 15:50:21, about three minutes
after the stream's announcement. The record it returned then gives
`closedTime` 15:45:53, before the announcement, and `updatedAt` 15:47:01. So
the lookup's timestamps do not say when a client could first see the market
closed. For the fifth settlement, at 16:06:59, `repro_settlement.py` polled
Gamma from the moment `market_resolved` arrived. Gamma first showed the market
closed 51 s later, with a `closedTime` of 16:05:53.

Five settlements were observed in all:

| Market | Book emptied | `market_resolved` | The connection |
| --- | --- | --- | --- |
| 5-minute, ending 14:35 (run settle-2) | 14:37:28.420 | 14:37:28.448, 148 s after the end | stayed open; the 15-minute market was still subscribed |
| 15-minute, ending 14:45 (run settle-2) | from 14:47:38.336 | never received | ended 14:47:38.852 without a close frame; no unresolved market was left |
| 5-minute, ending 15:45 (`settlement` reproduction) | 15:47:01.600 | 15:47:01.835, 122 s after the end | closed `1000 all subscribed assets resolved` |
| 5-minute, ending 15:55 (`slow-consumer` reproduction) | 15:57:00.931 | 15:57:00.944, 121 s after the end | stayed open; the 15-minute market was still subscribed |
| 5-minute, ending 16:05 (`settlement` reproduction) | 16:06:59.468 | 16:06:59.594, 120 s after the end | closed `1000 all subscribed assets resolved` |

`market_resolved` arrived only for subscribed markets. No run received one
for a market it had not subscribed to, although other Bitcoin markets settled
every few minutes. `new_market`, by contrast, arrives for every new market
whenever `custom_feature_enabled` is set. Its rate ranged from under one a
minute to about five a second, depending on the period.

### A subscription to a settled market

Subscribing to the Bitcoin market that had ended at 14:20 returned a single
frame, `[]`, and then nothing for that market. That held for 5 minutes in run
settled with `custom_feature_enabled`, and in the idle runs without it.
There was no error and no close. In run mixed, subscribed together with
Harris's two tokens, the opening frame held `book` events for Harris's tokens
only, and the settled tokens were left out. The REST book for a settled token
returns HTTP 404:

```json
{"error":"No orderbook exists for the requested token id"}
```

### Answer

When a subscribed market settled, the stream emptied its book, then sent
`market_resolved` with the winning token, two to two and a half minutes
after the end date, and then nothing more for that market. When that market
was the last unresolved one on the connection, the server also closed the
connection, with `1000 all subscribed assets resolved`. Of five settlements,
four were announced. For the other, the last unresolved market on its
connection, the connection dropped between the emptied book and any
announcement. A subscription to an already-settled market, including a
resubscription after a disconnect, returns no book and no error. So the
stream does not reliably announce a settlement, and a close is not always an
interruption. The client must expect to learn of a settlement from a token
that gets no `book`, which an unknown token would also produce. It confirms
the settlement through market lookup or the REST 404. Gamma showed settled
markets as closed 51 s and about three minutes after `market_resolved`, so the
client has to allow for that lag.

Open: settlement of markets resolved through a UMA proposal, such as the
election markets, which may take longer and need not clear the book the same
way; whether `market_resolved` is ever sent more than once or late; whether
the dropped connection at 14:47:38 was the all-resolved close. Only five
automatically resolved crypto markets were observed settling.

## Open questions

These carry into the live run's unresolved source behavior.

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
