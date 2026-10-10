"""The token state machine: each transition, T1 to T18, each case that
changes nothing, and the order of the records (spec/client.md, Per-token
state machine, Recovery contract, Settlement, and Record order).

Frames are decoded from JSON in the shapes of spec/conformance.md's frame
notation, so the machine sees what the client will give it.
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from polymarket_market_data import (
    BestBidAskEvent,
    BookEvent,
    CaptureGap,
    ConnectionState,
    ConnectionStateChange,
    LastTradePriceEvent,
    Level,
    Market,
    MarketInfo,
    MarketResolvedEvent,
    NewMarketEvent,
    PriceChangeEvent,
    TickSizeChangeEvent,
    TokenState,
    TokenStateChange,
    UndecodableFrame,
    UnknownEvent,
)
from polymarket_market_data._decode import decode_frame
from polymarket_market_data._state import (
    EndConfirmation,
    Record,
    StartConfirmation,
    StateMachine,
)

# The synthetic markets of spec/conformance.md.
A = "0x00000000000000000000000000000000000000000000000000000000000000a1"
B = "0x00000000000000000000000000000000000000000000000000000000000000b2"
S = "0x000000000000000000000000000000000000000000000000000000000000005e"
U = "0x000000000000000000000000000000000000000000000000000000000000000e"
N = "0x00000000000000000000000000000000000000000000000000000000000000c3"
PREFIX = "1" + "0" * 74
A1, A2 = PREFIX + "11", PREFIX + "12"
B1, B2 = PREFIX + "21", PREFIX + "22"
S1, S2 = PREFIX + "31", PREFIX + "32"
U1, U2 = PREFIX + "41", PREFIX + "42"
N1, N2 = PREFIX + "51", PREFIX + "52"
MARKET_A = Market(A, (A1, A2), "synthetic-a")
MARKET_B = Market(B, (B1, B2), "synthetic-b")
MARKET_S = Market(S, (S1, S2), "synthetic-s")
MARKET_U = Market(U, (U1, U2), "synthetic-u")
MARKET_N = Market(N, (N1, N2), None)
MARKETS = {
    m.condition_id: m for m in (MARKET_A, MARKET_B, MARKET_S, MARKET_U, MARKET_N)
}
OWNER = {token: m.condition_id for m in MARKETS.values() for token in m.token_ids}
NAMES = {A: "A", B: "B", S: "S", U: "U", N: "N"} | {
    token: name
    for name, token in {
        "A1": A1, "A2": A2, "B1": B1, "B2": B2, "S1": S1,
        "S2": S2, "U1": U1, "U2": U2, "N1": N1, "N2": N2,
    }.items()
}  # fmt: skip
T0 = 1791200000000
START = datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC)
ZEROS = "0" * 40

# Standard books: bids and asks in the order the frame notation writes them.
STANDARD = {
    A1: ([("0.47", "250"), ("0.48", "100")], [("0.53", "300"), ("0.52", "120")]),
    A2: ([("0.47", "300"), ("0.48", "120")], [("0.53", "250"), ("0.52", "100")]),
    B1: ([("0.38", "200"), ("0.39", "80")], [("0.42", "150"), ("0.41", "90")]),
    B2: ([("0.58", "150"), ("0.59", "90")], [("0.62", "200"), ("0.61", "80")]),
}

Pairs = list[tuple[str, str]]


# Frames in the conformance notation's shapes.


def text(value: object) -> str:
    return json.dumps(value, separators=(",", ":"))


def levels(pairs: Pairs) -> list[dict[str, str]]:
    return [{"price": price, "size": size} for price, size in pairs]


def opening_book(
    token: str, t: int = -30000, bids: Pairs | None = None, asks: Pairs | None = None
) -> dict[str, Any]:
    standard_bids, standard_asks = STANDARD[token]
    return {
        "market": OWNER[token],
        "asset_id": token,
        "timestamp": str(T0 + t),
        "hash": ZEROS,
        "bids": levels(standard_bids if bids is None else bids),
        "asks": levels(standard_asks if asks is None else asks),
        "tick_size": "0.01",
        "event_type": "book",
        "last_trade_price": "0.500",
    }


def opening(*tokens: str) -> str:
    return text([opening_book(token) for token in tokens]) + "\n"


def book(
    token: str, t: int = -30000, bids: Pairs | None = None, asks: Pairs | None = None
) -> str:
    standard_bids, standard_asks = STANDARD[token]
    return text(
        {
            "market": OWNER[token],
            "asset_id": token,
            "bids": levels(standard_bids if bids is None else bids),
            "asks": levels(standard_asks if asks is None else asks),
            "hash": ZEROS,
            "timestamp": str(T0 + t),
            "event_type": "book",
        }
    )


def pc_object(
    market: str, t: int, *entries: tuple[str, str, str, str]
) -> dict[str, Any]:
    return {
        "market": market,
        "price_changes": [
            {
                "asset_id": token,
                "price": price,
                "size": size,
                "side": side,
                "hash": ZEROS,
                "best_bid": "0.48",
                "best_ask": "0.52",
            }
            for token, side, price, size in entries
        ],
        "timestamp": str(T0 + t),
        "event_type": "price_change",
    }


def pc(market: str, t: int, *entries: tuple[str, str, str, str]) -> str:
    return text(pc_object(market, t, *entries))


def bba(token: str, t: int) -> str:
    return text(
        {
            "market": OWNER[token],
            "asset_id": token,
            "best_bid": "0.48",
            "best_ask": "0.52",
            "spread": "0.04",
            "timestamp": str(T0 + t),
            "event_type": "best_bid_ask",
        }
    )


def ltp(token: str, t: int) -> str:
    return text(
        {
            "market": OWNER[token],
            "asset_id": token,
            "price": "0.51",
            "size": "10",
            "fee_rate_bps": "0",
            "side": "BUY",
            "timestamp": str(T0 + t),
            "event_type": "last_trade_price",
            "transaction_hash": "0x" + "0" * 63 + "1",
        }
    )


def tsc(token: str, t: int) -> str:
    return text(
        {
            "market": OWNER[token],
            "asset_id": token,
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
            "timestamp": str(T0 + t),
            "event_type": "tick_size_change",
        }
    )


def resolved(market: str, t: int, winner: str) -> str:
    return text(
        {
            "id": "9000001",
            "market": market,
            "assets_ids": list(MARKETS[market].token_ids),
            "winning_asset_id": winner,
            "winning_outcome": "No",
            "event_message": None,
            "timestamp": str(T0 + t),
            "event_type": "market_resolved",
            "tags": [],
        }
    )


def new_market() -> str:
    return text(
        {"id": "9100001", "timestamp": str(T0 + 100), "event_type": "new_market"}
    )


def closed(market: Market, winner: str | None) -> MarketInfo:
    return MarketInfo(
        condition_id=market.condition_id,
        slug=market.slug,
        question=None,
        token_ids=market.token_ids,
        outcomes=("Yes", "No"),
        closed=True,
        end_date=None,
        winning_asset_id=winner,
    )


def open_market(market: Market) -> MarketInfo:
    return MarketInfo(
        condition_id=market.condition_id,
        slug=market.slug,
        question=None,
        token_ids=market.token_ids,
        outcomes=("Yes", "No"),
        closed=False,
        end_date=None,
    )


class Run:
    """Drives a machine as the client will: connections, frames decoded as
    they arrive, timers, and lookup answers, each at the run's time."""

    def __init__(
        self,
        *markets: Market,
        deliver_new_market: bool = False,
        can_look_up: bool = True,
    ) -> None:
        self.machine = StateMachine(
            repeat_window=1.0,
            deliver_new_market=deliver_new_market,
            can_look_up=can_look_up,
            markets=markets,
        )
        self.seconds = 0.0
        self.generation = 0
        self.frame_number = 0

    @property
    def now(self) -> datetime:
        return START + timedelta(seconds=self.seconds)

    def wait(self, seconds: float) -> None:
        self.seconds += seconds

    def connect(self, attempt: int = 1) -> None:
        self.machine.connecting(attempt, self.now)
        self.generation += 1
        self.frame_number = 0
        self.machine.opened(self.generation, self.now)
        self.machine.subscribed(self.machine.subscription(), self.now)

    def send(self, data: str | bytes) -> datetime:
        self.frame_number += 1
        items = decode_frame(
            data,
            received_at=self.now,
            connection=self.generation,
            frame=self.frame_number,
            keep_raw=False,
        )
        self.machine.frame(
            self.generation, items, received_at=self.now, clock=self.seconds
        )
        return self.now

    def start(self) -> None:
        """The conformance notation's start macro: connect, and send every
        desired token's opening book."""
        self.connect()
        self.send(opening(*self.machine.subscription()))

    def drop(self, cause: str = "dropped", **close: Any) -> None:
        self.machine.interrupted(cause, self.now, **close)
        self.machine.recovering(1, "backoff", self.now, retry_in=0.1)

    def book_timeout(self) -> None:
        self.machine.book_timeout(self.generation, self.now)

    def take(self) -> list[Record]:
        return self.machine.take()

    def summary(self) -> list[tuple[str, ...]]:
        return [describe(record) for record in self.take()]


def describe(record: Record) -> tuple[str, ...]:
    """A record in the conformance notation's terms."""
    match record:
        case TokenStateChange():
            return ("token", NAMES[record.token_id], record.state.value, record.reason)
        case ConnectionStateChange():
            return ("conn", record.state.value)
        case CaptureGap():
            return ("gap", NAMES[record.token_id], record.end)
        case BookEvent():
            return ("book", NAMES[record.asset_id])
        case PriceChangeEvent() | MarketResolvedEvent():
            return (record.event_type, NAMES[record.market])
        case BestBidAskEvent() | LastTradePriceEvent() | TickSizeChangeEvent():
            return (record.event_type, NAMES[record.asset_id])
        case NewMarketEvent():
            return ("new_market",)
        case UnknownEvent():
            return ("unknown",)
        case UndecodableFrame():
            return ("undecodable", record.reason)


def started(*markets: Market, **options: Any) -> Run:
    run = Run(*markets, **options)
    run.start()
    run.take()
    return run


def interrupted_and_back(run: Run, *tokens: str) -> None:
    """Drop the connection, reconnect, and send these tokens' opening books."""
    run.drop()
    run.wait(0.2)
    run.connect()
    run.send(opening(*tokens))


def sides(run: Run, token: str) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    held = run.machine.book(token)
    assert held is not None
    return (
        [(str(level.price), str(level.size)) for level in held.bids],
        [(str(level.price), str(level.size)) for level in held.asks],
    )


# The desired set.


def test_subscription_names_every_desired_token_in_order() -> None:
    machine = StateMachine(
        repeat_window=1.0,
        deliver_new_market=False,
        can_look_up=True,
        markets=[MARKET_B, MARKET_A],
    )
    assert machine.desired == (MARKET_B, MARKET_A)
    assert machine.subscription() == (B1, B2, A1, A2)


def test_subscribe_adds_to_the_end_and_leaves_a_desired_market_in_place() -> None:
    run = Run(MARKET_A)
    assert run.machine.subscribe([MARKET_B, MARKET_A]) is True
    assert run.machine.subscribe([MARKET_A]) is False
    assert run.machine.desired == (MARKET_A, MARKET_B)
    # Adding markets is not a record.
    assert run.take() == []


def test_subscribe_refuses_a_token_of_another_desired_market() -> None:
    run = Run(MARKET_A)
    thief = Market("0x" + "f" * 64, (A1,), "thief")
    with pytest.raises(ValueError, match="belongs to market"):
        run.machine.subscribe([MARKET_B, thief])
    assert run.machine.desired == (MARKET_A,)


def test_subscribe_refuses_a_token_listed_twice() -> None:
    with pytest.raises(ValueError, match="twice"):
        Run(Market(A, (A1, A1), "synthetic-a"))


# T1, T3: the first connection.


def test_t1_the_subscription_frame_makes_tokens_synchronizing() -> None:
    run = Run(MARKET_A)
    run.connect()
    records = run.take()
    assert [describe(r) for r in records] == [
        ("conn", "connecting"),
        ("conn", "open"),
        ("conn", "subscribed"),
        ("token", "A1", "synchronizing", "subscribed"),
        ("token", "A2", "synchronizing", "subscribed"),
    ]
    connecting, opened, subscribed, first, _ = records
    assert isinstance(connecting, ConnectionStateChange)
    assert (connecting.attempt, connecting.connection) == (1, None)
    assert isinstance(opened, ConnectionStateChange)
    assert opened.connection == 1
    assert isinstance(subscribed, ConnectionStateChange)
    assert subscribed.connection == 1
    assert first == TokenStateChange(
        token_id=A1,
        market=A,
        state=TokenState.SYNCHRONIZING,
        previous=None,
        reason="subscribed",
        at=START,
        connection=1,
        last_confirmed_at=None,
        winning_asset_id=None,
    )


def test_t3_opening_books_make_tokens_ready_in_frame_order() -> None:
    run = Run(MARKET_A, MARKET_B)
    run.connect()
    run.take()
    run.wait(0.3)
    run.send(opening(A1, A2, B1, B2))
    records = run.take()
    assert [describe(r) for r in records] == [
        ("book", "A1"),
        ("token", "A1", "ready", "book"),
        ("book", "A2"),
        ("token", "A2", "ready", "book"),
        ("book", "B1"),
        ("token", "B1", "ready", "book"),
        ("book", "B2"),
        ("token", "B2", "ready", "book"),
    ]
    first_book, first_ready = records[0], records[1]
    assert isinstance(first_book, BookEvent)
    assert (first_book.opening, first_book.held_book_matched, first_book.repeat) == (
        True,
        None,
        False,
    )
    assert isinstance(first_ready, TokenStateChange)
    assert (first_ready.previous, first_ready.at, first_ready.connection) == (
        TokenState.SYNCHRONIZING,
        run.now,
        1,
    )
    assert sides(run, A1) == (
        [("0.47", "250"), ("0.48", "100")],
        [("0.53", "300"), ("0.52", "120")],
    )


# T5, T6: a ready token.


def test_t6_entries_are_applied_in_arrival_order() -> None:
    run = started(MARKET_A)
    run.send(pc(A, 100, (A1, "BUY", "0.49", "50"), (A1, "SELL", "0.52", "0")))
    run.send(pc(A, 110, (A1, "BUY", "0.49", "60"), (A2, "SELL", "0.51", "50")))
    records = run.take()
    assert [describe(r) for r in records] == [
        ("price_change", "A"),
        ("price_change", "A"),
    ]
    for record in records:
        assert isinstance(record, PriceChangeEvent)
        assert all(c.applied and not c.before_book for c in record.changes)
    assert sides(run, A1) == (
        [("0.47", "250"), ("0.48", "100"), ("0.49", "60")],
        [("0.53", "300")],
    )
    assert run.machine.state(A1) is TokenState.READY


def test_t5_a_later_book_replaces_the_book_and_reports_whether_it_matched() -> None:
    run = started(MARKET_A)
    run.send(pc(A, 100, (A1, "BUY", "0.49", "50")))
    run.send(book(A1, 100, bids=[("0.47", "250"), ("0.48", "100"), ("0.49", "50")]))
    run.send(book(A1, 200, bids=[("0.46", "500")]))
    records = run.take()
    assert [describe(r) for r in records] == [
        ("price_change", "A"),
        ("book", "A1"),
        ("book", "A1"),
    ]
    matched, differs = records[1], records[2]
    assert isinstance(matched, BookEvent)
    assert (matched.held_book_matched, matched.opening) == (True, False)
    assert isinstance(differs, BookEvent)
    assert differs.held_book_matched is False
    assert sides(run, A1) == ([("0.46", "500")], [("0.53", "300"), ("0.52", "120")])
    assert run.machine.reason(A1) == "book"


# T4, T11, T12, T13: a missing book and settlement confirmation.


def test_t4_a_token_without_a_book_becomes_uncertain() -> None:
    run = Run(MARKET_A)
    run.connect()
    run.send(opening(A1))
    run.take()
    run.wait(1.0)
    run.book_timeout()
    records = run.take()
    assert [describe(r) for r in records] == [("token", "A2", "uncertain", "no_book")]
    no_book = records[0]
    assert isinstance(no_book, TokenStateChange)
    assert (no_book.previous, no_book.connection) == (TokenState.SYNCHRONIZING, 1)
    assert run.machine.take_effects() == [StartConfirmation(A, "synthetic-a")]
    assert run.machine.state(A1) is TokenState.READY


def test_t4_ignores_a_timeout_of_an_earlier_connection() -> None:
    run = Run(MARKET_A)
    run.connect()
    run.drop()
    run.connect()
    run.take()
    run.machine.book_timeout(1, run.now)
    assert run.take() == []
    assert run.machine.take_effects() == []


def test_t12_lookup_finds_no_market() -> None:
    run = Run(MARKET_U)
    run.connect()
    run.send(opening())
    run.book_timeout()
    run.take()
    assert run.machine.take_effects() == [StartConfirmation(U, "synthetic-u")]
    run.machine.confirmation(U, None, run.now)
    assert run.summary() == [
        ("token", "U1", "uncertain", "unknown_market"),
        ("token", "U2", "uncertain", "unknown_market"),
    ]
    assert run.machine.take_effects() == [EndConfirmation(U)]
    # The confirmation has ended: later answers change nothing.
    run.machine.confirmation(U, closed(MARKET_U, U1), run.now)
    run.machine.confirmation_timeout(U, run.now)
    assert run.take() == []


def test_t11_lookup_shows_the_market_closed() -> None:
    # settled-at-subscription
    run = Run(MARKET_S)
    run.connect()
    run.send(opening())
    run.wait(1.0)
    run.book_timeout()
    assert run.summary() == [
        ("conn", "connecting"),
        ("conn", "open"),
        ("conn", "subscribed"),
        ("token", "S1", "synchronizing", "subscribed"),
        ("token", "S2", "synchronizing", "subscribed"),
        ("token", "S1", "uncertain", "no_book"),
        ("token", "S2", "uncertain", "no_book"),
    ]
    run.machine.confirmation(S, open_market(MARKET_S), run.now)
    assert run.take() == []
    run.machine.confirmation(S, closed(MARKET_S, S2), run.now)
    records = run.take()
    assert [describe(r) for r in records] == [
        ("token", "S1", "settled", "lookup_closed"),
        ("token", "S2", "settled", "lookup_closed"),
    ]
    settled = records[0]
    assert isinstance(settled, TokenStateChange)
    assert (settled.previous, settled.winning_asset_id) == (TokenState.UNCERTAIN, S2)
    assert run.machine.take_effects() == [
        StartConfirmation(S, "synthetic-s"),
        EndConfirmation(S),
    ]
    assert run.machine.desired == ()
    # The client closes the connection, and the idle record follows its end.
    run.machine.closed(run.now)
    [idle] = run.take()
    assert isinstance(idle, ConnectionStateChange)
    assert (idle.state, idle.reason, idle.connection) == (
        ConnectionState.IDLE,
        "no_subscriptions",
        1,
    )
    assert run.machine.phase is ConnectionState.IDLE


def test_t11_settles_every_token_of_the_market_whatever_its_state() -> None:
    # settle-lookup-after-late-book
    run = Run(MARKET_A)
    run.connect()
    run.send(opening())
    run.book_timeout()
    run.send(book(A1))
    assert run.summary()[-4:] == [
        ("token", "A1", "uncertain", "no_book"),
        ("token", "A2", "uncertain", "no_book"),
        ("book", "A1"),
        ("token", "A1", "ready", "book"),
    ]
    run.machine.confirmation(A, closed(MARKET_A, A1), run.now)
    records = run.take()
    assert [describe(r) for r in records] == [
        ("token", "A1", "settled", "lookup_closed"),
        ("token", "A2", "settled", "lookup_closed"),
    ]
    ready_one = records[0]
    assert isinstance(ready_one, TokenStateChange)
    assert (ready_one.previous, ready_one.winning_asset_id) == (TokenState.READY, A1)
    # The market left the desired set; its events are discarded.
    run.send(pc(A, 300, (A1, "BUY", "0.49", "10")))
    assert run.take() == []
    assert run.machine.counts.discarded_outside == 1


def test_t11_ends_open_gaps_without_resuming() -> None:
    # settle-unannounced-drop
    run = started(MARKET_A)
    run.send(pc(A, 1000, (A1, "BUY", "0.48", "0"), (A2, "BUY", "0.48", "0")))
    last_frame = run.now
    run.wait(0.1)
    run.drop()
    interrupted_at = run.now
    run.wait(0.2)
    run.connect()
    run.send(opening())
    run.wait(1.0)
    run.book_timeout()
    run.take()
    run.wait(1.0)
    run.machine.confirmation(A, closed(MARKET_A, A2), run.now)
    records = run.take()
    assert [describe(r) for r in records] == [
        ("gap", "A1", "settled"),
        ("token", "A1", "settled", "lookup_closed"),
        ("gap", "A2", "settled"),
        ("token", "A2", "settled", "lookup_closed"),
    ]
    assert records[0] == CaptureGap(
        token_id=A1,
        market=A,
        cause="dropped",
        close_code=None,
        close_reason=None,
        last_confirmed_at=last_frame,
        detected_at=interrupted_at,
        resumed_at=None,
        end="settled",
        connection_before=1,
        connection_after=None,
        held_book_matched=None,
        at=run.now,
    )
    run.machine.closed(run.now)
    [idle] = run.take()
    assert isinstance(idle, ConnectionStateChange)
    assert idle.connection == 2


def test_t13_settlement_unconfirmed_after_the_timeout() -> None:
    # settlement-unconfirmed, with one late book: only tokens still without a
    # book become settlement_unconfirmed.
    run = Run(MARKET_A)
    run.connect()
    run.send(opening())
    run.book_timeout()
    run.send(book(A1))
    run.take()
    run.wait(3.0)
    run.machine.confirmation_timeout(A, run.now)
    assert run.summary() == [("token", "A2", "uncertain", "settlement_unconfirmed")]
    assert run.machine.take_effects() == [
        StartConfirmation(A, "synthetic-a"),
        EndConfirmation(A),
    ]
    assert run.machine.state(A1) is TokenState.READY


def test_t13_at_once_for_a_market_without_a_slug() -> None:
    # settlement-without-slug
    run = Run(MARKET_N)
    run.connect()
    run.send(opening())
    run.take()
    run.book_timeout()
    assert run.summary() == [
        ("token", "N1", "uncertain", "no_book"),
        ("token", "N2", "uncertain", "no_book"),
        ("token", "N1", "uncertain", "settlement_unconfirmed"),
        ("token", "N2", "uncertain", "settlement_unconfirmed"),
    ]
    assert run.machine.take_effects() == []


def test_t13_at_once_when_no_lookup_is_available() -> None:
    run = Run(MARKET_A, can_look_up=False)
    run.connect()
    run.send(opening())
    run.take()
    run.book_timeout()
    assert [r[3] for r in run.summary()] == [
        "no_book",
        "no_book",
        "settlement_unconfirmed",
        "settlement_unconfirmed",
    ]
    assert run.machine.take_effects() == []


def test_t4_joins_a_running_confirmation_and_the_next_one_starts_afresh() -> None:
    # settlement-confirmation-across-reconnect
    run = Run(MARKET_A)
    run.connect()
    run.send(opening())
    run.book_timeout()
    assert run.machine.take_effects() == [StartConfirmation(A, "synthetic-a")]
    run.take()
    run.drop()
    assert run.summary() == [
        ("conn", "interrupted"),
        ("token", "A1", "uncertain", "interrupted"),
        ("token", "A2", "uncertain", "interrupted"),
        ("conn", "recovering"),
    ]
    run.connect()
    run.send(opening())
    run.book_timeout()
    assert run.summary()[-2:] == [
        ("token", "A1", "uncertain", "no_book"),
        ("token", "A2", "uncertain", "no_book"),
    ]
    # It joined the confirmation already running: no new lookup.
    assert run.machine.take_effects() == []
    run.machine.confirmation_timeout(A, run.now)
    assert run.summary() == [
        ("token", "A1", "uncertain", "settlement_unconfirmed"),
        ("token", "A2", "uncertain", "settlement_unconfirmed"),
    ]
    assert run.machine.take_effects() == [EndConfirmation(A)]
    run.drop()
    run.connect()
    run.send(opening())
    run.book_timeout()
    assert run.machine.take_effects() == [StartConfirmation(A, "synthetic-a")]
    run.take()
    run.machine.confirmation(A, closed(MARKET_A, A2), run.now)
    assert run.summary() == [
        ("token", "A1", "settled", "lookup_closed"),
        ("token", "A2", "settled", "lookup_closed"),
    ]
    run.machine.closed(run.now)
    assert run.summary() == [("conn", "idle")]


# T14, T2, T3 with a gap: interruption and recovery.


def test_t14_an_interruption_makes_the_connection_s_tokens_uncertain() -> None:
    run = started(MARKET_A)
    run.wait(0.5)
    run.send(pc(A, 100, (A1, "BUY", "0.49", "50")))
    run.wait(0.4)
    run.machine.pong(run.generation, run.now)
    confirmed = run.now
    run.wait(0.3)
    run.take()
    run.machine.interrupted(
        "close_frame", run.now, close_code=1001, close_reason="going away"
    )
    records = run.take()
    assert [describe(r) for r in records] == [
        ("conn", "interrupted"),
        ("token", "A1", "uncertain", "interrupted"),
        ("token", "A2", "uncertain", "interrupted"),
    ]
    assert records[0] == ConnectionStateChange(
        state=ConnectionState.INTERRUPTED,
        at=run.now,
        connection=1,
        attempt=None,
        reason="close_frame",
        detail=None,
        close_code=1001,
        close_reason="going away",
        retry_in=None,
        last_confirmed_at=confirmed,
    )
    token = records[1]
    assert isinstance(token, TokenStateChange)
    assert (token.previous, token.connection, token.last_confirmed_at) == (
        TokenState.READY,
        1,
        confirmed,
    )
    assert run.machine.counts.interruptions == {"close_frame": 1}


def test_t14_nothing_more_from_the_connection_is_delivered_or_applied() -> None:
    run = started(MARKET_A)
    run.drop()
    run.take()
    run.send(pc(A, 100, (A1, "BUY", "0.49", "50")))
    frames = run.machine.counts.frames
    run.machine.pong(run.generation, run.now)
    assert run.take() == []
    # The PONG is counted with the frame, though ``frames`` leaves it out.
    assert run.machine.counts.frames_after_interruption == 2
    assert run.machine.counts.frames == frames
    assert sides(run, A1)[0] == [("0.47", "250"), ("0.48", "100")]


def test_t14_then_t2_and_t3_end_the_gap_with_the_fresh_book() -> None:
    # drop-without-close
    run = started(MARKET_A)
    run.send(pc(A, 100, (A1, "BUY", "0.49", "50")))
    last_frame = run.now
    run.wait(0.2)
    run.drop()
    detected = run.now
    run.wait(0.3)
    run.connect()
    run.wait(0.2)
    changed = opening_book(
        A1,
        2000,
        bids=[("0.46", "500"), ("0.47", "250"), ("0.48", "100"), ("0.49", "50")],
    )
    run.send(text([changed, opening_book(A2)]))
    records = run.take()
    assert [describe(r) for r in records] == [
        ("price_change", "A"),
        ("conn", "interrupted"),
        ("token", "A1", "uncertain", "interrupted"),
        ("token", "A2", "uncertain", "interrupted"),
        ("conn", "recovering"),
        ("conn", "connecting"),
        ("conn", "open"),
        ("conn", "subscribed"),
        ("token", "A1", "synchronizing", "subscribed"),
        ("token", "A2", "synchronizing", "subscribed"),
        ("book", "A1"),
        ("gap", "A1", "book"),
        ("token", "A1", "ready", "book"),
        ("book", "A2"),
        ("gap", "A2", "book"),
        ("token", "A2", "ready", "book"),
    ]
    synchronizing = records[8]
    assert isinstance(synchronizing, TokenStateChange)
    assert (synchronizing.previous, synchronizing.connection) == (
        TokenState.UNCERTAIN,
        2,
    )
    first_book, first_gap = records[10], records[11]
    assert isinstance(first_book, BookEvent)
    assert first_book.held_book_matched is False
    assert first_gap == CaptureGap(
        token_id=A1,
        market=A,
        cause="dropped",
        close_code=None,
        close_reason=None,
        last_confirmed_at=last_frame,
        detected_at=detected,
        resumed_at=run.now,
        end="book",
        connection_before=1,
        connection_after=2,
        held_book_matched=False,
        at=run.now,
    )
    second_gap = records[14]
    assert isinstance(second_gap, CaptureGap)
    assert second_gap.held_book_matched is True


def test_t14_a_token_without_a_book_opens_no_gap() -> None:
    # subscribe-before-first-frame: interrupted before any frame arrived.
    run = Run(MARKET_A)
    run.connect()
    run.take()
    run.machine.interrupted("subscription_change", run.now)
    records = run.take()
    interruption = records[0]
    assert isinstance(interruption, ConnectionStateChange)
    assert interruption.last_confirmed_at is None
    assert [describe(r) for r in records[1:]] == [
        ("token", "A1", "uncertain", "interrupted"),
        ("token", "A2", "uncertain", "interrupted"),
    ]
    run.machine.recovering(1, "subscription_change", run.now, retry_in=0)
    run.connect()
    run.send(opening(A1, A2))
    assert run.summary()[-4:] == [
        ("book", "A1"),
        ("token", "A1", "ready", "book"),
        ("book", "A2"),
        ("token", "A2", "ready", "book"),
    ]


def test_t14_an_open_gap_stays_open_across_another_interruption() -> None:
    # recovery-exhausted-no-frame, then T18
    run = started(MARKET_A)
    run.drop()
    run.connect()
    run.take()
    run.machine.interrupted("dropped", run.now)
    assert run.summary() == [
        ("conn", "interrupted"),
        ("token", "A1", "uncertain", "interrupted"),
        ("token", "A2", "uncertain", "interrupted"),
    ]
    run.machine.failed("max_attempts", run.now)
    records = run.take()
    assert [describe(r) for r in records] == [
        ("gap", "A1", "recovery_failed"),
        ("gap", "A2", "recovery_failed"),
        ("conn", "failed"),
    ]
    gap = records[0]
    assert isinstance(gap, CaptureGap)
    assert (gap.connection_before, gap.connection_after, gap.resumed_at) == (
        1,
        None,
        None,
    )


def test_t14_an_uncertain_token_s_reason_becomes_interrupted() -> None:
    run = Run(MARKET_A)
    run.connect()
    run.send(opening())
    run.book_timeout()
    run.take()
    run.machine.interrupted("dropped", run.now)
    records = run.take()
    token = records[1]
    assert isinstance(token, TokenStateChange)
    assert (token.previous, token.reason) == (TokenState.UNCERTAIN, "interrupted")


def test_t18_reconnection_exhausts_its_bounds() -> None:
    # recovery-exhausted-attempts
    run = started(MARKET_A)
    run.drop()
    run.machine.connecting(1, run.now)
    run.machine.recovering(2, "backoff", run.now, retry_in=0.2, detail="HTTP 503")
    run.take()
    run.machine.failed("max_attempts", run.now)
    records = run.take()
    assert [describe(r) for r in records] == [
        ("gap", "A1", "recovery_failed"),
        ("gap", "A2", "recovery_failed"),
        ("conn", "failed"),
    ]
    gap, _, failed = records
    assert isinstance(gap, CaptureGap)
    assert (gap.cause, gap.resumed_at, gap.connection_after, gap.held_book_matched) == (
        "dropped",
        None,
        None,
        None,
    )
    assert failed == ConnectionStateChange(
        state=ConnectionState.FAILED,
        at=run.now,
        connection=None,
        attempt=None,
        reason="max_attempts",
        detail=None,
        close_code=None,
        close_reason=None,
        retry_in=None,
        last_confirmed_at=None,
    )
    # Tokens keep their last state.
    assert run.machine.state(A1) is TokenState.UNCERTAIN
    assert run.machine.reason(A1) == "interrupted"


def test_recovering_and_connecting_records() -> None:
    run = started(MARKET_A)
    run.machine.interrupted("dropped", run.now)
    run.machine.recovering(1, "backoff", run.now, retry_in=0.1)
    run.machine.connecting(1, run.now)
    run.machine.recovering(2, "backoff", run.now, retry_in=0.2, detail="HTTP 503")
    records = run.take()[3:]
    assert records == [
        ConnectionStateChange(
            ConnectionState.RECOVERING,
            run.now,
            None,
            1,
            "backoff",
            None,
            None,
            None,
            0.1,
            None,
        ),
        ConnectionStateChange(
            ConnectionState.CONNECTING,
            run.now,
            None,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        ConnectionStateChange(
            ConnectionState.RECOVERING,
            run.now,
            None,
            2,
            "backoff",
            "HTTP 503",
            None,
            None,
            0.2,
            None,
        ),
    ]


# T15, T16: settlement by the stream.


def test_t15_market_resolved_settles_its_market() -> None:
    # settle-announced-others-open
    run = started(MARKET_A, MARKET_B)
    run.send(resolved(A, 1001, A2))
    records = run.take()
    assert [describe(r) for r in records] == [
        ("market_resolved", "A"),
        ("token", "A1", "settled", "market_resolved"),
        ("token", "A2", "settled", "market_resolved"),
    ]
    settled = records[1]
    assert isinstance(settled, TokenStateChange)
    assert (settled.previous, settled.winning_asset_id) == (TokenState.READY, A2)
    assert run.machine.desired == (MARKET_B,)
    # A repeated announcement is discarded as outside the desired set, before
    # repeat detection.
    run.send(resolved(A, 1001, A2))
    assert run.take() == []
    assert run.machine.counts.discarded_outside == 1
    assert run.machine.counts.repeats == 0
    # A later reconnect does not subscribe it.
    assert run.machine.subscription() == (B1, B2)


def test_t15_the_last_market_goes_idle() -> None:
    # settle-all-resolved-close
    run = started(MARKET_A)
    run.send(resolved(A, 1001, A1))
    run.machine.all_resolved(run.now)
    records = run.take()
    assert [describe(r) for r in records] == [
        ("market_resolved", "A"),
        ("token", "A1", "settled", "market_resolved"),
        ("token", "A2", "settled", "market_resolved"),
        ("conn", "idle"),
    ]
    idle = records[-1]
    assert isinstance(idle, ConnectionStateChange)
    assert (idle.reason, idle.connection) == ("no_subscriptions", 1)
    assert run.machine.counts.interruptions == {}


def test_t15_ends_an_open_gap() -> None:
    run = started(MARKET_A)
    run.drop()
    run.connect()
    run.take()
    run.send(resolved(A, 1001, A1))
    assert run.summary() == [
        ("market_resolved", "A"),
        ("gap", "A1", "settled"),
        ("token", "A1", "settled", "market_resolved"),
        ("gap", "A2", "settled"),
        ("token", "A2", "settled", "market_resolved"),
    ]
    run.machine.closed(run.now)
    assert run.summary() == [("conn", "idle")]


def test_the_idle_record_follows_the_frames_of_the_connection_the_client_closes() -> (
    None
):
    # What arrives while the client closes the connection the last market
    # left is handled as before, and one idle record follows the end, even
    # after a market has been added.
    run = started(MARKET_A)
    run.send(resolved(A, 1001, A1))
    run.take()
    run.send('{"event_type":"something_new","market":"x"}')
    run.send(pc(A, 1002, (A1, "BUY", "0.49", "10")))
    run.machine.subscribe([MARKET_B])
    assert run.summary() == [("unknown",)]
    assert run.machine.counts.discarded_outside == 1
    run.machine.closed(run.now)
    records = run.take()
    assert [describe(r) for r in records] == [("conn", "idle")]
    idle = records[0]
    assert isinstance(idle, ConnectionStateChange)
    assert idle.connection == 1
    run.machine.closed(run.now)
    assert run.take() == []
    run.connect()
    assert run.summary() == [
        ("conn", "connecting"),
        ("conn", "open"),
        ("conn", "subscribed"),
        ("token", "B1", "synchronizing", "subscribed"),
        ("token", "B2", "synchronizing", "subscribed"),
    ]


def test_the_last_market_leaving_before_the_subscription_frame_idles_at_the_end() -> (
    None
):
    # A confirmation outlives its connection, so lookup can settle the last
    # market while the next connection's subscription frame is being sent.
    run = Run(MARKET_A)
    run.connect()
    run.send(opening())
    run.book_timeout()
    run.drop()
    run.machine.connecting(1, run.now)
    run.generation += 1
    run.machine.opened(run.generation, run.now)
    run.take()
    run.machine.confirmation(A, closed(MARKET_A, A2), run.now)
    assert run.summary() == [
        ("token", "A1", "settled", "lookup_closed"),
        ("token", "A2", "settled", "lookup_closed"),
    ]
    run.machine.closed(run.now)
    records = run.take()
    assert [describe(r) for r in records] == [("conn", "idle")]
    idle = records[0]
    assert isinstance(idle, ConnectionStateChange)
    assert idle.connection == 2


def test_t16_the_all_resolved_close_settles_every_token_on_the_connection() -> None:
    # settle-all-resolved-close-unannounced
    run = started(MARKET_A)
    run.machine.all_resolved(run.now)
    records = run.take()
    assert [describe(r) for r in records] == [
        ("conn", "ended"),
        ("token", "A1", "settled", "all_resolved_close"),
        ("token", "A2", "settled", "all_resolved_close"),
        ("conn", "idle"),
    ]
    ended, settled, _, idle = records
    assert isinstance(ended, ConnectionStateChange)
    assert (ended.connection, ended.close_code, ended.close_reason) == (
        1,
        1000,
        "all subscribed assets resolved",
    )
    assert isinstance(settled, TokenStateChange)
    assert (settled.previous, settled.winning_asset_id) == (TokenState.READY, None)
    assert isinstance(idle, ConnectionStateChange)
    assert idle.connection == 1
    assert run.machine.counts.interruptions == {}


def test_t16_leaves_desired_markets_that_were_not_on_the_connection() -> None:
    run = started(MARKET_A)
    run.machine.subscribe([MARKET_B])
    run.machine.all_resolved(run.now)
    assert run.summary() == [
        ("conn", "ended"),
        ("token", "A1", "settled", "all_resolved_close"),
        ("token", "A2", "settled", "all_resolved_close"),
    ]
    assert run.machine.desired == (MARKET_B,)
    assert run.machine.phase is ConnectionState.ENDED


# T17: removal.


def test_t17_removing_a_market() -> None:
    # unsubscribe
    run = started(MARKET_A, MARKET_B)
    run.machine.unsubscribe([A, "0x" + "e" * 64], run.now)
    records = run.take()
    assert [describe(r) for r in records] == [
        ("token", "A1", "removed", "removed"),
        ("token", "A2", "removed", "removed"),
    ]
    removed = records[0]
    assert isinstance(removed, TokenStateChange)
    assert (removed.previous, removed.connection) == (TokenState.READY, 1)
    run.send(pc(A, 100, (A1, "BUY", "0.49", "10")))
    run.send(pc(B, 100, (B1, "BUY", "0.39", "90")))
    assert run.summary() == [("price_change", "B")]
    assert run.machine.counts.discarded_outside == 1
    assert run.machine.subscription() == (B1, B2)


def test_t17_ends_an_open_gap_and_idles_during_recovery() -> None:
    run = started(MARKET_A)
    run.drop()
    run.take()
    run.machine.unsubscribe([A], run.now)
    records = run.take()
    assert [describe(r) for r in records] == [
        ("gap", "A1", "removed"),
        ("token", "A1", "removed", "removed"),
        ("gap", "A2", "removed"),
        ("token", "A2", "removed", "removed"),
        ("conn", "idle"),
    ]
    idle = records[-1]
    assert isinstance(idle, ConnectionStateChange)
    # No connection is open during recovery.
    assert idle.connection is None


def test_t17_removing_every_market_goes_idle() -> None:
    # unsubscribe-all-then-subscribe
    run = started(MARKET_A)
    run.machine.unsubscribe([A], run.now)
    assert run.summary() == [
        ("token", "A1", "removed", "removed"),
        ("token", "A2", "removed", "removed"),
    ]
    run.machine.closed(run.now)
    assert run.summary() == [("conn", "idle")]
    run.machine.subscribe([MARKET_B])
    run.connect()
    assert run.summary()[-2:] == [
        ("token", "B1", "synchronizing", "subscribed"),
        ("token", "B2", "synchronizing", "subscribed"),
    ]


def test_t17_a_market_no_subscription_frame_named_has_no_records() -> None:
    run = started(MARKET_A)
    run.machine.subscribe([MARKET_B])
    run.machine.unsubscribe([B], run.now)
    assert run.take() == []
    assert run.machine.desired == (MARKET_A,)


def test_only_an_addition_after_the_subscription_frame_needs_a_new_connection() -> None:
    # D7: an addition reconnects; a removal does not.
    run = started(MARKET_A, MARKET_B)
    assert not run.machine.outdated()
    run.machine.subscribe([MARKET_S])
    assert run.machine.outdated()
    # Undone before it was applied, it needs nothing.
    run.machine.unsubscribe([S], run.now)
    assert not run.machine.outdated()
    run.machine.unsubscribe([A], run.now)
    assert not run.machine.outdated()
    # A market added again is outside the desired set until a subscription
    # frame names it.
    run.machine.subscribe([MARKET_A])
    assert run.machine.outdated()
    run.machine.interrupted("subscription_change", run.now)
    run.machine.recovering(1, "subscription_change", run.now, retry_in=0)
    run.connect()
    assert not run.machine.outdated()


def test_t1_a_market_added_again_starts_over() -> None:
    # resubscribe-removed
    run = started(MARKET_A, MARKET_B)
    run.machine.unsubscribe([A], run.now)
    run.take()
    assert run.machine.subscribe([MARKET_A]) is True
    assert run.machine.state(A1) is TokenState.REMOVED
    run.machine.interrupted("subscription_change", run.now)
    # Its tokens take no part in the interruption.
    assert run.summary() == [
        ("conn", "interrupted"),
        ("token", "B1", "uncertain", "interrupted"),
        ("token", "B2", "uncertain", "interrupted"),
    ]
    run.machine.recovering(1, "subscription_change", run.now, retry_in=0)
    assert run.machine.subscription() == (B1, B2, A1, A2)
    run.connect()
    records = run.take()[-4:]
    assert [describe(r) for r in records] == [
        ("token", "B1", "synchronizing", "subscribed"),
        ("token", "B2", "synchronizing", "subscribed"),
        ("token", "A1", "synchronizing", "subscribed"),
        ("token", "A2", "synchronizing", "subscribed"),
    ]
    again = records[2]
    assert isinstance(again, TokenStateChange)
    assert (again.previous, again.connection) == (TokenState.REMOVED, 2)
    run.send(opening(B1, B2, A1, A2))
    records = run.take()
    assert [describe(r) for r in records] == [
        ("book", "B1"),
        ("gap", "B1", "book"),
        ("token", "B1", "ready", "book"),
        ("book", "B2"),
        ("gap", "B2", "book"),
        ("token", "B2", "ready", "book"),
        ("book", "A1"),
        ("token", "A1", "ready", "book"),
        ("book", "A2"),
        ("token", "A2", "ready", "book"),
    ]
    a1_book = records[6]
    assert isinstance(a1_book, BookEvent)
    # Nothing held from before to compare it with.
    assert a1_book.held_book_matched is None


def test_t1_a_settled_market_added_again_starts_over() -> None:
    run = started(MARKET_A)
    run.send(resolved(A, 1001, A1))
    run.take()
    run.machine.subscribe([MARKET_A])
    assert run.machine.book(A1) is None
    run.connect()
    records = run.take()
    first = records[-2]
    assert isinstance(first, TokenStateChange)
    assert (first.previous, first.state) == (
        TokenState.SETTLED,
        TokenState.SYNCHRONIZING,
    )


@pytest.mark.parametrize("left", [TokenState.REMOVED, TokenState.SETTLED])
def test_a_market_added_again_is_outside_until_subscribed(left: TokenState) -> None:
    # A late market_resolved for it is discarded, so the market stays in the
    # desired set and the next subscription frame names its tokens.
    run = started(MARKET_A, MARKET_B)
    if left is TokenState.REMOVED:
        run.machine.unsubscribe([A], run.now)
    else:
        run.send(resolved(A, 1001, A2))
    run.machine.subscribe([MARKET_A])
    run.take()
    run.wait(2.0)  # past repeat_window
    run.send(resolved(A, 1001, A2))
    assert run.take() == []
    assert run.machine.counts.discarded_outside == 1
    assert run.machine.desired == (MARKET_B, MARKET_A)
    assert run.machine.subscription() == (B1, B2, A1, A2)
    assert run.machine.state(A1) is left
    # Once a subscription frame names its tokens, a resolution settles it.
    interrupted_and_back(run, B1, B2, A1, A2)
    run.take()
    run.send(resolved(A, 1002, A2))
    assert run.machine.state(A1) is TokenState.SETTLED
    assert run.machine.subscription() == (B1, B2)


# T8, T9, T10, T7: undecodable frames and events, and hash results.


def test_t8_a_frame_that_cannot_be_attributed_puts_every_ready_book_in_doubt() -> None:
    # malformed-frames
    run = started(MARKET_A, MARKET_B)
    run.send("not json")
    records = run.take()
    assert [describe(r) for r in records] == [
        ("undecodable", "invalid_json"),
        ("token", "A1", "uncertain", "undecodable"),
        ("token", "A2", "uncertain", "undecodable"),
        ("token", "B1", "uncertain", "undecodable"),
        ("token", "B2", "uncertain", "undecodable"),
    ]
    undecodable = records[0]
    assert isinstance(undecodable, UndecodableFrame)
    assert undecodable.affected == (A1, A2, B1, B2)
    run.send(book(A1))
    assert run.summary() == [("book", "A1"), ("token", "A1", "ready", "book")]
    run.send(b"\x00\xff")
    records = run.take()
    binary = records[0]
    assert isinstance(binary, UndecodableFrame)
    assert binary.affected == (A1,)
    run.send("42")
    run.send("[7]")
    records = run.take()
    assert [r.affected for r in records if isinstance(r, UndecodableFrame)] == [(), ()]
    assert len(records) == 2


def test_t8_an_invalid_event_affects_the_tokens_it_names() -> None:
    # invalid-known-event
    run = started(MARKET_A, MARKET_B)
    bad_book = book(A1).replace('"price":"0.47"', '"price":"abc"')
    run.send(bad_book)
    records = run.take()
    assert [describe(r) for r in records] == [
        ("undecodable", "invalid_event"),
        ("token", "A1", "uncertain", "undecodable"),
    ]
    no_token = pc(B, 130, (B1, "BUY", "0.39", "1")).replace(f'"asset_id":"{B1}",', "")
    run.send(no_token)
    records = run.take()
    undecodable = records[0]
    assert isinstance(undecodable, UndecodableFrame)
    assert undecodable.affected == (B1, B2)
    # Types that change no book affect nothing.
    run.send(bba(A2, 120).replace('"best_bid":"0.48"', '"best_bid":"x"'))
    records = run.take()
    assert [describe(r) for r in records] == [("undecodable", "invalid_event")]
    assert isinstance(records[0], UndecodableFrame)
    assert records[0].affected == ()


def test_t10_a_book_restores_an_uncertain_token() -> None:
    run = started(MARKET_A)
    run.send("not json")
    run.take()
    run.send(book(A1))
    records = run.take()
    assert [describe(r) for r in records] == [
        ("book", "A1"),
        ("token", "A1", "ready", "book"),
    ]
    ready = records[1]
    assert isinstance(ready, TokenStateChange)
    assert ready.previous is TokenState.UNCERTAIN


def test_t9_a_later_burst_s_verifying_check_restores_the_token() -> None:
    # hash-check-predates-undecodable
    run = started(MARKET_A)
    run.send(pc(A, 100, (A1, "BUY", "0.49", "50")))
    run.send("not json")  # frame 3
    run.take()
    # A burst whose last entry came in frame 2, before the undecodable frame,
    # predates any change it may have carried.
    run.machine.hash_verified(A1, (1, 2, 0), run.now)
    assert run.take() == []
    run.send(pc(A, 200, (A1, "BUY", "0.49", "60")))  # frame 4
    run.take()
    run.machine.hash_verified(A1, (1, 4, 0), run.now)
    records = run.take()
    assert [describe(r) for r in records] == [("token", "A1", "ready", "hash_verified")]
    assert isinstance(records[0], TokenStateChange)
    assert records[0].previous is TokenState.UNCERTAIN


def test_t9_counts_a_frame_s_items_in_their_order() -> None:
    run = started(MARKET_A)
    entry = pc_object(A, 100, (A1, "BUY", "0.49", "50"))
    run.send(text([entry, 7, entry]))  # frame 2: item 1 is undecodable
    run.take()
    run.machine.hash_verified(A1, (1, 2, 0), run.now)
    assert run.take() == []
    run.machine.hash_verified(A1, (1, 2, 2), run.now)
    assert run.summary() == [("token", "A1", "ready", "hash_verified")]


def test_t7_divergence_makes_a_ready_token_uncertain() -> None:
    # hash-divergence
    run = started(MARKET_A)
    run.send(pc(A, 300, (A1, "BUY", "0.49", "70")))
    run.machine.hash_diverged(A1, run.now)
    assert run.summary() == [
        ("price_change", "A"),
        ("token", "A1", "uncertain", "hash_mismatch"),
    ]
    # Entries are still applied, and the next book restores it (T10).
    run.send(pc(A, 400, (A1, "BUY", "0.49", "80")))
    run.send(book(A1, 400))
    records = run.take()
    change = records[0]
    assert isinstance(change, PriceChangeEvent)
    assert change.changes[0].applied
    assert [describe(r) for r in records[1:]] == [
        ("book", "A1"),
        ("token", "A1", "ready", "book"),
    ]


def test_t9_restores_a_token_after_divergence() -> None:
    run = started(MARKET_A)
    run.machine.hash_diverged(A1, run.now)
    run.take()
    run.send(pc(A, 300, (A1, "BUY", "0.49", "70")))
    run.take()
    run.machine.hash_verified(A1, (1, 2, 0), run.now)
    assert run.summary() == [("token", "A1", "ready", "hash_verified")]


def test_hash_results_change_nothing_for_other_states() -> None:
    run = Run(MARKET_A)
    run.connect()
    run.send(opening())
    run.book_timeout()
    run.take()
    run.machine.hash_diverged(A1, run.now)
    run.machine.hash_verified(A1, (1, 9, 0), run.now)
    run.machine.hash_diverged("999", run.now)
    assert run.take() == []
    ready = started(MARKET_A)
    ready.machine.hash_verified(A1, (1, 9, 0), ready.now)
    assert ready.take() == []


# Cases that change nothing about a token's state.


def test_an_entry_for_a_token_with_no_book_is_delivered_not_applied() -> None:
    # change-before-any-book
    run = Run(MARKET_A)
    run.connect()
    run.take()
    run.send(pc(A, -29000, (A1, "BUY", "0.49", "50")))
    records = run.take()
    change = records[0]
    assert isinstance(change, PriceChangeEvent)
    assert (change.changes[0].applied, change.changes[0].before_book) == (False, False)
    assert run.machine.state(A1) is TokenState.SYNCHRONIZING
    run.send(opening(A1, A2))
    first = run.take()[0]
    assert isinstance(first, BookEvent)
    assert first.held_book_matched is None


def test_an_entry_for_a_token_without_a_book_after_t4_is_not_applied() -> None:
    run = Run(MARKET_A)
    run.connect()
    run.send(opening(A1))
    run.book_timeout()
    run.take()
    run.send(pc(A, 100, (A1, "BUY", "0.49", "50"), (A2, "BUY", "0.49", "50")))
    change = run.take()[0]
    assert isinstance(change, PriceChangeEvent)
    assert [c.applied for c in change.changes] == [True, False]


def test_an_entry_for_a_token_uncertain_for_undecodable_is_applied() -> None:
    run = started(MARKET_A)
    run.send("not json")
    run.take()
    run.send(pc(A, 100, (A1, "BUY", "0.49", "50")))
    records = run.take()
    assert [describe(r) for r in records] == [("price_change", "A")]
    change = records[0]
    assert isinstance(change, PriceChangeEvent)
    assert change.changes[0].applied
    assert ("0.49", "50") in sides(run, A1)[0]


def test_a_repeat_is_delivered_flagged_and_not_applied() -> None:
    # repeated-messages
    run = started(MARKET_A)
    first = pc_object(A, 100, (A1, "BUY", "0.49", "50"), (A2, "SELL", "0.51", "50"))
    run.send(text(first))
    run.wait(0.05)
    run.send(pc(A, 105, (A1, "BUY", "0.49", "70")))
    run.wait(0.05)
    reversed_entries = dict(first, price_changes=first["price_changes"][::-1])
    run.send(text(reversed_entries))
    records = run.take()
    assert [describe(r) for r in records] == [("price_change", "A")] * 3
    again = records[2]
    assert isinstance(again, PriceChangeEvent)
    assert again.repeat
    assert not any(c.applied for c in again.changes)
    # The later change at the same level stands.
    assert ("0.49", "70") in sides(run, A1)[0]
    run.send(bba(A1, 110))
    run.send(bba(A1, 110))
    run.wait(1.2)
    run.send(bba(A1, 110))
    flags = [r.repeat for r in run.take() if isinstance(r, BestBidAskEvent)]
    assert flags == [False, True, False]
    assert run.machine.counts.repeats == 2
    assert run.machine.state(A1) is TokenState.READY


def test_a_repeated_book_is_not_applied() -> None:
    run = started(MARKET_A)
    changed = book(A1, 100, bids=[("0.40", "1")])
    run.send(changed)
    run.send(pc(A, 110, (A1, "BUY", "0.41", "2")))
    run.send(changed)
    records = run.take()
    repeated = records[-1]
    assert isinstance(repeated, BookEvent)
    assert repeated.repeat
    assert ("0.41", "2") in sides(run, A1)[0]


def test_an_entry_stamped_before_the_opening_book_is_applied_and_flagged() -> None:
    # change-before-book
    run = started(MARKET_A)
    run.send(pc(A, -30001, (A1, "BUY", "0.48", "100")))
    run.send(pc(A, 50, (A1, "BUY", "0.47", "200")))
    run.send(book(A1, 50, bids=[("0.47", "200"), ("0.48", "100")]))
    records = run.take()
    early, late, later_book = records
    assert isinstance(early, PriceChangeEvent)
    assert (early.changes[0].applied, early.changes[0].before_book) == (True, True)
    assert isinstance(late, PriceChangeEvent)
    assert (late.changes[0].applied, late.changes[0].before_book) == (True, False)
    # A later book is not the opening book.
    run.send(pc(A, 40, (A1, "BUY", "0.46", "1")))
    later = run.take()[0]
    assert isinstance(later, PriceChangeEvent)
    assert later.changes[0].before_book is False
    assert isinstance(later_book, BookEvent)
    assert later_book.held_book_matched is True
    assert run.machine.state(A1) is TokenState.READY


def test_an_emptied_book_changes_nothing() -> None:
    run = started(MARKET_A)
    run.send(
        pc(
            A,
            1000,
            (A1, "BUY", "0.48", "0"),
            (A1, "BUY", "0.47", "0"),
            (A1, "SELL", "0.52", "0"),
            (A1, "SELL", "0.53", "0"),
        )
    )
    assert run.summary() == [("price_change", "A")]
    assert sides(run, A1) == ([], [])
    assert run.machine.state(A1) is TokenState.READY


def test_events_of_other_types_change_nothing() -> None:
    # market-event-types and out-of-order-types
    run = started(MARKET_A)
    run.send(pc(A, 200, (A1, "BUY", "0.49", "50")))
    run.send(bba(A1, 150))
    run.send(ltp(A1, 120))
    run.send(tsc(A1, 200))
    run.send('{"event_type":"something_new","market":"x"}')
    assert run.summary() == [
        ("price_change", "A"),
        ("best_bid_ask", "A1"),
        ("last_trade_price", "A1"),
        ("tick_size_change", "A1"),
        ("unknown",),
    ]
    assert run.machine.state(A1) is TokenState.READY
    assert run.machine.counts.unknown == 1


def test_a_pong_changes_nothing() -> None:
    run = started(MARKET_A)
    run.machine.pong(run.generation, run.now)
    assert run.take() == []


def test_a_pong_from_an_earlier_connection_confirms_nothing() -> None:
    run = started(MARKET_A)
    run.drop()
    run.connect()
    run.take()
    run.wait(1.0)
    run.machine.pong(1, run.now)
    run.machine.interrupted("dropped", run.now)
    interruption = run.take()[0]
    assert isinstance(interruption, ConnectionStateChange)
    assert interruption.last_confirmed_at is None


def test_events_outside_the_desired_set_are_discarded_before_repeat_detection() -> None:
    run = started(MARKET_A, MARKET_B)
    run.machine.unsubscribe([A], run.now)
    run.take()
    for data in (pc(A, 100, (A1, "BUY", "0.49", "10")), bba(A1, 100), book(A1)):
        run.send(data)
        run.send(data)
    assert run.take() == []
    assert run.machine.counts.discarded_outside == 6
    assert run.machine.counts.repeats == 0


def test_an_event_before_the_subscription_frame_names_its_token_is_outside() -> None:
    # A token takes part on a connection only once its subscription frame
    # names it; the book held from before stays as it was for comparison.
    run = started(MARKET_A)
    run.send(pc(A, 100, (A1, "BUY", "0.49", "50")))
    run.drop()
    run.machine.connecting(1, run.now)
    run.generation += 1
    run.frame_number = 0
    run.machine.opened(run.generation, run.now)
    run.take()
    run.send(book(A1))
    assert run.take() == []
    assert run.machine.counts.discarded_outside == 1
    assert run.machine.reason(A1) == "interrupted"
    assert ("0.49", "50") in sides(run, A1)[0]


def test_an_event_naming_a_market_outside_the_desired_set_is_outside() -> None:
    # Whatever token it names, and before repeat detection.
    run = started(MARKET_A)
    held = sides(run, A1)
    for data in (
        book(A1, bids=[("0.10", "1")]),
        bba(A1, 100),
        ltp(A1, 100),
        tsc(A1, 100),
    ):
        event = json.loads(data)
        event["market"] = B
        run.send(text(event))
        run.send(text(event))
    assert run.take() == []
    assert run.machine.counts.discarded_outside == 8
    assert run.machine.counts.repeats == 0
    assert sides(run, A1) == held


def test_a_price_change_naming_some_desired_tokens_is_delivered() -> None:
    run = started(MARKET_A)
    run.send(pc(A, 100, (A1, "BUY", "0.49", "10"), ("999", "BUY", "0.10", "1")))
    change = run.take()[0]
    assert isinstance(change, PriceChangeEvent)
    assert [c.applied for c in change.changes] == [True, False]


def test_new_market_events_are_dropped_unless_delivered() -> None:
    dropped = started(MARKET_A)
    dropped.send(new_market())
    assert dropped.take() == []
    assert dropped.machine.counts.new_market_dropped == 1
    delivered = started(MARKET_A, deliver_new_market=True)
    delivered.send(new_market())
    assert delivered.summary() == [("new_market",)]
    assert delivered.machine.counts.new_market_dropped == 0


# Whether a frame can reach the queue's limit (spec/client.md, Consumer
# handoff): it can when it would deliver a market-event record.

STATUS_RECORDS = (TokenStateChange, ConnectionStateChange, CaptureGap)


def delivers(run: Run, data: str) -> bool:
    """Ask the machine whether the frame would deliver a market-event
    record, then send it and check that the answer was right."""
    items = decode_frame(
        data,
        received_at=run.now,
        connection=run.generation,
        frame=run.frame_number + 1,
        keep_raw=False,
    )
    answer = run.machine.delivers(run.generation, items)
    run.send(data)
    records = run.take()
    assert answer == any(not isinstance(r, STATUS_RECORDS) for r in records)
    return answer


def test_delivers_says_whether_a_frame_gives_a_market_event_record() -> None:
    run = started(MARKET_A)
    assert not delivers(run, new_market())
    assert not delivers(run, "[]\n")
    assert not delivers(run, pc(B, 100, (B1, "BUY", "0.39", "10")))
    assert delivers(run, bba(A1, 100))
    assert delivers(run, "not json")
    assert delivers(run, text({"event_type": "something_new"}))
    # The first item is dropped, and the second delivered.
    assert delivers(run, text([json.loads(new_market()), json.loads(bba(A1, 110))]))
    delivered = started(MARKET_A, deliver_new_market=True)
    assert delivers(delivered, new_market())


def test_a_frame_from_an_interrupted_connection_delivers_nothing() -> None:
    run = started(MARKET_A)
    run.drop()
    items = decode_frame(
        "not json", received_at=run.now, connection=1, frame=2, keep_raw=False
    )
    assert not run.machine.delivers(run.generation, items)


def test_counts() -> None:
    run = started(MARKET_A)
    run.send(pc(A, 100, (A1, "BUY", "0.49", "10")))
    run.send("not json")
    run.send(b"\x00")
    counts = run.machine.counts
    assert (counts.frames, counts.connections) == (4, 1)
    assert counts.events == {"book": 2, "price_change": 1}
    assert counts.undecodable == {"invalid_json": 1, "binary": 1}


def test_the_book_held_while_uncertain_is_the_one_compared() -> None:
    run = started(MARKET_A)
    run.send(pc(A, 100, (A1, "BUY", "0.49", "50")))
    interrupted_and_back(run, A1, A2)
    books = [r for r in run.take() if isinstance(r, BookEvent)][-2:]
    # A1's held book has the change the fresh book lacks; A2's matches.
    assert [b.held_book_matched for b in books] == [False, True]
    assert Decimal("0.49") not in {level.price for level in books[0].bids}
    assert books[0].bids == (
        Level(Decimal("0.47"), Decimal("250")),
        Level(Decimal("0.48"), Decimal("100")),
    )
