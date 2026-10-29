"""Order-book hash verification: the recipe against the documents' vectors,
the trade-price retries, when a burst is checked, the divergence rule, and
what a book that fails its own check stops (spec/client.md, Order-book hash;
D4).

The recipe is checked against the hashes the documents give, computed with
the investigation's helper. The other tests build their hashes with the
recipe, once it has passed that check.
"""

import hashlib
import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from polymarket_market_data import BookEvent, BookParameters, Level, PriceChange, Side
from polymarket_market_data._book import Book
from polymarket_market_data._hash import (
    GRID,
    Check,
    Outcome,
    Position,
    Verifier,
    hashed_text,
    trade_price_text,
)

ROOT = Path(__file__).resolve().parents[1]

# The synthetic markets of spec/conformance.md.
A = "0x" + "a1".rjust(64, "0")
B = "0x" + "b2".rjust(64, "0")
PREFIX = "1" + "0" * 74
A1, A2 = PREFIX + "11", PREFIX + "12"
B1, B2 = PREFIX + "21", PREFIX + "22"
TOKENS = {"A1": (A, A1), "A2": (A, A2), "B1": (B, B1), "B2": (B, B2)}
TRADE_PRICES = {A: "0.500", B: "0.400"}
T0 = 1791200000000
PARAMETERS = BookParameters(Decimal("5"), False)
TICK = Decimal("0.01")
NOW = datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC)
BAD = "0" * 40


def bad(n: int) -> str:
    """A wrong hash, a different one for each ``n``: consecutive entries
    with the same hash are one burst."""
    return str(n) * 40


# A1's standard book.
A1_BIDS = (("0.48", "100"), ("0.47", "250"))
A1_ASKS = (("0.52", "120"), ("0.53", "300"))


def book(bids: tuple[tuple[str, str], ...], asks: tuple[tuple[str, str], ...]) -> Book:
    return Book(
        [Level(Decimal(p), Decimal(s)) for p, s in bids],
        [Level(Decimal(p), Decimal(s)) for p, s in asks],
        timestamp=T0 - 30000,
    )


def digest(
    held: Book,
    timestamp: int,
    *,
    token: str = A1,
    market: str = A,
    trade_price: str = "0.500",
    tick_size: Decimal = TICK,
) -> str:
    """The hash the source sends for this book."""
    text = hashed_text(
        market=market,
        asset_id=token,
        timestamp=timestamp,
        bids=held.bids,
        asks=held.asks,
        parameters=PARAMETERS,
        tick_size=tick_size,
        trade_price=trade_price,
    )
    return hashlib.sha1(text.encode()).hexdigest()


# The recipe.


def test_the_recipe_reproduces_the_contract_test_vector() -> None:
    text = (ROOT / "spec/client.md").read_text()
    section = text[text.index("Test vector, computed") :]
    hashed = re.search(r"```text\n(.*?)\n```", section, re.S)
    hashes_to = re.search(r"hashes to `([0-9a-f]{40})`", section)
    assert hashed is not None
    assert hashes_to is not None
    held = book(A1_BIDS, A1_ASKS)
    assert hashed_text(
        market=A,
        asset_id=A1,
        timestamp=1791199970000,
        bids=held.bids,
        asks=held.asks,
        parameters=PARAMETERS,
        tick_size=TICK,
        trade_price="0.500",
    ) == hashed.group(1)
    assert digest(held, 1791199970000) == hashes_to.group(1)


def test_the_recipe_reproduces_the_standard_book_hashes() -> None:
    text = (ROOT / "spec/conformance.md").read_text()
    rows = re.findall(
        r"^\| `([AB][12])` \| ([^|]+) \| ([^|]+) \| `([0-9a-f]{40})` \|$", text, re.M
    )
    assert [row[0] for row in rows] == ["A1", "A2", "B1", "B2"]
    for name, bids, asks, expected in rows:
        market, token = TOKENS[name]
        held = book(levels(bids), levels(asks))
        assert (
            digest(
                held,
                T0 - 30000,
                token=token,
                market=market,
                trade_price=TRADE_PRICES[market],
            )
            == expected
        ), name


def levels(cell: str) -> tuple[tuple[str, str], ...]:
    """Levels written as in the standard books' table: ``0.48 × 100, …``."""
    pairs = (level.split(" × ") for level in cell.split(", "))
    return tuple((price, size) for price, size in pairs)


def test_a_trade_price_is_written_with_three_decimals() -> None:
    assert trade_price_text(Decimal("0.5")) == "0.500"
    assert trade_price_text(Decimal("0.53")) == "0.530"
    assert trade_price_text(Decimal("0.500")) == "0.500"


def test_a_trade_price_too_large_to_write_has_no_text() -> None:
    # Decodable (spec/client.md, Values), but beyond what Decimal can write
    # with three decimals, or a million digits long once written.
    assert trade_price_text(Decimal("1e999999999999999999")) is None
    assert trade_price_text(Decimal("1e1000000")) is None
    assert trade_price_text(Decimal("1e25")) is None
    assert trade_price_text(Decimal("-1e25")) is None
    assert trade_price_text(Decimal("1e24")) == "1" + "0" * 24 + ".000"


def test_the_grid_holds_every_price_from_0_to_1_by_0_001() -> None:
    assert len(GRID) == 1001
    assert (GRID[0], GRID[1], GRID[530], GRID[-1]) == (
        "0.000",
        "0.001",
        "0.530",
        "1.000",
    )
    assert len(set(GRID)) == 1001


# Verification.


class Run:
    """Drives a verifier for token A1 as the state machine does: the book
    held, each entry checked before it is applied, and the clock."""

    def __init__(self, *, hash_grace: float = 0.5, burst_quiet: float = 0.1) -> None:
        self.verifier = Verifier(hash_grace=hash_grace, burst_quiet=burst_quiet)
        self.verifier.want(A1, A)
        self.held: Book | None = None
        self.source = book(A1_BIDS, A1_ASKS)
        """The source's book, from which the hashes are built."""
        self.trade_price = "0.500"
        """The trade price that enters the source's hashes."""
        self.clock = 0.0
        self.frame = 0

    def parameters(self) -> None:
        self.verifier.parameters(A1, PARAMETERS)

    def book(self, *, hash: str | None = None) -> list[Check]:
        """The source sends its book, as a subscription's opening book."""
        self.frame += 1
        bids, asks = self.source.bids, self.source.asks
        timestamp = self.source.timestamp
        event = BookEvent(
            event_type="book",
            market=A,
            source_timestamp_ms=timestamp,
            received_at=NOW,
            connection=1,
            frame=self.frame,
            index=0,
            repeat=False,
            raw=None,
            asset_id=A1,
            hash=self.hash(timestamp) if hash is None else hash,
            bids=bids,
            asks=asks,
            tick_size=TICK,
            last_trade_price=Decimal("0.500"),
            opening=True,
            held_book_matched=None,
        )
        fresh = Book.from_event(event)
        checks = self.verifier.book(A1, event, self.held, fresh, self.clock)
        self.held = fresh
        return checks

    def entry(
        self,
        t: int,
        price: str,
        size: str,
        *,
        hash: str | None = None,
        silent: bool = False,
    ) -> Check | None:
        """The source changes a bid and sends the entry, unless ``silent``:
        a change the stream omits."""
        change = self.change(price, size, BAD)
        self.source.apply(change, T0 + t)
        if silent:
            return None
        sent = self.change(price, size, self.hash(T0 + t) if hash is None else hash)
        return self.receive(sent, T0 + t)

    def receive(self, change: PriceChange, timestamp: int) -> Check | None:
        assert self.held is not None
        self.frame += 1
        position: Position = (1, self.frame, 0)
        check = self.verifier.entry(
            A1, change, timestamp, self.held, position, self.clock
        )
        self.held.apply(change, timestamp)
        return check

    def hash(self, timestamp: int) -> str:
        return digest(self.source, timestamp, trade_price=self.trade_price)

    @staticmethod
    def change(price: str, size: str, hash: str) -> PriceChange:
        return PriceChange(
            asset_id=A1,
            side=Side.BUY,
            price=Decimal(price),
            size=Decimal(size),
            hash=hash,
            best_bid=None,
            best_ask=None,
            applied=True,
            before_book=False,
        )

    def wait(self, seconds: float) -> None:
        self.clock += seconds

    def quiet(self) -> list[Check]:
        checks = []
        for token_id in self.verifier.due(self.clock):
            check = self.verifier.quiet(token_id, self.held)
            if check is not None:
                checks.append(check)
        return checks

    def counts(self) -> tuple[int, int, int]:
        counts = self.verifier.counts
        return (counts.verified, counts.retried, counts.failed)


def started(**options: float) -> Run:
    """Lookup has found A1's hash inputs, and its opening book verified."""
    run = Run(**options)
    run.parameters()
    assert run.book() == [Check(A1, Outcome.VERIFIED, None, diverged=False)]
    return run


def test_no_token_is_checked_until_lookup_finds_its_hash_inputs() -> None:
    run = Run()
    assert run.book(hash=BAD) == []
    assert run.entry(100, "0.49", "50", hash=BAD) is None
    assert run.verifier.deadline() is None
    assert run.counts() == (0, 0, 0)
    run.parameters()
    assert run.entry(200, "0.49", "60", hash=BAD) is None  # a burst begins
    run.wait(0.2)
    assert [check.outcome for check in run.quiet()] == [Outcome.FAILED]


def test_a_token_lookup_finds_no_rest_book_for_is_never_checked() -> None:
    run = Run()
    run.verifier.parameters(A1, None)
    assert run.book(hash=BAD) == []
    assert run.entry(100, "0.49", "50", hash=BAD) is None
    run.wait(1.0)
    assert run.quiet() == []
    assert run.counts() == (0, 0, 0)


def test_a_burst_is_checked_once_when_the_next_entry_carries_another_hash() -> None:
    run = started()
    # Two entries of one burst, in separate frames: each carries the hash of
    # the book after both.
    run.source.apply(Run.change("0.49", "50", BAD), T0 + 100)
    run.source.apply(Run.change("0.47", "0", BAD), T0 + 100)
    shared = run.hash(T0 + 100)
    assert run.receive(Run.change("0.49", "50", shared), T0 + 100) is None
    run.wait(0.05)
    assert run.receive(Run.change("0.47", "0", shared), T0 + 100) is None
    run.wait(0.05)
    # Checked with the book before the entry that ended the burst changed it.
    assert run.entry(110, "0.49", "60") == Check(
        A1, Outcome.VERIFIED, (1, 3, 0), diverged=False
    )
    assert run.counts() == (2, 0, 0)


def test_a_book_ends_the_pending_burst_before_its_own_check() -> None:
    run = started()
    run.entry(100, "0.49", "50")
    assert run.book() == [
        Check(A1, Outcome.VERIFIED, (1, 2, 0), diverged=False),
        Check(A1, Outcome.VERIFIED, None, diverged=False),
    ]
    assert run.verifier.deadline() is None


def test_a_burst_is_checked_burst_quiet_after_its_last_entry() -> None:
    run = started(burst_quiet=0.5)
    run.source.apply(Run.change("0.49", "50", BAD), T0 + 100)
    run.source.apply(Run.change("0.47", "0", BAD), T0 + 100)
    shared = run.hash(T0 + 100)
    run.receive(Run.change("0.49", "50", shared), T0 + 100)
    assert run.verifier.deadline() == pytest.approx(0.5)
    run.wait(0.3)
    # Each entry applied puts the check off again.
    run.receive(Run.change("0.47", "0", shared), T0 + 100)
    assert run.verifier.deadline() == pytest.approx(0.8)
    run.wait(0.4)
    assert run.verifier.due(run.clock) == []
    run.wait(0.2)
    assert run.quiet() == [Check(A1, Outcome.VERIFIED, (1, 3, 0), diverged=False)]
    assert run.verifier.deadline() is None


def test_an_entry_after_its_burst_was_checked_starts_a_new_burst() -> None:
    run = started()
    run.entry(100, "0.49", "50")
    run.wait(0.2)
    assert len(run.quiet()) == 1
    # The source sends its book's hash again, unchanged.
    again = Run.change("0.49", "50", run.hash(T0 + 100))
    assert run.receive(again, T0 + 100) is None
    run.wait(0.2)
    assert run.quiet() == [Check(A1, Outcome.VERIFIED, (1, 3, 0), diverged=False)]
    assert run.counts() == (3, 0, 0)


def test_bursts_are_due_in_the_order_of_their_last_entries() -> None:
    verifier = Verifier(hash_grace=0.5, burst_quiet=0.1)
    held = book(A1_BIDS, A1_ASKS)
    for token in (A1, A2):
        verifier.want(token, A)
        verifier.parameters(token, PARAMETERS)
        verifier.tick_size(token, TICK)

    def entry(token: str, hash: str, clock: float) -> None:
        change = Run.change("0.49", "50", hash)
        verifier.entry(token, change, T0 + 100, held, (1, 1, 0), clock)

    entry(A1, "1" * 40, 0.0)
    entry(A2, "2" * 40, 0.05)
    entry(A1, "1" * 40, 0.08)  # extends A1's burst
    assert verifier.deadline() == pytest.approx(0.15)
    assert verifier.due(0.16) == [A2]
    assert verifier.due(0.2) == [A2, A1]


def test_a_failed_check_is_retried_with_the_current_announced_trade_price() -> None:
    run = started()
    # A trade's price enters the hash before last_trade_price announces it.
    run.trade_price = "0.530"
    run.entry(100, "0.49", "50")
    run.verifier.announced(A, Decimal("0.53"))
    # The check of the burst, whose hash came before the announcement.
    assert run.entry(110, "0.49", "55") == Check(
        A1, Outcome.RETRIED, (1, 2, 0), diverged=False
    )
    # The next hash came after it, so its check verifies at once.
    run.wait(0.2)
    assert [check.outcome for check in run.quiet()] == [Outcome.VERIFIED]
    assert run.counts() == (2, 1, 0)


def test_a_trade_price_with_no_text_leaves_the_grid_alone_to_try() -> None:
    run = started()
    run.verifier.announced(A, Decimal("1e999999999999999999"))
    run.entry(100, "0.49", "50")
    run.wait(0.2)
    # The price announced before it, which the hash holds, is no longer
    # current, so only the search finds it.
    assert [check.outcome for check in run.quiet()] == [Outcome.RETRIED]
    assert run.counts() == (1, 1, 0)


def test_a_trade_price_found_by_search_verifies_after_a_retry() -> None:
    run = started()
    # On busy markets the price in the hash did not follow announced trades.
    run.trade_price = "0.537"
    run.entry(100, "0.49", "50")
    assert run.entry(110, "0.49", "55") == Check(
        A1, Outcome.RETRIED, (1, 2, 0), diverged=False
    )
    run.wait(0.2)
    assert [check.outcome for check in run.quiet()] == [Outcome.RETRIED]
    assert run.counts() == (1, 2, 0)


def test_a_check_that_no_trade_price_verifies_fails() -> None:
    run = started()
    run.entry(100, "0.49", "50", hash=BAD)
    run.wait(0.2)
    assert run.quiet() == [Check(A1, Outcome.FAILED, (1, 2, 0), diverged=False)]
    assert run.counts() == (1, 0, 1)


def test_failures_count_as_divergence_across_hash_grace_and_two_checks() -> None:
    run = started(hash_grace=0.5)
    run.entry(50, "0.45", "30", silent=True)  # a change the stream omits
    run.entry(100, "0.49", "50")
    run.wait(0.3)
    failed = run.entry(200, "0.49", "60")
    assert failed == Check(A1, Outcome.FAILED, (1, 2, 0), diverged=False)
    run.wait(0.4)
    # The first and the latest failed checks arrived 0.3 s apart.
    failed = run.entry(300, "0.49", "70")
    assert failed == Check(A1, Outcome.FAILED, (1, 3, 0), diverged=False)
    # Now 0.7 s apart.
    run.wait(0.2)
    assert run.quiet() == [Check(A1, Outcome.FAILED, (1, 4, 0), diverged=True)]


def test_one_failure_across_hash_grace_is_no_divergence() -> None:
    run = started(hash_grace=0.5)
    run.entry(100, "0.49", "50", hash=BAD)
    run.wait(1.0)
    assert run.quiet() == [Check(A1, Outcome.FAILED, (1, 2, 0), diverged=False)]


def test_a_verifying_check_between_failures_starts_them_again() -> None:
    run = started(hash_grace=0.5)
    run.entry(100, "0.49", "50", hash=bad(1))
    run.wait(0.4)
    assert run.entry(200, "0.49", "60") == Check(
        A1, Outcome.FAILED, (1, 2, 0), diverged=False
    )
    run.wait(0.4)
    assert run.entry(300, "0.49", "70", hash=bad(2)) == Check(
        A1, Outcome.VERIFIED, (1, 3, 0), diverged=False
    )
    run.wait(0.4)
    # Two failed checks, 0.8 s apart, but one verified between them.
    assert run.entry(400, "0.49", "80", hash=bad(3)) == Check(
        A1, Outcome.FAILED, (1, 4, 0), diverged=False
    )
    run.wait(0.4)
    # Two in a row, 0.4 s apart.
    assert run.entry(500, "0.49", "90", hash=bad(4)) == Check(
        A1, Outcome.FAILED, (1, 5, 0), diverged=False
    )
    run.wait(0.2)
    # Three in a row, the first 0.8 s before the latest.
    assert run.quiet() == [Check(A1, Outcome.FAILED, (1, 6, 0), diverged=True)]
    assert run.counts() == (2, 0, 4)


def test_a_book_that_fails_its_own_check_stops_checks_until_one_verifies() -> None:
    run = started(hash_grace=0.5)
    assert run.book(hash=BAD) == [Check(A1, Outcome.FAILED, None, diverged=False)]
    for t in (100, 200, 300):
        assert run.entry(t, "0.49", str(t), hash=BAD) is None
        run.wait(0.4)
    assert run.quiet() == []
    assert run.counts() == (1, 0, 1)
    assert run.book() == [Check(A1, Outcome.VERIFIED, None, diverged=False)]
    run.entry(400, "0.49", "80", hash=BAD)
    run.wait(0.2)
    assert run.quiet() == [Check(A1, Outcome.FAILED, (1, 7, 0), diverged=False)]


def test_a_book_s_own_failure_is_no_divergence() -> None:
    run = started(hash_grace=0.5)
    run.entry(100, "0.49", "50", hash=BAD)
    run.wait(1.0)
    # The burst's check fails, then the book's own: two failures 1 s apart,
    # but the book's says only that the recipe or its inputs are wrong.
    assert run.book(hash=BAD) == [
        Check(A1, Outcome.FAILED, (1, 2, 0), diverged=False),
        Check(A1, Outcome.FAILED, None, diverged=False),
    ]


def test_a_token_that_leaves_the_desired_set_is_forgotten() -> None:
    run = started()
    run.entry(100, "0.49", "50")
    run.verifier.forget(A1)
    assert run.verifier.deadline() is None
    assert run.verifier.quiet(A1, run.held) is None
    assert run.entry(200, "0.49", "60") is None
    # Wanted again, it waits for its hash inputs again.
    run.verifier.want(A1, A)
    assert run.book() == []
