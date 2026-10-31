"""Order-book hash verification: the recipe, the trade-price retries, when a
burst is checked, and when failed checks count as divergence
(spec/client.md, Order-book hash; D4).

Pure: no I/O and no timers. The state machine feeds the verifier what it
needs, in arrival order: the tokens that enter and leave the desired set,
the hash inputs lookup fetched for them, the trade prices and tick sizes the
stream announces, each entry about to be applied and each book, with the
held book as it stood before the entry or book changed it, and the clock
times at which ``burst_quiet`` may have passed. Each check comes back as a
``Check``, which the state machine turns into T7 and T9.

The recipe writes the timestamp, each level, and the tick size with the text
the source sent for them, which the decoder and the book keep beside their
values, since ``str()`` of a ``Decimal`` can spell a value otherwise.
"""

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Context, Decimal, InvalidOperation
from enum import Enum

from ._book import Book, LevelText
from ._decode import Sent
from ._lookup import BookParameters
from ._records import BookEvent, PriceChange

Position = tuple[int, int, int]
"""Where an item arrived: its connection's generation, its frame's number on
that connection, and its position in the frame, 0 for a frame holding one
object or none. Positions compare in arrival order."""

GRID = tuple(f"{k // 1000}.{k % 1000:03d}" for k in range(1001))
"""Every trade price on the 0.001 grid, from 0.000 to 1.000, as the hash
writes it."""


_THOUSANDTH = Decimal("0.001")

_ROUNDING = Context(prec=28, rounding=ROUND_HALF_EVEN, traps=[InvalidOperation])
"""Rounds as the investigation's helper did, in Python's default context,
whatever context the consumer's code has set, and signals a result of more
than 28 digits."""


def trade_price_text(price: Decimal) -> str | None:
    """A trade price as the hash writes it, with three decimals
    (spec/client.md, Order-book hash); ``None`` if that takes more than 28
    digits, for a price of 10**25 or more.

    A large finite price is decodable (spec/client.md, Values), but its
    text with three decimals can be far longer than the value as sent, or
    beyond what ``Decimal`` can format at all. The bound keeps a single
    value from ending the client or filling its memory."""
    try:
        rounded = price.quantize(_THOUSANDTH, context=_ROUNDING)
    except InvalidOperation:
        return None
    return f"{rounded:f}"


def hashed_text(
    *,
    market: str,
    asset_id: str,
    timestamp: str,
    bids: Sequence[LevelText],
    asks: Sequence[LevelText],
    parameters: BookParameters,
    tick_size: str,
    trade_price: str,
) -> str:
    """The text a token's hash covers: its book as compact JSON, in the REST
    book's key order, with ``hash`` empty (spec/client.md, Order-book hash).
    ``timestamp``, the levels, and ``tick_size`` are the text the source sent
    for them. ``bids`` are in ascending price order and ``asks`` in
    descending, as ``Book`` gives them."""
    return json.dumps(
        {
            "market": market,
            "asset_id": asset_id,
            "timestamp": timestamp,
            "hash": "",
            "bids": [_level(level) for level in bids],
            "asks": [_level(level) for level in asks],
            # From lookup, which has no text as sent.
            "min_order_size": str(parameters.min_order_size),
            "tick_size": tick_size,
            "neg_risk": parameters.neg_risk,
            "last_trade_price": trade_price,
        },
        separators=(",", ":"),
    )


def _level(level: LevelText) -> dict[str, str]:
    return {"price": level.price, "size": level.size}


class _Hashed:
    """A book's hash for any trade price. The price comes last in the text,
    so the text before it is hashed once, and each price tried costs only
    the few bytes after it."""

    def __init__(self, text: str) -> None:
        # The text ends '"last_trade_price":""}': cut before the empty value.
        self._head = hashlib.sha1(text[:-2].encode())

    def digest(self, trade_price: str) -> str:
        hashed = self._head.copy()
        hashed.update(f'{trade_price}"}}'.encode())
        return hashed.hexdigest()

    def search(self, want: str, first: str | None) -> str | None:
        """The price on the 0.001 grid that gives ``want``, if one does,
        trying ``first`` before the others: the order changes how long the
        search takes, never what it finds."""
        prices: Iterable[str] = GRID if first is None else (first, *GRID)
        return next((price for price in prices if self.digest(price) == want), None)


class Outcome(Enum):
    VERIFIED = "verified"
    """Verified with the first trade price tried."""
    RETRIED = "retried"
    """Verified after a retry with another trade price."""
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Check:
    """One check's result, for the state machine (T7, T9)."""

    token_id: str
    outcome: Outcome
    burst_end: Position | None
    """Where the checked burst's last entry arrived; ``None`` for a book's
    own check."""
    diverged: bool
    """Whether the token's failed checks now count as divergence."""


@dataclass(slots=True)
class HashCounts:
    """The checks' counters (spec/client.md, Statistics)."""

    verified: int = 0
    retried: int = 0
    failed: int = 0


@dataclass(slots=True)
class _Burst:
    """A run of applied entries for a token that share a hash: that of the
    book after the last of them (spec/client.md, Order-book hash)."""

    hash: str
    timestamp: str
    """The source timestamp of the event that carried its last entry, as the
    source sent it."""
    end: Position
    arrived: float
    """When its last entry arrived, on the event loop's clock."""
    trade_price: str | None
    """The market's announced trade price when its last entry arrived."""


@dataclass(slots=True)
class _Token:
    market: str
    parameters: BookParameters | None = None
    """``min_order_size`` and ``neg_risk``, once lookup has found them."""
    tick_size: str | None = None
    """The tick size last announced, as the source sent it."""
    checking: bool = True
    """False after a book failed its own check, until a later one verifies."""
    first_failure: float | None = None
    """When the first of the failed checks since the last verifying one
    arrived, if any has failed."""
    failures: int = 0


class Verifier:
    """Each desired token's hash inputs, its pending burst, and its failed
    checks; each market's trade prices; and the counts of checks."""

    def __init__(self, *, hash_grace: float, burst_quiet: float) -> None:
        self._grace = hash_grace
        self._quiet = burst_quiet
        self._tokens: dict[str, _Token] = {}
        # The pending bursts, in the order their last entries arrived, so that
        # the first is the first due.
        self._bursts: dict[str, _Burst] = {}
        # By market, as the hash writes them: the trade price last announced,
        # by an opening book or a last_trade_price event, and the one a search
        # last found.
        self._announced: dict[str, str] = {}
        self._found: dict[str, str] = {}
        self.counts = HashCounts()

    # The desired set and lookup.

    def want(self, token_id: str, market: str) -> None:
        """A token entered the desired set. It is not checked until lookup
        has found its hash inputs (D4)."""
        self._bursts.pop(token_id, None)
        self._tokens[token_id] = _Token(market)

    def forget(self, token_id: str) -> None:
        """A token left the desired set."""
        self._bursts.pop(token_id, None)
        token = self._tokens.pop(token_id, None)
        if token is not None and all(
            other.market != token.market for other in self._tokens.values()
        ):
            self._announced.pop(token.market, None)
            self._found.pop(token.market, None)

    def parameters(self, token_id: str, parameters: BookParameters | None) -> None:
        """Lookup answered for a token's hash inputs: ``None`` if it found no
        REST book, after which the token is never checked (D4)."""
        token = self._tokens.get(token_id)
        if token is not None:
            token.parameters = parameters

    # The stream.

    def announced(self, market: str, price: Decimal) -> None:
        """A ``last_trade_price`` event, or an opening book, gave the market's
        trade price (spec/client.md, Decoding)."""
        text = trade_price_text(price)
        if text is None:
            # The price announced before it is no longer current, so the
            # market has none to try, and its checks search the grid alone.
            self._announced.pop(market, None)
        else:
            self._announced[market] = text

    def tick_size(self, token_id: str, tick_size: str) -> None:
        """A ``tick_size_change`` event gave the token's tick size, as the
        source sent it (spec/client.md, Decoding)."""
        token = self._tokens.get(token_id)
        if token is not None:
            token.tick_size = tick_size

    def entry(
        self,
        token_id: str,
        change: PriceChange,
        timestamp: str,
        held: Book,
        end: Position,
        clock: float,
    ) -> Check | None:
        """An entry is about to be applied to the token's ``held`` book, from
        an event whose ``timestamp`` is as the source sent it. If it carries
        another hash than the pending burst, that burst has ended and is
        checked first, with the book as it stood. Then the entry starts a
        burst, or extends the pending one (D4)."""
        token = self._checkable(token_id)
        if token is None:
            return None
        check = None
        burst = self._bursts.pop(token_id, None)
        if burst is not None and burst.hash != change.hash:
            check = self._check_burst(token_id, token, burst, held)
            burst = None
        trade_price = self._announced.get(token.market)
        if burst is None:
            burst = _Burst(change.hash, timestamp, end, clock, trade_price)
        else:
            burst.timestamp, burst.end = timestamp, end
            burst.arrived, burst.trade_price = clock, trade_price
        # Last in the order of arrival.
        self._bursts[token_id] = burst
        return check

    def book(
        self,
        token_id: str,
        event: BookEvent,
        sent: Sent,
        held: Book | None,
        fresh: Book,
        clock: float,
    ) -> list[Check]:
        """A book arrived for the token, with ``sent``, the text the source
        sent for its timestamp and tick size. It ends the pending burst,
        which is checked with the ``held`` book as it stood before the book
        replaced it; then the book's own hash is checked, with ``fresh``,
        the book it brings (D4). A book that fails its own check stops the
        token's checks until a later book verifies, and is no divergence:
        the recipe or its inputs, not the source's book, are then wrong."""
        token = self._tokens.get(token_id)
        if token is None:
            return []
        checks: list[Check] = []
        burst = self._bursts.pop(token_id, None)
        if burst is not None and held is not None:
            checks.append(self._check_burst(token_id, token, burst, held))
        # Only a subscription's opening books carry these.
        if sent.tick_size is not None:
            token.tick_size = sent.tick_size
        if event.last_trade_price is not None:
            self.announced(token.market, event.last_trade_price)
        if token.parameters is None or token.tick_size is None:
            return checks
        outcome = self._check(
            token_id,
            token,
            fresh,
            sent.timestamp,
            event.hash,
            self._announced.get(token.market),
        )
        token.checking = outcome is not Outcome.FAILED
        token.first_failure, token.failures = None, 0
        checks.append(Check(token_id, outcome, None, diverged=False))
        return checks

    # burst_quiet.

    def deadline(self) -> float | None:
        """When the first pending burst is due, on the event loop's clock:
        ``burst_quiet`` after its last entry arrived."""
        burst = next(iter(self._bursts.values()), None)
        return None if burst is None else burst.arrived + self._quiet

    def due(self, clock: float) -> list[str]:
        """The tokens whose bursts are due at ``clock``: ``burst_quiet`` has
        passed with no entry applied to them (D4)."""
        due: list[str] = []
        for token_id, burst in self._bursts.items():
            if burst.arrived + self._quiet > clock:
                break
            due.append(token_id)
        return due

    def quiet(self, token_id: str, held: Book | None) -> Check | None:
        """Check a burst that is due, with the token's ``held`` book."""
        burst = self._bursts.pop(token_id, None)
        token = self._tokens.get(token_id)
        if burst is None or token is None or held is None:
            return None
        return self._check_burst(token_id, token, burst, held)

    # Checks.

    def _checkable(self, token_id: str) -> _Token | None:
        """The token, if its bursts are checked: lookup found its hash
        inputs, an opening book gave its tick size, and no book has failed
        its own check since one last verified (D4)."""
        token = self._tokens.get(token_id)
        if (
            token is None
            or token.parameters is None
            or token.tick_size is None
            or not token.checking
        ):
            return None
        return token

    def _check_burst(
        self, token_id: str, token: _Token, burst: _Burst, held: Book
    ) -> Check:
        # The trade price is the burst's, retried with the current one, but
        # the tick size is the token's at the check, a tick_size_change
        # while the burst was pending included. The one change observed
        # seemed to enter the hash before its event announced it, and the
        # investigation's replay checked with the new tick size too
        # (docs/source-behavior.md, §4; spec/client.md, Decoding).
        outcome = self._check(
            token_id, token, held, burst.timestamp, burst.hash, burst.trade_price
        )
        return Check(
            token_id, outcome, burst.end, self._diverged(token, outcome, burst.arrived)
        )

    def _check(
        self,
        token_id: str,
        token: _Token,
        book: Book,
        timestamp: str,
        want: str,
        trade_price: str | None,
    ) -> Outcome:
        """Check a hash with the event's timestamp and ``trade_price``, the
        market's announced trade price when the hash arrived. On failure,
        retry with the market's current announced trade price, then with
        every price on the 0.001 grid (D4)."""
        # Only a token with both is checked.
        assert token.parameters is not None
        assert token.tick_size is not None
        hashed = _Hashed(
            hashed_text(
                market=token.market,
                asset_id=token_id,
                timestamp=timestamp,
                bids=book.bid_texts,
                asks=book.ask_texts,
                parameters=token.parameters,
                tick_size=token.tick_size,
                trade_price="",
            )
        )
        current = self._announced.get(token.market)
        if trade_price is not None and hashed.digest(trade_price) == want:
            outcome = Outcome.VERIFIED
        elif (
            current is not None
            and current != trade_price
            and hashed.digest(current) == want
        ):
            outcome = Outcome.RETRIED
        else:
            # Starting from the price the last search found, which on busy
            # markets the hash keeps while announced trades do not follow it
            # (docs/source-behavior.md, §4).
            found = hashed.search(want, self._found.get(token.market))
            if found is None:
                outcome = Outcome.FAILED
            else:
                self._found[token.market] = found
                outcome = Outcome.RETRIED
        counts = self.counts
        if outcome is Outcome.VERIFIED:
            counts.verified += 1
        elif outcome is Outcome.RETRIED:
            counts.retried += 1
        else:
            counts.failed += 1
        return outcome

    def _diverged(self, token: _Token, outcome: Outcome, arrived: float) -> bool:
        """Whether the token's failed checks, with this one, count as
        divergence: at least two have failed, with no verifying check between
        them, and the first and the latest arrived at least ``hash_grace``
        apart (D4)."""
        if outcome is not Outcome.FAILED:
            token.first_failure, token.failures = None, 0
            return False
        if token.first_failure is None:
            token.first_failure = arrived
        token.failures += 1
        return token.failures >= 2 and arrived - token.first_failure >= self._grace
