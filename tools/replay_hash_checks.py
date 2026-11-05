"""Replay a research timeline's hash checks with the client's own verifier,
and say which checks failed, when, and what came shortly before each
(docs/implementation-plan.md, 13. Investigate the live run's failed hash
checks; D4).

    python tools/replay_hash_checks.py TIMELINE [--failures]

The timeline is one the research example wrote: its ``run`` line, its
records, and its ``stats`` line (docs/implementation-plan.md, 11. Add the
research example and check the built wheel). The script feeds the records'
events to ``Verifier`` in their order, as the state machine does, with each
record's ``received_at`` in place of the event loop's clock. It applies an
entry only where the record says the client applied it, and skips the events
the client flagged as repeats, so the books it checks are the client's. A
burst whose ``burst_quiet`` passed before a frame arrived is checked before
that frame, as if the client's timer had fired first.

The timeline lacks the two hash inputs the client fetched through lookup,
``min_order_size`` and ``neg_risk`` (D4). The script infers each token's by
trying the values the investigation's replay tried against the hashes of
the token's ``book`` events, the first that verifies deciding. It checks no
token whose inputs it cannot infer, as the client checks none whose inputs
lookup does not find, and takes those it infers as known from the start, as
if lookup had answered before the first frame.

For each failed check it then tries what the findings and the timeline
suggest could explain it, none of which the client tries: the held book a
few entries later, or with the repeats the client skipped applied; a tick
size or a timestamp other than the client's; and the books that differed
from the held book (docs/source-behavior.md, §4; spec/client.md, Repeated
messages).

It prints counts, times, and IDs, never a price, a size, or a hash: the
timeline is the source's data, which stays out of git.
"""

import argparse
import json
import sys
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, TextIO

from pydantic import TypeAdapter

from polymarket_market_data import BookEvent, PriceChange, Side
from polymarket_market_data._book import Book, LevelText
from polymarket_market_data._decode import Sent
from polymarket_market_data._hash import (
    Check,
    Outcome,
    Position,
    Verifier,
    _Hashed,
    hashed_text,
    trade_price_text,
)
from polymarket_market_data._lookup import BookParameters

MIN_ORDER_SIZES = ("5", "1", "10", "15", "20", "50", "100")
"""The values tried for ``min_order_size``, in the order the investigation's
replay tried them for a market with no REST book (docs/source-behavior.md,
§4)."""

SETTLING_PREFIX = "btc-updown-5m-"
SETTLING_WINDOW = 300
"""A ``btc-updown-5m-`` slug holds the Unix time its five-minute window
began, and the market ends when the window does (docs/source-behavior.md,
Markets)."""

NEAR = 2.0
"""How many seconds before a check an event counts as shortly before it:
``hash_grace``'s default, the scale on which D4 declares divergence."""

AHEAD = 400
"""How many of the token's next applied entries a failed burst check is
tried against, of those that arrive within ``burst_quiet`` of the check."""

SECOND_START = 100
"""Milliseconds: a hash stamped this close after a whole second of the
source's clock counts as at the start of a second."""

CAUSES = (
    "book_own",
    "stale_book",
    "differing_book",
    "repeat",
    "tick_size",
    "timestamp",
    "ahead",
    "none",
)
"""What each failed check is put down to, the first that applies:

- ``book_own``: a book's own check;
- ``stale_book``: it came after a book stamped earlier than the held book's
  last change replaced the held book (T5), with no verifying check between;
- ``differing_book``: the token's next book differed from the held book,
  with no verifying check between;
- ``repeat``: it verifies with the held book as it would stand had the
  client applied the repeats it skipped that would have changed it
  (spec/client.md, Repeated messages);
- ``tick_size``: it verifies with the tick size the token had before a
  ``tick_size_change`` that arrived after the checked hash;
- ``timestamp``: it verifies with a timestamp 1 ms from the event's, or
  with the burst's first entry's;
- ``ahead``: it verifies with the held book after some of the token's next
  applied entries, so its hash already held changes that came after it,
  which the findings saw one change late;
- ``none``: nothing tried verifies it."""

_BOOK = TypeAdapter(BookEvent)


# Reading the timeline.


def lines(path: Path) -> Iterator[tuple[str, dict[str, Any]]]:
    """Each line's type and its record, or the ``run`` line itself."""
    with path.open(encoding="utf-8") as timeline:
        for line in timeline:
            value = json.loads(line)
            yield value["type"], value.get("record", value)


def clock(text: str) -> float:
    """A record's time, in seconds since the epoch."""
    return datetime.fromisoformat(text).timestamp()


def when(seconds: float) -> str:
    """A time of day in UTC, to the millisecond."""
    return datetime.fromtimestamp(seconds, UTC).strftime("%H:%M:%S.%f")[:-3]


def short(token_id: str) -> str:
    return f"{token_id[:8]}…"


def book_of(record: dict[str, Any]) -> tuple[BookEvent, Sent, Book]:
    """A ``book`` record, with the text the hash writes for its values and
    the book it brings. The timeline writes each decimal as ``str()`` of the
    value, which is the text the source sent unless the source spelled it
    otherwise; ``exponents`` counts the values where it must have."""
    event = _BOOK.validate_python(record)
    bids = tuple(LevelText(level["price"], level["size"]) for level in record["bids"])
    asks = tuple(LevelText(level["price"], level["size"]) for level in record["asks"])
    sent = Sent(
        timestamp=str(event.source_timestamp_ms),
        bids=bids,
        asks=asks,
        tick_size=record["tick_size"],
    )
    return event, sent, Book.from_event(event, bids, asks)


def exponents(record: dict[str, Any]) -> int:
    """How many of an event's level and tick-size values the timeline writes
    with an exponent, as ``str()`` writes a small value: the source sent
    those with none, so the replay hashes other text than the client did."""
    values: list[str | None] = []
    for level in (*record.get("bids", ()), *record.get("asks", ())):
        values += (level["price"], level["size"])
    for change in record.get("changes", ()):
        values += (change["price"], change["size"])
    values += (record.get("tick_size"), record.get("new_tick_size"))
    return sum(1 for value in values if value is not None and "E" in value)


def hashed(
    token_id: str,
    market: str,
    book: Book,
    timestamp: str,
    parameters: BookParameters,
    tick_size: str,
) -> _Hashed:
    """A book's hash for any trade price, as the verifier computes it."""
    return _Hashed(
        hashed_text(
            market=market,
            asset_id=token_id,
            timestamp=timestamp,
            bids=book.bid_texts,
            asks=book.ask_texts,
            parameters=parameters,
            tick_size=tick_size,
            trade_price="",
        )
    )


# The hash inputs the timeline lacks.


@dataclass(slots=True)
class Inferred:
    parameters: BookParameters | None = None
    at: float | None = None
    """When the book whose hash gave them arrived."""
    books_tried: int = 0


def infer(path: Path) -> dict[str, Inferred]:
    """Each token's ``min_order_size`` and ``neg_risk``, from the first of
    its ``book`` events whose hash verifies with one of the values tried and
    some trade price on the 0.001 grid, the announced one first."""
    inferred: dict[str, Inferred] = {}
    tick_sizes: dict[str, str] = {}
    announced: dict[str, str] = {}
    for kind, record in lines(path):
        if kind == "run":
            for market in record["markets"]:
                for token_id in market["token_ids"]:
                    inferred[token_id] = Inferred()
        elif kind == "TickSizeChangeEvent" and not record["repeat"]:
            tick_sizes[record["asset_id"]] = record["new_tick_size"]
        elif kind == "LastTradePriceEvent" and not record["repeat"]:
            text = trade_price_text(Decimal(record["price"]))
            if text is not None:
                announced[record["market"]] = text
        elif kind == "BookEvent" and not record["repeat"]:
            event, sent, book = book_of(record)
            if sent.tick_size is not None:
                tick_sizes[event.asset_id] = sent.tick_size
            if event.last_trade_price is not None:
                text = trade_price_text(event.last_trade_price)
                if text is not None:
                    announced[event.market] = text
            found = inferred.setdefault(event.asset_id, Inferred())
            tick_size = tick_sizes.get(event.asset_id)
            if found.parameters is not None or tick_size is None:
                continue
            found.books_tried += 1
            trials = (
                BookParameters(Decimal(size), neg_risk)
                for size in MIN_ORDER_SIZES
                for neg_risk in (False, True)
            )
            for parameters in trials:
                trial = hashed(
                    event.asset_id,
                    event.market,
                    book,
                    sent.timestamp,
                    parameters,
                    tick_size,
                )
                if trial.search(event.hash, announced.get(event.market)):
                    found.parameters = parameters
                    found.at = clock(record["received_at"])
                    break
    return inferred


# The replay.


@dataclass(slots=True)
class Failure:
    """A failed check, and what may explain it."""

    token_id: str
    market: str
    at: float
    """When the event or timer that made the check came."""
    arrived: float
    """When the checked hash arrived: the burst's last entry, or the book."""
    stamped: int
    """The checked hash's source timestamp, in milliseconds."""
    kind: str
    """``book``, for a book's own check, or what ended the burst:
    ``entry``, ``book_after``, ``quiet``, or ``end``."""
    before: dict[str, float]
    """For each kind of event that came within ``NEAR`` before the check,
    how many seconds before it the latest came."""
    stale_book: bool = False
    differing_book: bool = False
    repeat: bool = False
    tick_size: bool = False
    timestamp: bool = False
    ahead: int | None = None
    """How many of the token's next applied entries it took to verify."""
    ahead_after: float | None = None
    """How many seconds after the check the last of those arrived."""

    @property
    def cause(self) -> str:
        if self.kind == "book":
            return "book_own"
        flags = {
            "stale_book": self.stale_book,
            "differing_book": self.differing_book,
            "repeat": self.repeat,
            "tick_size": self.tick_size,
            "timestamp": self.timestamp,
            "ahead": self.ahead is not None,
        }
        return next((cause for cause, flag in flags.items() if flag), "none")


@dataclass(slots=True)
class Episode:
    """Failed checks of a token with no verifying check between them, as
    D4's rule for divergence counts them (D4)."""

    token_id: str
    failures: list[Failure] = field(default_factory=list)
    ended_by: str = "end"
    ended_at: float | None = None

    @property
    def span(self) -> float:
        """From the first failed hash's arrival to the latest's, as D4's
        rule measures it."""
        return self.failures[-1].arrived - self.failures[0].arrived


@dataclass(frozen=True, slots=True)
class DifferingBook:
    """A book that differed from the held book (T5)."""

    token_id: str
    at: float
    after_held: int
    """Its timestamp less the held book's last change's, in milliseconds."""
    levels: int
    """How many prices it gives another size than the held book, or holds
    on one side only."""
    stale_levels: int
    """How many of those an entry stamped after the book had last set."""


@dataclass(frozen=True, slots=True)
class SkippedRepeat:
    """A repeat the client did not apply that would have changed the held
    book (spec/client.md, Repeated messages)."""

    at: float
    token_ids: tuple[str, ...]
    """The tokens whose books it would have changed."""
    after: float | None
    """Seconds since the event it repeats arrived, if the timeline has it."""


@dataclass(slots=True)
class _Entry:
    position: Position
    arrived: float
    timestamp: str
    first: str
    """The timestamp of its burst's first entry."""
    hash: str


@dataclass(slots=True)
class _Ahead:
    failure: Failure
    hash: str
    timestamp: str
    tried: int = 0


class Replay:
    """The verifier, the books it checks, and what the report needs."""

    def __init__(self, run: dict[str, Any], inferred: dict[str, Inferred]) -> None:
        config = run["config"]
        self.grace: float = config["hash_grace"]
        self.burst_quiet: float = config["burst_quiet"]
        self.verifier = Verifier(hash_grace=self.grace, burst_quiet=self.burst_quiet)
        self.slugs: dict[str, str | None] = {}
        self.markets: dict[str, str] = {}
        self.parameters: dict[str, BookParameters] = {}
        for market in run["markets"]:
            self.slugs[market["condition_id"]] = market["slug"]
            for token_id in market["token_ids"]:
                self.markets[token_id] = market["condition_id"]
                self.verifier.want(token_id, market["condition_id"])
                parameters = inferred[token_id].parameters
                self.verifier.parameters(token_id, parameters)
                if parameters is not None:
                    self.parameters[token_id] = parameters
        self.books: dict[str, Book] = {}
        self.tick_sizes: dict[str, str] = {}
        # The tick size before the latest change, and when the change came.
        self.tick_before: dict[str, tuple[str, float]] = {}
        # The last applied entry of each token, and when each level was last
        # set, by the source timestamp of the entry that set it.
        self.last_entry: dict[str, _Entry] = {}
        self.set_at: dict[str, dict[tuple[Side, Decimal], int]] = {}
        self.seen: dict[str, dict[str, float]] = {}
        """For each token, when each kind of event that may bear on a
        check last came."""
        self.stale: set[str] = set()
        self.shadow: dict[str, Book] = {}
        """For each token whose book a skipped repeat would have changed, the
        held book with those repeats applied, while the two differ."""
        self.contents: dict[tuple[Any, ...], float] = {}
        self.skipped: list[SkippedRepeat] = []
        self.ahead: dict[str, list[_Ahead]] = {}
        self.checks: Counter[tuple[str, Outcome]] = Counter()
        self.near: Counter[tuple[str, str]] = Counter()
        """For each token and kind of event, the burst checks that came
        within ``NEAR`` after one, failed or not."""
        self.token_checks: Counter[str] = Counter()
        self.burst_checks: Counter[str] = Counter()
        self.second_start: Counter[str] = Counter()
        """For each token, the burst checks of hashes stamped at the start
        of a second, failed or not."""
        self.failures: list[Failure] = []
        self.episodes: list[Episode] = []
        self.open: dict[str, Episode] = {}
        self.differing: list[DifferingBook] = []
        self.diverged = 0
        self.exponents = 0
        self.end_checks = 0
        self.now = 0.0

    # The records.

    def record(self, kind: str, record: dict[str, Any]) -> None:
        if kind == "TokenStateChange":
            if record["state"] in ("settled", "removed"):
                self._forget(record["token_id"], record["state"], clock(record["at"]))
            return
        if "source_timestamp_ms" not in record:
            return  # a status record, or an event the verifier takes nothing from
        self.now = clock(record["received_at"])
        self.quiet(self.now)
        self.exponents += exponents(record)
        repeat = record["repeat"]
        if kind == "BookEvent":
            self._saw(record["asset_id"], "repeat" if repeat else "book")
            if not repeat:
                self._book(record)
        elif kind == "PriceChangeEvent":
            if repeat:
                for change in record["changes"]:
                    self._saw(change["asset_id"], "repeat")
                self._repeat(record)
            else:
                self.contents[_content(record)] = self.now
                self._price_change(record)
        elif kind == "LastTradePriceEvent":
            for token_id, market in self.markets.items():
                if market == record["market"]:
                    self._saw(token_id, "repeat" if repeat else "trade")
            if not repeat:
                self.verifier.announced(record["market"], Decimal(record["price"]))
        elif kind == "TickSizeChangeEvent":
            token_id = record["asset_id"]
            self._saw(token_id, "repeat" if repeat else "tick_size_change")
            if not repeat:
                if token_id in self.tick_sizes:
                    self.tick_before[token_id] = (self.tick_sizes[token_id], self.now)
                self.tick_sizes[token_id] = record["new_tick_size"]
                self.verifier.tick_size(token_id, record["new_tick_size"])

    def _forget(self, token_id: str, state: str, at: float) -> None:
        """The token left the desired set, with its pending burst unchecked."""
        self.verifier.forget(token_id)
        self.books.pop(token_id, None)
        self.shadow.pop(token_id, None)
        self.last_entry.pop(token_id, None)
        self.ahead.pop(token_id, None)
        self._end_episode(token_id, state, at)

    def _book(self, record: dict[str, Any]) -> None:
        event, sent, fresh = book_of(record)
        token_id = event.asset_id
        held = self.books.get(token_id)
        differs = held is not None and record["held_book_matched"] is False
        stale = (
            differs and held is not None and event.source_timestamp_ms < held.timestamp
        )
        if differs and held is not None:
            self._saw(token_id, "differing_book")
            self._differing(token_id, event, held, fresh)
        if sent.tick_size is not None:
            self.tick_sizes[token_id] = sent.tick_size
        self.ahead.pop(token_id, None)
        checks = self.verifier.book(token_id, event, sent, held, fresh, self.now)
        # The pending burst's check comes first, with the held book (D4).
        for check in checks:
            if check.burst_end is not None:
                self._result(check, "book_after", self.now, self.last_entry[token_id])
        episode = self.open.get(token_id)
        if differs and episode is not None:
            # These failed while the held book differed from this one.
            for failure in episode.failures:
                failure.differing_book = True
        for check in checks:
            if check.burst_end is None:
                self._result(check, "book", self.now, None, event.source_timestamp_ms)
        self.books[token_id] = fresh
        self.shadow.pop(token_id, None)
        self.last_entry.pop(token_id, None)
        self.set_at[token_id] = {}
        if stale:
            self.stale.add(token_id)
        else:
            self.stale.discard(token_id)

    def _price_change(self, record: dict[str, Any]) -> None:
        position = (record["connection"], record["frame"], record["index"])
        stamped = record["source_timestamp_ms"]
        timestamp = str(stamped)
        for item in record["changes"]:
            if not item["applied"]:
                continue
            token_id = item["asset_id"]
            change = PriceChange(
                asset_id=token_id,
                side=Side(item["side"]),
                price=Decimal(item["price"]),
                size=Decimal(item["size"]),
                hash=item["hash"],
                best_bid=None,
                best_ask=None,
                applied=True,
                before_book=item["before_book"],
            )
            book = self.books[token_id]
            last = self.last_entry.get(token_id)
            check = self.verifier.entry(
                token_id, change, timestamp, book, position, self.now
            )
            if check is not None:
                assert last is not None
                self._result(check, "entry", self.now, last)
            text = LevelText(item["price"], item["size"])
            book.apply(change, text, stamped)
            shadow = self.shadow.get(token_id)
            if shadow is not None:
                shadow.apply(change, text, stamped)
                if shadow.matches(book):
                    del self.shadow[token_id]
            self.set_at.setdefault(token_id, {})[change.side, change.price] = stamped
            # The verifier extended the pending burst if it shares its hash
            # and was not checked; otherwise the entry starts one.
            extends = check is None and last is not None and last.hash == change.hash
            first = last.first if extends and last is not None else timestamp
            self.last_entry[token_id] = _Entry(
                position, self.now, timestamp, first, change.hash
            )
            self._try_ahead(token_id)

    def _repeat(self, record: dict[str, Any]) -> None:
        """A repeat, which the client did not apply (spec/client.md, Repeated
        messages). Where applying it would have changed the held book, or
        the shadow book already kept for the token, the shadow book takes
        it."""
        stamped = record["source_timestamp_ms"]
        changed: list[str] = []
        for item in record["changes"]:
            token_id = item["asset_id"]
            book = self.books.get(token_id)
            if book is None:
                continue
            side, price = Side(item["side"]), Decimal(item["price"])
            size = Decimal(item["size"])
            shadow = self.shadow.get(token_id, book)
            if _levels(shadow).get((side, price), Decimal(0)) == size:
                continue
            change = PriceChange(
                token_id, side, price, size, item["hash"], None, None, True, False
            )
            shadow = self.shadow.setdefault(token_id, book.copy())
            shadow.apply(change, LevelText(item["price"], item["size"]), stamped)
            changed.append(token_id)
        if changed:
            original = self.contents.get(_content(record))
            self.skipped.append(
                SkippedRepeat(
                    self.now,
                    tuple(dict.fromkeys(changed)),
                    None if original is None else self.now - original,
                )
            )

    # Timers and the end.

    def quiet(self, until: float) -> None:
        """Check each burst whose ``burst_quiet`` passed by ``until``, as
        the client's timer does once it fires (D4)."""
        verifier = self.verifier
        while (deadline := verifier.deadline()) is not None and deadline <= until:
            for token_id in verifier.due(deadline):
                check = verifier.quiet(token_id, self.books.get(token_id))
                if check is not None:
                    kind = "end" if until == float("inf") else "quiet"
                    self._result(check, kind, deadline, self.last_entry[token_id])
                    # The token's next entry starts a new burst.
                    self.last_entry.pop(token_id, None)

    def end(self) -> None:
        """Check the bursts still pending when the timeline ends, as the
        client does if it runs ``burst_quiet`` past their last entries."""
        before = self.checks.total()
        self.quiet(float("inf"))
        self.end_checks = self.checks.total() - before
        self.ahead.clear()
        for token_id in list(self.open):
            self._end_episode(token_id, "end", None)

    # Results.

    def _result(
        self,
        check: Check,
        kind: str,
        at: float,
        burst: _Entry | None,
        stamped: int | None = None,
    ) -> None:
        token_id = check.token_id
        self.checks[kind, check.outcome] += 1
        self.token_checks[token_id] += 1
        if check.diverged:
            self.diverged += 1
        before = {
            seen: at - time
            for seen, time in self.seen.get(token_id, {}).items()
            if at - time <= NEAR
        }
        if burst is not None:
            self.burst_checks[token_id] += 1
            for seen in before:
                self.near[token_id, seen] += 1
            if int(burst.timestamp) % 1000 < SECOND_START:
                self.second_start[token_id] += 1
        if check.outcome is not Outcome.FAILED:
            if burst is not None or kind == "book":
                # A book's own check resets D4's count, verified or not.
                self._end_episode(token_id, f"{kind}_verified", at)
                self.stale.discard(token_id)
            return
        failure = Failure(
            token_id=token_id,
            market=self.markets[token_id],
            at=at,
            arrived=at if burst is None else burst.arrived,
            stamped=int(burst.timestamp) if burst is not None else stamped or 0,
            kind=kind,
            before=before,
            stale_book=token_id in self.stale,
        )
        self.failures.append(failure)
        if burst is None:
            # A failed book stops the token's checks until a later book
            # verifies (D4).
            self._end_episode(token_id, "book_failed", at)
            self.episodes.append(Episode(token_id, [failure], "book_failed", at))
            return
        self.open.setdefault(token_id, Episode(token_id)).failures.append(failure)
        shadow = self.shadow.get(token_id)
        failure.repeat = shadow is not None and self._verifies(
            token_id, shadow, burst.timestamp, burst.hash, self.tick_sizes[token_id]
        )
        failure.tick_size = self._other_tick_size(token_id, burst)
        failure.timestamp = self._other_timestamp(token_id, burst)
        if kind in ("entry", "quiet"):
            self.ahead.setdefault(token_id, []).append(
                _Ahead(failure, burst.hash, burst.timestamp)
            )

    def _end_episode(self, token_id: str, by: str, at: float | None) -> None:
        episode = self.open.pop(token_id, None)
        if episode is not None:
            episode.ended_by, episode.ended_at = by, at
            self.episodes.append(episode)

    def _saw(self, token_id: str, kind: str) -> None:
        self.seen.setdefault(token_id, {})[kind] = self.now

    def _differing(
        self, token_id: str, event: BookEvent, held: Book, fresh: Book
    ) -> None:
        mine = _levels(held)
        theirs = _levels(fresh)
        levels = {
            key
            for key in mine.keys() | theirs.keys()
            if mine.get(key) != theirs.get(key)
        }
        set_at = self.set_at.get(token_id, {})
        stale = sum(
            1 for key in levels if set_at.get(key, 0) > event.source_timestamp_ms
        )
        self.differing.append(
            DifferingBook(
                token_id,
                self.now,
                event.source_timestamp_ms - held.timestamp,
                len(levels),
                stale,
            )
        )

    # What else would have verified a failed check.

    def _verifies(
        self, token_id: str, book: Book, timestamp: str, want: str, tick_size: str
    ) -> bool:
        parameters = self.parameters[token_id]
        trial = hashed(
            token_id, self.markets[token_id], book, timestamp, parameters, tick_size
        )
        return trial.search(want, None) is not None

    def _other_tick_size(self, token_id: str, burst: _Entry) -> bool:
        """Whether the check verifies with the token's tick size before its
        latest change, if that change arrived after the checked hash."""
        previous = self.tick_before.get(token_id)
        if previous is None or previous[1] < burst.arrived:
            return False
        book = self.books[token_id]
        return self._verifies(token_id, book, burst.timestamp, burst.hash, previous[0])

    def _other_timestamp(self, token_id: str, burst: _Entry) -> bool:
        """Whether the check verifies with a timestamp 1 ms from the
        event's, or with the burst's first entry's."""
        stamped = int(burst.timestamp)
        tried = dict.fromkeys((burst.first, str(stamped - 1), str(stamped + 1)))
        tried.pop(burst.timestamp, None)
        book, tick_size = self.books[token_id], self.tick_sizes[token_id]
        return any(
            self._verifies(token_id, book, timestamp, burst.hash, tick_size)
            for timestamp in tried
        )

    def _try_ahead(self, token_id: str) -> None:
        """Try the token's failed burst checks, each against the held book
        after each of the token's next ``AHEAD`` applied entries that arrive
        within ``burst_quiet`` of the check."""
        pending = self.ahead.get(token_id)
        if not pending:
            return
        book, tick_size = self.books[token_id], self.tick_sizes[token_id]
        left = []
        for ahead in pending:
            if self.now - ahead.failure.at > self.burst_quiet:
                continue
            ahead.tried += 1
            if self._verifies(token_id, book, ahead.timestamp, ahead.hash, tick_size):
                ahead.failure.ahead = ahead.tried
                ahead.failure.ahead_after = self.now - ahead.failure.at
            elif ahead.tried < AHEAD:
                left.append(ahead)
        self.ahead[token_id] = left


def _content(record: dict[str, Any]) -> tuple[Any, ...]:
    """A ``price_change`` record's content, enough to find the event a repeat
    repeats: its market, timestamp, and entries as a multiset."""
    entries = Counter(
        (c["asset_id"], c["side"], c["price"], c["size"], c["hash"])
        for c in record["changes"]
    )
    return (record["market"], record["source_timestamp_ms"], frozenset(entries.items()))


def _levels(book: Book) -> dict[tuple[Side, Decimal], Decimal]:
    return {
        **{(Side.BUY, level.price): level.size for level in book.bids},
        **{(Side.SELL, level.price): level.size for level in book.asks},
    }


def replay_timeline(
    path: Path,
) -> tuple[Replay, dict[str, Inferred], dict[str, Any] | None]:
    """Replay a timeline: the replay, the inferred hash inputs, and the
    client's statistics from its last line, if it has one."""
    inferred = infer(path)
    replay: Replay | None = None
    stats = None
    for kind, record in lines(path):
        if kind == "run":
            replay = Replay(record, inferred)
        elif kind == "stats":
            stats = record
        elif replay is not None:
            replay.record(kind, record)
    if replay is None:
        raise ValueError(f"{path} has no run line")
    replay.end()
    return replay, inferred, stats


# The report.


def window(slug: str | None) -> tuple[float, float] | None:
    """When a market's five-minute window began and ended, where its slug
    tells it."""
    start = (slug or "").removeprefix(SETTLING_PREFIX)
    if slug is None or not slug.startswith(SETTLING_PREFIX) or not start.isdigit():
        return None
    return float(start), float(start) + SETTLING_WINDOW


def report(
    replay: Replay,
    inferred: dict[str, Inferred],
    stats: dict[str, Any] | None,
    out: TextIO,
    *,
    each: bool = False,
) -> None:
    """Print what the replay found: counts, times, and IDs only."""

    def say(text: str = "") -> None:
        print(text, file=out)

    def name(token_id: str) -> str:
        market = replay.markets.get(token_id, "")
        return f"{short(token_id)} ({replay.slugs.get(market) or market})"

    say("Hash inputs inferred from book hashes:")
    for token_id, found in inferred.items():
        if found.at is None:
            say(
                f"  {name(token_id)}: not inferred from {found.books_tried} "
                "books, so not checked"
            )
        else:
            say(f"  {name(token_id)}: from its book at {when(found.at)}")
    if replay.exponents:
        say(
            f"  values the timeline writes with an exponent, whose text as sent "
            f"it lacks: {replay.exponents}"
        )

    counts = replay.verifier.counts
    say()
    say("Checks:")
    for field_name, replayed in (
        ("hash_verified", counts.verified),
        ("hash_retried", counts.retried),
        ("hash_failed", counts.failed),
    ):
        line = f"  {field_name}: {replayed} replayed"
        if stats is not None:
            client = stats[field_name]
            line += f", {client} by the client, difference {replayed - client:+d}"
        say(line)
    say(f"  checked when the timeline ends: {replay.end_checks}")
    say(f"  divergence declared: {replay.diverged}")
    for kind in ("book", "entry", "book_after", "quiet", "end"):
        outcomes = [
            f"{outcome.value} {replay.checks[kind, outcome]}"
            for outcome in Outcome
            if replay.checks[kind, outcome]
        ]
        if outcomes:
            say(f"  made by {kind}: {', '.join(outcomes)}")

    failed = replay.failures
    say()
    say(f"Failed checks: {len(failed)}")
    if failed:
        _failures(replay, failed, say, name)
        _episodes(replay, say)
    if replay.differing:
        say()
        say(f"Books that differed from the held book: {len(replay.differing)}")
        for book in replay.differing:
            say(
                f"  {when(book.at)} {name(book.token_id)}: stamped "
                f"{book.after_held:+d} ms from the held book's last change; "
                f"differing levels {book.levels}, of them last set by an entry "
                f"stamped after the book {book.stale_levels}"
            )
    if replay.skipped:
        say()
        say(
            f"Repeats skipped that would have changed the held book: "
            f"{len(replay.skipped)}"
        )
        for skipped in replay.skipped:
            after = (
                "its first copy not found"
                if skipped.after is None
                else f"{skipped.after * 1000:.1f} ms after its first copy"
            )
            tokens = ", ".join(short(token_id) for token_id in skipped.token_ids)
            say(f"  {when(skipped.at)} {tokens}: {after}")
    if each and failed:
        say()
        say("Each failed check: when, token, what made it, when its hash arrived,")
        say("what came within the last 2 s, and what it is put down to")
        for failure in failed:
            before = ", ".join(
                f"{kind} {seconds:.3f} s"
                for kind, seconds in sorted(failure.before.items(), key=lambda i: i[1])
            )
            ahead = (
                f" ({failure.ahead} entries, {failure.ahead_after:.3f} s)"
                if failure.ahead is not None and failure.ahead_after is not None
                else ""
            )
            say(
                f"  {when(failure.at)} {short(failure.token_id)} {failure.kind} "
                f"arrived {when(failure.arrived)}; {before or 'nothing'}; "
                f"{failure.cause}{ahead}"
            )


def _failures(
    replay: Replay,
    failed: Sequence[Failure],
    say: Callable[[str], None],
    name: Callable[[str], str],
) -> None:
    tokens = Counter(failure.token_id for failure in failed)
    for token_id, n in tokens.most_common():
        say(f"  {name(token_id)}: {n} of its {replay.token_checks[token_id]} checks")
    say(f"  first at {when(failed[0].at)}, last at {when(failed[-1].at)}")
    say("  made by: " + _list(Counter(failure.kind for failure in failed)))
    causes = Counter(failure.cause for failure in failed)
    say("  put down to: " + ", ".join(f"{c} {causes[c]}" for c in CAUSES if causes[c]))
    ahead = sorted(
        (failure.ahead, failure.ahead_after or 0.0)
        for failure in failed
        if failure.cause == "ahead" and failure.ahead is not None
    )
    if ahead:
        say(
            f"    ahead: {sum(1 for n, _ in ahead if n == 1)} with the next entry, "
            f"at most {ahead[-1][0]} entries and "
            f"{max(after for _, after in ahead):.3f} s after the check"
        )
    # Against the other burst checks of the same tokens.
    burst = [failure for failure in failed if failure.kind != "book"]
    others = sum(replay.burst_checks[token] for token in tokens) - len(burst)
    if burst and others:
        say("  of failed burst checks, and of the others on the same tokens:")
        start = sum(1 for f in burst if f.stamped % 1000 < SECOND_START)
        all_start = sum(replay.second_start[token] for token in tokens)
        say(
            f"    hash stamped in the first {SECOND_START} ms of a second: "
            f"{_share(start, len(burst))} and {_share(all_start - start, others)}"
        )
        kinds = sorted({kind for token, kind in replay.near if token in tokens})
        for kind in kinds:
            near = sum(1 for f in burst if kind in f.before)
            all_near = sum(replay.near[token, kind] for token in tokens)
            say(
                f"    {kind} within {NEAR:g} s before: {_share(near, len(burst))} "
                f"and {_share(all_near - near, others)}"
            )
        by_cause = Counter(f.cause for f in burst if f.stamped % 1000 < SECOND_START)
        say(
            "    stamped at the start of a second, by cause: "
            + ", ".join(f"{c} {by_cause[c]}" for c in CAUSES if by_cause[c])
        )
    bands: Counter[str] = Counter()
    for failure in failed:
        span = window(replay.slugs.get(failure.market))
        if span is None:
            bands["no window"] += 1
        elif failure.at < span[0]:
            bands["before its window"] += 1
        elif failure.at < span[1]:
            bands["in its window"] += 1
        else:
            bands["after its window ended"] += 1
    say("  against the market's five-minute window: " + _list(bands))


def _episodes(replay: Replay, say: Callable[[str], None]) -> None:
    episodes = sorted(replay.episodes, key=lambda e: (e.failures[0].at, e.token_id))
    say("")
    say(f"Episodes: {len(episodes)}")
    sizes = Counter(len(episode.failures) for episode in episodes)
    say(
        "  failed checks in each: "
        + ", ".join(f"{n} in {count}" for n, count in sorted(sizes.items()))
    )
    say("  ended by: " + _list(Counter(e.ended_by for e in episodes)))
    lasted = sorted(
        e.ended_at - e.failures[0].at for e in episodes if e.ended_at is not None
    )
    if lasted:
        say(
            f"  from the first failed check to the end: median "
            f"{lasted[len(lasted) // 2]:.3f} s, longest {lasted[-1]:.3f} s"
        )
    several = [e for e in episodes if len(e.failures) >= 2]
    if several:
        nearest = max(several, key=lambda e: e.span)
        say(
            f"  nearest to divergence: {len(nearest.failures)} failed checks of "
            f"{short(nearest.token_id)} from {when(nearest.failures[0].at)}, "
            f"{nearest.span:.3f} s apart, against {replay.grace:g} s"
        )
    else:
        say("  nearest to divergence: no episode had two failed checks")
    say("  each: first failed check, token, failed checks, span, end, causes")
    for e in episodes:
        causes = Counter(failure.cause for failure in e.failures)
        say(
            f"    {when(e.failures[0].at)} {short(e.token_id)} {len(e.failures)} "
            f"{e.span:.3f} s, {e.ended_by}; {_list(causes)}"
        )


def _list(counts: Counter[str]) -> str:
    return ", ".join(f"{kind} {n}" for kind, n in counts.most_common())


def _share(n: int, of: int) -> str:
    return f"{n} of {of} ({n / of:.1%})"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Replay a research timeline's hash checks."
    )
    parser.add_argument("timeline", type=Path)
    parser.add_argument("--failures", action="store_true", help="list each one")
    args = parser.parse_args(argv)
    replay, inferred, stats = replay_timeline(args.timeline)
    report(replay, inferred, stats, sys.stdout, each=args.failures)
    return 0


if __name__ == "__main__":
    sys.exit(main())
