"""Token states, capture gaps, and the order of records, driven by inputs
(spec/client.md, Per-token state machine, Recovery contract, Settlement, and
Record order).

Pure: no I/O and no timers. The client feeds the machine what happened, in
the order it happened: changes to the desired set, each step of a
connection, each decoded frame, timers that fired, lookup answers, and hash
check results. The machine produces every record, in the contract's record
order, and the effects the client must carry out: starting and ending a
market's settlement confirmation. The connection's own decisions, such as
attempt numbers and delays, come in as inputs.

A token takes part only once a subscription frame has named it: until then
it has no state, holds no book, and no transition applies to it.
"""

from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime

from ._book import Book
from ._decode import Decoded, DecodedEvent, EventRecord, RepeatDetector, Undecodable
from ._lookup import MarketInfo
from ._records import (
    BookEvent,
    CaptureGap,
    ConnectionState,
    ConnectionStateChange,
    Market,
    MarketResolvedEvent,
    NewMarketEvent,
    PriceChangeEvent,
    TokenState,
    TokenStateChange,
    UndecodableFrame,
    UnknownEvent,
)

Record = (
    EventRecord
    | UnknownEvent
    | UndecodableFrame
    | TokenStateChange
    | ConnectionStateChange
    | CaptureGap
)

# Where an item arrived: its connection's generation, its frame's number on
# that connection, and its position in the frame, 0 for a frame holding one
# object or none. Positions compare in arrival order.
Position = tuple[int, int, int]

ALL_RESOLVED = "all subscribed assets resolved"

_TAKING_PART = (TokenState.SYNCHRONIZING, TokenState.READY, TokenState.UNCERTAIN)
_CONNECTED = (ConnectionState.OPEN, ConnectionState.SUBSCRIBED, ConnectionState.ENDED)


@dataclass(frozen=True, slots=True)
class StartConfirmation:
    """Confirm a market's settlement through lookup: ask now, again every
    ``settlement_poll_interval``, and report ``confirmation_timeout`` when
    ``settlement_confirm_timeout`` has passed (spec/client.md, Settlement)."""

    condition_id: str
    slug: str


@dataclass(frozen=True, slots=True)
class EndConfirmation:
    """Stop confirming a market's settlement: its confirmation has ended."""

    condition_id: str


Effect = StartConfirmation | EndConfirmation


@dataclass(slots=True)
class Counts:
    """The counters only the machine can keep (spec/client.md, Statistics)."""

    frames: int = 0
    frames_after_interruption: int = 0
    events: Counter[str] = field(default_factory=Counter)
    repeats: int = 0
    unknown: int = 0
    undecodable: Counter[str] = field(default_factory=Counter)
    new_market_dropped: int = 0
    discarded_outside: int = 0
    connections: int = 0
    interruptions: Counter[str] = field(default_factory=Counter)


@dataclass(slots=True)
class _Gap:
    cause: str
    close_code: int | None
    close_reason: str | None
    last_confirmed_at: datetime
    detected_at: datetime
    connection_before: int


@dataclass(slots=True)
class _Token:
    id: str
    market: str
    state: TokenState | None = None
    reason: str | None = None
    # The generation of the last connection whose subscription frame named it.
    connection: int | None = None
    # The book the client holds, kept while the token is uncertain so that a
    # fresh book can be compared with it.
    book: Book | None = None
    # Whether a book arrived for it on the current connection, and the source
    # timestamp of the first that did.
    booked: bool = False
    opening_timestamp: int | None = None
    gap: _Gap | None = None
    # The latest undecodable frame or event that may affect it.
    undecodable_at: Position | None = None


class StateMachine:
    """The desired set, each token's state and book, and the records."""

    def __init__(
        self,
        *,
        repeat_window: float,
        deliver_new_market: bool,
        can_look_up: bool,
        markets: Iterable[Market] = (),
    ) -> None:
        self._repeat_window = repeat_window
        self._deliver_new_market = deliver_new_market
        self._can_look_up = can_look_up
        # The desired set, in the order the markets were added.
        self._markets: dict[str, Market] = {}
        self._tokens: dict[str, _Token] = {}
        self._confirming: set[str] = set()
        self._phase: ConnectionState | None = None
        self._generation = 0
        self._interrupted = False
        self._last_frame_at: datetime | None = None
        self._repeats = RepeatDetector(repeat_window)
        self._records: list[Record] = []
        self._effects: list[Effect] = []
        self.counts = Counts()
        self.subscribe(markets)

    # Outputs and queries.

    def take(self) -> list[Record]:
        """The records produced since the last call, in order."""
        records, self._records = self._records, []
        return records

    def take_effects(self) -> list[Effect]:
        """The effects produced since the last call, in order."""
        effects, self._effects = self._effects, []
        return effects

    @property
    def desired(self) -> tuple[Market, ...]:
        return tuple(self._markets.values())

    @property
    def phase(self) -> ConnectionState | None:
        """The state of the latest connection record, or ``None`` before
        the first."""
        return self._phase

    def state(self, token_id: str) -> TokenState | None:
        token = self._tokens.get(token_id)
        return None if token is None else token.state

    def reason(self, token_id: str) -> str | None:
        token = self._tokens.get(token_id)
        return None if token is None else token.reason

    def book(self, token_id: str) -> Book | None:
        token = self._tokens.get(token_id)
        return None if token is None else token.book

    def subscription(self) -> tuple[str, ...]:
        """The tokens the next subscription frame names: every token of
        every desired market, in desired-set order."""
        return tuple(
            token for market in self._markets.values() for token in market.token_ids
        )

    # Changes to the desired set (spec/client.md, Public interface).

    def subscribe(self, markets: Iterable[Market]) -> bool:
        """Add markets to the end of the desired set, leaving any already in
        it where it is, and return whether any was added. A market removed
        or settled before starts over. Raises ``ValueError`` if a token
        would belong to two markets of the desired set."""
        added: dict[str, Market] = {}
        owners = {
            token_id: market.condition_id
            for market in self._markets.values()
            for token_id in market.token_ids
        }
        for market in markets:
            if market.condition_id in self._markets or market.condition_id in added:
                continue
            if len(set(market.token_ids)) < len(market.token_ids):
                raise ValueError(f"market {market.condition_id} lists a token twice")
            for token_id in market.token_ids:
                owner = owners.setdefault(token_id, market.condition_id)
                if owner != market.condition_id:
                    raise ValueError(
                        f"token {token_id} belongs to market {owner} in the desired set"
                    )
            added[market.condition_id] = market
        for condition_id, market in added.items():
            self._markets[condition_id] = market
            for token_id in market.token_ids:
                token = self._tokens.get(token_id)
                if token is None:
                    self._tokens[token_id] = _Token(token_id, condition_id)
                else:
                    # A token removed or settled before starts over: it holds
                    # no book and no gap, and keeps its state until a
                    # subscription frame names it.
                    token.market = condition_id
                    self._forget(token)
        return bool(added)

    def unsubscribe(self, condition_ids: Iterable[str], at: datetime) -> None:
        """Remove markets from the desired set (T17). An ID not in it is
        ignored."""
        removing = set(condition_ids)
        for condition_id in [c for c in self._markets if c in removing]:
            for token in self._taking_part(self._markets[condition_id]):
                self._end_gap(token, "removed", at)
                self._move(token, TokenState.REMOVED, "removed", at)
                self._forget(token)
            self._leave(condition_id)
        self._idle_if_empty(at)

    # A connection's steps (spec/client.md, The connection).

    def connecting(self, attempt: int, at: datetime) -> None:
        self._connection_record(ConnectionState.CONNECTING, at, attempt=attempt)

    def opened(self, generation: int, at: datetime) -> None:
        """The handshake of a new connection completed."""
        self._generation = generation
        self._interrupted = False
        self._last_frame_at = None
        self.counts.connections += 1
        self._connection_record(ConnectionState.OPEN, at, connection=generation)

    def subscribed(self, tokens: Sequence[str], at: datetime) -> None:
        """The subscription frame naming ``tokens``, as
        ``subscription()`` gave them, has been sent on the current
        connection (T1, T2)."""
        self._repeats = RepeatDetector(self._repeat_window)
        self._connection_record(
            ConnectionState.SUBSCRIBED, at, connection=self._generation
        )
        for token_id in tokens:
            token = self._tokens.get(token_id)
            if token is None or token.market not in self._markets:
                continue
            token.connection = self._generation
            token.booked = False
            token.opening_timestamp = None
            # An open gap stays open (T2).
            self._move(token, TokenState.SYNCHRONIZING, "subscribed", at)

    def pong(self, generation: int, received_at: datetime) -> None:
        """A ``PONG`` arrived on the connection of ``generation``. It
        confirms the data before it, since it is queued behind that data."""
        if generation == self._generation and not self._interrupted:
            self._last_frame_at = received_at

    def frame(
        self,
        generation: int,
        items: Sequence[Decoded],
        *,
        received_at: datetime,
        clock: float,
    ) -> None:
        """A frame other than ``PONG``, decoded, arrived on the connection
        of ``generation``. ``clock`` is its arrival on a monotonic clock, in
        seconds, for repeat detection."""
        self.counts.frames += 1
        if generation != self._generation or self._interrupted:
            # Nothing more from a connection is delivered or applied once it
            # has been interrupted.
            self.counts.frames_after_interruption += 1
            return
        self._last_frame_at = received_at
        for item in items:
            self._item(item, received_at, clock)

    def delivers(self, generation: int, items: Sequence[Decoded]) -> bool:
        """Whether ``frame`` would deliver a market-event record for these
        items, which decides whether the frame can reach the queue's limit
        (spec/client.md, Consumer handoff). The items before the first such
        record are dropped or discarded and change no state, so the state
        the frame finds decides."""
        if generation != self._generation or self._interrupted:
            return False
        for item in items:
            if not isinstance(item, DecodedEvent):
                return True  # UnknownEvent and UndecodableFrame always are
            record = item.record
            if isinstance(record, NewMarketEvent):
                if self._deliver_new_market:
                    return True
            elif not self._outside(record):
                return True
        return False

    def book_timeout(self, generation: int, at: datetime) -> None:
        """``book_timeout`` has passed since the subscription frame of the
        connection of ``generation`` (T4)."""
        if generation != self._generation or self._interrupted:
            return
        waiting = [
            token
            for token in self._on_connection()
            if token.state is TokenState.SYNCHRONIZING
        ]
        for token in waiting:
            self._move(token, TokenState.UNCERTAIN, "no_book", at)
        unconfirmable: list[_Token] = []
        for condition_id in dict.fromkeys(token.market for token in waiting):
            market = self._markets[condition_id]
            if condition_id in self._confirming:
                continue  # it joins the confirmation already running
            if market.slug is None or not self._can_look_up:
                unconfirmable += [t for t in waiting if t.market == condition_id]
            else:
                self._confirming.add(condition_id)
                self._effects.append(StartConfirmation(condition_id, market.slug))
        # T13 at once, after the no_book records, in the same order.
        for token in unconfirmable:
            self._move(token, TokenState.UNCERTAIN, "settlement_unconfirmed", at)

    def interrupted(
        self,
        cause: str,
        at: datetime,
        *,
        close_code: int | None = None,
        close_reason: str | None = None,
    ) -> None:
        """The current connection, subscribed, ended in a way that may have
        lost events (T14; spec/client.md, Record order, rule 4). ``cause`` is
        the interruption's reason."""
        self._interrupted = True
        self.counts.interruptions[cause] += 1
        confirmed = self._last_frame_at
        self._connection_record(
            ConnectionState.INTERRUPTED,
            at,
            connection=self._generation,
            reason=cause,
            close_code=close_code,
            close_reason=close_reason,
            last_confirmed_at=confirmed,
        )
        for token in self._on_connection():
            if token.book is not None and token.gap is None:
                # A gap opens for a token holding a book. One already open,
                # since it got no fresh book after an earlier interruption,
                # stays as it is.
                token.gap = _Gap(
                    cause=cause,
                    close_code=close_code,
                    close_reason=close_reason,
                    # A token holding a book with no open gap got its book on
                    # this connection, so a frame confirmed it.
                    last_confirmed_at=confirmed or at,
                    detected_at=at,
                    connection_before=self._generation,
                )
            token.booked = False
            self._move(
                token,
                TokenState.UNCERTAIN,
                "interrupted",
                at,
                last_confirmed_at=confirmed,
            )

    def recovering(
        self,
        attempt: int,
        reason: str,
        at: datetime,
        *,
        retry_in: float | None = None,
        detail: str | None = None,
    ) -> None:
        self._connection_record(
            ConnectionState.RECOVERING,
            at,
            attempt=attempt,
            reason=reason,
            retry_in=retry_in,
            detail=detail,
        )

    def all_resolved(self, at: datetime) -> None:
        """The server closed the current connection with ``1000 all
        subscribed assets resolved`` (T16). It is not an interruption."""
        self._interrupted = True
        unsettled = self._on_connection()
        if unsettled:
            self._connection_record(
                ConnectionState.ENDED,
                at,
                connection=self._generation,
                close_code=1000,
                close_reason=ALL_RESOLVED,
            )
            for condition_id in dict.fromkeys(token.market for token in unsettled):
                self._settle(condition_id, "all_resolved_close", None, at)
        self._idle_if_empty(at)

    def failed(self, reason: str, at: datetime, *, detail: str | None = None) -> None:
        """Reconnection exhausted its bounds (T18; spec/client.md, Record
        order, rule 4). ``reason`` is ``max_attempts`` or
        ``max_recovery_time``; ``detail`` says how the last attempt failed,
        if it did."""
        for market in self._markets.values():
            for token_id in market.token_ids:
                self._end_gap(self._tokens[token_id], "recovery_failed", at)
        self._connection_record(
            ConnectionState.FAILED, at, reason=reason, detail=detail
        )

    # Settlement confirmation (spec/client.md, Settlement).

    def confirmation(
        self, condition_id: str, info: MarketInfo | None, at: datetime
    ) -> None:
        """Lookup answered for a market being confirmed: ``None`` if it
        found no market (T12), or what it found, which settles the market if
        it is closed (T11). Lookup that failed or shows the market open
        changes nothing, so the client need not report it."""
        if condition_id not in self._confirming:
            return
        if info is None:
            self._end_confirmation(condition_id)
            for token in self._no_book(condition_id):
                self._move(token, TokenState.UNCERTAIN, "unknown_market", at)
        elif info.closed:
            self._settle(condition_id, "lookup_closed", info.winning_asset_id, at)
            self._idle_if_empty(at)

    def confirmation_timeout(self, condition_id: str, at: datetime) -> None:
        """``settlement_confirm_timeout`` passed without lookup showing the
        market closed (T13)."""
        if condition_id not in self._confirming:
            return
        self._end_confirmation(condition_id)
        for token in self._no_book(condition_id):
            self._move(token, TokenState.UNCERTAIN, "settlement_unconfirmed", at)

    # Hash checks, whose results step 10 supplies (D4).

    def hash_diverged(self, token_id: str, at: datetime) -> None:
        """Hash verification reports divergence for a token (T7)."""
        token = self._tokens.get(token_id)
        if token is not None and token.state is TokenState.READY:
            self._move(token, TokenState.UNCERTAIN, "hash_mismatch", at)

    def hash_verified(self, token_id: str, burst_end: Position, at: datetime) -> None:
        """A burst's check verified for a token; ``burst_end`` is where the
        burst's last entry arrived (T9)."""
        token = self._tokens.get(token_id)
        if (
            token is not None
            and token.state is TokenState.UNCERTAIN
            and token.reason in ("hash_mismatch", "undecodable")
            and (token.undecodable_at is None or burst_end > token.undecodable_at)
        ):
            self._move(token, TokenState.READY, "hash_verified", at)

    # A frame's items (spec/client.md, Event handling).

    def _item(self, item: Decoded, received_at: datetime, clock: float) -> None:
        """One item of a frame, called for each in turn by ``frame``
        (spec/client.md, Record order, rule 1)."""
        if isinstance(item, UnknownEvent):
            self.counts.unknown += 1
            self._records.append(item)
        elif isinstance(item, Undecodable):
            self._undecodable(item, received_at)
        else:
            self._event(item, received_at, clock)

    def _event(self, item: DecodedEvent, received_at: datetime, clock: float) -> None:
        record = item.record
        self.counts.events[record.event_type] += 1
        if isinstance(record, NewMarketEvent):
            if not self._deliver_new_market:
                self.counts.new_market_dropped += 1
                return
        elif self._outside(record):
            # Before repeat detection, so it does not count as a repeat.
            self.counts.discarded_outside += 1
            return
        repeat = self._repeats.check(item.content, clock)
        if repeat:
            self.counts.repeats += 1
        if isinstance(record, BookEvent):
            self._book(record, repeat, received_at)
        elif isinstance(record, PriceChangeEvent):
            self._price_change(record, repeat)
        elif isinstance(record, MarketResolvedEvent):
            self._records.append(replace(record, repeat=repeat))
            if not repeat:
                self._settle(
                    record.market,
                    "market_resolved",
                    record.winning_asset_id,
                    received_at,
                )
                self._idle_if_empty(received_at)
        else:
            self._records.append(replace(record, repeat=repeat))

    def _outside(self, record: EventRecord) -> bool:
        """Whether an event names only tokens outside the desired set, or a
        market outside it. A token counts as inside only on a connection
        whose subscription frame named it, so a market added again stays
        outside until one names its tokens."""
        if isinstance(record, NewMarketEvent):
            # A new_market names a new market, never a subscribed one.
            return False
        market = self._markets.get(record.market)
        if market is None:
            return True
        tokens: Iterable[str]
        if isinstance(record, PriceChangeEvent):
            tokens = (change.asset_id for change in record.changes)
        elif isinstance(record, MarketResolvedEvent):
            # It settles its market's tokens that take part (T15).
            tokens = market.token_ids
        else:
            tokens = (record.asset_id,)
        return not any(self._takes_part(token) for token in tokens)

    def _book(self, record: BookEvent, repeat: bool, received_at: datetime) -> None:
        token = self._tokens[record.asset_id]
        fresh = Book.from_event(record)
        matched = None if token.book is None else token.book.matches(fresh)
        self._records.append(replace(record, repeat=repeat, held_book_matched=matched))
        if repeat:
            return
        token.book = fresh
        if not token.booked:
            token.booked = True
            token.opening_timestamp = record.source_timestamp_ms
        if token.state is TokenState.SYNCHRONIZING or (
            token.state is TokenState.UNCERTAIN and token.reason != "interrupted"
        ):
            # T3 and T10 (spec/client.md, Record order, rule 3), the BookEvent
            # already appended above. A book for a ready token replaces its
            # book (T5).
            self._end_gap(
                token,
                "book",
                received_at,
                resumed_at=received_at,
                connection_after=self._generation,
                held_book_matched=matched,
            )
            self._move(token, TokenState.READY, "book", received_at)

    def _price_change(self, record: PriceChangeEvent, repeat: bool) -> None:
        changes = []
        for change in record.changes:
            token = self._tokens.get(change.asset_id)
            booked = (
                token is not None
                and token.booked
                and token.state in _TAKING_PART
                and token.connection == self._generation
            )
            before_book = (
                booked
                and token is not None
                and token.opening_timestamp is not None
                and record.source_timestamp_ms < token.opening_timestamp
            )
            # Applied in arrival order, never for a repeat (T6). A token with
            # no book on this connection has nothing to apply it to.
            applied = booked and not repeat
            if applied and token is not None and token.book is not None:
                token.book.apply(change, record.source_timestamp_ms)
            changes.append(replace(change, applied=applied, before_book=before_book))
        self._records.append(replace(record, repeat=repeat, changes=tuple(changes)))

    def _undecodable(self, item: Undecodable, received_at: datetime) -> None:
        record = item.record
        self.counts.undecodable[record.reason] += 1
        position = (
            record.connection,
            record.frame,
            0 if record.index is None else record.index,
        )
        on_connection = {token.id: token for token in self._on_connection()}
        may_affect = [
            on_connection[token_id]
            for token_id in item.impact.may_affect(self._markets.values())
            if token_id in on_connection
        ]
        for token in may_affect:
            token.undecodable_at = position
        # T8 for each ready token it may affect.
        affected = [token for token in may_affect if token.state is TokenState.READY]
        self._records.append(
            replace(record, affected=tuple(token.id for token in affected))
        )
        for token in affected:
            self._move(token, TokenState.UNCERTAIN, "undecodable", received_at)

    # Transitions and records.

    def _move(
        self,
        token: _Token,
        state: TokenState,
        reason: str,
        at: datetime,
        *,
        last_confirmed_at: datetime | None = None,
        winning_asset_id: str | None = None,
    ) -> None:
        """Give a token a new state or reason, with its record. A record is
        emitted whenever the state or the reason changes, and only then."""
        if token.state is state and token.reason == reason:
            return
        self._records.append(
            TokenStateChange(
                token_id=token.id,
                market=token.market,
                state=state,
                previous=token.state,
                reason=reason,
                at=at,
                connection=token.connection,
                last_confirmed_at=last_confirmed_at,
                winning_asset_id=winning_asset_id,
            )
        )
        token.state = state
        token.reason = reason

    def _end_gap(
        self,
        token: _Token,
        end: str,
        at: datetime,
        *,
        resumed_at: datetime | None = None,
        connection_after: int | None = None,
        held_book_matched: bool | None = None,
    ) -> None:
        """End a token's open gap, if it has one, with its record."""
        gap = token.gap
        if gap is None:
            return
        self._records.append(
            CaptureGap(
                token_id=token.id,
                market=token.market,
                cause=gap.cause,
                close_code=gap.close_code,
                close_reason=gap.close_reason,
                last_confirmed_at=gap.last_confirmed_at,
                detected_at=gap.detected_at,
                resumed_at=resumed_at,
                end=end,
                connection_before=gap.connection_before,
                connection_after=connection_after,
                held_book_matched=held_book_matched,
                at=at,
            )
        )
        token.gap = None

    def _settle(
        self,
        condition_id: str,
        reason: str,
        winning_asset_id: str | None,
        at: datetime,
    ) -> None:
        """Settle every token of a market that takes part (T11, T15, T16;
        spec/client.md, Record order, rule 3); the market leaves the desired
        set."""
        market = self._markets.get(condition_id)
        if market is None:
            return
        for token in self._taking_part(market):
            self._end_gap(token, "settled", at)
            self._move(
                token,
                TokenState.SETTLED,
                reason,
                at,
                winning_asset_id=winning_asset_id,
            )
            self._forget(token)
        self._leave(condition_id)

    def _leave(self, condition_id: str) -> None:
        del self._markets[condition_id]
        if condition_id in self._confirming:
            self._end_confirmation(condition_id)

    def _end_confirmation(self, condition_id: str) -> None:
        self._confirming.discard(condition_id)
        self._effects.append(EndConfirmation(condition_id))

    def _idle_if_empty(self, at: datetime) -> None:
        """No connection is needed once the desired set has no unsettled
        market; one ``idle`` record follows."""
        if self._markets or self._phase in (
            None,
            ConnectionState.IDLE,
            ConnectionState.FAILED,
        ):
            return
        connected = self._phase in _CONNECTED
        self._connection_record(
            ConnectionState.IDLE,
            at,
            connection=self._generation if connected else None,
            reason="no_subscriptions",
        )

    def _connection_record(
        self,
        state: ConnectionState,
        at: datetime,
        *,
        connection: int | None = None,
        attempt: int | None = None,
        reason: str | None = None,
        detail: str | None = None,
        close_code: int | None = None,
        close_reason: str | None = None,
        retry_in: float | None = None,
        last_confirmed_at: datetime | None = None,
    ) -> None:
        self._phase = state
        self._records.append(
            ConnectionStateChange(
                state=state,
                at=at,
                connection=connection,
                attempt=attempt,
                reason=reason,
                detail=detail,
                close_code=close_code,
                close_reason=close_reason,
                retry_in=retry_in,
                last_confirmed_at=last_confirmed_at,
            )
        )

    # Which tokens.

    def _takes_part(self, token_id: str) -> bool:
        """Whether a token takes part on the current connection."""
        token = self._tokens.get(token_id)
        return (
            token is not None
            and token.state in _TAKING_PART
            and token.connection == self._generation
        )

    def _taking_part(self, market: Market) -> list[_Token]:
        """The market's tokens a subscription frame has named and that have
        not settled or been removed since, in ``token_ids`` order
        (spec/client.md, Record order, rule 2)."""
        tokens = (self._tokens[token_id] for token_id in market.token_ids)
        return [token for token in tokens if token.state in _TAKING_PART]

    def _on_connection(self) -> list[_Token]:
        """The desired tokens on the current connection, in desired-set
        order (spec/client.md, Record order, rule 2)."""
        return [
            token
            for market in self._markets.values()
            for token in self._taking_part(market)
            if token.connection == self._generation
        ]

    def _no_book(self, condition_id: str) -> list[_Token]:
        market = self._markets.get(condition_id)
        if market is None:
            return []
        return [
            token
            for token in self._taking_part(market)
            if token.state is TokenState.UNCERTAIN and token.reason == "no_book"
        ]

    @staticmethod
    def _forget(token: _Token) -> None:
        """Drop what a token held, when it leaves the desired set or starts
        over."""
        token.book = None
        token.gap = None
        token.booked = False
        token.opening_timestamp = None
        token.undecodable_at = None
