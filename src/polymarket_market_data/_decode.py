"""Frame decoding, repeat detection, and the tokens an undecodable frame or
event may affect (spec/client.md, Decoding and Repeated messages).

Pure: no I/O and no timers. A frame is parsed with ``json.loads`` with
``parse_float=Decimal`` and ``parse_constant=Decimal``, so that no value
passes through ``float``, and each object in it is then validated by the
Pydantic model for its ``event_type`` (D8). The models are private and
lenient: they declare only the fields the client uses and ignore the rest.
A validation error makes the event undecodable; no frame makes the decoder
raise.

The decoder copies the fields it uses from a model into the record and sets
the client's own fields itself. Those that need the client's state are left
for the state machine to set: ``repeat``, ``PriceChange.applied`` and
``before_book`` are false, ``BookEvent.held_book_matched`` is ``None``, and
``UndecodableFrame.affected`` is empty.
"""

import json
import re
from collections import Counter, deque
from collections.abc import Hashable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import Annotated, Any, ClassVar, Literal, TypedDict

from pydantic import BaseModel, ConfigDict, Field, PlainValidator, ValidationError

from ._records import (
    BestBidAskEvent,
    BookEvent,
    LastTradePriceEvent,
    Level,
    Market,
    MarketResolvedEvent,
    NewMarketEvent,
    PriceChange,
    PriceChangeEvent,
    Side,
    TickSizeChangeEvent,
    UndecodableFrame,
    UnknownEvent,
)

EventRecord = (
    BookEvent
    | PriceChangeEvent
    | BestBidAskEvent
    | LastTradePriceEvent
    | TickSizeChangeEvent
    | MarketResolvedEvent
    | NewMarketEvent
)


@dataclass(frozen=True, slots=True)
class Impact:
    """What an undecodable frame or event may have changed.

    ``tokens`` are the tokens it names. ``markets`` are the markets it names
    where a part of it names no token the client can read, and
    ``connection`` is set when such a part names no market it can read
    either, or when nothing in the frame can be attributed.
    """

    tokens: tuple[str, ...] = ()
    markets: tuple[str, ...] = ()
    connection: bool = False

    def may_affect(self, markets: Iterable[Market]) -> tuple[str, ...]:
        """The tokens of ``markets``, the desired markets on the connection
        in desired-set order, that this may have changed, in that order."""
        named = set(self.tokens)
        attributed = set(self.markets)
        return tuple(
            token
            for market in markets
            for token in market.token_ids
            if self.connection or market.condition_id in attributed or token in named
        )


NO_IMPACT = Impact()
EVERY_TOKEN = Impact(connection=True)


@dataclass(frozen=True, slots=True)
class DecodedEvent:
    """A decoded event, with its content for repeat detection."""

    record: EventRecord
    content: Hashable


@dataclass(frozen=True, slots=True)
class Undecodable:
    """An undecodable frame or event, with what it may have changed."""

    record: UndecodableFrame
    impact: Impact


Decoded = DecodedEvent | UnknownEvent | Undecodable


@dataclass(frozen=True, slots=True)
class _Frame:
    text: str
    received_at: datetime
    connection: int
    frame: int
    keep_raw: bool


def decode_frame(
    data: str | bytes,
    *,
    received_at: datetime,
    connection: int,
    frame: int,
    keep_raw: bool,
) -> list[Decoded]:
    """Decode one frame that arrived after the subscription frame, other
    than ``PONG``, into its items in the order of the frame.

    ``data`` is a text frame's text or a binary frame's bytes. ``frame`` is
    its number on the connection, 1 for the first.
    """
    if isinstance(data, bytes):
        return [
            Undecodable(
                UndecodableFrame(
                    reason="binary",
                    event_type=None,
                    error=f"binary frame of {len(data)} bytes",
                    raw=data.hex(),
                    received_at=received_at,
                    connection=connection,
                    frame=frame,
                    index=None,
                    affected=(),
                ),
                EVERY_TOKEN,
            )
        ]
    context = _Frame(data, received_at, connection, frame, keep_raw)
    try:
        return _decode_text(context)
    except RecursionError:
        # Nesting deeper than json.loads can parse.
        return [_invalid_json(context, "JSON nested too deeply to decode")]


# A \u escape of a UTF-16 surrogate, paired or not, as JSON text writes one.
_SURROGATE_ESCAPE = re.compile(r"\\u[dD][89a-fA-F]")
_SURROGATE = re.compile("[\ud800-\udfff]")


def _decode_text(context: _Frame) -> list[Decoded]:
    # A member that a later one of the same name overwrites is not in the
    # parsed value. So when the text may hold a surrogate, the values of
    # every object whose names repeat are kept as it is parsed, and searched
    # too.
    escapes = _SURROGATE_ESCAPE.search(context.text) is not None
    overwritten: list[object] = []
    try:
        parsed = json.loads(
            context.text,
            parse_float=Decimal,
            parse_constant=Decimal,
            object_pairs_hook=partial(_object, overwritten) if escapes else None,
        )
    except ValueError as error:
        # JSONDecodeError, or an integer too long to convert.
        return [_invalid_json(context, str(error))]
    except InvalidOperation:
        # A number whose exponent is beyond Decimal's range, such as
        # 1e9999999999999999999, which parse_float cannot convert.
        return [_invalid_json(context, "a number's exponent is beyond Decimal's range")]
    if escapes and _holds_surrogate([parsed, overwritten]):
        # A frame holding an unpaired surrogate anywhere is not JSON, so no
        # record holds a string that cannot be encoded as UTF-8
        # (spec/client.md, Unpaired surrogates).
        return [_invalid_json(context, "a string holds an unpaired surrogate escape")]
    if isinstance(parsed, list):
        return [_decode_item(context, item, index) for index, item in enumerate(parsed)]
    return [_decode_item(context, parsed, None)]


def _object(
    overwritten: list[object], members: list[tuple[str, Any]]
) -> dict[str, Any]:
    """Build an object as json.loads does, where the last member of a name
    gives its value, and keep the values of every member when a name
    repeats."""
    value = dict(members)
    if len(value) < len(members):
        overwritten.extend(member for _, member in members)
    return value


def _holds_surrogate(value: object) -> bool:
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            if _SURROGATE.search(item):
                return True
        elif isinstance(item, dict):
            for name, member in item.items():
                if _SURROGATE.search(name):
                    return True
                pending.append(member)
        elif isinstance(item, list):
            pending.extend(item)
    return False


def _invalid_json(context: _Frame, error: str) -> Undecodable:
    return Undecodable(
        UndecodableFrame(
            reason="invalid_json",
            event_type=None,
            error=error,
            raw=context.text,
            received_at=context.received_at,
            connection=context.connection,
            frame=context.frame,
            index=None,
            affected=(),
        ),
        EVERY_TOKEN,
    )


_JSON_KINDS: dict[type, str] = {
    str: "string",
    int: "number",
    Decimal: "number",
    bool: "boolean",
    type(None): "null",
    list: "array",
}


def _decode_item(context: _Frame, item: object, index: int | None) -> Decoded:
    if not isinstance(item, dict):
        kind = _JSON_KINDS.get(type(item), type(item).__name__)
        where = "the frame" if index is None else f"item {index}"
        return Undecodable(
            UndecodableFrame(
                reason="not_object",
                event_type=None,
                error=f"{where} is a JSON {kind}, not an object",
                raw=context.text,
                received_at=context.received_at,
                connection=context.connection,
                frame=context.frame,
                index=index,
                affected=(),
            ),
            EVERY_TOKEN,
        )
    event_type = item.get("event_type")
    if not isinstance(event_type, str) or event_type not in _MODELS:
        return UnknownEvent(
            event_type=event_type if isinstance(event_type, str) else None,
            payload=item,
            raw=context.text,
            received_at=context.received_at,
            connection=context.connection,
            frame=context.frame,
            index=0 if index is None else index,
        )
    model = _MODELS[event_type]
    try:
        wire = model.model_validate(item)
    except ValidationError as error:
        return Undecodable(
            UndecodableFrame(
                reason="invalid_event",
                event_type=event_type,
                error=_describe(error),
                raw=context.text,
                received_at=context.received_at,
                connection=context.connection,
                frame=context.frame,
                index=index,
                affected=(),
            ),
            model.impact(item, _failed(error)),
        )
    record = wire.to_record(context, 0 if index is None else index, item)
    return DecodedEvent(record, content_key(item))


def _describe(error: ValidationError) -> str:
    return "; ".join(
        f"{'.'.join(str(part) for part in detail['loc']) or 'event'}: {detail['msg']}"
        for detail in error.errors(include_url=False)
    )


def _failed(error: ValidationError) -> set[tuple[int | str, ...]]:
    """The locations at which the event failed validation."""
    return {tuple(detail["loc"]) for detail in error.errors(include_url=False)}


# Wire models: one per event type, private to this module, declaring only the
# fields the decoder uses (spec/client.md, Decoding).


def _timestamp(value: object) -> int:
    """Milliseconds as the source sends them: a string of digits, or an
    integer."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.isascii() and value.isdigit():
        return int(value)
    raise ValueError("timestamp must be an integer of milliseconds")


_Timestamp = Annotated[int, PlainValidator(_timestamp)]
# A finite decimal; NaN and infinities are refused, as strings or literals.
_Decimal = Annotated[Decimal, Field(allow_inf_nan=False)]
_Side = Literal["BUY", "SELL"]


class _Wire(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


class _Common(TypedDict):
    event_type: str
    market: str
    source_timestamp_ms: int
    received_at: datetime
    connection: int
    frame: int
    index: int
    repeat: bool
    raw: str | None


def _common(
    context: _Frame, index: int, event_type: str, market: str, timestamp: int
) -> _Common:
    return _Common(
        event_type=event_type,
        market=market,
        source_timestamp_ms=timestamp,
        received_at=context.received_at,
        connection=context.connection,
        frame=context.frame,
        index=index,
        repeat=False,
        raw=context.text if context.keep_raw else None,
    )


class _EventWire(_Wire):
    event_type: ClassVar[str]

    def to_record(
        self, context: _Frame, index: int, item: dict[str, Any]
    ) -> EventRecord:
        raise NotImplementedError

    @classmethod
    def impact(cls, item: dict[str, Any], failed: set[tuple[int | str, ...]]) -> Impact:
        """What the event may have changed when it fails validation. Only
        the types that change a book change any token's state."""
        return NO_IMPACT


def _market_impact(
    item: dict[str, Any], failed: set[tuple[int | str, ...]], tokens: list[str]
) -> Impact:
    """A part of the event names no token the client can read: its market's
    tokens are in doubt, or, with no readable market, every token on the
    connection."""
    named = tuple(dict.fromkeys(tokens))
    if ("market",) in failed:
        return Impact(tokens=named, connection=True)
    return Impact(tokens=named, markets=(item["market"],))


class _OneTokenWire(_EventWire):
    @classmethod
    def impact(cls, item: dict[str, Any], failed: set[tuple[int | str, ...]]) -> Impact:
        if ("asset_id",) in failed:
            return _market_impact(item, failed, [])
        return Impact(tokens=(item["asset_id"],))


class _LevelWire(_Wire):
    price: _Decimal
    size: _Decimal


def _levels(levels: list[_LevelWire]) -> tuple[Level, ...]:
    return tuple(Level(level.price, level.size) for level in levels)


class _BookWire(_OneTokenWire):
    event_type = "book"

    market: str
    asset_id: str
    timestamp: _Timestamp
    hash: str
    bids: list[_LevelWire]
    asks: list[_LevelWire]
    tick_size: _Decimal | None = None
    last_trade_price: _Decimal | None = None

    def to_record(self, context: _Frame, index: int, item: dict[str, Any]) -> BookEvent:
        return BookEvent(
            **_common(context, index, "book", self.market, self.timestamp),
            asset_id=self.asset_id,
            hash=self.hash,
            bids=_levels(self.bids),
            asks=_levels(self.asks),
            tick_size=self.tick_size,
            last_trade_price=self.last_trade_price,
            opening=context.frame == 1,
            held_book_matched=None,
        )


class _ChangeWire(_Wire):
    asset_id: str
    price: _Decimal
    size: _Decimal
    side: _Side
    hash: str
    best_bid: _Decimal | None = None
    best_ask: _Decimal | None = None


class _PriceChangeWire(_EventWire):
    event_type = "price_change"

    market: str
    timestamp: _Timestamp
    price_changes: list[_ChangeWire]

    def to_record(
        self, context: _Frame, index: int, item: dict[str, Any]
    ) -> PriceChangeEvent:
        return PriceChangeEvent(
            **_common(context, index, "price_change", self.market, self.timestamp),
            changes=tuple(
                PriceChange(
                    asset_id=change.asset_id,
                    side=Side(change.side),
                    price=change.price,
                    size=change.size,
                    hash=change.hash,
                    best_bid=change.best_bid,
                    best_ask=change.best_ask,
                    applied=False,
                    before_book=False,
                )
                for change in self.price_changes
            ),
        )

    @classmethod
    def impact(cls, item: dict[str, Any], failed: set[tuple[int | str, ...]]) -> Impact:
        if ("price_changes",) in failed:
            # Not a list: no entry names a token the client can read.
            return _market_impact(item, failed, [])
        tokens: list[str] = []
        unattributed = False
        for position, entry in enumerate(item["price_changes"]):
            if ("price_changes", position) in failed or (
                "price_changes",
                position,
                "asset_id",
            ) in failed:
                unattributed = True
            else:
                tokens.append(entry["asset_id"])
        if unattributed:
            return _market_impact(item, failed, tokens)
        return Impact(tokens=tuple(dict.fromkeys(tokens)))


class _BestBidAskWire(_EventWire):
    event_type = "best_bid_ask"

    market: str
    asset_id: str
    timestamp: _Timestamp
    best_bid: _Decimal
    best_ask: _Decimal
    spread: _Decimal | None = None

    def to_record(
        self, context: _Frame, index: int, item: dict[str, Any]
    ) -> BestBidAskEvent:
        return BestBidAskEvent(
            **_common(context, index, "best_bid_ask", self.market, self.timestamp),
            asset_id=self.asset_id,
            best_bid=self.best_bid,
            best_ask=self.best_ask,
            spread=self.spread,
        )


class _LastTradePriceWire(_EventWire):
    event_type = "last_trade_price"

    market: str
    asset_id: str
    timestamp: _Timestamp
    price: _Decimal
    size: _Decimal
    side: _Side
    fee_rate_bps: _Decimal | None = None
    transaction_hash: str | None = None

    def to_record(
        self, context: _Frame, index: int, item: dict[str, Any]
    ) -> LastTradePriceEvent:
        return LastTradePriceEvent(
            **_common(context, index, "last_trade_price", self.market, self.timestamp),
            asset_id=self.asset_id,
            price=self.price,
            size=self.size,
            side=Side(self.side),
            fee_rate_bps=self.fee_rate_bps,
            transaction_hash=self.transaction_hash,
        )


class _TickSizeChangeWire(_OneTokenWire):
    event_type = "tick_size_change"

    market: str
    asset_id: str
    timestamp: _Timestamp
    old_tick_size: _Decimal
    new_tick_size: _Decimal

    def to_record(
        self, context: _Frame, index: int, item: dict[str, Any]
    ) -> TickSizeChangeEvent:
        return TickSizeChangeEvent(
            **_common(context, index, "tick_size_change", self.market, self.timestamp),
            asset_id=self.asset_id,
            old_tick_size=self.old_tick_size,
            new_tick_size=self.new_tick_size,
        )


class _MarketResolvedWire(_EventWire):
    event_type = "market_resolved"

    market: str
    timestamp: _Timestamp
    assets_ids: list[str]
    winning_asset_id: str
    id: str | None = None
    winning_outcome: str | None = None
    tags: list[str] | None = None

    def to_record(
        self, context: _Frame, index: int, item: dict[str, Any]
    ) -> MarketResolvedEvent:
        return MarketResolvedEvent(
            **_common(context, index, "market_resolved", self.market, self.timestamp),
            id=self.id,
            assets_ids=tuple(self.assets_ids),
            winning_asset_id=self.winning_asset_id,
            winning_outcome=self.winning_outcome,
            tags=tuple(self.tags or ()),
        )


class _NewMarketWire(_EventWire):
    event_type = "new_market"

    id: str
    timestamp: _Timestamp
    market: str | None = None
    condition_id: str | None = None
    slug: str | None = None
    question: str | None = None
    assets_ids: list[str] | None = None

    def to_record(
        self, context: _Frame, index: int, item: dict[str, Any]
    ) -> NewMarketEvent:
        return NewMarketEvent(
            **_common(context, index, "new_market", self.market or "", self.timestamp),
            id=self.id,
            condition_id=(
                self.condition_id if self.condition_id is not None else self.market
            ),
            slug=self.slug,
            question=self.question,
            assets_ids=tuple(self.assets_ids or ()),
            payload=item,
        )


_MODELS: dict[str, type[_EventWire]] = {
    model.event_type: model
    for model in (
        _BookWire,
        _PriceChangeWire,
        _BestBidAskWire,
        _LastTradePriceWire,
        _TickSizeChangeWire,
        _MarketResolvedWire,
        _NewMarketWire,
    )
}


# Repeat detection (spec/client.md, Repeated messages).


def content_key(item: Mapping[str, Any]) -> Hashable:
    """An event's content, compared as its JSON value: members in any order,
    and a ``price_change``'s entries as a multiset."""
    entries = item.get("price_changes")
    multiset = item.get("event_type") == "price_change" and isinstance(entries, list)
    return frozenset(
        (
            name,
            ("multiset", frozenset(Counter(map(_tokens, value)).items()))
            if multiset and name == "price_changes"
            else _tokens(value),
        )
        for name, value in item.items()
    )


# The end of an object or an array, among a value's tokens.
_END = ("end",)


def _tokens(value: object) -> tuple[Hashable, ...]:
    """A JSON value as a flat sequence of tokens, with an object's members
    in order of name, so that two values are equal as JSON values when
    their tokens are.

    The tokens are built, hashed, and compared without recursion, so that a
    member nested as deeply as json.loads allows, even one no model
    declares, never makes a known event undecodable.
    """
    tokens: list[Hashable] = []
    pending: list[object] = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, tuple):
            # A token pushed below; no JSON value is a tuple.
            tokens.append(item)
        elif isinstance(item, dict):
            tokens.append(("object",))
            pending.append(_END)
            for name in sorted(item, reverse=True):
                pending += (item[name], ("member", name))
        elif isinstance(item, list):
            tokens.append(("array",))
            pending.append(_END)
            pending += reversed(item)
        else:
            tokens.append(_scalar(item))
    return tuple(tokens)


def _scalar(value: object) -> Hashable:
    if isinstance(value, bool):
        return ("boolean", value)
    if isinstance(value, Decimal) and value.is_nan():
        # NaN equals nothing, itself included.
        return ("nan",)
    if isinstance(value, int | Decimal):
        # JSON numbers compare by value, so 1 and 1.0 are the same.
        return ("number", value)
    if isinstance(value, str):
        return ("string", value)
    return ("null",)


class RepeatDetector:
    """Judges events repeats on one connection: an event is a repeat when
    one with the same content arrived within ``window`` seconds before it.

    Times are seconds on a monotonic clock, given by the caller.
    """

    def __init__(self, window: float) -> None:
        self._window = window
        self._last: dict[Hashable, float] = {}
        self._arrivals: deque[tuple[float, Hashable]] = deque()

    def check(self, content: Hashable, now: float) -> bool:
        """Record that an event with this content arrived at ``now``, and
        return whether it is a repeat."""
        while self._arrivals and now - self._arrivals[0][0] > self._window:
            arrived, expired = self._arrivals.popleft()
            if self._last.get(expired) == arrived:
                del self._last[expired]
        previous = self._last.get(content)
        self._last[content] = now
        self._arrivals.append((now, content))
        return previous is not None and now - previous <= self._window
