"""Matching records and statistics (spec/conformance.md, Record notation and
Expectation steps).

A pattern is compiled once, when its scenario is parsed, from the record
type's field annotations, so a word that cannot match its field is a
notation error, not a failed scenario.
"""

import dataclasses
import json
import re
import types
import typing
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, NamedTuple

from polymarket_market_data import (
    Backlog,
    BestBidAskEvent,
    BookEvent,
    CaptureGap,
    ClientStats,
    ConnectionState,
    ConnectionStateChange,
    LastTradePriceEvent,
    Level,
    MarketResolvedEvent,
    NewMarketEvent,
    PriceChange,
    PriceChangeEvent,
    TickSizeChangeEvent,
    TokenState,
    TokenStateChange,
    UndecodableFrame,
    UnknownEvent,
)

from .synthetic import MARKETS, NAMES, T0, TOKEN_IDS, ids
from .words import NotationError, Word

EVENT_RECORDS = (
    BookEvent,
    PriceChangeEvent,
    BestBidAskEvent,
    LastTradePriceEvent,
    TickSizeChangeEvent,
    MarketResolvedEvent,
    NewMarketEvent,
)

FLOAT_TOLERANCE = 0.001


class Mismatch(NamedTuple):
    """The first difference between a pattern and a record, each side as
    the notation writes it."""

    field: str
    expected: str
    actual: str


Check = Callable[[Any], Mismatch | None]


@dataclass(frozen=True, slots=True)
class _Kind:
    record: type
    subject: str | None
    """The field the subject word names: a token's or a market's ID."""
    subject_is_market: bool = False
    states: type[Enum] | None = None


KINDS = {
    "conn": _Kind(ConnectionStateChange, None, states=ConnectionState),
    "token": _Kind(TokenStateChange, "token_id", states=TokenState),
    "gap": _Kind(CaptureGap, "token_id"),
    "backlog": _Kind(Backlog, None),
    "book": _Kind(BookEvent, "asset_id"),
    "best_bid_ask": _Kind(BestBidAskEvent, "asset_id"),
    "last_trade_price": _Kind(LastTradePriceEvent, "asset_id"),
    "tick_size_change": _Kind(TickSizeChangeEvent, "asset_id"),
    "price_change": _Kind(PriceChangeEvent, "market", subject_is_market=True),
    "market_resolved": _Kind(MarketResolvedEvent, "market", subject_is_market=True),
    "new_market": _Kind(NewMarketEvent, None),
    "unknown": _Kind(UnknownEvent, None),
    "undecodable": _Kind(UndecodableFrame, None),
}


@dataclass(frozen=True, slots=True)
class RecordPattern:
    """The record notation after ``expect``."""

    text: str
    record: type
    checks: tuple[Check, ...]

    def match(self, record: object) -> Mismatch | None:
        if not isinstance(record, self.record):
            return Mismatch("record", self.record.__name__, type(record).__name__)
        for check in self.checks:
            mismatch = check(record)
            if mismatch is not None:
                return mismatch
        return None


def compile_pattern(words: Sequence[Word]) -> RecordPattern:
    text = " ".join(word.text for word in words)
    if not words or words[0].text not in KINDS:
        raise NotationError(f"an unknown record kind: {text}")
    kind = KINDS[words[0].text]
    rest = list(words[1:])
    checks: list[Check] = []
    if kind.subject is not None:
        if not rest or rest[0].key is not None:
            raise NotationError(f"{words[0].text} needs a token or market: {text}")
        subject = rest.pop(0).value
        names = MARKETS if kind.subject_is_market else TOKEN_IDS
        if subject not in names:
            raise NotationError(f"an unknown market or token: {subject}")
        checks.append(_equals(kind.subject, ids(subject), subject))
    if kind.states is not None:
        if not rest or rest[0].key is not None:
            raise NotationError(f"{words[0].text} needs a state: {text}")
        word = rest.pop(0)
        checks.append(_equals("state", _member(kind.states, word.value), word.value))
    hints = typing.get_type_hints(kind.record)
    for word in rest:
        checks.append(_compile_field(kind.record, hints, word))
    return RecordPattern(text, kind.record, tuple(checks))


def _compile_field(record: type, hints: Mapping[str, Any], word: Word) -> Check:
    if word.key is None:
        raise NotationError(f"expected <field>=<value>: {word.text}")
    name, value = word.key, word.value
    if name == "t" and "source_timestamp_ms" in hints:
        offset = _integer(value)
        return _equals("source_timestamp_ms", T0 + offset, value, _offset, "t")
    if name == "resumed" and record is CaptureGap:
        resumed = _boolean(value)
        return _predicate(
            "resumed",
            value,
            lambda gap: (gap.resumed_at is not None) == resumed,
            lambda gap: f"resumed_at={_render(gap.resumed_at)}",
        )
    if name in ("applied", "before_book") and record is PriceChangeEvent:
        return _entry_flags(name, [_boolean(flag) for flag in value.split(",")])
    if name.startswith("payload.") and "payload" in hints:
        return _payload(name.removeprefix("payload."), value, word)
    if name not in hints:
        raise NotationError(f"{record.__name__} has no field {name}")
    if not word.quoted and value == "none":
        return _predicate(name, "none", lambda r: getattr(r, name) is None)
    if not word.quoted and value == "set":
        return _predicate(name, "set", lambda r: getattr(r, name) is not None)
    base = _base(hints[name])
    if base is bool:
        return _equals(name, _boolean(value), value)
    if base is int:
        return _equals(name, _integer(value), value)
    if base is float:
        expected = _number(value)
        return _predicate(
            name,
            value,
            lambda r: (
                isinstance(getattr(r, name), int | float)
                and not isinstance(getattr(r, name), bool)
                and abs(getattr(r, name) - expected) <= FLOAT_TOLERANCE
            ),
        )
    if base is Decimal:
        _decimal(value)
        return _predicate(
            name,
            value,
            lambda r: (
                isinstance(getattr(r, name), Decimal) and str(getattr(r, name)) == value
            ),
        )
    if base is str:
        expected_text = value if word.quoted else _id_or_text(value)
        return _predicate(
            name,
            value,
            lambda r: (
                isinstance(getattr(r, name), str) and getattr(r, name) == expected_text
            ),
        )
    if isinstance(base, type) and issubclass(base, Enum):
        return _equals(name, _member(base, value), value)
    if typing.get_origin(base) is tuple:
        item = typing.get_args(base)[0]
        if item is str:
            return _equals(name, _id_list(value), value)
        if item is Level:
            expected_levels = _levels(value)
            return _predicate(
                name, value, lambda r: _level_texts(getattr(r, name)) == expected_levels
            )
        if item is PriceChange:
            expected_entries = _entries(value)
            return _predicate(
                name,
                value,
                lambda r: _entry_texts(getattr(r, name)) == expected_entries,
            )
    raise NotationError(f"{record.__name__}.{name} cannot be matched: {word.text}")


def _equals(
    name: str,
    expected: object,
    written: str,
    render: Callable[[object], str] | None = None,
    shown_as: str | None = None,
) -> Check:
    shown = shown_as or name

    def check(record: Any) -> Mismatch | None:
        actual = getattr(record, name)
        if type(actual) is type(expected) and actual == expected:
            return None
        if render is None:
            return Mismatch(shown, f"{shown}={written}", f"{shown}={_render(actual)}")
        return Mismatch(shown, f"{shown}={written}", render(actual))

    return check


def _predicate(
    name: str,
    written: str,
    passes: Callable[[Any], bool],
    actual: Callable[[Any], str] | None = None,
) -> Check:
    def check(record: Any) -> Mismatch | None:
        if passes(record):
            return None
        shown = actual(record) if actual else f"{name}={_render(getattr(record, name))}"
        return Mismatch(name, f"{name}={written}", shown)

    return check


def _entry_flags(name: str, flags: list[bool]) -> Check:
    written = ",".join("true" if flag else "false" for flag in flags)

    def check(record: Any) -> Mismatch | None:
        actual = [getattr(change, name) for change in record.changes]
        expected = flags * len(actual) if len(flags) == 1 else flags
        if actual == expected:
            return None
        shown = ",".join(_render(flag) for flag in actual) or "()"
        return Mismatch(name, f"{name}={written}", f"{name}={shown}")

    return check


def _payload(key: str, value: str, word: Word) -> Check:
    try:
        expected = json.loads(
            json.dumps(value) if word.quoted else value,
            parse_float=Decimal,
            parse_constant=Decimal,
        )
    except ValueError:
        raise NotationError(f"not a JSON value: {word.text}") from None

    def check(record: Any) -> Mismatch | None:
        if key in record.payload and record.payload[key] == expected:
            return None
        name = f"payload.{key}"
        if key not in record.payload:
            return Mismatch(name, word.text, f"{name} is absent")
        actual = json.dumps(record.payload[key], default=str)
        return Mismatch(name, word.text, f"{name}={actual}")

    return check


def _base(annotation: Any) -> Any:
    """The annotation without ``| None``."""
    if typing.get_origin(annotation) in (typing.Union, types.UnionType):
        rest = [arg for arg in typing.get_args(annotation) if arg is not type(None)]
        if len(rest) == 1:
            return rest[0]
    return annotation


def _boolean(text: str) -> bool:
    if text not in ("true", "false"):
        raise NotationError(f"not true or false: {text}")
    return text == "true"


def _integer(text: str) -> int:
    if not re.fullmatch(r"-?[0-9]+", text):
        raise NotationError(f"not an integer: {text}")
    return int(text)


def _number(text: str) -> float:
    if not re.fullmatch(r"-?[0-9]+(\.[0-9]+)?", text):
        raise NotationError(f"not a number: {text}")
    return float(text)


def _decimal(text: str) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise NotationError(f"not a decimal: {text}") from None
    if not value.is_finite():
        raise NotationError(f"not a finite decimal: {text}")
    return value


def _member(enum: type[Enum], text: str) -> Enum:
    for member in enum:
        if member.name.lower() == text:
            return member
    raise NotationError(f"not a {enum.__name__}: {text}")


def _id_or_text(text: str) -> str:
    """A word that names a synthetic market or token stands for its ID."""
    return ids(text) if text in MARKETS or text in TOKEN_IDS else text


def _id_list(text: str) -> tuple[str, ...]:
    if text == "()":
        return ()
    names = text.split(",")
    for name in names:
        if name not in MARKETS and name not in TOKEN_IDS:
            raise NotationError(f"not a synthetic market or token: {name}")
    return tuple(ids(name) for name in names)


def _levels(text: str) -> tuple[tuple[str, str], ...]:
    if text == "-":
        return ()
    levels = []
    for pair in text.split(","):
        price, colon, size = pair.partition(":")
        if not colon:
            raise NotationError(f"not <price>:<size>: {pair}")
        _decimal(price)
        _decimal(size)
        levels.append((price, size))
    return tuple(levels)


def _entries(text: str) -> tuple[tuple[str, str, str, str], ...]:
    entries = []
    for entry in text.split(","):
        parts = entry.split(":")
        if (
            len(parts) != 4
            or parts[0] not in TOKEN_IDS
            or parts[1] not in ("BUY", "SELL")
        ):
            raise NotationError(f"not <T>:<BUY or SELL>:<price>:<size>: {entry}")
        _decimal(parts[2])
        _decimal(parts[3])
        entries.append((TOKEN_IDS[parts[0]], parts[1], parts[2], parts[3]))
    return tuple(entries)


def _level_texts(levels: object) -> tuple[tuple[str, str], ...] | None:
    if not isinstance(levels, tuple) or not all(isinstance(x, Level) for x in levels):
        return None
    return tuple((_decimal_text(x.price), _decimal_text(x.size)) for x in levels)


def _entry_texts(changes: object) -> tuple[tuple[str, str, str, str], ...] | None:
    if not isinstance(changes, tuple):
        return None
    texts = []
    for change in changes:
        if not isinstance(change, PriceChange):
            return None
        side = change.side.value if isinstance(change.side, Enum) else repr(change.side)
        price, size = _decimal_text(change.price), _decimal_text(change.size)
        texts.append((change.asset_id, side, price, size))
    return tuple(texts)


def _decimal_text(value: object) -> str:
    # A float never matches a Decimal's text.
    return str(value) if isinstance(value, Decimal) else repr(value)


def _offset(value: object) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        return f"t={value - T0}"
    return f"source_timestamp_ms={_render(value)}"


def render_record(record: object) -> str:
    """A record in the notation's terms, for failure reports."""
    if not dataclasses.is_dataclass(record) or isinstance(record, type):
        return repr(record)
    fields = (
        f"{field.name}={_render(getattr(record, field.name))}"
        for field in dataclasses.fields(record)
    )
    return f"{type(record).__name__}({', '.join(fields)})"


def _render(value: object) -> str:
    match value:
        case None:
            return "none"
        case bool():
            return "true" if value else "false"
        case Enum():
            return value.name.lower()
        case Decimal() | int():
            return str(value)
        case float():
            return repr(value)
        case str():
            if value in NAMES:
                return NAMES[value]
            return (
                value
                if value and re.fullmatch(r"[^\s\"\\]+", value)
                else json.dumps(value)
            )
        case datetime():
            return value.isoformat()
        case Level():
            return f"{_decimal_text(value.price)}:{_decimal_text(value.size)}"
        case PriceChange():
            side = (
                value.side.value if isinstance(value.side, Enum) else repr(value.side)
            )
            return (
                f"{_render(value.asset_id)}:{side}:"
                f"{_decimal_text(value.price)}:{_decimal_text(value.size)}"
            )
        case tuple():
            if not value:
                return "()"
            return ",".join(_render(item) for item in value)
        case _:
            return repr(value)


def misplaced_decimal(record: object) -> str | None:
    """Where an event record has something other than a ``decimal.Decimal``
    in a ``Decimal`` field, if anywhere."""
    if not isinstance(record, EVENT_RECORDS):
        return None
    return _misplaced(record, type(record).__name__)


def _misplaced(value: Any, path: str) -> str | None:
    hints = typing.get_type_hints(type(value))
    for field in dataclasses.fields(value):
        item = getattr(value, field.name)
        base = _base(hints[field.name])
        where = f"{path}.{field.name}"
        if base is Decimal:
            # None only where the annotation allows it, as Decimal | None.
            if item is None and base is not hints[field.name]:
                continue
            if not isinstance(item, Decimal):
                return f"{where} is a {type(item).__name__}: {item!r}"
        elif typing.get_origin(base) is tuple and typing.get_args(base)[0] in (
            Level,
            PriceChange,
        ):
            for index, element in enumerate(item):
                found = _misplaced(element, f"{where}[{index}]")
                if found is not None:
                    return found
    return None


@dataclass(frozen=True, slots=True)
class StatCheck:
    """``<counter><op><value>``: a counter of ``client.stats()``, or one key
    of a mapping counter, or a mapping counter's sum."""

    text: str
    counter: str
    key: str | None
    op: str
    value: float

    def check(self, stats: ClientStats) -> Mismatch | None:
        actual = getattr(stats, self.counter)
        if isinstance(actual, Mapping):
            actual = actual.get(self.key, 0) if self.key else sum(actual.values())
        if self.op == "=":
            passed = abs(actual - self.value) <= FLOAT_TOLERANCE
        elif self.op == ">=":
            passed = actual >= self.value
        else:
            passed = actual <= self.value
        if passed:
            return None
        name = self.counter if self.key is None else f"{self.counter}.{self.key}"
        return Mismatch(name, self.text, f"{name}={actual}")


_STAT = re.compile(r"([a-z_]+)(?:\.([a-z_]+))?(>=|<=|=)(-?[0-9]+(?:\.[0-9]+)?)")


def compile_stat(text: str) -> StatCheck:
    match = _STAT.fullmatch(text)
    if match is None:
        raise NotationError(f"not <counter><op><value>: {text}")
    counter, key, op, value = match.groups()
    hints = typing.get_type_hints(ClientStats)
    if counter not in hints:
        raise NotationError(f"ClientStats has no counter {counter}")
    is_mapping = (
        typing.get_origin(hints[counter]) is Mapping or hints[counter] is Mapping
    )
    if key is not None and not is_mapping:
        raise NotationError(f"{counter} is not a mapping: {text}")
    if hints[counter] is int or is_mapping:
        _integer(value)
    return StatCheck(text, counter, key, op, float(value))
