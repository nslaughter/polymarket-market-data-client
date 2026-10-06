"""The scenario notation (spec/conformance.md, Notation).

Every fenced code block whose info string is ``scenario`` is one scenario.
``load`` parses them all; a block the notation does not allow raises
``NotationError`` with its line in the document.
"""

import builtins
import math
import re
import typing
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

import polymarket_market_data
from polymarket_market_data import ClientConfig, ReconnectPolicy

from .failures import SPEC
from .frames import (
    BestBidAskFrame,
    BookFrame,
    BookSpec,
    Entry,
    Frame,
    LastTradeFrame,
    NewMarketFrame,
    Opening,
    PriceChangeFrame,
    ResolvedFrame,
    TickSizeFrame,
)
from .lookup import Answer
from .matching import RecordPattern, StatCheck, compile_pattern, compile_stat
from .synthetic import MARKETS, T0, TOKEN_IDS, TOKEN_MARKET, TRADING, Levels, ids
from .words import NotationError, Word, split_words

__all__ = [
    "NotationError",
    "Scenario",
    "Step",
    "load",
    "parse_document",
    "parse_scenario",
]

# Server steps.


@dataclass(frozen=True, slots=True)
class Accept:
    pass


@dataclass(frozen=True, slots=True)
class Refuse:
    n: int


@dataclass(frozen=True, slots=True)
class RefuseAll:
    pass


@dataclass(frozen=True, slots=True)
class RecvSubscribe:
    tokens: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RecvPing:
    pass


@dataclass(frozen=True, slots=True)
class Send:
    frame: Frame


@dataclass(frozen=True, slots=True)
class SendText:
    text: str


@dataclass(frozen=True, slots=True)
class SendBinary:
    data: bytes


@dataclass(frozen=True, slots=True)
class SendAgain:
    label: str
    reverse: bool


@dataclass(frozen=True, slots=True)
class SendBurst:
    market: str
    t: int
    every: float
    entries: tuple[Entry, ...]


@dataclass(frozen=True, slots=True)
class Pong:
    mode: Literal["auto", "off", "hold"]


@dataclass(frozen=True, slots=True)
class ReleasePongs:
    pass


@dataclass(frozen=True, slots=True)
class Silent:
    token: str
    t: int
    entry: Entry


@dataclass(frozen=True, slots=True)
class Trade:
    market: str
    price: str


@dataclass(frozen=True, slots=True)
class IdleClose:
    seconds: float


@dataclass(frozen=True, slots=True)
class Close:
    code: int
    reason: str


@dataclass(frozen=True, slots=True)
class Drop:
    pass


@dataclass(frozen=True, slots=True)
class ExpectClientClose:
    code: int
    reason: str


@dataclass(frozen=True, slots=True)
class ExpectNoConnect:
    seconds: float


# Runner steps.


@dataclass(frozen=True, slots=True)
class Wait:
    seconds: float


@dataclass(frozen=True, slots=True)
class Subscribe:
    market: str


@dataclass(frozen=True, slots=True)
class Unsubscribe:
    market: str


@dataclass(frozen=True, slots=True)
class Resolve:
    slug: str


@dataclass(frozen=True, slots=True)
class SetLookup:
    market: str
    answer: Answer


@dataclass(frozen=True, slots=True)
class Exit:
    pass


@dataclass(frozen=True, slots=True)
class Cancel:
    pass


@dataclass(frozen=True, slots=True)
class ReadTimeout:
    seconds: float


# Expectation steps.


@dataclass(frozen=True, slots=True)
class Expect:
    pattern: RecordPattern


@dataclass(frozen=True, slots=True)
class ExpectNothing:
    seconds: float


@dataclass(frozen=True, slots=True)
class ExpectError:
    exception: type[BaseException]


@dataclass(frozen=True, slots=True)
class ExpectEnd:
    pass


@dataclass(frozen=True, slots=True)
class ExpectBacklog:
    n: int


@dataclass(frozen=True, slots=True)
class ExpectStats:
    checks: tuple[StatCheck, ...]


ServerAction = (
    Accept
    | Refuse
    | RefuseAll
    | RecvSubscribe
    | RecvPing
    | Send
    | SendText
    | SendBinary
    | SendAgain
    | SendBurst
    | Pong
    | ReleasePongs
    | Silent
    | Trade
    | IdleClose
    | Close
    | Drop
    | ExpectClientClose
    | ExpectNoConnect
)
RunnerAction = (
    Wait | Subscribe | Unsubscribe | Resolve | SetLookup | Exit | Cancel | ReadTimeout
)
ExpectAction = (
    Expect | ExpectNothing | ExpectError | ExpectEnd | ExpectBacklog | ExpectStats
)
Action = ServerAction | RunnerAction | ExpectAction


@dataclass(frozen=True, slots=True)
class Within:
    """``within <lo>..<hi> of <label>``."""

    lo: float
    hi: float
    label: str


@dataclass(frozen=True, slots=True)
class Step:
    line: int
    """The step's line in spec/conformance.md."""
    text: str
    label: str | None
    action: Action
    within: Within | None = None
    raises: type[BaseException] | None = None


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    line: int
    markets: tuple[str, ...]
    config: Mapping[str, Any]
    """Overrides of the profile, converted to their fields' types."""
    reconnect: Mapping[str, Any]
    """Overrides of the profile's reconnect policy."""
    lookup: Mapping[str, Answer]
    owner_specs: tuple[str, ...]
    steps: tuple[Step, ...]


def load(path: Path = SPEC) -> list[Scenario]:
    """Every scenario in the document."""
    return parse_document(path.read_text())


def parse_document(text: str) -> list[Scenario]:
    scenarios = []
    names: set[str] = set()
    heading = None
    block: list[tuple[int, str]] | None = None
    other_block = False
    opened = 0
    for number, line in enumerate(text.splitlines(), start=1):
        if other_block:
            other_block = not line.startswith("```")
        elif block is not None:
            if line.startswith("```"):
                scenario = _parse_block(block, opened)
                if heading != f"`{scenario.name}`":
                    raise NotationError(
                        f"line {opened}: scenario {scenario.name} is under the heading "
                        f"{heading!r}"
                    )
                if scenario.name in names:
                    raise NotationError(f"line {opened}: a second {scenario.name}")
                names.add(scenario.name)
                scenarios.append(scenario)
                block = None
            else:
                block.append((number, line))
        elif re.fullmatch(r"```scenario\s*", line):
            block, opened = [], number
        elif line.startswith("```"):
            other_block = True
        elif line.startswith("#"):
            heading = line.lstrip("#").strip()
    if block is not None:
        raise NotationError(f"line {opened}: the scenario block is not closed")
    return scenarios


def parse_scenario(text: str, first_line: int = 1) -> Scenario:
    """One scenario block's contents, numbered from ``first_line``."""
    lines = list(enumerate(text.splitlines(), start=first_line))
    return _parse_block(lines, first_line - 1)


def _parse_block(lines: list[tuple[int, str]], opened: int) -> Scenario:
    logical = _logical_lines(lines)
    if not logical:
        raise NotationError(f"line {opened}: an empty scenario block")
    parser = _Parser()
    try:
        return parser.parse(logical)
    except NotationError as error:
        raise NotationError(f"line {parser.line}: {error}") from None


def _logical_lines(lines: Iterable[tuple[int, str]]) -> list[tuple[int, str]]:
    """Blank lines and comments dropped, and continuations joined to the line
    above with one space."""
    logical: list[tuple[int, str]] = []
    for number, line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.startswith(" "):
            if not logical:
                raise NotationError(f"line {number}: a continuation with no line above")
            first, text = logical[-1]
            logical[-1] = (first, f"{text} {line.strip()}")
        else:
            logical.append((number, line.rstrip()))
    return logical


_LABEL = re.compile(r"(?:([a-z0-9-]+):\s+)?(\S+)(?:\s+(.*))?")
_HEADERS = ("markets", "config", "lookup", "owner-spec")
_OWNER_SPECS = {f"D{n}" for n in range(1, 9)}
_SENDS = (Send, SendText, SendBinary, SendAgain)
_CALLS = ("subscribe", "unsubscribe", "resolve")


class _Parser:
    def __init__(self) -> None:
        self.line = 0
        self.labels: dict[str, Action] = {}
        self.desired: list[str] = []
        self.accepted = False

    def parse(self, lines: list[tuple[int, str]]) -> Scenario:
        self.line, first = lines[0]
        words = split_words(first)
        if len(words) != 2 or words[0].text != "scenario":
            raise NotationError("a block begins with scenario <name>")
        name = words[1].text
        markets: tuple[str, ...] = ()
        config: dict[str, Any] = {}
        reconnect: dict[str, Any] = {}
        lookup: dict[str, Answer] = {}
        owner_specs: list[str] = []
        rest = lines[1:]
        while rest and rest[0][1].split(" ", 1)[0] in _HEADERS:
            self.line, text = rest.pop(0)
            keyword, *args = split_words(text)
            match keyword.text:
                case "markets":
                    if markets:
                        raise NotationError("a second markets line")
                    markets = tuple(_market(arg.text) for arg in args)
                    if len(set(markets)) != len(markets) or not markets:
                        raise NotationError("markets lists each market once")
                case "config":
                    for arg in args:
                        _config(arg, config, reconnect)
                case "lookup":
                    market, answer = _lookup(args)
                    lookup[market] = answer
                case _:
                    for arg in args:
                        if arg.text not in _OWNER_SPECS:
                            raise NotationError(
                                f"not an owner specification: {arg.text}"
                            )
                        owner_specs.append(arg.text)
        self.desired = list(markets)
        steps: list[Step] = []
        for self.line, text in rest:
            steps.extend(self._steps(self.line, text))
        if not steps:
            raise NotationError(f"scenario {name} has no steps")
        return Scenario(
            name=name,
            line=lines[0][0],
            markets=markets,
            config=MappingProxyType(config),
            reconnect=MappingProxyType(reconnect),
            lookup=MappingProxyType(lookup),
            owner_specs=tuple(owner_specs),
            steps=tuple(steps),
        )

    def _steps(self, line: int, text: str) -> list[Step]:
        match = _LABEL.fullmatch(text)
        assert match is not None
        label, keyword, rest = match.groups()
        rest = rest or ""
        if keyword == "start":
            if label is not None:
                raise NotationError("start takes no label")
            return self._start(line, split_words(rest))
        if keyword == "send-text":
            # The rest of the line is the frame's text.
            if not rest:
                raise NotationError("send-text needs text")
            step = Step(line, text, label, SendText(_substitute(rest)))
        else:
            step = self._step(line, text, label, keyword, split_words(rest))
        if label is not None:
            if label in self.labels:
                raise NotationError(f"the label {label} is defined twice")
            self.labels[label] = step.action
        return [step]

    def _step(
        self, line: int, text: str, label: str | None, keyword: str, words: list[Word]
    ) -> Step:
        within = None
        if (
            (keyword == "expect" or keyword.startswith("recv-"))
            and len(words) >= 4
            and words[-4].text == "within"
            and words[-2].text == "of"
        ):
            within = self._within(words[-3].text, words[-1].text)
            words = words[:-4]
        raises = None
        if keyword in _CALLS and len(words) >= 2 and words[-2].text == "raises":
            raises = _exception(words[-1].text)
            words = words[:-2]
        action = self._action(keyword, words, raises)
        return Step(line, text, label, action, within, raises)

    def _within(self, span: str, label: str) -> Within:
        lo, dots, hi = span.partition("..")
        if not dots:
            raise NotationError(f"not <lo>..<hi>: {span}")
        if label not in self.labels:
            raise NotationError(f"the label {label} is not defined before this step")
        return Within(_seconds(lo), _seconds(hi), label)

    def _action(
        self, keyword: str, words: list[Word], raises: type[BaseException] | None
    ) -> Action:
        args = [word.text for word in words]
        match keyword, args:
            case "accept", []:
                self.accepted = True
                return Accept()
            case "refuse", [n]:
                count = _count(n)
                if count < 1:
                    raise NotationError("refuse needs at least one attempt")
                return Refuse(count)
            case "refuse-all", []:
                return RefuseAll()
            case "recv-subscribe", [_, *_]:
                return RecvSubscribe(tuple(_token(word.text) for word in words))
            case "recv-ping", []:
                return RecvPing()
            case "send", [_, *_]:
                return Send(_frame(words))
            case "send-binary", [data]:
                try:
                    return SendBinary(bytes.fromhex(data))
                except ValueError:
                    raise NotationError(f"not hexadecimal: {data}") from None
            case "send-again", [label] | [label, "reversed"]:
                if not isinstance(self.labels.get(label), _SENDS):
                    raise NotationError(
                        f"{label} labels no earlier step that sent a frame"
                    )
                return SendAgain(label, len(args) == 2)
            case "send-burst", [market, _, every, _, *_]:
                name = _trading(market)
                every_word = words[2]
                if every_word.key != "every":
                    raise NotationError(f"not every=<seconds>: {every}")
                entries = tuple(_entry(word.text, name) for word in words[3:])
                return SendBurst(
                    name, _t(words[1]), _seconds(every_word.value), entries
                )
            case "pong", ["auto" | "off" | "hold" as mode]:
                return Pong(mode)
            case "release-pongs", []:
                return ReleasePongs()
            case "silent", [token, _, entry]:
                name = _trading_token(token)
                parts = entry.split(":")
                if len(parts) != 3:
                    raise NotationError(f"not <side>:<price>:<size>: {entry}")
                return Silent(name, _t(words[1]), _entry(f"{name}:{entry}", None))
            case "trade", [market, price]:
                return Trade(_trading(market), _price(price))
            case "idle-close", [seconds]:
                return IdleClose(_seconds(seconds))
            case "close", [code]:
                return Close(_count(code), "")
            case "close", [code, _]:
                return Close(_count(code), words[1].value)
            case "drop", []:
                return Drop()
            case "expect-client-close", [code, _]:
                return ExpectClientClose(_count(code), words[1].value)
            case "expect-no-connect", [seconds]:
                return ExpectNoConnect(_seconds(seconds))
            case "wait", [seconds]:
                return Wait(_seconds(seconds))
            case "subscribe", [market]:
                name = _market(market)
                if raises is None and name not in self.desired:
                    self.desired.append(name)
                return Subscribe(name)
            case "unsubscribe", [market]:
                name = _market(market)
                if raises is None and name in self.desired:
                    self.desired.remove(name)
                return Unsubscribe(name)
            case "resolve", [slug]:
                known = any(market.slug == slug for market in MARKETS.values())
                if raises is None and not known:
                    raise NotationError(f"no synthetic market has the slug {slug}")
                return Resolve(slug)
            case "lookup", [_, _, *_]:
                market, answer = _lookup(words)
                return SetLookup(market, answer)
            case "exit", []:
                return Exit()
            case "cancel", []:
                return Cancel()
            case "read-timeout", [seconds]:
                return ReadTimeout(_seconds(seconds))
            case "expect", [_, *_]:
                return Expect(compile_pattern(words))
            case "expect-nothing", [seconds]:
                return ExpectNothing(_seconds(seconds))
            case "expect-error", [name]:
                return ExpectError(_exception(name))
            case "expect-end", []:
                return ExpectEnd()
            case "expect-backlog", [n]:
                return ExpectBacklog(_count(n))
            case "expect-stats", [_, *_]:
                return ExpectStats(tuple(compile_stat(arg) for arg in args))
        raise NotationError(f"not a step: {keyword} {' '.join(args)}".rstrip())

    def _start(self, line: int, words: list[Word]) -> list[Step]:
        """The ``start`` macro, its steps numbered with its line."""
        markets = [_market(word.text) for word in words]
        if not markets:
            raise NotationError("start needs markets")
        if markets != self.desired:
            raise NotationError(
                f"start {' '.join(markets)} is not the desired set, "
                f"{' '.join(self.desired) or 'empty'}"
            )
        if self.accepted:
            raise NotationError("start after a connection was opened")
        tokens = [token for market in markets for token in MARKETS[market].tokens]
        joined = " ".join(tokens)
        lines = [
            "expect conn connecting attempt=1",
            "accept",
            f"recv-subscribe {joined}",
            "expect conn open connection=1",
            "expect conn subscribed connection=1",
            *(
                f"expect token {token} synchronizing previous=none reason=subscribed"
                for token in tokens
            ),
            f"send opening {joined}",
        ]
        for token in tokens:
            lines.append(
                f"expect book {token} opening=true connection=1 held_book_matched=none"
            )
            lines.append(
                f"expect token {token} ready previous=synchronizing reason=book"
            )
        steps = []
        for text in lines:
            keyword, _, rest = text.partition(" ")
            step = self._step(line, text, None, keyword, split_words(rest))
            steps.append(
                Step(line, f"start {' '.join(markets)}: {text}", None, step.action)
            )
        return steps


def _market(name: str) -> str:
    if name not in MARKETS:
        raise NotationError(f"not a synthetic market: {name}")
    return name


def _trading(name: str) -> str:
    if _market(name) not in TRADING:
        raise NotationError(f"the server keeps no reference books for {name}")
    return name


def _token(name: str) -> str:
    if name not in TOKEN_IDS:
        raise NotationError(f"not a synthetic token: {name}")
    return name


def _trading_token(name: str) -> str:
    if TOKEN_MARKET[_token(name)] not in TRADING:
        raise NotationError(f"the server keeps no reference book for {name}")
    return name


def _count(text: str) -> int:
    if not re.fullmatch(r"[0-9]+", text):
        raise NotationError(f"not a whole number: {text}")
    return int(text)


def _seconds(text: str) -> float:
    if not re.fullmatch(r"[0-9]+(\.[0-9]+)?", text):
        raise NotationError(f"not seconds: {text}")
    return float(text)


def _price(text: str) -> str:
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise NotationError(f"not a decimal: {text}") from None
    if not value.is_finite() or value < 0:
        raise NotationError(f"not a price or size: {text}")
    return text


def _t(word: Word) -> int:
    if word.key != "t" or not re.fullmatch(r"-?[0-9]+", word.value):
        raise NotationError(f"not t=<offset>: {word.text}")
    return int(word.value)


def _keyed(word: Word, key: str) -> str:
    if word.key != key or word.quoted:
        raise NotationError(f"not {key}=<value>: {word.text}")
    return word.value


def _side(text: str) -> str:
    if text not in ("BUY", "SELL"):
        raise NotationError(f"not BUY or SELL: {text}")
    return text


def _entry(text: str, market: str | None) -> Entry:
    """``<T>:<BUY or SELL>:<price>:<size>``, its token one of ``market``'s
    if one is given."""
    parts = text.split(":")
    if len(parts) != 4:
        raise NotationError(f"not <T>:<BUY or SELL>:<price>:<size>: {text}")
    token, side, price, size = parts
    _trading_token(token)
    if market is not None and TOKEN_MARKET[token] != market:
        raise NotationError(f"{token} is not a token of {market}")
    return Entry(token, _side(side), _price(price), _price(size))


def _levels(text: str) -> Levels:
    if text == "-":
        return ()
    levels = []
    for pair in text.split(","):
        price, colon, size = pair.partition(":")
        if not colon:
            raise NotationError(f"not <price>:<size>: {pair}")
        levels.append((_price(price), _price(size)))
    return tuple(levels)


def _book_spec(token: str, words: list[Word]) -> BookSpec:
    if [word.key for word in words] != ["t", "bids", "asks"]:
        raise NotationError(
            "a replacing book needs t=, bids=, and asks=, in that order"
        )
    return BookSpec(
        token,
        _t(words[0]),
        _levels(_keyed(words[1], "bids")),
        _levels(_keyed(words[2], "asks")),
    )


def _frame(words: list[Word]) -> Frame:
    kind, *rest = words
    bad_hash = bool(rest) and rest[-1].text == "hash=bad"
    if bad_hash:
        if kind.text not in ("book", "pc"):
            raise NotationError("hash=bad follows only a book or pc")
        rest = rest[:-1]
    args = [word.text for word in rest]
    match kind.text, args:
        case "opening", _:
            return Opening(_opening_items(rest))
        case "book", [token]:
            return BookFrame(_trading_token(token), None, bad_hash)
        case "book", [token, _, _, _]:
            name = _trading_token(token)
            return BookFrame(name, _book_spec(name, rest[1:]), bad_hash)
        case "pc", [market, _, _, *_]:
            name = _trading(market)
            entries = tuple(
                _entry(word.text, name) for word in rest[1:] if word.key is None
            )
            if len(entries) != len(rest) - 2:
                raise NotationError(
                    f"not pc <M> t=<offset> <entry> ...: {' '.join(args)}"
                )
            return PriceChangeFrame(name, _t(rest[1]), entries, bad_hash)
        case "bba", [token, _]:
            return BestBidAskFrame(_trading_token(token), _t(rest[1]))
        case "ltp", [token, _, _, _, _]:
            return LastTradeFrame(
                _trading_token(token),
                _t(rest[1]),
                _price(_keyed(rest[2], "price")),
                _price(_keyed(rest[3], "size")),
                _side(_keyed(rest[4], "side")),
            )
        case "tsc", [token, _, _]:
            return TickSizeFrame(
                _trading_token(token), _t(rest[1]), _price(_keyed(rest[2], "new"))
            )
        case "resolved", [market, _, _]:
            name = _market(market)
            winner = _token(_keyed(rest[2], "winner"))
            if TOKEN_MARKET[winner] != name:
                raise NotationError(f"{winner} is not a token of {name}")
            return ResolvedFrame(name, _t(rest[1]), winner)
        case "new-market", [n, _]:
            number = _count(n)
            if number < 1:
                raise NotationError("synthetic new markets are numbered from 1")
            return NewMarketFrame(number, _t(rest[1]))
    raise NotationError(f"not a frame: {' '.join(word.text for word in words)}")


def _opening_items(words: list[Word]) -> tuple[str | BookSpec, ...]:
    items: list[str | BookSpec] = []
    group: list[Word] | None = None
    for word in words:
        if group is None and word.text.startswith("("):
            group = []
            word = Word(word.text[1:], word.key, word.value, word.quoted)
        if group is None:
            items.append(_trading_token(word.text))
            continue
        closes = word.text.endswith(")")
        if closes:
            text = word.text[:-1]
            value = word.value[:-1] if word.key else text
            word = Word(text, word.key, value, word.quoted)
        group.append(word)
        if closes:
            if len(group) != 5 or group[0].text != "book":
                raise NotationError(
                    "not (book <T> t=<offset> bids=<levels> asks=<levels>)"
                )
            token = _trading_token(group[1].text)
            items.append(_book_spec(token, group[2:]))
            group = None
    if group is not None:
        raise NotationError("an opening item's parenthesis is not closed")
    return tuple(items)


def _lookup(words: list[Word]) -> tuple[str, Answer]:
    args = [word.text for word in words]
    match args:
        case [market, "open" | "missing" | "error" as kind]:
            return _market(market), Answer(kind)
        case [market, "closed", winner_word]:
            name = _market(market)
            winner = _token(_keyed(words[2], "winner"))
            if TOKEN_MARKET[winner] != name:
                raise NotationError(f"{winner} is not a token of {name}: {winner_word}")
            return name, Answer("closed", winner)
    raise NotationError(f"not lookup <M> <answer>: {' '.join(args)}")


def _exception(name: str) -> type[BaseException]:
    found = getattr(polymarket_market_data, name, None) or getattr(builtins, name, None)
    if not (isinstance(found, type) and issubclass(found, BaseException)):
        raise NotationError(f"not an exception: {name}")
    return found


_REFERENCE = re.compile(r"\$\{([^}]*)\}")


def _substitute(text: str) -> str:
    """``send-text``'s references: ``${A}``, ``${A1}``, and ``${t:<offset>}``."""

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in MARKETS or name in TOKEN_IDS:
            return ids(name)
        offset = re.fullmatch(r"t:(-?[0-9]+)", name)
        if offset is None:
            raise NotationError(f"not a reference: {match.group(0)}")
        return str(T0 + int(offset.group(1)))

    return _REFERENCE.sub(replace, text)


def _config(word: Word, config: dict[str, Any], reconnect: dict[str, Any]) -> None:
    """``<field>=<value>`` or ``reconnect.<field>=<value>``, converted to the
    field's type, since the models convert nothing themselves."""
    if word.key is None:
        raise NotationError(f"not <field>=<value>: {word.text}")
    name = word.key
    if name.startswith("reconnect."):
        field = name.removeprefix("reconnect.")
        reconnect[field] = _convert(ReconnectPolicy.model_fields, field, word)
    elif name in ("url", "reconnect"):
        raise NotationError(f"a scenario cannot set {name}")
    else:
        config[name] = _convert(ClientConfig.model_fields, name, word)


def _convert(fields: Mapping[str, Any], name: str, word: Word) -> Any:
    if name not in fields:
        raise NotationError(f"not a configuration field: {word.key}")
    annotation = fields[name].annotation
    value = word.value
    if annotation is bool:
        if value not in ("true", "false"):
            raise NotationError(f"not true or false: {word.text}")
        return value == "true"
    if annotation is int:
        return _count(value)
    if annotation is float:
        number = float(_seconds(value))
        if not math.isfinite(number):
            raise NotationError(f"not finite: {word.text}")
        return number
    if typing.get_origin(annotation) is Literal:
        if value not in typing.get_args(annotation):
            raise NotationError(
                f"not one of {typing.get_args(annotation)}: {word.text}"
            )
        return value
    return value
