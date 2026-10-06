"""The records the client delivers, and the other public data types
(spec/client.md, Records).

Every type here is a frozen dataclass with slots, not a Pydantic model (D8).
They carry data already validated, so they validate nothing themselves.
Records serialize through ``TypeAdapter(<type>).dump_json`` and read back
with ``validate_json``.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class Side(StrEnum):
    """The side of a ``price_change`` entry or a trade, as the source sends
    it: ``BUY`` changes the bids, ``SELL`` the asks."""

    BUY = "BUY"
    SELL = "SELL"


class TokenState(StrEnum):
    """A token's state (spec/client.md, Per-token state machine)."""

    SYNCHRONIZING = "synchronizing"
    READY = "ready"
    UNCERTAIN = "uncertain"
    SETTLED = "settled"
    REMOVED = "removed"


class ConnectionState(StrEnum):
    """The connection's state (spec/client.md, Connection states)."""

    CONNECTING = "connecting"
    OPEN = "open"
    SUBSCRIBED = "subscribed"
    INTERRUPTED = "interrupted"
    RECOVERING = "recovering"
    ENDED = "ended"
    IDLE = "idle"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Market:
    """A market in the desired set: its condition ID, its tokens in outcome
    order, and its slug, if it has one."""

    condition_id: str
    token_ids: tuple[str, ...]
    slug: str | None


@dataclass(frozen=True, slots=True)
class Level:
    """One price level of a book."""

    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class PriceChange:
    """One entry of a ``price_change`` event.

    ``applied`` and ``before_book`` are set by the client: whether it applied
    the entry to its book, and whether the entry is stamped earlier than the
    token's opening book on this connection but arrived after it.
    """

    asset_id: str
    side: Side
    price: Decimal
    size: Decimal
    hash: str
    best_bid: Decimal | None
    best_ask: Decimal | None
    applied: bool
    before_book: bool


@dataclass(frozen=True, slots=True)
class _Event:
    """The fields of every event record (spec/client.md, Fields of every
    event record)."""

    event_type: str
    market: str
    source_timestamp_ms: int
    received_at: datetime
    connection: int
    frame: int
    index: int
    repeat: bool
    raw: str | None


@dataclass(frozen=True, slots=True)
class BookEvent(_Event):
    """A ``book`` event.

    ``tick_size`` and ``last_trade_price`` come only on a subscription's
    opening books. ``held_book_matched`` compares the book with the one the
    client held for the token just before it; ``None`` if it held none.
    """

    asset_id: str
    hash: str
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    tick_size: Decimal | None
    last_trade_price: Decimal | None
    opening: bool
    held_book_matched: bool | None


@dataclass(frozen=True, slots=True)
class PriceChangeEvent(_Event):
    """A ``price_change`` event, its entries in the order sent."""

    changes: tuple[PriceChange, ...]


@dataclass(frozen=True, slots=True)
class BestBidAskEvent(_Event):
    """A ``best_bid_ask`` event."""

    asset_id: str
    best_bid: Decimal
    best_ask: Decimal
    spread: Decimal | None


@dataclass(frozen=True, slots=True)
class LastTradePriceEvent(_Event):
    """A ``last_trade_price`` event."""

    asset_id: str
    price: Decimal
    size: Decimal
    side: Side
    fee_rate_bps: Decimal | None
    transaction_hash: str | None


@dataclass(frozen=True, slots=True)
class TickSizeChangeEvent(_Event):
    """A ``tick_size_change`` event."""

    asset_id: str
    old_tick_size: Decimal
    new_tick_size: Decimal


@dataclass(frozen=True, slots=True)
class MarketResolvedEvent(_Event):
    """A ``market_resolved`` event."""

    id: str | None
    assets_ids: tuple[str, ...]
    winning_asset_id: str
    winning_outcome: str | None
    tags: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class NewMarketEvent(_Event):
    """A ``new_market`` event, delivered only if ``new_market`` is
    ``deliver``.

    ``payload`` is the whole event as parsed, a plain ``dict``. Its decimals
    serialize as strings and read back as strings (D8). ``market`` is ``""``
    when the event has none.
    """

    id: str
    condition_id: str | None
    slug: str | None
    question: str | None
    assets_ids: tuple[str, ...]
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class UnknownEvent:
    """A JSON object whose ``event_type`` the client does not know, or that
    has none. ``payload`` is the object as parsed, a plain ``dict``."""

    event_type: str | None
    payload: Mapping[str, Any]
    raw: str
    received_at: datetime
    connection: int
    frame: int
    index: int


@dataclass(frozen=True, slots=True)
class UndecodableFrame:
    """A frame, or an item of one, that the client cannot decode.

    ``reason`` is ``invalid_json``, ``not_object``, ``binary``, or
    ``invalid_event``. ``raw`` is the frame's text, or a binary frame's bytes
    in lowercase hexadecimal. ``index`` is the item's position if the frame
    was a JSON array, otherwise ``None``. ``affected`` names the tokens it
    made uncertain.
    """

    reason: str
    event_type: str | None
    error: str
    raw: str
    received_at: datetime
    connection: int
    frame: int
    index: int | None
    affected: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TokenStateChange:
    """A token's state or its reason changed.

    ``last_confirmed_at`` is set when the reason is ``interrupted``, and
    ``winning_asset_id`` when the token is settled and the winner is known.
    """

    token_id: str
    market: str
    state: TokenState
    previous: TokenState | None
    reason: str
    at: datetime
    connection: int | None
    last_confirmed_at: datetime | None
    winning_asset_id: str | None


@dataclass(frozen=True, slots=True)
class ConnectionStateChange:
    """The connection's state changed (spec/client.md, Reconnecting)."""

    state: ConnectionState
    at: datetime
    connection: int | None
    attempt: int | None
    reason: str | None
    detail: str | None
    close_code: int | None
    close_reason: str | None
    retry_in: float | None
    last_confirmed_at: datetime | None


@dataclass(frozen=True, slots=True)
class CaptureGap:
    """An interval in which the client may have missed a token's events,
    emitted when it ends (spec/client.md, Recovery contract)."""

    token_id: str
    market: str
    cause: str
    close_code: int | None
    close_reason: str | None
    last_confirmed_at: datetime
    detected_at: datetime
    resumed_at: datetime | None
    end: str
    connection_before: int
    connection_after: int | None
    held_book_matched: bool | None
    at: datetime


@dataclass(frozen=True, slots=True)
class Backlog:
    """The market-event backlog crossed ``backlog_warning × queue_size``:
    upwards if ``rising``, otherwise back below it."""

    queued: int
    limit: int
    rising: bool
    at: datetime


@dataclass(frozen=True, slots=True)
class ClientStats:
    """A snapshot of the client's counters, cumulative since it started
    (spec/client.md, Statistics)."""

    frames: int
    frames_after_interruption: int
    events: Mapping[str, int]
    repeats: int
    unknown: int
    undecodable: Mapping[str, int]
    new_market_dropped: int
    discarded_outside: int
    pongs: int
    pongs_unsolicited: int
    pong_delay_last: float
    pong_delay_max: float
    connections: int
    interruptions: Mapping[str, int]
    lookups: int
    lookup_failures: int
    rest_book_not_found: int
    hash_verified: int
    hash_retried: int
    hash_failed: int
