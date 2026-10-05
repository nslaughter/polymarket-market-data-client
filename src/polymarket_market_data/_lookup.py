"""The market lookup interface (spec/client.md, Market lookup).

The default lookup, which the SDK extra provides (D6), comes with plan
step 7.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True, slots=True)
class MarketInfo:
    """A market as lookup found it. ``token_ids`` and ``outcomes`` are in the
    same order; ``winning_asset_id`` and ``resolution_status`` are set when
    known."""

    condition_id: str
    slug: str | None
    question: str | None
    token_ids: tuple[str, ...]
    outcomes: tuple[str, ...]
    closed: bool
    end_date: datetime | None
    winning_asset_id: str | None = None
    resolution_status: str | None = None


@dataclass(frozen=True, slots=True)
class BookParameters:
    """The two hash inputs the stream does not carry (D4)."""

    min_order_size: Decimal
    neg_risk: bool


class MarketLookup(Protocol):
    """How the client looks markets up. ``None`` means not found."""

    async def market(self, *, slug: str) -> MarketInfo | None: ...

    async def book_parameters(self, token_id: str) -> BookParameters | None: ...
