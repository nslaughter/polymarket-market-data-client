"""Market lookup: the interface, the calls the client makes through it, and
the default lookup the SDK extra provides (spec/client.md, Market lookup;
D6).
"""

import asyncio
import importlib.util
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from ._errors import LookupFailed


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


class Lookups:
    """The lookup calls the client makes, each limited to ``lookup_timeout``,
    with the counts of calls and of those that failed (spec/client.md,
    Market lookup and Statistics)."""

    def __init__(self, lookup: MarketLookup, timeout: float) -> None:
        self._lookup = lookup
        self._timeout = timeout
        self.calls = 0
        self.failures = 0

    async def market(self, slug: str) -> MarketInfo | None:
        """The market with this slug, or ``None`` if lookup found none.
        Raises ``LookupFailed`` if the lookup raised or timed out, with that
        exception as its ``__cause__``; the caller decides what a failure
        means."""
        self.calls += 1
        try:
            async with asyncio.timeout(self._timeout):
                return await self._lookup.market(slug=slug)
        except Exception as error:
            # The lookup is the application's or the SDK's code, not the
            # client's, so any exception it raises is a failed call.
            self.failures += 1
            raise LookupFailed(
                f"looking up the market {slug!r} failed: {error!r}"
            ) from error


def default_lookup() -> MarketLookup | None:
    """The lookup the SDK extra provides, or ``None`` if the SDK is not
    installed (D6). The SDK is imported at the first call, not here."""
    if importlib.util.find_spec("polymarket") is None:
        return None
    return SdkLookup()


class SdkLookup:
    """``MarketLookup`` through ``polymarket-client`` 0.12.0, as the
    investigation used it (D6): ``get_market(slug=…)`` for markets and
    ``get_order_book(token_id=…)`` for the hash inputs.

    Each call opens an ``AsyncPublicClient`` and closes it, so that the
    lookup holds no connection between calls and needs no closing:
    ``MarketLookup`` has no point at which to close one. (The
    investigation's scripts made several calls through one client.) An HTTP
    404 means not found, as the REST book's did for settled tokens
    (spec/client.md, Market lookup); the findings do not record what market
    lookup answers for a slug it does not know. Any other failure is raised,
    and the client counts it.
    """

    async def market(self, *, slug: str) -> MarketInfo | None:
        from polymarket import AsyncPublicClient, RequestRejectedError

        async with AsyncPublicClient() as client:
            try:
                market = await client.get_market(slug=slug)
            except RequestRejectedError as error:
                if error.status == 404:
                    return None
                raise
        outcomes = (market.outcomes.yes, market.outcomes.no)
        token_ids = tuple(outcome.token_id for outcome in outcomes)
        if market.condition_id is None or None in token_ids:
            raise ValueError(
                f"market lookup returned the market {slug!r} without its "
                "condition ID or its token IDs"
            )
        status = market.resolution.uma_resolution_status
        return MarketInfo(
            condition_id=market.condition_id,
            slug=market.slug,
            question=market.question,
            token_ids=tuple(str(token_id) for token_id in token_ids),
            outcomes=tuple(outcome.label for outcome in outcomes),
            closed=market.state.closed is True,
            end_date=market.state.end_date,
            # The SDK's market names no winner, and the findings do not say
            # how lookup shows one, so it is left unknown.
            winning_asset_id=None,
            resolution_status=None if status is None else status.value,
        )

    async def book_parameters(self, token_id: str) -> BookParameters | None:
        from polymarket import AsyncPublicClient, RequestRejectedError

        async with AsyncPublicClient() as client:
            try:
                book = await client.get_order_book(token_id=token_id)
            except RequestRejectedError as error:
                if error.status == 404:
                    return None
                raise
        return BookParameters(
            min_order_size=book.min_order_size, neg_risk=book.neg_risk
        )
