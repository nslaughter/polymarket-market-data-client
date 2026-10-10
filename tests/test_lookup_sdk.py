"""The default lookup the SDK extra provides: ``get_market(slug=…)`` for
markets and ``get_order_book(token_id=…)`` for the hash inputs, and the
client's use of it when it is given no lookup (D6; spec/client.md, Market
lookup and Public interface).

The SDK's HTTP layer is replaced by synthetic responses, shaped like the
market and the REST book the SDK reads, for synthetic market A of the
conformance scenarios, never by the live service
(docs/implementation-plan.md, 7. Settle markets through the stream and
lookup). The guard in ``conftest.py``, which refuses every request through
``httpx``, is checked here too. Without the SDK extra installed, as in CI's
job for the oldest dependencies, this module is skipped: it tests the extra.
"""

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from polymarket_market_data import (
    BookParameters,
    ClientConfig,
    Market,
    MarketDataClient,
    MarketInfo,
)
from polymarket_market_data._lookup import SdkLookup, default_lookup

polymarket = pytest.importorskip("polymarket", reason="the sdk extra is not installed")
httpx = pytest.importorskip("httpx", reason="the sdk extra is not installed")

# Synthetic market A of spec/conformance.md.
PREFIX = "1" + "0" * 74
CONDITION_A = "0x" + "a1".rjust(64, "0")
A1, A2 = PREFIX + "11", PREFIX + "12"


def test_the_sdk_provides_the_default_lookup() -> None:
    assert isinstance(default_lookup(), SdkLookup)


GAMMA_A = {
    "id": "9200001",
    "question": "Synthetic market A?",
    "conditionId": CONDITION_A,
    "slug": "synthetic-a",
    "endDate": "2026-10-11T14:00:00Z",
    "outcomes": json.dumps(["Yes", "No"]),
    "active": True,
    "closed": False,
    "clobTokenIds": json.dumps([A1, A2]),
}
"""Market A as market lookup describes it, with string-encoded lists."""

BOOK_A1 = {
    "market": CONDITION_A,
    "asset_id": A1,
    "timestamp": "1791199970000",
    "hash": "b17e93a1f6202e13d8e0dd3aeb958b4a882dca3c",
    "bids": [{"price": "0.47", "size": "250"}, {"price": "0.48", "size": "100"}],
    "asks": [{"price": "0.53", "size": "300"}, {"price": "0.52", "size": "120"}],
    "min_order_size": "5",
    "tick_size": "0.01",
    "neg_risk": False,
    "last_trade_price": "0.500",
}
"""A1's REST book, in the key order of the hash recipe (spec/client.md,
Order-book hash)."""

NOT_FOUND = {"error": "No orderbook exists for the requested token id"}


class Responses:
    """The SDK's HTTP layer, replaced: each request gets the response
    ``routes`` gives for its path, or HTTP 404, and is recorded."""

    def __init__(self, routes: dict[str, tuple[int, object]]) -> None:
        self.routes = routes
        self.requests: list[str] = []

    def __call__(self, request: Any) -> Any:
        self.requests.append(
            f"{request.url.host}{request.url.path}?{request.url.query.decode()}"
        )
        status, body = self.routes.get(request.url.path, (404, NOT_FOUND))
        return httpx.Response(status, json=body)


@pytest.fixture
def responses(monkeypatch: pytest.MonkeyPatch) -> Responses:
    replaced = Responses({})
    real = httpx.AsyncClient

    class Recorded(real):  # type: ignore[misc, valid-type]
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs, transport=httpx.MockTransport(replaced))

    monkeypatch.setattr(httpx, "AsyncClient", Recorded)
    return replaced


def test_a_market_is_looked_up_by_its_slug(responses: Responses) -> None:
    responses.routes["/markets/slug/synthetic-a"] = (200, GAMMA_A)
    info = asyncio.run(SdkLookup().market(slug="synthetic-a"))
    assert info == MarketInfo(
        condition_id=CONDITION_A,
        slug="synthetic-a",
        question="Synthetic market A?",
        token_ids=(A1, A2),
        outcomes=("Yes", "No"),
        closed=False,
        end_date=datetime(2026, 10, 11, 14, tzinfo=UTC),
        winning_asset_id=None,
        resolution_status=None,
    )
    assert responses.requests == ["gamma-api.polymarket.com/markets/slug/synthetic-a?"]


def test_a_client_given_no_lookup_uses_the_default_one(responses: Responses) -> None:
    responses.routes["/markets/slug/synthetic-a"] = (200, GAMMA_A)
    client = MarketDataClient(ClientConfig(verify_hash=False))
    market = asyncio.run(client.resolve("synthetic-a"))
    assert market == Market(CONDITION_A, (A1, A2), "synthetic-a")
    assert client.stats().lookups == 1
    assert responses.requests == ["gamma-api.polymarket.com/markets/slug/synthetic-a?"]


def test_a_closed_market_reads_closed_with_its_resolution_status(
    responses: Responses,
) -> None:
    closed = {**GAMMA_A, "closed": True, "umaResolutionStatus": "resolved"}
    responses.routes["/markets/slug/synthetic-a"] = (200, closed)
    info = asyncio.run(SdkLookup().market(slug="synthetic-a"))
    assert info is not None
    # The SDK's market names no winner, so it stays unknown.
    assert (info.closed, info.resolution_status, info.winning_asset_id) == (
        True,
        "resolved",
        None,
    )


def test_a_market_not_found_is_none(responses: Responses) -> None:
    assert asyncio.run(SdkLookup().market(slug="synthetic-u")) is None


# Any other answer raises, and the client counts it as a failed call
# (spec/client.md, Market lookup).


def test_a_market_lookup_that_fails_otherwise_raises(responses: Responses) -> None:
    responses.routes["/markets/slug/synthetic-a"] = (500, {"error": "internal"})
    with pytest.raises(polymarket.RequestRejectedError):
        asyncio.run(SdkLookup().market(slug="synthetic-a"))


@pytest.mark.parametrize(
    "missing",
    [
        pytest.param({"conditionId": ""}, id="condition ID"),
        pytest.param({"clobTokenIds": "[]"}, id="token IDs"),
    ],
)
def test_a_market_without_its_identities_raises(
    responses: Responses, missing: dict[str, str]
) -> None:
    responses.routes["/markets/slug/synthetic-a"] = (200, {**GAMMA_A, **missing})
    with pytest.raises(ValueError, match="without its condition ID or its token IDs"):
        asyncio.run(SdkLookup().market(slug="synthetic-a"))


def test_a_market_the_sdk_cannot_read_raises(responses: Responses) -> None:
    # The SDK's market is binary: it refuses three outcomes.
    three = {**GAMMA_A, "outcomes": json.dumps(["A", "B", "C"])}
    responses.routes["/markets/slug/synthetic-a"] = (200, three)
    with pytest.raises(polymarket.PolymarketError):
        asyncio.run(SdkLookup().market(slug="synthetic-a"))


def test_book_parameters_come_from_the_rest_book(responses: Responses) -> None:
    responses.routes["/book"] = (200, BOOK_A1)
    parameters = asyncio.run(SdkLookup().book_parameters(A1))
    assert parameters == BookParameters(min_order_size=Decimal("5"), neg_risk=False)
    assert str(parameters.min_order_size) == "5"
    assert responses.requests == [f"clob.polymarket.com/book?token_id={A1}"]


def test_a_rest_book_not_found_is_none(responses: Responses) -> None:
    # As the REST book answered for a settled token (docs/source-behavior.md,
    # §6).
    assert asyncio.run(SdkLookup().book_parameters(A2)) is None


def test_a_rest_book_that_fails_otherwise_raises(responses: Responses) -> None:
    responses.routes["/book"] = (503, {"error": "unavailable"})
    with pytest.raises(polymarket.RequestRejectedError):
        asyncio.run(SdkLookup().book_parameters(A1))


# The guard in conftest.py, which keeps every test from the live service.


def test_no_request_through_httpx_leaves_the_tests(no_live_http: list[str]) -> None:
    # Synchronous and asynchronous alike: the SDK's PublicClient and
    # AsyncPublicClient each sit on one of httpx's transports.
    url = "https://example.invalid/markets"
    with pytest.raises(httpx.ConnectError), httpx.Client() as client:
        client.get(url)

    async def get() -> None:
        async with httpx.AsyncClient() as client:
            await client.get(url)

    with pytest.raises(httpx.ConnectError):
        asyncio.run(get())
    assert no_live_http == [f"GET {url}", f"GET {url}"]
    no_live_http.clear()  # refused as they should be, so the test passes
