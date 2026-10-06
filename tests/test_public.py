"""The public names, the exceptions, and the interfaces without behavior yet."""

import polymarket_market_data
from polymarket_market_data import (
    BookParameters,
    ClientError,
    ClientStateError,
    ConfigError,
    LookupFailed,
    MarketInfo,
    MarketLookup,
    MarketNotFound,
    RecoveryFailed,
)

# Every public name spec/client.md gives.
PUBLIC_NAMES = {
    "MarketDataClient",
    "ClientConfig",
    "ReconnectPolicy",
    "Market",
    "ClientStats",
    "MarketLookup",
    "MarketInfo",
    "BookParameters",
    # Records
    "BookEvent",
    "PriceChangeEvent",
    "BestBidAskEvent",
    "LastTradePriceEvent",
    "TickSizeChangeEvent",
    "MarketResolvedEvent",
    "NewMarketEvent",
    "UnknownEvent",
    "UndecodableFrame",
    "TokenStateChange",
    "ConnectionStateChange",
    "CaptureGap",
    "Backlog",
    "Level",
    "PriceChange",
    # Enums
    "TokenState",
    "ConnectionState",
    "Side",
    # Exceptions
    "ClientError",
    "ConfigError",
    "ClientStateError",
    "MarketNotFound",
    "LookupFailed",
    "RecoveryFailed",
}


def test_every_public_name_is_importable_from_the_top_level() -> None:
    assert set(polymarket_market_data.__all__) == PUBLIC_NAMES
    namespace: dict[str, object] = {}
    exec("from polymarket_market_data import *", namespace)
    assert namespace.keys() >= PUBLIC_NAMES


def test_exception_hierarchy() -> None:
    assert issubclass(ConfigError, ClientError)
    assert issubclass(ConfigError, ValueError)
    assert issubclass(ClientStateError, ClientError)
    assert issubclass(ClientStateError, RuntimeError)
    assert issubclass(MarketNotFound, ClientError)
    assert issubclass(MarketNotFound, LookupError)
    assert issubclass(LookupFailed, ClientError)
    assert issubclass(RecoveryFailed, ClientError)
    assert issubclass(ClientError, Exception)


class ScriptedLookup:
    async def market(self, *, slug: str) -> MarketInfo | None:
        return None

    async def book_parameters(self, token_id: str) -> BookParameters | None:
        return None


def test_a_lookup_satisfies_the_protocol() -> None:
    # Checked by mypy: the assignment fails type checking if ScriptedLookup
    # does not match MarketLookup.
    lookup: MarketLookup = ScriptedLookup()
    assert lookup is not None
