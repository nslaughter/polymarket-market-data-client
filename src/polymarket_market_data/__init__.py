"""A read-only client for Polymarket's market WebSocket.

spec/client.md is the contract this package implements.
"""

from ._config import ClientConfig, ReconnectPolicy
from ._errors import (
    ClientError,
    ClientStateError,
    ConfigError,
    LookupFailed,
    MarketNotFound,
    RecoveryFailed,
)
from ._lookup import BookParameters, MarketInfo, MarketLookup
from ._records import (
    Backlog,
    BestBidAskEvent,
    BookEvent,
    CaptureGap,
    ClientStats,
    ConnectionState,
    ConnectionStateChange,
    LastTradePriceEvent,
    Level,
    Market,
    MarketResolvedEvent,
    NewMarketEvent,
    PriceChange,
    PriceChangeEvent,
    Side,
    TickSizeChangeEvent,
    TokenState,
    TokenStateChange,
    UndecodableFrame,
    UnknownEvent,
)

__all__ = [
    "Backlog",
    "BestBidAskEvent",
    "BookEvent",
    "BookParameters",
    "CaptureGap",
    "ClientConfig",
    "ClientError",
    "ClientStateError",
    "ClientStats",
    "ConfigError",
    "ConnectionState",
    "ConnectionStateChange",
    "LastTradePriceEvent",
    "Level",
    "LookupFailed",
    "Market",
    "MarketInfo",
    "MarketLookup",
    "MarketNotFound",
    "MarketResolvedEvent",
    "NewMarketEvent",
    "PriceChange",
    "PriceChangeEvent",
    "ReconnectPolicy",
    "RecoveryFailed",
    "Side",
    "TickSizeChangeEvent",
    "TokenState",
    "TokenStateChange",
    "UndecodableFrame",
    "UnknownEvent",
]
