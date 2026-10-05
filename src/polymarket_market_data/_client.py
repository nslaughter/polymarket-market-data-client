"""``MarketDataClient``.

Only a stub until plan step 5 gives it behavior.
"""

from collections.abc import Iterable

from ._config import ClientConfig
from ._lookup import MarketLookup
from ._records import Market

_DEFAULT_CONFIG = ClientConfig()


class MarketDataClient:
    """The client for Polymarket's market WebSocket (spec/client.md, Public
    interface). Not implemented yet."""

    def __init__(
        self,
        config: ClientConfig = _DEFAULT_CONFIG,
        *,
        markets: Iterable[Market] = (),
        lookup: MarketLookup | None = None,
    ) -> None:
        raise NotImplementedError("MarketDataClient is not implemented yet")
