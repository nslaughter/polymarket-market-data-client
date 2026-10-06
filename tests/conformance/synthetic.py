"""The synthetic markets, standard books, and profile of spec/conformance.md.

The tables are copied here, not read from the document, which executes only
its scenario blocks; ``test_synthetic.py`` checks the copies against the
tables.
"""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from polymarket_market_data import Market

T0 = 1791200000000
"""The scenarios' time origin, in milliseconds; ``t=<offset>`` is T0 plus
the offset."""


@dataclass(frozen=True, slots=True)
class SyntheticMarket:
    name: str
    condition_id: str
    tokens: tuple[str, ...]
    """Token names, in outcome order."""
    outcomes: tuple[str, ...]
    slug: str | None

    def market(self) -> Market:
        """The market as a scenario passes it to the client."""
        return Market(
            self.condition_id,
            tuple(TOKEN_IDS[token] for token in self.tokens),
            self.slug,
        )


def _condition(suffix: str) -> str:
    return "0x" + suffix.rjust(64, "0")


def _token(digits: str) -> str:
    return "1" + digits.rjust(76, "0")


MARKETS = MappingProxyType(
    {
        market.name: market
        for market in (
            SyntheticMarket(
                "A", _condition("a1"), ("A1", "A2"), ("Yes", "No"), "synthetic-a"
            ),
            SyntheticMarket(
                "B", _condition("b2"), ("B1", "B2"), ("Yes", "No"), "synthetic-b"
            ),
            SyntheticMarket(
                "S", _condition("5e"), ("S1", "S2"), ("Up", "Down"), "synthetic-s"
            ),
            SyntheticMarket(
                "U", _condition("0e"), ("U1", "U2"), ("Yes", "No"), "synthetic-u"
            ),
            SyntheticMarket("N", _condition("c3"), ("N1", "N2"), ("Yes", "No"), None),
        )
    }
)

TOKEN_IDS = MappingProxyType(
    {
        "A1": _token("11"),
        "A2": _token("12"),
        "B1": _token("21"),
        "B2": _token("22"),
        "S1": _token("31"),
        "S2": _token("32"),
        "U1": _token("41"),
        "U2": _token("42"),
        "N1": _token("51"),
        "N2": _token("52"),
    }
)

TOKEN_MARKET = MappingProxyType(
    {token: market.name for market in MARKETS.values() for token in market.tokens}
)

NAMES = MappingProxyType(
    {
        **{market.condition_id: name for name, market in MARKETS.items()},
        **{token_id: name for name, token_id in TOKEN_IDS.items()},
    }
)
"""The name of each synthetic ID, for reports."""


def ids(name: str) -> str:
    """The ID a market or token name stands for."""
    if name in MARKETS:
        return MARKETS[name].condition_id
    return TOKEN_IDS[name]


def outcome(token: str) -> str:
    """The outcome a token stands for."""
    market = MARKETS[TOKEN_MARKET[token]]
    return market.outcomes[market.tokens.index(token)]


TRADING = ("A", "B")
"""The markets the server keeps reference books for."""

MIN_ORDER_SIZE = "5"
NEG_RISK = False
TICK_SIZE = "0.01"
TRADE_PRICES = MappingProxyType({"A": "0.500", "B": "0.400"})

STANDARD_OFFSET = -30000
"""When each standard book last changed."""

Levels = tuple[tuple[str, str], ...]
"""Price and size pairs, as text."""

STANDARD_BOOKS: MappingProxyType[str, tuple[Levels, Levels]] = MappingProxyType(
    {
        "A1": ((("0.48", "100"), ("0.47", "250")), (("0.52", "120"), ("0.53", "300"))),
        "A2": ((("0.48", "120"), ("0.47", "300")), (("0.52", "100"), ("0.53", "250"))),
        "B1": ((("0.39", "80"), ("0.38", "200")), (("0.41", "90"), ("0.42", "150"))),
        "B2": ((("0.59", "90"), ("0.58", "150")), (("0.61", "80"), ("0.62", "200"))),
    }
)
"""Each token's bids and asks, as the standard books table lists them."""

PROFILE: MappingProxyType[str, Any] = MappingProxyType(
    {
        "ping_interval": 0.5,
        "pong_timeout": 2.0,
        "connect_timeout": 1.0,
        "close_timeout": 0.5,
        "book_timeout": 1.0,
        "repeat_window": 1.0,
        "queue_size": 1000,
        "backlog_warning": 0.5,
        "overflow": "disconnect",
        "resume_below": 0.1,
        "verify_hash": False,
        "hash_grace": 0.5,
        "burst_quiet": 0.1,
        "settlement_poll_interval": 0.5,
        "settlement_confirm_timeout": 3.0,
        "lookup_timeout": 1.0,
        "new_market": "drop",
        "keep_raw": False,
    }
)
"""The profile's fields other than ``url`` and ``reconnect``."""

PROFILE_RECONNECT: MappingProxyType[str, Any] = MappingProxyType(
    {
        "base_delay": 0.1,
        "max_delay": 0.4,
        "max_attempts": 3,
        "max_recovery_time": 10.0,
        "jitter": False,
    }
)
