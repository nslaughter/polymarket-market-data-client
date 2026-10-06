"""The harness's copies of spec/conformance.md's tables match the tables
(spec/conformance.md, Synthetic markets and Profile)."""

import re

from polymarket_market_data import ClientConfig, Market, ReconnectPolicy

from .failures import SPEC
from .synthetic import (
    MARKETS,
    MIN_ORDER_SIZE,
    NEG_RISK,
    PROFILE,
    PROFILE_RECONNECT,
    STANDARD_BOOKS,
    T0,
    TICK_SIZE,
    TOKEN_IDS,
    TRADE_PRICES,
    ids,
    outcome,
)

TEXT = SPEC.read_text()


def table(heading: str) -> list[list[str]]:
    """The rows of the first table after a line, each a list of cells."""
    rest = TEXT[TEXT.index(heading) :]
    start = rest.index("\n|")
    lines = rest[start + 1 :].split("\n\n", 1)[0].splitlines()
    return [[cell.strip() for cell in line.strip("|").split("|")] for line in lines[2:]]


def test_t0() -> None:
    assert f"`T0` is `{T0}`" in TEXT


def test_the_synthetic_markets() -> None:
    rows = table("### Synthetic markets")
    assert [row[0].strip("`") for row in rows] == list(MARKETS)
    for name, condition_id, tokens, outcomes, slug in rows:
        market = MARKETS[name.strip("`")]
        assert market.condition_id == condition_id.strip("`")
        pairs = re.findall(r"`([A-Z][12])` `([0-9]+)`", tokens)
        assert tuple(token for token, _ in pairs) == market.tokens
        assert all(TOKEN_IDS[token] == token_id for token, token_id in pairs)
        assert ", ".join(market.outcomes) == outcomes
        assert market.slug == (None if slug == "none" else slug.strip("`"))


def test_the_trading_markets_hash_inputs() -> None:
    assert f'`min_order_size` `"{MIN_ORDER_SIZE}"`' in TEXT
    assert f"`neg_risk` {str(NEG_RISK).lower()}" in TEXT
    assert f'tick size `"{TICK_SIZE}"`' in TEXT
    sentence = (
        f'the last trade price is `"{TRADE_PRICES["A"]}"` for `A` and\n'
        f'`"{TRADE_PRICES["B"]}"` for `B`'
    )
    assert sentence in TEXT


def test_the_standard_books() -> None:
    rows = table("Standard books, each last changed at `t=-30000`")
    books = {}
    for token, bids, asks, _ in rows:
        books[token.strip("`")] = (_levels(bids), _levels(asks))
    assert books == dict(STANDARD_BOOKS)


def _levels(cell: str) -> tuple[tuple[str, str], ...]:
    levels = []
    for level in cell.split(", "):
        price, size = level.split(" × ")
        levels.append((price, size))
    return tuple(levels)


def test_the_profile() -> None:
    rows = {name.strip("`"): value for name, value in table("### Profile")}
    assert rows.pop("url") == "the scripted server"
    policy = dict(
        re.findall(r"`([a-z_]+)` ([0-9.]+|true|false)", rows.pop("reconnect"))
    )
    assert {name: _value(value) for name, value in policy.items()} == dict(
        PROFILE_RECONNECT
    )
    assert {name: _value(value.strip("`")) for name, value in rows.items()} == dict(
        PROFILE
    )


def _value(text: str) -> object:
    if text in ("true", "false"):
        return text == "true"
    if re.fullmatch(r"[0-9]+", text):
        return int(text)
    if re.fullmatch(r"[0-9]+\.[0-9]+", text):
        return float(text)
    return text


def test_the_profile_is_a_valid_configuration() -> None:
    config = ClientConfig(
        url="ws://127.0.0.1:1",
        reconnect=ReconnectPolicy(**PROFILE_RECONNECT),
        **PROFILE,
    )
    assert config.reconnect.max_attempts == 3


def test_a_market_is_passed_with_its_tokens_and_slug() -> None:
    assert MARKETS["A"].market() == Market(
        ids("A"), (ids("A1"), ids("A2")), "synthetic-a"
    )
    assert MARKETS["N"].market().slug is None
    assert outcome("S2") == "Down"
