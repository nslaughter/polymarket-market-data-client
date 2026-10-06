"""The scripted lookup (spec/conformance.md, The scripted lookup)."""

import asyncio
from decimal import Decimal
from typing import Any

import pytest

from polymarket_market_data import BookParameters, MarketLookup

from .lookup import Answer, ScriptedLookup
from .synthetic import MARKETS, TOKEN_IDS


def market(lookup: ScriptedLookup, slug: str) -> Any:
    return asyncio.run(lookup.market(slug=slug))


def parameters(lookup: ScriptedLookup, token: str) -> Any:
    return asyncio.run(lookup.book_parameters(TOKEN_IDS[token]))


def test_it_is_a_market_lookup() -> None:
    lookup: MarketLookup = ScriptedLookup()
    assert lookup is not None


def test_open_markets() -> None:
    lookup = ScriptedLookup()
    info = market(lookup, "synthetic-a")
    assert info.condition_id == MARKETS["A"].condition_id
    assert info.slug == "synthetic-a"
    assert info.token_ids == (TOKEN_IDS["A1"], TOKEN_IDS["A2"])
    assert info.outcomes == ("Yes", "No")
    assert (info.closed, info.winning_asset_id) == (False, None)
    assert market(lookup, "synthetic-b").closed is False
    assert parameters(lookup, "B2") == BookParameters(Decimal("5"), False)


def test_closed_markets_name_their_winner() -> None:
    lookup = ScriptedLookup()
    info = market(lookup, "synthetic-s")
    assert (info.closed, info.winning_asset_id) == (True, TOKEN_IDS["S2"])
    assert parameters(lookup, "S1") is None
    assert parameters(lookup, "N1") is None


def test_a_missing_market_is_none() -> None:
    lookup = ScriptedLookup()
    assert market(lookup, "synthetic-u") is None
    assert market(lookup, "no-such-market") is None
    assert parameters(lookup, "U2") is None


def test_an_error_raises() -> None:
    lookup = ScriptedLookup({"A": Answer("error")})
    with pytest.raises(RuntimeError):
        market(lookup, "synthetic-a")
    with pytest.raises(RuntimeError):
        parameters(lookup, "A1")


def test_answers_can_change() -> None:
    lookup = ScriptedLookup({"A": Answer("missing")})
    assert market(lookup, "synthetic-a") is None
    lookup.answers["A"] = Answer("closed", "A2")
    assert market(lookup, "synthetic-a").winning_asset_id == TOKEN_IDS["A2"]


def test_every_call_is_counted() -> None:
    lookup = ScriptedLookup()
    market(lookup, "synthetic-a")
    market(lookup, "synthetic-u")
    parameters(lookup, "A1")
    assert lookup.calls == [
        "market(slug='synthetic-a')",
        "market(slug='synthetic-u')",
        f"book_parameters('{TOKEN_IDS['A1']}')",
    ]
    assert lookup.violations == []


def test_a_call_market_lookup_does_not_define_is_a_violation() -> None:
    lookup = ScriptedLookup()
    with pytest.raises(TypeError):
        asyncio.run(lookup.market(condition_id=MARKETS["A"].condition_id))
    with pytest.raises(TypeError):
        lookup.market_by_condition_id(MARKETS["A"].condition_id)
    with pytest.raises(TypeError):
        asyncio.run(lookup.book_parameters())
    assert len(lookup.violations) == 3
    assert "market(condition_id=" in lookup.violations[0]
    assert lookup.calls == []
