"""The scripted market lookup (spec/conformance.md, The scripted lookup)."""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

from polymarket_market_data import BookParameters, MarketInfo

from .synthetic import MARKETS, MIN_ORDER_SIZE, NEG_RISK, TOKEN_IDS, TOKEN_MARKET


@dataclass(frozen=True, slots=True)
class Answer:
    """``open``, ``closed winner=<T>``, ``missing``, or ``error``."""

    kind: Literal["open", "closed", "missing", "error"]
    winner: str | None = None

    def __str__(self) -> str:
        return f"closed winner={self.winner}" if self.kind == "closed" else self.kind


DEFAULT_ANSWERS = {
    "A": Answer("open"),
    "B": Answer("open"),
    "S": Answer("closed", "S2"),
    "N": Answer("closed", "N2"),
    "U": Answer("missing"),
}


class ScriptedLookup:
    """Implements ``MarketLookup`` for the synthetic markets. It finds a
    market only by slug, counts every call, and records any call
    ``MarketLookup`` does not define, which fails the scenario."""

    def __init__(self, answers: Mapping[str, Answer] = DEFAULT_ANSWERS) -> None:
        self.answers = {**DEFAULT_ANSWERS, **answers}
        self.calls: list[str] = []
        self.violations: list[str] = []

    async def market(self, *args: object, **kwargs: object) -> MarketInfo | None:
        if args or kwargs.keys() != {"slug"} or not isinstance(kwargs["slug"], str):
            raise self._violation("market", args, kwargs)
        slug = kwargs["slug"]
        self.calls.append(f"market(slug={slug!r})")
        name = next((m.name for m in MARKETS.values() if m.slug == slug), None)
        if name is None:
            return None
        answer = self.answers[name]
        if answer.kind == "error":
            raise RuntimeError(f"scripted lookup error for {slug}")
        if answer.kind == "missing":
            return None
        market = MARKETS[name]
        return MarketInfo(
            condition_id=market.condition_id,
            slug=market.slug,
            question=None,
            token_ids=tuple(TOKEN_IDS[token] for token in market.tokens),
            outcomes=market.outcomes,
            closed=answer.kind == "closed",
            end_date=None,
            winning_asset_id=None
            if answer.winner is None
            else TOKEN_IDS[answer.winner],
        )

    async def book_parameters(
        self, *args: object, **kwargs: object
    ) -> BookParameters | None:
        if len(args) + len(kwargs) != 1 or kwargs.keys() - {"token_id"}:
            raise self._violation("book_parameters", args, kwargs)
        token_id = args[0] if args else kwargs["token_id"]
        if not isinstance(token_id, str):
            raise self._violation("book_parameters", args, kwargs)
        self.calls.append(f"book_parameters({token_id!r})")
        name = next((n for n, i in TOKEN_IDS.items() if i == token_id), None)
        if name is None:
            return None
        answer = self.answers[TOKEN_MARKET[name]]
        if answer.kind == "error":
            raise RuntimeError(f"scripted lookup error for {name}")
        if answer.kind == "open":
            return BookParameters(Decimal(MIN_ORDER_SIZE), NEG_RISK)
        return None

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        def undefined(*args: object, **kwargs: object) -> Any:
            raise self._violation(name, args, kwargs)

        return undefined

    def _violation(
        self, name: str, args: tuple[object, ...], kwargs: Mapping[str, object]
    ) -> TypeError:
        words = [repr(arg) for arg in args] + [f"{k}={v!r}" for k, v in kwargs.items()]
        call = f"{name}({', '.join(words)})"
        self.violations.append(f"a call MarketLookup does not define: {call}")
        return TypeError(f"MarketLookup does not define {call}")
