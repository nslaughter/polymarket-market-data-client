"""Market lookup: the calls the client makes, each limited to
``lookup_timeout`` and counted, and whether the default lookup is available
(spec/client.md, Market lookup and Statistics; D6). ``test_lookup_sdk.py``
tests the default lookup itself.
"""

import asyncio
import sys
from collections.abc import Callable
from typing import Any

import pytest

from polymarket_market_data import BookParameters, LookupFailed, MarketInfo
from polymarket_market_data._lookup import Lookups, default_lookup

# Synthetic market A of spec/conformance.md.
PREFIX = "1" + "0" * 74
CONDITION_A = "0x" + "a1".rjust(64, "0")
A1, A2 = PREFIX + "11", PREFIX + "12"

INFO_A = MarketInfo(
    condition_id=CONDITION_A,
    slug="synthetic-a",
    question=None,
    token_ids=(A1, A2),
    outcomes=("Yes", "No"),
    closed=False,
    end_date=None,
)


class Scripted:
    """A lookup that answers ``market`` with what ``answer`` gives or
    raises."""

    def __init__(self, answer: Callable[[str], Any]) -> None:
        self.answer = answer

    async def market(self, *, slug: str) -> MarketInfo | None:
        result = self.answer(slug)
        if isinstance(result, BaseException):
            raise result
        if asyncio.iscoroutine(result):
            return await result  # type: ignore[no-any-return]
        return result  # type: ignore[no-any-return]

    async def book_parameters(self, token_id: str) -> BookParameters | None:
        raise AssertionError("not called")


# The calls the client makes.


def test_a_call_returns_what_lookup_found_and_is_counted() -> None:
    lookups = Lookups(Scripted(lambda slug: INFO_A), timeout=1.0)
    assert asyncio.run(lookups.market("synthetic-a")) == INFO_A
    assert (lookups.calls, lookups.failures) == (1, 0)


def test_finding_no_market_is_not_a_failure() -> None:
    lookups = Lookups(Scripted(lambda slug: None), timeout=1.0)
    assert asyncio.run(lookups.market("synthetic-u")) is None
    assert (lookups.calls, lookups.failures) == (1, 0)


def test_a_lookup_that_raises_fails_with_its_exception_as_the_cause() -> None:
    error = RuntimeError("scripted")
    lookups = Lookups(Scripted(lambda slug: error), timeout=1.0)
    with pytest.raises(LookupFailed) as info:
        asyncio.run(lookups.market("synthetic-a"))
    assert info.value.__cause__ is error
    assert (lookups.calls, lookups.failures) == (1, 1)


def test_a_lookup_past_lookup_timeout_fails_with_the_timeout_as_the_cause() -> None:
    async def slow() -> MarketInfo:
        await asyncio.sleep(5)
        return INFO_A

    async def main() -> float:
        lookups = Lookups(Scripted(lambda slug: slow()), timeout=0.1)
        loop = asyncio.get_running_loop()
        start = loop.time()
        with pytest.raises(LookupFailed) as info:
            await lookups.market("synthetic-a")
        assert isinstance(info.value.__cause__, TimeoutError)
        assert (lookups.calls, lookups.failures) == (1, 1)
        return loop.time() - start

    assert asyncio.run(main()) < 1.0


def test_a_cancelled_call_is_no_failure() -> None:
    async def main() -> None:
        lookups = Lookups(Scripted(lambda slug: asyncio.sleep(5)), timeout=10.0)
        call = asyncio.ensure_future(lookups.market("synthetic-a"))
        await asyncio.sleep(0.05)
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        assert (lookups.calls, lookups.failures) == (1, 0)

    asyncio.run(main())


# Whether the default lookup is available.


def test_without_the_sdk_there_is_no_default_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "polymarket", None)
    assert default_lookup() is None
