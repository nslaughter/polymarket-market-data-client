"""``MarketDataClient``: its configuration, its desired set before the
block, and its lifecycle (spec/client.md, Public interface, Configuration,
and Cancellation and shutdown). None of these tests needs a server; the
client's behavior against the scripted server is tested in
``conformance/test_client_scripted.py``.
"""

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import ValidationError

from polymarket_market_data import (
    ClientConfig,
    ClientError,
    ClientStateError,
    ConfigError,
    Market,
    MarketDataClient,
    ReconnectPolicy,
)

# Synthetic markets A and B of spec/conformance.md.
PREFIX = "1" + "0" * 74
MARKET_A = Market(
    "0x" + "a1".rjust(64, "0"), (PREFIX + "11", PREFIX + "12"), "synthetic-a"
)
MARKET_B = Market(
    "0x" + "b2".rjust(64, "0"), (PREFIX + "21", PREFIX + "22"), "synthetic-b"
)


def config(**overrides: Any) -> ClientConfig:
    """A configuration that verifies no hashes and points nowhere."""
    return ClientConfig(
        **{"url": "ws://127.0.0.1:9", "verify_hash": False, **overrides}
    )


# The configuration.


@pytest.mark.parametrize(
    "unchecked",
    [
        pytest.param(
            ClientConfig().model_copy(update={"queue_size": 0}), id="model_copy"
        ),
        pytest.param(ClientConfig.model_construct(ping_interval=-1.0), id="construct"),
        pytest.param(
            ClientConfig().model_copy(
                update={
                    "reconnect": ReconnectPolicy().model_copy(
                        update={"max_attempts": 0}
                    )
                }
            ),
            id="nested",
        ),
    ],
)
def test_the_client_validates_its_configuration_again(unchecked: ClientConfig) -> None:
    # model_copy(update=...) and model_construct skip validation.
    with pytest.raises(ConfigError) as info:
        MarketDataClient(unchecked)
    assert isinstance(info.value.__cause__, ValidationError)


def test_the_client_refuses_a_configuration_of_another_type() -> None:
    with pytest.raises(ConfigError):
        MarketDataClient({"queue_size": 5})  # type: ignore[arg-type]


def test_building_a_client_does_no_io() -> None:
    # Outside any event loop: the constructor starts nothing.
    client = MarketDataClient(config(), markets=[MARKET_A, MARKET_B])
    assert client.desired == (MARKET_A, MARKET_B)
    assert client.backlog == 0
    stats = client.stats()
    assert (stats.frames, stats.connections, stats.pongs) == (0, 0, 0)
    assert stats.events == {}


# The desired set before the block.


def test_subscribe_and_unsubscribe_before_the_block() -> None:
    client = MarketDataClient(config(), markets=[MARKET_A])
    client.subscribe(MARKET_B, MARKET_A)
    added = client.desired
    client.unsubscribe(MARKET_A.condition_id, "0xnot-desired")
    assert (added, client.desired) == ((MARKET_A, MARKET_B), (MARKET_B,))


def test_subscribe_refuses_a_token_of_another_desired_market() -> None:
    client = MarketDataClient(config(), markets=[MARKET_A])
    clash = Market("0xother", (MARKET_A.token_ids[0],), None)
    with pytest.raises(ValueError, match="belongs to market"):
        client.subscribe(clash)
    assert client.desired == (MARKET_A,)


# The lifecycle.


def test_records_can_be_called_once() -> None:
    client = MarketDataClient(config())
    client.records()
    with pytest.raises(ClientStateError):
        client.records()


async def read_all(records: AsyncIterator[object]) -> tuple[list[object], object]:
    """Every record up to the iterator's end, and what ended it: the
    exception it raised, or ``StopAsyncIteration``."""
    read: list[object] = []
    while True:
        try:
            async with asyncio.timeout(5):
                read.append(await anext(records))
        except StopAsyncIteration:
            return read, StopAsyncIteration
        except ClientError as error:
            return read, error


def test_a_client_can_be_entered_once() -> None:
    async def main() -> None:
        client = MarketDataClient(config())
        async with client:
            with pytest.raises(ClientStateError):
                await client.__aenter__()
        with pytest.raises(ClientStateError):
            await client.__aenter__()

    asyncio.run(main())


def test_an_empty_desired_set_starts_nothing() -> None:
    async def main() -> None:
        client = MarketDataClient(config())
        records = client.records()
        before = asyncio.all_tasks()
        async with client:
            assert asyncio.all_tasks() == before
            waiting = asyncio.ensure_future(anext(records))
            await asyncio.sleep(0.1)
            assert not waiting.done()
        # Leaving the block ends a read that was waiting.
        with pytest.raises(StopAsyncIteration):
            await waiting
        assert await read_all(records) == ([], StopAsyncIteration)

    asyncio.run(main())


def test_changes_after_shutdown_raise() -> None:
    async def main() -> None:
        client = MarketDataClient(config())
        async with client:
            pass
        with pytest.raises(ClientStateError):
            client.subscribe(MARKET_A)
        with pytest.raises(ClientStateError):
            client.unsubscribe(MARKET_A.condition_id)

    asyncio.run(main())


def test_changes_while_running_are_not_implemented_yet() -> None:
    # Plan steps 7 and 8 apply them; until then they are refused, not lost.
    async def main() -> None:
        async with MarketDataClient(config()) as client:
            with pytest.raises(NotImplementedError):
                client.subscribe(MARKET_A)
            with pytest.raises(NotImplementedError):
                client.unsubscribe(MARKET_A.condition_id)

    asyncio.run(main())
