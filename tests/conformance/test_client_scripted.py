"""The client against the scripted server, for behavior no conformance
scenario covers: shutdown, the heartbeat's counters, ``keep_raw``, a defect
in the client's own code, and the stopgaps that end the client until later
plan steps replace them (spec/client.md, Cancellation and shutdown,
Heartbeat, and Errors).

Most are scenarios in the conformance notation, which the runner runs as it
runs those in spec/conformance.md; a test that needs the client itself
drives it step by step.
"""

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from polymarket_market_data import (
    ClientConfig,
    ClientError,
    ClientStateError,
    ConnectionState,
    ConnectionStateChange,
    MarketDataClient,
    ReconnectPolicy,
    TokenStateChange,
    _connection,
)

from .notation import parse_scenario
from .runner import run
from .server import ScriptedServer
from .synthetic import MARKETS, PROFILE, PROFILE_RECONNECT

MARKET_A = MARKETS["A"].market()
MARKET_B = MARKETS["B"].market()


def config(url: str, **overrides: Any) -> ClientConfig:
    """The conformance profile, with ``url`` and these fields set."""
    policy = ReconnectPolicy(**PROFILE_RECONNECT)
    return ClientConfig(url=url, reconnect=policy, **{**PROFILE, **overrides})


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


# Scenarios in the conformance notation, for behavior the conformance
# scenarios do not cover.

SCENARIOS = {
    "an-empty-desired-set-connects-nowhere": """
expect-no-connect 0.5
expect-nothing 0.1
exit
expect-end
""",
    "cancelling-the-block-closes-the-connection": """
markets A
start A
cancel
expect-client-close 1000 "client exit"
expect-end
""",
    "leaving-the-block-discards-queued-records": """
markets A
start A
send pc A t=100 A1:BUY:0.49:10
wait 0.2
expect-backlog 1
exit
expect-client-close 1000 "client exit"
expect-end
""",
    "pongs-are-counted-and-are-not-frames": """
markets A
start A
send-text PONG
expect-nothing 0.1
expect-stats pongs=1 pongs_unsolicited=1 frames=1
send pc A t=100 A1:BUY:0.49:10
expect price_change A t=100 frame=2 raw=none
recv-ping
recv-ping
expect-nothing 0.1
expect-stats pongs>=3 pongs_unsolicited=1 pong_delay_max<=0.2 frames=2
""",
    "keep-raw-attaches-the-frame-text": """
markets A
config keep_raw=true
start A
send bba A1 t=100
expect best_bid_ask A1 t=100 raw=set
""",
    # Stopgaps: until plan step 6 reconnects, and step 9 responds at the
    # queue's limit, each ends the client with a ClientError, raised after
    # the records already queued.
    "a-dropped-connection-ends-the-client": """
markets A
start A
send pc A t=100 A1:BUY:0.49:10
wait 0.2
drop
expect price_change A t=100
expect-error ClientError
expect-end
subscribe B raises ClientStateError
""",
    "a-close-frame-ends-the-client": """
markets A
start A
close 1001 "going away"
expect-error ClientError
expect-end
""",
    "a-frame-larger-than-max-message-bytes-ends-the-client": """
markets A
config max_message_bytes=64
expect conn connecting attempt=1
accept
recv-subscribe A1 A2
expect conn open connection=1
expect conn subscribed connection=1
expect token A1 synchronizing
expect token A2 synchronizing
send opening A1 A2
expect-error ClientError
expect-end
""",
    "a-frame-reaching-the-queue-limit-ends-the-client": """
markets A
config queue_size=1
start A
send pc A t=100 A1:BUY:0.49:10
send pc A t=101 A1:BUY:0.49:20
expect-client-close 1000 "client exit"
expect price_change A t=100
expect-error ClientError
expect-end
expect-stats events.price_change=1
""",
}


@pytest.mark.parametrize("name", SCENARIOS)
def test_scenario(name: str) -> None:
    run(parse_scenario(f"scenario {name}\n{SCENARIOS[name]}"))


# A defect in the client's own code ends the client: the exception is raised
# once, as the __cause__ of a ClientError.


def broken(*args: object, **kwargs: object) -> list[Any]:
    raise RuntimeError("a defect")


async def connect_and_send(server: ScriptedServer, text: str) -> None:
    await server.call(server.accept())
    await server.call(server.recv_subscribe(("A1", "A2")))
    await server.call(server.send_text(text))


def run_with(main: Callable[[ScriptedServer], Any]) -> None:
    with ScriptedServer() as server:
        asyncio.run(main(server))


def test_a_defect_is_raised_by_the_iterator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_connection, "decode_frame", broken)

    async def main(server: ScriptedServer) -> None:
        client = MarketDataClient(config(server.url), markets=[MARKET_A])
        records = client.records()
        async with client:
            await connect_and_send(server, "[]\n")
            read, ended = await read_all(records)
            # The client closed its connection as it ended.
            await server.call(server.expect_client_close(1000, "client exit"))
            with pytest.raises(ClientStateError):
                client.subscribe(MARKET_B)
        assert isinstance(ended, ClientError)
        assert isinstance(ended.__cause__, RuntimeError)
        # The records queued before it came first.
        assert [r.state for r in read if isinstance(r, ConnectionStateChange)] == [
            ConnectionState.CONNECTING,
            ConnectionState.OPEN,
            ConnectionState.SUBSCRIBED,
        ]
        assert len([r for r in read if isinstance(r, TokenStateChange)]) == 2
        assert await read_all(records) == ([], StopAsyncIteration)

    run_with(main)


def test_a_defect_no_consumer_read_is_raised_when_the_block_is_left(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_connection, "decode_frame", broken)

    async def main(server: ScriptedServer) -> None:
        client = MarketDataClient(config(server.url), markets=[MARKET_A])
        records = client.records()
        raised: ClientError | None = None
        try:
            async with client:
                await connect_and_send(server, "[]\n")
                await server.call(server.expect_client_close(1000, "client exit"))
        except ClientError as error:
            raised = error
        assert raised is not None
        assert isinstance(raised.__cause__, RuntimeError)
        # Raised once: the iterator then only ends.
        assert await read_all(records) == ([], StopAsyncIteration)

    run_with(main)
