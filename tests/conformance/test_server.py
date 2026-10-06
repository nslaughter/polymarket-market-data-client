"""Each server step, against a plain ``websockets`` client (spec/conformance.md,
Server steps)."""

import asyncio
import json
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

import pytest
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosedError, InvalidStatus
from websockets.frames import Close
from websockets.protocol import State

from .failures import StepFailed
from .frames import BookFrame, Entry, Opening, PriceChangeFrame
from .server import ScriptedServer
from .synthetic import MARKETS, T0, TOKEN_IDS

Test = Callable[[ScriptedServer], Coroutine[Any, Any, None]]


def serve(test: Test) -> Callable[[], None]:
    """Run an async test with a scripted server of its own."""

    def run() -> None:
        with ScriptedServer() as server:
            asyncio.run(asyncio.wait_for(test(server), 10))

    run.__name__ = test.__name__
    run.__doc__ = test.__doc__
    return run


async def attempt(server: ScriptedServer) -> ClientConnection:
    return await connect(server.url, ping_interval=None, open_timeout=5)


async def opened(server: ScriptedServer) -> ClientConnection:
    """A client connection the server has accepted."""
    client = asyncio.create_task(attempt(server))
    await server.call(server.accept())
    return await client


def subscription(*tokens: str) -> str:
    ids = [TOKEN_IDS[token] for token in tokens]
    return json.dumps(
        {"type": "market", "assets_ids": ids, "custom_feature_enabled": True}
    )


async def text(client: ClientConnection) -> str:
    message = await asyncio.wait_for(client.recv(), 2)
    assert isinstance(message, str)
    return message


@serve
async def test_accept_holds_each_attempt_and_numbers_the_connections(
    server: ScriptedServer,
) -> None:
    client = asyncio.create_task(attempt(server))
    await asyncio.sleep(0.3)
    assert not client.done()
    arrived = await server.call(server.accept())
    first = await client
    assert (datetime.now(UTC) - arrived).total_seconds() > 0.25
    await server.call(server.drop())
    second = await opened(server)
    await second.send("PING")
    assert await text(second) == "PONG"
    with pytest.raises(ConnectionClosedError):
        await first.recv()
    await second.close()


@serve
async def test_an_abandoned_attempt_is_discarded(server: ScriptedServer) -> None:
    with pytest.raises(TimeoutError):
        await connect(server.url, ping_interval=None, open_timeout=0.2)
    await asyncio.sleep(0.2)
    client = await opened(server)
    await server.call(server.send(Opening(())))
    assert await text(client) == "[]\n"
    await client.close()


@serve
async def test_refuse_answers_attempts_with_503_and_waits_for_all(
    server: ScriptedServer,
) -> None:
    refusing = asyncio.create_task(server.call(server.refuse(2)))
    with pytest.raises(InvalidStatus) as first:
        await attempt(server)
    assert first.value.response.status_code == 503
    assert not refusing.done()
    with pytest.raises(InvalidStatus):
        await attempt(server)
    sent = await refusing
    assert (datetime.now(UTC) - sent).total_seconds() < 1
    client = await opened(server)
    await client.close()


@serve
async def test_refuse_all_refuses_until_the_next_accept(server: ScriptedServer) -> None:
    held = asyncio.create_task(attempt(server))
    await asyncio.sleep(0.2)
    await server.call(server.refuse_all())
    with pytest.raises(InvalidStatus):
        await held
    with pytest.raises(InvalidStatus):
        await attempt(server)
    accepting = asyncio.create_task(server.call(server.accept()))
    await asyncio.sleep(0.1)
    client = await attempt(server)
    await accepting
    await client.close()


@serve
async def test_recv_subscribe_takes_the_next_text_frame_other_than_ping(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    await client.send("PING")
    await client.send(subscription("A1", "A2"))
    await server.call(server.recv_subscribe(["A1", "A2"]))
    assert await text(client) == "PONG"
    assert await server.call(server.finish()) == []
    await client.close()


@pytest.mark.parametrize(
    "frame",
    [
        subscription("A2", "A1"),
        subscription("A1"),
        json.dumps(
            {
                "type": "market",
                "assets_ids": [TOKEN_IDS["A1"], TOKEN_IDS["A2"]],
                "custom_feature_enabled": 1,
            }
        ),
        json.dumps(
            {
                "type": "market",
                "assets_ids": [TOKEN_IDS["A1"], TOKEN_IDS["A2"]],
                "custom_feature_enabled": True,
                "initial_dump": True,
            }
        ),
        "not json",
    ],
)
def test_recv_subscribe_refuses_any_other_frame(frame: str) -> None:
    @serve
    async def check(server: ScriptedServer) -> None:
        client = await opened(server)
        await client.send(frame)
        with pytest.raises(StepFailed, match="subscription frame differs"):
            await server.call(server.recv_subscribe(["A1", "A2"]))
        await client.close()

    check()


@serve
async def test_an_unconsumed_text_frame_is_a_violation(server: ScriptedServer) -> None:
    client = await opened(server)
    await client.send(subscription("A1", "A2"))
    await client.send(subscription("A1", "A2"))
    await server.call(server.recv_subscribe(["A1", "A2"]))
    await asyncio.sleep(0.1)
    (violation,) = await server.call(server.finish())
    assert "no recv-subscribe consumed" in violation
    await client.close()


@serve
async def test_a_binary_frame_from_the_client_is_a_violation(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    await client.send(b"\x00")
    await asyncio.sleep(0.1)
    assert server.violations() == ["a binary frame on connection 1"]
    await client.close()


@serve
async def test_recv_ping_waits_for_a_ping_after_it_starts(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    await client.send("PING")
    await text(client)
    waiting = asyncio.create_task(server.call(server.recv_ping()))
    await asyncio.sleep(0.2)
    assert not waiting.done()
    await client.send("PING")
    arrived = await waiting
    assert (datetime.now(UTC) - arrived).total_seconds() < 0.5
    await client.close()


@serve
async def test_pong_off_never_answers(server: ScriptedServer) -> None:
    client = await opened(server)
    await server.call(server.pong("off"))
    await client.send("PING")
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(client.recv(), 0.3)
    await server.call(server.pong("auto"))
    await client.send("PING")
    assert await text(client) == "PONG"
    await client.close()


@serve
async def test_pong_hold_keeps_the_answers_owed_until_released(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    await server.call(server.pong("hold"))
    await client.send("PING")
    await client.send("PING")
    await asyncio.sleep(0.1)
    await server.call(server.send(Opening(())))
    assert await text(client) == "[]\n"
    await server.call(server.release_pongs())
    assert [await text(client), await text(client)] == ["PONG", "PONG"]
    await client.close()


@serve
async def test_the_pong_setting_lasts_across_connections(
    server: ScriptedServer,
) -> None:
    await server.call(server.pong("off"))
    first = await opened(server)
    await server.call(server.drop())
    second = await opened(server)
    await second.send("PING")
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(second.recv(), 0.3)
    await first.close()
    await second.close()


@serve
async def test_send_writes_the_frame_notation(server: ScriptedServer) -> None:
    client = await opened(server)
    sent = await server.call(server.send(BookFrame("A1", None, False)))
    frame = json.loads(await text(client))
    assert (datetime.now(UTC) - sent).total_seconds() < 0.5
    assert frame["asset_id"] == TOKEN_IDS["A1"]
    assert frame["hash"] == "b17e93a1f6202e13d8e0dd3aeb958b4a882dca3c"
    await client.close()


@serve
async def test_send_text_and_send_binary_send_as_given(server: ScriptedServer) -> None:
    client = await opened(server)
    await server.call(server.send_text("not json"))
    await server.call(server.send_binary(bytes.fromhex("00ff")))
    assert await text(client) == "not json"
    assert await client.recv() == b"\x00\xff"
    await client.close()


@serve
async def test_send_again_repeats_a_labelled_frame(server: ScriptedServer) -> None:
    client = await opened(server)
    entries = (Entry("A1", "BUY", "0.49", "50"), Entry("A2", "SELL", "0.51", "50"))
    await server.call(server.send(PriceChangeFrame("A", 100, entries, False), "x"))
    first = await text(client)
    await server.call(server.send_again("x"))
    assert await text(client) == first
    await server.call(server.send_again("x", reverse=True, label="y"))
    reversed_ = json.loads(await text(client))
    await server.call(server.send_again("y"))
    assert json.loads(await text(client)) == reversed_
    original = json.loads(first)
    assert reversed_["price_changes"] == original["price_changes"][::-1]
    assert list(reversed_) == list(original)
    # The reference books do not change.
    await server.call(server.send(BookFrame("A1", None, False)))
    book = json.loads(await text(client))
    assert book["bids"][-1] == {"price": "0.49", "size": "50"}
    await client.close()


@serve
async def test_send_burst_sends_one_entry_per_frame_apart(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    entries = [Entry("A1", "BUY", "0.49", "50"), Entry("A1", "BUY", "0.47", "0")]
    sending = asyncio.create_task(
        server.call(server.send_burst("A", 100, 0.3, entries))
    )
    first = json.loads(await text(client))
    second = json.loads(await text(client))
    finished = datetime.now(UTC)
    sent = await sending
    assert abs((finished - sent).total_seconds()) < 0.3
    assert [len(f["price_changes"]) for f in (first, second)] == [1, 1]
    assert first["price_changes"][0]["hash"] == second["price_changes"][0]["hash"]
    await client.close()


@serve
async def test_send_burst_waits_between_frames(server: ScriptedServer) -> None:
    client = await opened(server)
    entries = [Entry("A1", "BUY", "0.49", "50"), Entry("A1", "BUY", "0.47", "0")]
    started = asyncio.get_running_loop().time()
    await server.call(server.send_burst("A", 100, 0.3, entries))
    assert asyncio.get_running_loop().time() - started >= 0.3
    await text(client)
    await text(client)
    await client.close()


@serve
async def test_silent_and_trade_change_only_the_reference(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    await server.call(server.silent("A1", 200, Entry("A1", "BUY", "0.46", "500")))
    await server.call(server.trade("A", "0.530"))
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(client.recv(), 0.2)
    await server.call(server.send(BookFrame("A1", None, False)))
    book = json.loads(await text(client))
    assert book["bids"][0] == {"price": "0.46", "size": "500"}
    assert book["timestamp"] == str(T0 + 200)
    assert book["hash"] == server.source.hash("A1", T0 + 200)
    assert '"last_trade_price":"0.530"' in server.source.hashed_text("A1", T0)
    await client.close()


@serve
async def test_idle_close_drops_a_quiet_connection(server: ScriptedServer) -> None:
    client = await opened(server)
    started = asyncio.get_running_loop().time()
    await server.call(server.idle_close(0.4))
    with pytest.raises(ConnectionClosedError) as closed:
        await asyncio.wait_for(client.recv(), 2)
    assert closed.value.rcvd is None
    assert 0.35 < asyncio.get_running_loop().time() - started < 1


@serve
async def test_idle_close_waits_while_the_client_sends(server: ScriptedServer) -> None:
    client = await opened(server)
    await server.call(server.idle_close(0.4))
    for _ in range(4):
        await asyncio.sleep(0.2)
        await client.send("PING")
        assert await text(client) == "PONG"
    await client.close()


@serve
async def test_close_sends_a_close_frame_and_completes_the_handshake(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    await server.call(server.close(1001, "going away"))
    await client.wait_closed()
    assert client.protocol.close_rcvd == Close(1001, "going away")
    # The server's close came first, and the client answered it.
    assert client.protocol.close_rcvd_then_sent is True


@serve
async def test_close_completes_a_closing_handshake_the_client_started(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    await client.close(1000, "pong timeout")
    await server.call(server.expect_client_close(1000, "pong timeout"))
    await server.call(server.close(1000, ""))
    await client.wait_closed()
    assert client.protocol.state is State.CLOSED


@serve
async def test_drop_ends_the_connection_without_a_close_frame(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    await server.call(server.drop())
    with pytest.raises(ConnectionClosedError) as closed:
        await client.recv()
    assert closed.value.rcvd is None


@serve
async def test_expect_client_close_checks_the_code_and_reason(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    await client.close(1000, "client exit")
    with pytest.raises(StepFailed, match="close frame differs"):
        await server.call(server.expect_client_close(1000, "no subscriptions"))
    arrived = await server.call(server.expect_client_close(1000, "client exit"))
    assert (datetime.now(UTC) - arrived).total_seconds() < 1


@serve
async def test_expect_client_close_fails_on_a_dropped_connection(
    server: ScriptedServer,
) -> None:
    client = await opened(server)
    client.transport.abort()
    with pytest.raises(StepFailed, match="without a close frame"):
        await server.call(server.expect_client_close(1000, "client exit"))


@serve
async def test_expect_no_connect_fails_when_an_attempt_arrives(
    server: ScriptedServer,
) -> None:
    assert await server.call(server.expect_no_connect(0.2))
    late = asyncio.create_task(attempt(server))
    with pytest.raises(StepFailed, match="attempt arrived"):
        await server.call(server.expect_no_connect(1.0))
    await server.call(server.refuse(1))
    with pytest.raises(InvalidStatus):
        await late


@serve
async def test_a_step_on_no_open_connection_fails(server: ScriptedServer) -> None:
    with pytest.raises(StepFailed, match="no connection"):
        await server.call(server.send(Opening(())))
    client = await opened(server)
    await server.call(server.drop())
    await asyncio.sleep(0.1)
    with pytest.raises(StepFailed, match="not open"):
        await server.call(server.send(Opening(())))
    with pytest.raises(ConnectionClosedError):
        await client.recv()


@serve
async def test_the_server_keeps_reference_books_for_a_and_b(
    server: ScriptedServer,
) -> None:
    assert set(server.source.books) == {
        token for name in ("A", "B") for token in MARKETS[name].tokens
    }
