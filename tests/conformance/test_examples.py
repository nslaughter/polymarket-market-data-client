"""The examples (docs/implementation-plan.md, 11. Add the research example and
check the built wheel): the quickstart, run against the scripted server and
the scripted lookup, and the README's copy of it.

They are here, beside the harness they use, so CI runs them against the
installed wheel with the rest of the conformance suite.
"""

import ast
import asyncio
import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest

from polymarket_market_data import ClientConfig

from .frames import Entry, Opening, PriceChangeFrame, ResolvedFrame
from .lookup import ScriptedLookup
from .server import ScriptedServer
from .synthetic import TOKEN_IDS

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"


def load(name: str) -> ModuleType:
    """An example, loaded from its file, as running it would."""
    spec = importlib.util.spec_from_file_location(
        f"example_{name}", EXAMPLES / f"{name}.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# The quickstart.


def test_the_quickstart_prints_each_record_and_returns(
    capsys: pytest.CaptureFixture[str],
) -> None:
    quickstart = load("quickstart")
    lookup = ScriptedLookup()

    async def play(server: ScriptedServer) -> None:
        await server.call(server.accept())
        await server.call(server.recv_subscribe(("A1", "A2")))
        await server.call(server.send(Opening(("A1", "A2"))))
        change = (Entry("A1", "BUY", "0.49", "50"),)
        frame = PriceChangeFrame("A", 100, change, bad_hash=False)
        await server.call(server.send(frame))
        await server.call(server.send(ResolvedFrame("A", 101, "A1")))
        # No market is left, so the client closes (spec/client.md, Connection
        # states).
        await server.call(server.expect_client_close(1000, "no subscriptions"))

    async def run(server: ScriptedServer) -> None:
        # The URL alone would leave the default lookup, which calls the live
        # service, to resolve the slug and fetch the hash inputs (D4; D6).
        config = ClientConfig(url=server.url)
        async with asyncio.timeout(10), asyncio.TaskGroup() as tasks:
            tasks.create_task(play(server))
            await quickstart.main("synthetic-a", config, lookup)

    with ScriptedServer() as server:
        asyncio.run(run(server))
        assert asyncio.run(server.call(server.finish())) == []
    assert lookup.violations == []
    a1, a2 = TOKEN_IDS["A1"], TOKEN_IDS["A2"]
    assert capsys.readouterr().out.splitlines() == [
        "connection connecting",
        "connection open",
        "connection subscribed",
        f"token {a1} synchronizing: subscribed",
        f"token {a2} synchronizing: subscribed",
        f"book {a1}: 2 bid and 2 ask levels",
        f"token {a1} ready: its book can be used",
        f"book {a2}: 2 bid and 2 ask levels",
        f"token {a2} ready: its book can be used",
        "price change: BUY 0.49 x 50",
        "MarketResolvedEvent",
        f"token {a1} settled, winner {a1}",
        f"token {a2} settled, winner {a1}",
        "idle: no unsettled market is left",
    ]


def test_the_readme_holds_the_quickstart_program() -> None:
    source = (EXAMPLES / "quickstart.py").read_text()
    docstring = ast.parse(source).body[0]
    assert isinstance(docstring, ast.Expr)
    assert docstring.end_lineno is not None
    program = "".join(source.splitlines(keepends=True)[docstring.end_lineno :])
    blocks = re.findall(r"```python\n(.*?)```", (ROOT / "README.md").read_text(), re.S)
    assert program.lstrip("\n") in blocks
