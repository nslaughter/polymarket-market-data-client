"""The examples (docs/implementation-plan.md, 11. Add the research example and
check the built wheel): the quickstart, run against the scripted server and
the scripted lookup, and the README's copy of it; the research example's
controlled mode and its timeline; and the live mode's choice of markets,
with the SDK's HTTP replaced by synthetic responses, never the live service.

They are here, beside the harness they use, so CI runs them against the
installed wheel with the rest of the conformance suite.
"""

import ast
import asyncio
import importlib.util
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from pydantic import TypeAdapter

import polymarket_market_data
from polymarket_market_data import ClientConfig

from .frames import Entry, Opening, PriceChangeFrame, ResolvedFrame
from .lookup import ScriptedLookup
from .server import ScriptedServer
from .synthetic import NAMES, T0, TOKEN_IDS

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


# The research example.


def summary(line: dict[str, Any]) -> tuple[object, ...]:
    """A timeline line in short: its type and what the scenario turns on,
    with synthetic IDs named as in spec/conformance.md."""
    record = line["record"]
    match line["type"]:
        case "ConnectionStateChange":
            return ("conn", record["state"], record["connection"])
        case "TokenStateChange":
            return (
                "token",
                NAMES[record["token_id"]],
                record["state"],
                record["reason"],
            )
        case "BookEvent":
            matched = record["held_book_matched"]
            return ("book", NAMES[record["asset_id"]], record["connection"], matched)
        case "CaptureGap":
            return ("gap", NAMES[record["token_id"]], record["cause"], record["end"])
        case "PriceChangeEvent":
            return (
                "change",
                NAMES[record["market"]],
                record["source_timestamp_ms"] - T0,
            )
        case "MarketResolvedEvent":
            winner = NAMES[record["winning_asset_id"]]
            return ("resolved", NAMES[record["market"]], winner)
    return (line["type"],)


def connected(n: int, matched: dict[str, bool | None]) -> list[tuple[object, ...]]:
    """Connection ``n`` opening on A and B: each book, its gap after a
    reconnect, and the token's return to ``ready`` (spec/conformance.md, The
    start macro and drop-without-close)."""
    lines: list[tuple[object, ...]] = [("conn", "open", n), ("conn", "subscribed", n)]
    lines += [("token", t, "synchronizing", "subscribed") for t in matched]
    for token, held in matched.items():
        lines.append(("book", token, n, held))
        if n > 1:
            lines.append(("gap", token, "dropped", "book"))
        lines.append(("token", token, "ready", "book"))
    return lines


@pytest.fixture(scope="module")
def timeline(tmp_path_factory: pytest.TempPathFactory) -> list[str]:
    """The lines the controlled mode writes."""
    output = tmp_path_factory.mktemp("research") / "timeline.jsonl"
    assert load("research").main(["--controlled", "--output", str(output)]) == 0
    return output.read_text().splitlines()


def test_the_research_example_records_a_drop_and_two_settlements(
    timeline: list[str],
) -> None:
    run, *records, stats = (json.loads(line) for line in timeline)
    assert (run["type"], run["mode"]) == ("run", "controlled")
    assert [market["slug"] for market in run["markets"]] == [
        "synthetic-a",
        "synthetic-b",
    ]
    assert run["config"]["verify_hash"] is True
    tokens = ("A1", "A2", "B1", "B2")
    assert [summary(line) for line in records] == [
        ("conn", "connecting", None),
        *connected(1, dict.fromkeys(tokens)),
        ("change", "A", 100),
        ("conn", "interrupted", 1),
        *[("token", token, "uncertain", "interrupted") for token in tokens],
        ("conn", "recovering", None),
        ("conn", "connecting", None),
        # Only A1 changed during the gap.
        *connected(2, {"A1": False, "A2": True, "B1": True, "B2": True}),
        ("change", "A", 3000),
        ("resolved", "A", "A1"),
        ("token", "A1", "settled", "market_resolved"),
        ("token", "A2", "settled", "market_resolved"),
        ("change", "B", 3100),
        ("change", "B", 4000),
        ("resolved", "B", "B2"),
        ("token", "B1", "settled", "market_resolved"),
        ("token", "B2", "settled", "market_resolved"),
        ("conn", "idle", 2),
    ]
    assert stats["type"] == "stats"
    counts = stats["record"]
    assert (counts["connections"], counts["interruptions"]) == (2, {"dropped": 1})
    assert (counts["hash_verified"], counts["hash_failed"]) == (10, 0)


def test_each_line_of_the_timeline_reads_back_as_its_record(
    timeline: list[str],
) -> None:
    # Through TypeAdapter, as D8 specifies.
    for text in timeline[1:]:
        line = json.loads(text)
        kind = "ClientStats" if line["type"] == "stats" else line["type"]
        adapter: TypeAdapter[Any] = TypeAdapter(getattr(polymarket_market_data, kind))
        record = json.dumps(line["record"], separators=(",", ":")).encode()
        assert adapter.dump_json(adapter.validate_json(record)) == record


def test_the_controlled_mode_exits_2_if_no_market_resolves(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    research = load("research")
    # The scripted lookup finds no synthetic-u (spec/conformance.md, The
    # scripted lookup).
    monkeypatch.setattr(research, "CONTROLLED_MARKETS", ("synthetic-u",))
    output = tmp_path / "timeline.jsonl"
    assert research.main(["--controlled", "--output", str(output)]) == 2
    err = capsys.readouterr().err.splitlines()
    assert err[0].startswith("skipped synthetic-u: ")
    assert err[1:] == ["none of the markets chosen could be resolved"]
    assert output.read_text() == ""


def test_the_controlled_mode_exits_1_if_recovery_fails(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    research = load("research")

    async def fails(*args: object, **kwargs: object) -> None:
        raise polymarket_market_data.RecoveryFailed("its bounds are exhausted")

    monkeypatch.setattr(research, "research", fails)
    output = tmp_path / "timeline.jsonl"
    assert research.main(["--controlled", "--output", str(output)]) == 1
    assert capsys.readouterr().err == "recovery failed: its bounds are exhausted\n"


def test_a_market_lookup_does_not_find_is_skipped(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    research = load("research")

    async def run(server: ScriptedServer) -> None:
        config = ClientConfig(url=server.url, verify_hash=False)
        with (tmp_path / "timeline.jsonl").open("w") as out:
            # The scripted lookup finds no synthetic-u (spec/conformance.md,
            # The scripted lookup).
            await research.research(
                ["synthetic-u"],
                config,
                ScriptedLookup(),
                research.Timeline(out),
                mode="controlled",
                duration=1.0,
            )

    with ScriptedServer() as server, pytest.raises(research.CannotRun):
        asyncio.run(run(server))
    assert "skipped synthetic-u" in capsys.readouterr().err
    assert (tmp_path / "timeline.jsonl").read_text() == ""


# The live mode's choice of markets.


def gamma_market(slug: str, n: int, *, accepting: bool = True) -> dict[str, object]:
    """A synthetic market, shaped as the pinned SDK reads a listed one."""
    return {
        "id": str(9300000 + n),
        "question": f"Synthetic market {slug}?",
        "conditionId": "0x" + f"{n:x}".rjust(64, "0"),
        "slug": slug,
        "endDate": "2026-12-31T00:00:00Z",
        "outcomes": json.dumps(["Yes", "No"]),
        "active": True,
        "closed": False,
        "acceptingOrders": accepting,
        "clobTokenIds": json.dumps([f"9{n}1", f"9{n}2"]),
    }


def test_the_live_mode_lists_markets_with_the_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("polymarket", reason="the sdk extra is not installed")
    httpx = pytest.importorskip("httpx", reason="the sdk extra is not installed")
    research = load("research")
    requests: list[tuple[str, dict[str, str]]] = []
    pages = {
        "volume24hr": [
            gamma_market("busy-closed-to-orders", 1, accepting=False),
            gamma_market("busy-1", 2),
            gamma_market("busy-2", 3),
            gamma_market("busy-3", 4),
        ],
        "endDate": [
            gamma_market("ending-other", 5),
            gamma_market("btc-updown-5m-1", 6),
            gamma_market("btc-updown-5m-2", 7),
            gamma_market("btc-updown-5m-3", 8),
        ],
    }

    def respond(request: Any) -> Any:
        query = dict(request.url.params)
        requests.append((request.url.path, query))
        return httpx.Response(
            200, json={"markets": pages[query["order"]], "next_cursor": None}
        )

    real = httpx.AsyncClient

    class Replaced(real):  # type: ignore[misc, valid-type]
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs, transport=httpx.MockTransport(respond))

    monkeypatch.setattr(httpx, "AsyncClient", Replaced)
    now = datetime(2026, 11, 1, 12, tzinfo=UTC)
    chosen = asyncio.run(
        research.choose(
            long=2, min_days=30, settling=2, prefix="btc-updown-5m-", now=now
        )
    )
    assert chosen == ["busy-1", "busy-2", "btc-updown-5m-1", "btc-updown-5m-2"]
    assert requests == [
        (
            "/markets/keyset",
            {
                "ascending": "false",
                "closed": "false",
                "end_date_min": "2026-12-01T12:00:00+00:00",
                "order": "volume24hr",
                "limit": "100",
            },
        ),
        (
            "/markets/keyset",
            {
                "ascending": "true",
                "closed": "false",
                "end_date_max": "2026-11-01T13:00:00+00:00",
                "end_date_min": "2026-11-01T12:00:00+00:00",
                "order": "endDate",
                "limit": "100",
            },
        ),
    ]
