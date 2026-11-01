"""The research example: it chooses active markets when it runs, so it keeps
working as markets settle, subscribes to them, and writes every record as
one JSON line, an inspectable recovery timeline
(docs/implementation-plan.md, 11. Add the research example and check the
built wheel).

    python examples/research.py --controlled
    python examples/research.py [--duration SECONDS] [--output PATH]

The live mode contacts Polymarket, whose Terms of Use govern its data, and
needs the sdk extra. A market lookup finds a market only by its slug
(spec/client.md, Market lookup), so the live mode lists open markets with
the pinned SDK's ``list_markets``, as the investigation's
``spikes/select_markets.py`` did, and resolves each through the client's
lookup. By default it chooses the two open markets ending at least 30 days
from now with the most 24-hour volume, and the two ``btc-updown-5m-``
markets ending soonest, which resolve on their own a few minutes after they
end (docs/source-behavior.md, Markets). The owner decided both on
2026-11-01. It runs until no chosen market is left unsettled or
``--duration`` has passed.

The controlled mode runs the same code against the scripted server and the
scripted lookup, in a built-in scenario with a dropped connection and two
settlements. They are test code in ``tests/conformance``, which the wheel
leaves out, so it needs a checkout of this repository, but no access to the
live service.

The timeline's first line is ``{"type": "run", ...}``: the mode, the
client's version, its configuration, and the markets. Each record's line is
``{"type": <record type>, "record": ...}``, with the record as
``TypeAdapter(<record type>).dump_json`` writes it (D8). The last line,
``{"type": "stats", "record": ...}``, holds the client's statistics at the
end.
"""

import argparse
import asyncio
import importlib.metadata
import importlib.util
import json
import sys
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from pydantic import TypeAdapter

from polymarket_market_data import (
    ClientConfig,
    ClientStats,
    ConnectionState,
    ConnectionStateChange,
    LookupFailed,
    Market,
    MarketDataClient,
    MarketLookup,
    MarketNotFound,
    PriceChangeEvent,
    ReconnectPolicy,
    RecoveryFailed,
)

if TYPE_CHECKING:
    from conformance.server import ScriptedServer
    from polymarket.models.gamma.market import Market as ListedMarket

LONG = 2
MIN_DAYS = 30
SETTLING = 2
SETTLING_PREFIX = "btc-updown-5m-"
SETTLING_WITHIN = timedelta(hours=1)
"""How far ahead the live mode looks for settling markets, as the
investigation's selection did."""
DURATION = 900.0
"""Seconds the live mode runs for unless every market settles first."""

CONTROLLED_MARKETS = ("synthetic-a", "synthetic-b")
CONTROLLED_LIMIT = 30.0
"""Seconds within which the built-in scenario must finish."""


class CannotRun(Exception):
    """The example cannot run: what its mode needs is missing, or no market
    it chose could be resolved."""


class Timeline:
    """Writes the timeline's lines to ``out``, flushing each one, so that the
    timeline can be read while the example runs."""

    def __init__(self, out: TextIO) -> None:
        self._out = out
        self._adapters: dict[type[Any], TypeAdapter[Any]] = {}

    def run(self, mode: str, config: ClientConfig, markets: Sequence[Market]) -> None:
        line = {
            "type": "run",
            "mode": mode,
            "version": importlib.metadata.version("polymarket-market-data-client"),
            "config": config.model_dump(mode="json"),
            "markets": TypeAdapter(list[Market]).dump_python(
                list(markets), mode="json"
            ),
        }
        self._write(json.dumps(line, separators=(",", ":")))

    def record(self, record: object) -> None:
        self._line(type(record).__name__, record)

    def stats(self, stats: ClientStats) -> None:
        self._line("stats", stats)

    def _line(self, kind: str, value: object) -> None:
        adapter = self._adapters.get(type(value))
        if adapter is None:
            adapter = self._adapters[type(value)] = TypeAdapter(type(value))
        record = adapter.dump_json(value).decode()
        self._write(f'{{"type":"{kind}","record":{record}}}')

    def _write(self, line: str) -> None:
        self._out.write(line + "\n")
        self._out.flush()


async def research(
    slugs: Sequence[str],
    config: ClientConfig,
    lookup: MarketLookup | None,
    timeline: Timeline,
    *,
    mode: str,
    duration: float | None,
) -> None:
    """Resolve each slug, subscribe to the markets found, and write every
    record until the client reports ``idle``, once no market is left
    unsettled (spec/client.md, Connection states), or until ``duration``
    seconds have passed. ``RecoveryFailed`` comes after the records that
    report the failure, and is left to the caller."""
    async with MarketDataClient(config, lookup=lookup) as client:
        markets = []
        for slug in dict.fromkeys(slugs):
            try:
                markets.append(await client.resolve(slug))
            except (MarketNotFound, LookupFailed) as error:
                print(f"skipped {slug}: {error}", file=sys.stderr)
        if not markets:
            raise CannotRun("none of the markets chosen could be resolved")
        timeline.run(mode, config, markets)
        client.subscribe(*markets)
        limit = asyncio.timeout(duration)
        try:
            async with limit:
                async for record in client.records():
                    timeline.record(record)
                    if (
                        isinstance(record, ConnectionStateChange)
                        and record.state is ConnectionState.IDLE
                    ):
                        break
        except TimeoutError:
            if not limit.expired():
                raise
        finally:
            timeline.stats(client.stats())


# The live mode.


async def choose(
    *, long: int, min_days: int, settling: int, prefix: str, now: datetime
) -> list[str]:
    """The slugs of the markets to watch, listed as the investigation's
    selection listed them: open markets accepting orders that end at least
    ``min_days`` after ``now``, by 24-hour volume, and then open markets
    whose slug starts with ``prefix`` that end within the next hour,
    soonest first."""
    from polymarket import AsyncPublicClient

    async with AsyncPublicClient() as sdk:
        busiest = sdk.list_markets(
            closed=False,
            end_date_min=now + timedelta(days=min_days),
            order="volume24hr",
            ascending=False,
            page_size=100,
        )
        soonest = sdk.list_markets(
            closed=False,
            end_date_min=now,
            end_date_max=now + SETTLING_WITHIN,
            order="endDate",
            ascending=True,
            page_size=100,
        )
        chosen = await _take(busiest.iter_items(), long, _accepting_orders)
        chosen += await _take(soonest.iter_items(), settling, _starts_with(prefix))
    return chosen


async def _take(
    markets: "AsyncIterator[ListedMarket]",
    count: int,
    keep: "Callable[[ListedMarket], bool]",
) -> list[str]:
    """The slugs of the first ``count`` markets with a slug that ``keep``
    keeps."""
    slugs: list[str] = []
    if count < 1:
        return slugs
    async for market in markets:
        if market.slug and keep(market):
            slugs.append(market.slug)
            if len(slugs) == count:
                break
    return slugs


def _accepting_orders(market: "ListedMarket") -> bool:
    return market.state.accepting_orders is True


def _starts_with(prefix: str) -> "Callable[[ListedMarket], bool]":
    return lambda market: (market.slug or "").startswith(prefix)


async def live(args: argparse.Namespace, out: TextIO) -> None:
    if importlib.util.find_spec("polymarket") is None:
        raise CannotRun("the live mode needs the sdk extra, which lists markets")
    slugs = await choose(
        long=args.long,
        min_days=args.min_days,
        settling=args.settling,
        prefix=args.settling_prefix,
        now=datetime.now(UTC),
    )
    if not slugs:
        raise CannotRun("the SDK listed no market to choose")
    # The default configuration and lookup: the live endpoint, and the lookup
    # the sdk extra provides.
    await research(
        slugs, ClientConfig(), None, Timeline(out), mode="live", duration=args.duration
    )


# The controlled mode.


class ScenarioTimeline(Timeline):
    """A timeline the built-in scenario can wait on: a dropped connection
    loses the frames the client has not read, so the server's side waits for
    the client's records before it drops one."""

    def __init__(self, out: TextIO) -> None:
        super().__init__(out)
        self._records: list[object] = []
        self._changed = asyncio.Event()

    def record(self, record: object) -> None:
        super().record(record)
        self._records.append(record)
        self._changed.set()
        self._changed = asyncio.Event()

    async def until(self, written: Callable[[object], bool]) -> None:
        while not any(written(record) for record in self._records):
            await self._changed.wait()


def _harness() -> None:
    """Make the conformance harness importable from this checkout."""
    tests = Path(__file__).resolve().parents[1] / "tests"
    if not (tests / "conformance").is_dir():
        raise CannotRun(
            "the controlled mode needs a checkout of the repository, whose "
            "tests/conformance holds the scripted server and lookup"
        )
    if "conformance" not in sys.modules:
        sys.path.insert(0, str(tests))


async def controlled(out: TextIO) -> None:
    """Run the built-in scenario: the scripted server plays it while the
    example runs against the server and the scripted lookup, with the
    conformance profile's short timings and hash checks on (spec/conformance.md,
    The scripted server, The scripted lookup, and Profile). It fails if the
    scenario does not finish within ``CONTROLLED_LIMIT`` seconds, or if the
    client did anything the server or the lookup records as a violation."""
    _harness()
    from conformance.lookup import ScriptedLookup
    from conformance.server import ScriptedServer
    from conformance.synthetic import PROFILE, PROFILE_RECONNECT

    timeline = ScenarioTimeline(out)
    lookup = ScriptedLookup()
    with ScriptedServer() as server:
        config = ClientConfig(
            url=server.url,
            reconnect=ReconnectPolicy(**PROFILE_RECONNECT),
            **{**PROFILE, "verify_hash": True},
        )
        try:
            async with (
                asyncio.timeout(CONTROLLED_LIMIT),
                asyncio.TaskGroup() as tasks,
            ):
                tasks.create_task(_play(server, timeline))
                await research(
                    CONTROLLED_MARKETS,
                    config,
                    lookup,
                    timeline,
                    mode="controlled",
                    duration=None,
                )
        except BaseExceptionGroup as group:
            # The task group wraps what research raised, so main would not
            # see the two it reports as the exit status.
            match group.exceptions:
                case [CannotRun() | RecoveryFailed() as error]:
                    raise error from None
            raise
        violations = [*await server.call(server.finish()), *lookup.violations]
    if violations:
        raise RuntimeError("the built-in scenario failed: " + "; ".join(violations))


async def _play(server: "ScriptedServer", timeline: ScenarioTimeline) -> None:
    """The server's side of the built-in scenario, on markets ``A`` and
    ``B``. The connection drops without a close frame, and a change during
    the gap shows only in the next book (spec/conformance.md,
    drop-without-close). Then each market's book empties and
    ``market_resolved`` names its winner, and the server closes once no
    market is left (spec/conformance.md, settle-announced-others-open and
    settle-all-resolved-close)."""
    from conformance.frames import Entry, Opening, PriceChangeFrame, ResolvedFrame
    from conformance.synthetic import T0

    tokens = ("A1", "A2", "B1", "B2")

    def change(market: str, t: int, *entries: str) -> PriceChangeFrame:
        """A ``price_change`` of entries written ``<T>:<side>:<price>:<size>``."""
        parsed = tuple(Entry(*entry.split(":")) for entry in entries)
        return PriceChangeFrame(market, t, parsed, bad_hash=False)

    def emptied(token: str, bids: Sequence[str], asks: Sequence[str]) -> list[str]:
        return [f"{token}:BUY:{price}:0" for price in bids] + [
            f"{token}:SELL:{price}:0" for price in asks
        ]

    await server.call(server.accept())
    await server.call(server.recv_subscribe(tokens))
    await server.call(server.send(Opening(tokens)))
    await server.call(server.send(change("A", 100, "A1:BUY:0.49:50")))
    await timeline.until(
        lambda record: (
            isinstance(record, PriceChangeEvent)
            and record.source_timestamp_ms == T0 + 100
        )
    )
    await server.call(server.drop())
    await server.call(server.silent("A1", 2000, Entry("A1", "BUY", "0.46", "500")))

    await server.call(server.accept())
    await server.call(server.recv_subscribe(tokens))
    await server.call(server.send(Opening(tokens)))
    a1 = emptied("A1", ("0.49", "0.48", "0.47", "0.46"), ("0.52", "0.53"))
    a2 = emptied("A2", ("0.48", "0.47"), ("0.52", "0.53"))
    await server.call(server.send(change("A", 3000, *a1, *a2)))
    await server.call(server.send(ResolvedFrame("A", 3001, "A1")))

    await server.call(server.send(change("B", 3100, "B1:BUY:0.39:90")))
    b1 = emptied("B1", ("0.39", "0.38"), ("0.41", "0.42"))
    b2 = emptied("B2", ("0.59", "0.58"), ("0.61", "0.62"))
    await server.call(server.send(change("B", 4000, *b1, *b2)))
    await server.call(server.send(ResolvedFrame("B", 4001, "B2")))
    await server.call(server.close(1000, "all subscribed assets resolved"))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write a timeline of every record for a few active markets."
    )
    parser.add_argument(
        "--controlled",
        action="store_true",
        help="run the built-in scenario against the scripted server instead",
    )
    parser.add_argument(
        "--output", type=Path, help="write the timeline here, not to standard output"
    )
    options = parser.add_argument_group("live mode")
    options.add_argument(
        "--duration",
        type=float,
        default=DURATION,
        help="seconds to run for (default: %(default)s)",
    )
    options.add_argument(
        "--long",
        type=int,
        default=LONG,
        help="markets to choose by 24-hour volume (default: %(default)s)",
    )
    options.add_argument(
        "--min-days",
        type=int,
        default=MIN_DAYS,
        help="days from now they must end after (default: %(default)s)",
    )
    options.add_argument(
        "--settling",
        type=int,
        default=SETTLING,
        help="markets to choose by soonest end (default: %(default)s)",
    )
    options.add_argument(
        "--settling-prefix",
        default=SETTLING_PREFIX,
        help="the slug those markets start with (default: %(default)s)",
    )
    args = parser.parse_args(argv)
    destination = (
        open(args.output, "w", encoding="utf-8")  # noqa: SIM115
        if args.output
        else nullcontext(sys.stdout)
    )
    with destination as out:
        try:
            asyncio.run(controlled(out) if args.controlled else live(args, out))
        except CannotRun as error:
            print(error, file=sys.stderr)
            return 2
        except RecoveryFailed as error:
            print(f"recovery failed: {error}", file=sys.stderr)
            return 1
        except KeyboardInterrupt:
            return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
