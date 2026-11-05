"""The hash-check replay (docs/implementation-plan.md, 13. Investigate the
live run's failed hash checks; D4), run on a synthetic timeline.

The timeline is the one the research example would write for a client fed
these frames: the client's own state machine and verifier take them, with a
clock the test sets, and the example's ``Timeline`` writes the records and
statistics. The frames come from the harness's ``Source``, with hashes by its
own implementation of the recipe (spec/conformance.md, Frame notation), and
plant each kind of failed check the replay looks for. The test is here,
beside the harness it uses.
"""

import importlib.util
import io
import json
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from polymarket_market_data import BookParameters, ClientConfig, ClientStats
from polymarket_market_data._decode import decode_frame
from polymarket_market_data._hash import Verifier
from polymarket_market_data._state import StateMachine

from .frames import (
    BAD_HASH,
    BookFrame,
    BookSpec,
    Entry,
    Frame,
    Opening,
    PriceChangeFrame,
    ResolvedFrame,
    Source,
    TickSizeFrame,
    dumps,
)
from .synthetic import MARKETS, MIN_ORDER_SIZE, NEG_RISK, T0, TOKEN_IDS, TOKEN_MARKET

ROOT = Path(__file__).resolve().parents[2]
START = T0 / 1000
"""The run's clock, in seconds since the epoch, at the frames' T0, so that
the event loop's clock and ``received_at`` read alike."""

A1, A2, B1, B2 = (TOKEN_IDS[name] for name in ("A1", "A2", "B1", "B2"))


def load(path: Path, name: str) -> ModuleType:
    """A script, loaded from its file, as running it would."""
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def replay() -> ModuleType:
    return load(ROOT / "tools" / "replay_hash_checks.py", "replay_hash_checks")


@pytest.fixture(scope="module")
def research() -> ModuleType:
    return load(ROOT / "examples" / "research.py", "example_research")


class Client:
    """The client's state machine and verifier, fed frames at set times on
    one connection, with its burst timer firing as the connection fires it
    (D4), and its records written to a research timeline."""

    def __init__(self, research: ModuleType, out: io.StringIO) -> None:
        self.config = ClientConfig()
        self.verifier = Verifier(
            hash_grace=self.config.hash_grace, burst_quiet=self.config.burst_quiet
        )
        self.markets = [MARKETS[name].market() for name in ("A", "B")]
        self.machine = StateMachine(
            repeat_window=self.config.repeat_window,
            deliver_new_market=False,
            can_look_up=True,
            markets=self.markets,
            verifier=self.verifier,
        )
        self.source = Source()
        self.timeline = research.Timeline(out)
        self.timeline.run("controlled", self.config, self.markets)
        self.frames = 0
        at = self.at(0)
        self.machine.connecting(1, at)
        self.machine.opened(1, at)
        self.machine.subscribed(self.machine.subscription(), at)
        for token_id in self.machine.subscription():
            self.machine.parameters(
                token_id, BookParameters(Decimal(MIN_ORDER_SIZE), NEG_RISK)
            )
        self.write()

    @staticmethod
    def at(seconds: float) -> datetime:
        return datetime.fromtimestamp(START + seconds, UTC)

    def until(self, seconds: float) -> None:
        """Let the burst timer fire for every deadline before ``seconds``."""
        machine = self.machine
        while (deadline := machine.burst_deadline()) is not None and (
            deadline <= START + seconds
        ):
            machine.bursts_quiet(deadline, datetime.fromtimestamp(deadline, UTC))
            self.write()

    def send(self, seconds: float, text: str) -> None:
        """A frame arrives ``seconds`` into the run."""
        self.until(seconds)
        self.frames += 1
        received_at = self.at(seconds)
        items = decode_frame(
            text,
            received_at=received_at,
            connection=1,
            frame=self.frames,
            keep_raw=False,
        )
        self.machine.frame(1, items, received_at=received_at, clock=START + seconds)
        self.write()

    def frame(self, seconds: float, frame: Frame) -> str:
        """Send a frame of the notation, and return its text."""
        text = self.source.frame(frame)
        self.send(seconds, text)
        return text

    def change(self, seconds: float, t: int, entry: str, *, bad: bool = False) -> str:
        """Send a ``price_change`` of one entry, ``<T>:<side>:<price>:<size>``."""
        token, side, price, size = entry.split(":")
        market = TOKEN_MARKET[token]
        frame = PriceChangeFrame(market, t, (Entry(token, side, price, size),), bad)
        return self.frame(seconds, frame)

    def silent(self, t: int, entry: str) -> None:
        """Change the source's book without sending the change."""
        token, side, price, size = entry.split(":")
        self.source.silent(token, t, Entry(token, side, price, size))

    def write(self) -> None:
        for record in self.machine.take():
            self.timeline.record(record)

    def stats(self) -> None:
        counts, checks = self.machine.counts, self.verifier.counts
        self.timeline.stats(
            ClientStats(
                frames=counts.frames,
                frames_after_interruption=counts.frames_after_interruption,
                events=dict(counts.events),
                repeats=counts.repeats,
                unknown=counts.unknown,
                undecodable=dict(counts.undecodable),
                new_market_dropped=counts.new_market_dropped,
                discarded_outside=counts.discarded_outside,
                pongs=0,
                pongs_unsolicited=0,
                pong_delay_last=0.0,
                pong_delay_max=0.0,
                connections=counts.connections,
                interruptions=dict(counts.interruptions),
                lookups=0,
                lookup_failures=0,
                rest_book_not_found=0,
                hash_verified=checks.verified,
                hash_retried=checks.retried,
                hash_failed=checks.failed,
            )
        )


def stamped(token: str, t: int, entry: str, hash: str) -> str:
    """A ``price_change`` of one entry with the hash given, which the
    notation cannot write."""
    _, side, price, size = entry.split(":")
    return dumps(
        {
            "market": MARKETS[TOKEN_MARKET[token]].condition_id,
            "price_changes": [
                {
                    "asset_id": TOKEN_IDS[token],
                    "price": price,
                    "size": size,
                    "side": side,
                    "hash": hash,
                    "best_bid": "0",
                    "best_ask": "1",
                }
            ],
            "timestamp": str(T0 + t),
            "event_type": "price_change",
        }
    )


def scenario(client: Client) -> None:
    """Each kind of failed check, on markets ``A`` and ``B``; ``t`` is in
    milliseconds and the arrival in seconds, both from T0."""
    source = client.source
    client.frame(0.5, Opening(("A1", "A2", "B1", "B2")))

    # A hash that verifies with nothing: one failed check, then one that
    # verifies, at burst_quiet.
    client.change(1.0, 1000, "A1:BUY:0.49:50")
    client.change(1.5, 1500, "A1:BUY:0.49:60", bad=True)
    client.change(1.75, 1750, "A1:SELL:0.52:100")

    # Two failed checks of B1, checked at burst_quiet, 2.5 s apart: D4's
    # divergence (T7). A burst that verifies restores the token (T9).
    client.change(3.0, 3000, "B1:BUY:0.39:70", bad=True)
    client.change(5.5, 5500, "B1:BUY:0.39:75", bad=True)
    client.change(7.0, 7000, "B1:BUY:0.38:210")

    # A trade the hash takes before the stream announces it: retried.
    source.trade("A", "0.510")
    client.change(8.5, 8500, "A2:BUY:0.48:130")

    # A tick-size change while B2's burst is pending: the burst's hash has
    # the old tick size, and the client checks it with the new one.
    client.change(10.0, 10000, "B2:SELL:0.61:85")
    client.frame(10.25, TickSizeFrame("B2", 10250, "0.001"))
    client.change(10.5, 10500, "B2:SELL:0.62:210")

    # A burst across two events, its hash written with the first's timestamp.
    source.silent("A1", 12000, Entry("A1", "BUY", "0.47", "260"))
    source.silent("A1", 12001, Entry("A1", "BUY", "0.46", "40"))
    shared = source.hash("A1", T0 + 12000)
    client.send(12.0, stamped("A1", 12000, "A1:BUY:0.47:260", shared))
    client.send(12.25, stamped("A1", 12001, "A1:BUY:0.46:40", shared))
    client.change(12.5, 12500, "A1:SELL:0.53:310")

    # A hash that already includes A2's next change, which arrives just after.
    client.silent(14000, "A2:SELL:0.53:260")
    client.change(14.0, 14000, "A2:SELL:0.52:110")
    client.change(14.0625, 14001, "A2:SELL:0.53:260")

    # A message sent twice, with a change to its level between: the client
    # takes the second copy for a repeat, though the source's book took it.
    first = client.change(16.0, 16000, "A2:BUY:0.47:310")
    client.change(16.0625, 16000, "A2:BUY:0.47:300")
    client.send(16.125, first)
    client.silent(16000, "A2:BUY:0.47:310")
    client.change(16.5, 16500, "A2:SELL:0.52:105")
    client.change(18.0, 18000, "A2:BUY:0.47:320")

    # A change the stream never sends, which B2's next book shows.
    client.silent(19750, "B2:BUY:0.57:40")
    client.change(20.0, 20000, "B2:BUY:0.58:160")
    client.frame(20.5, BookFrame("B2", None, bad_hash=False))

    # A book stamped before the held book's last change replaces it (T5),
    # though the source's later hashes still hold that change.
    client.change(22.0, 22000, "B1:SELL:0.41:95")
    book = source.books["B1"]
    bids = tuple(book.bids[price] for price in sorted(book.bids))
    asks = tuple(book.asks[price] for price in sorted(book.asks))
    older = tuple((price, "90" if price == "0.41" else size) for price, size in asks)
    client.frame(22.25, BookFrame("B1", BookSpec("B1", 21999, bids, older), False))
    book.replace(bids, asks, T0 + 22000)
    client.change(22.5, 22500, "B1:BUY:0.37:30")
    client.change(24.0, 24000, "B1:SELL:0.41:96")

    # B settles; A's last burst is pending when the timeline ends.
    client.frame(26.0, ResolvedFrame("B", 26000, "B1"))
    client.change(27.0, 27000, "A1:BUY:0.45:20")
    client.until(29.0)


@pytest.fixture(scope="module")
def timeline(tmp_path_factory: pytest.TempPathFactory, research: ModuleType) -> Path:
    out = io.StringIO()
    client = Client(research, out)
    scenario(client)
    client.stats()
    path = tmp_path_factory.mktemp("timeline") / "timeline.jsonl"
    path.write_text(out.getvalue())
    return path


def records(path: Path) -> Iterator[dict[str, Any]]:
    for line in path.read_text().splitlines():
        yield json.loads(line)


def test_the_synthetic_timeline_plants_what_it_says(timeline: Path) -> None:
    """The scenario's own claims about the client: one divergence and its
    restoration, one repeat, and two books that differed from the held
    book."""
    lines = list(records(timeline))
    changes = [
        line["record"]
        for line in lines
        if line["type"] == "TokenStateChange" and line["record"]["token_id"] == B1
    ]
    reasons = [(change["state"], change["reason"]) for change in changes]
    assert ("uncertain", "hash_mismatch") in reasons
    assert ("ready", "hash_verified") in reasons
    books = [
        line["record"]
        for line in lines
        if line["type"] == "BookEvent" and line["record"]["held_book_matched"] is False
    ]
    assert len(books) == 2
    stats = lines[-1]["record"]
    assert stats["repeats"] == 1
    assert stats["hash_failed"] == 9


def test_the_replay_reproduces_the_clients_counts(
    replay: ModuleType, timeline: Path
) -> None:
    result, inferred, stats = replay.replay_timeline(timeline)
    counts = result.verifier.counts
    assert stats is not None
    assert (counts.verified, counts.retried, counts.failed) == (
        stats["hash_verified"],
        stats["hash_retried"],
        stats["hash_failed"],
    )
    assert counts.retried > 0
    assert result.diverged == 1
    assert result.end_checks == 1
    assert all(found.parameters is not None for found in inferred.values())


def test_the_replay_puts_each_failed_check_down_to_its_cause(
    replay: ModuleType, timeline: Path
) -> None:
    result, _, _ = replay.replay_timeline(timeline)
    found = [
        (failure.token_id, failure.kind, round(failure.at - START, 4), failure.cause)
        for failure in result.failures
    ]
    assert found == [
        (A1, "entry", 1.75, "none"),
        (B1, "quiet", 4.0, "none"),
        (B1, "quiet", 6.5, "none"),
        (B2, "entry", 10.5, "tick_size"),
        (A1, "entry", 12.5, "timestamp"),
        (A2, "entry", 14.0625, "ahead"),
        (A2, "quiet", 17.5, "repeat"),
        (B2, "book_after", 20.5, "differing_book"),
        (B1, "quiet", 23.5, "stale_book"),
    ]
    ahead = result.failures[5]
    assert (ahead.ahead, ahead.ahead_after) == (1, 0.0)
    assert [(s.token_ids, round(s.at - START, 4)) for s in result.skipped] == [
        ((A2,), 16.125)
    ]
    assert [
        (b.token_id, b.after_held, b.levels, b.stale_levels) for b in result.differing
    ] == [(B2, 0, 1, 0), (B1, -1, 1, 1)]


def test_the_replay_counts_episodes_as_d4_does(
    replay: ModuleType, timeline: Path
) -> None:
    result, _, _ = replay.replay_timeline(timeline)
    episodes = Counter(
        (episode.token_id, len(episode.failures), episode.ended_by)
        for episode in result.episodes
    )
    assert episodes == Counter(
        {
            (A1, 1, "quiet_verified"): 2,
            (B1, 2, "quiet_verified"): 1,
            (B2, 1, "quiet_verified"): 1,
            (A2, 1, "quiet_verified"): 2,
            (B2, 1, "book_verified"): 1,
            (B1, 1, "quiet_verified"): 1,
        }
    )
    spans = sorted(round(e.span, 4) for e in result.episodes if len(e.failures) > 1)
    assert spans == [2.5]


def test_the_report_gives_counts_times_and_ids_but_no_hash(
    replay: ModuleType, timeline: Path
) -> None:
    out = io.StringIO()
    result, inferred, stats = replay.replay_timeline(timeline)
    replay.report(result, inferred, stats, out, each=True)
    text = out.getvalue()
    assert "hash_failed: 9 replayed, 9 by the client, difference +0" in text
    assert "divergence declared: 1" in text
    hashes = {
        change["hash"]
        for line in records(timeline)
        if line["type"] in ("BookEvent", "PriceChangeEvent")
        for change in line["record"].get("changes", [line["record"]])
    }
    assert hashes
    assert not [h for h in hashes if h in text]


def test_a_token_whose_hash_inputs_cannot_be_inferred_is_not_checked(
    replay: ModuleType, research: ModuleType, tmp_path: Path
) -> None:
    out = io.StringIO()
    client = Client(research, out)
    opening = json.loads(client.source.frame(Opening(("A1", "A2", "B1", "B2"))))
    opening[1]["hash"] = BAD_HASH
    client.send(0.5, dumps(opening))
    client.change(1.0, 1000, "A2:BUY:0.49:50")
    client.until(3.0)
    client.stats()
    path = tmp_path / "timeline.jsonl"
    path.write_text(out.getvalue())

    result, inferred, stats = replay.replay_timeline(path)
    assert inferred[A2].parameters is None
    assert inferred[A1].parameters == BookParameters(Decimal(MIN_ORDER_SIZE), NEG_RISK)
    # The client checked A2's book and failed it, which stopped A2's checks;
    # the replay checks none of A2's.
    assert stats is not None
    assert (stats["hash_failed"], result.verifier.counts.failed) == (1, 0)
    report = io.StringIO()
    replay.report(result, inferred, stats, report)
    assert "not inferred from 1 books, so not checked" in report.getvalue()


def test_the_command_reads_a_timeline(
    replay: ModuleType, timeline: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert replay.main([str(timeline), "--failures"]) == 0
    assert "Each failed check" in capsys.readouterr().out
