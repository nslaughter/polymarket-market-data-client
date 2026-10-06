"""The scenario parser (spec/conformance.md, Notation)."""

import pytest

from polymarket_market_data import ClientStateError, MarketNotFound

from .failures import SPEC
from .frames import BookSpec, Entry, Opening, PriceChangeFrame
from .lookup import Answer
from .notation import (
    Close,
    Expect,
    ExpectClientClose,
    NotationError,
    RecvPing,
    Resolve,
    Send,
    SendText,
    SetLookup,
    Silent,
    Subscribe,
    Within,
    load,
    parse_document,
    parse_scenario,
)
from .synthetic import MARKETS, T0
from .words import split_words

SCENARIOS = load()


def test_every_scenario_block_parses() -> None:
    blocks = SPEC.read_text().count("\n```scenario\n")
    assert len(SCENARIOS) == blocks == 57
    assert len({scenario.name for scenario in SCENARIOS}) == blocks


def test_a_json_string_with_spaces_is_one_word() -> None:
    words = split_words(
        'expect conn interrupted close_reason="going away" within 0..0.5 of c1'
    )
    assert [word.text for word in words][3] == 'close_reason="going away"'
    reason = words[3]
    assert (reason.key, reason.value, reason.quoted) == (
        "close_reason",
        "going away",
        True,
    )
    whole = split_words('close 1001 "going away"')[2]
    assert (whole.key, whole.value, whole.quoted) == (None, "going away", True)


def test_a_json_string_is_decoded() -> None:
    (word,) = split_words(r'raw="{\"event_type\":\"future\",\"note\":\"\\ud800\"}"')
    assert word.value == '{"event_type":"future","note":"\\ud800"}'
    with pytest.raises(NotationError, match="unterminated"):
        split_words('raw="open')
    with pytest.raises(NotationError, match="follows"):
        split_words('"a"b')


def test_comments_blank_lines_and_continuations() -> None:
    scenario = parse_scenario(
        "scenario example\n"
        "markets A\n"
        "\n"
        "  # a comment\n"
        "expect token A1 ready\n"
        "  previous=synchronizing\n"
        "   reason=book\n"
        "# another\n"
        "recv-ping\n",
        first_line=100,
    )
    first, second = scenario.steps
    assert (first.line, first.text) == (
        104,
        "expect token A1 ready previous=synchronizing reason=book",
    )
    assert (second.line, second.action) == (108, RecvPing())


def test_labels_and_within() -> None:
    scenario = parse_scenario(
        "scenario example\n"
        "o: send opening A1\n"
        "expect book A1 within 0..0.3 of o\n"
        "p: recv-ping\n"
        "recv-ping within 0.4..0.6 of p\n"
    )
    send, expect, ping, again = scenario.steps
    assert send.label == "o"
    assert send.action == Send(Opening(("A1",)))
    assert expect.within == Within(0.0, 0.3, "o")
    assert isinstance(expect.action, Expect)
    assert ping.label == "p"
    assert again.within == Within(0.4, 0.6, "p")


@pytest.mark.parametrize(
    "steps",
    [
        "expect book A1 within 0..0.3 of o\no: send opening A1",
        "o: send opening A1\no: send opening A1",
        "send-again x",
        "x: drop\nsend-again x",
        "start A: accept",
    ],
)
def test_labels_must_be_defined_before_use_and_once(steps: str) -> None:
    with pytest.raises(NotationError):
        parse_scenario(f"scenario example\nmarkets A\n{steps}\n")


def test_a_call_may_end_with_raises() -> None:
    scenario = parse_scenario(
        "scenario example\n"
        "resolve synthetic-u raises MarketNotFound\n"
        "resolve synthetic-a\n"
        "subscribe B raises ClientStateError\n"
    )
    missing, found, late = scenario.steps
    assert (missing.action, missing.raises) == (Resolve("synthetic-u"), MarketNotFound)
    assert found.raises is None
    assert (late.action, late.raises) == (Subscribe("B"), ClientStateError)
    with pytest.raises(NotationError, match="slug"):
        parse_scenario("scenario example\nresolve synthetic-x\n")
    with pytest.raises(NotationError, match="not an exception"):
        parse_scenario("scenario example\nsubscribe B raises Nonsense\n")


def test_start_expands_to_the_first_connection() -> None:
    scenario = parse_scenario("scenario example\nmarkets A\n\nstart A\n", first_line=10)
    texts = [step.text.removeprefix("start A: ") for step in scenario.steps]
    assert texts == [
        "expect conn connecting attempt=1",
        "accept",
        "recv-subscribe A1 A2",
        "expect conn open connection=1",
        "expect conn subscribed connection=1",
        "expect token A1 synchronizing previous=none reason=subscribed",
        "expect token A2 synchronizing previous=none reason=subscribed",
        "send opening A1 A2",
        "expect book A1 opening=true connection=1 held_book_matched=none",
        "expect token A1 ready previous=synchronizing reason=book",
        "expect book A2 opening=true connection=1 held_book_matched=none",
        "expect token A2 ready previous=synchronizing reason=book",
    ]
    assert {step.line for step in scenario.steps} == {13}


def test_start_follows_the_desired_set() -> None:
    scenario = parse_scenario("scenario example\nsubscribe B\nstart B\n")
    assert scenario.steps[1].text == "start B: expect conn connecting attempt=1"
    for text in [
        "markets A B\nstart A",
        "markets A\nstart A B",
        "markets A\nunsubscribe A\nstart A",
        "markets A\naccept\nstart A",
        "start A",
    ]:
        with pytest.raises(NotationError):
            parse_scenario(f"scenario example\n{text}\n")


def test_send_text_takes_the_rest_of_the_line_with_references() -> None:
    scenario = parse_scenario(
        "scenario example\n"
        'x: send-text {"market":"${A}","asset_id":"${A1}","timestamp":"${t:-5}",\n'
        '  "note":"two  spaces"}\n'
    )
    (step,) = scenario.steps
    assert step.label == "x"
    a, a1 = MARKETS["A"].condition_id, "1" + "0" * 74 + "11"
    assert step.action == SendText(
        f'{{"market":"{a}","asset_id":"{a1}","timestamp":"{T0 - 5}",'
        ' "note":"two  spaces"}'
    )
    with pytest.raises(NotationError, match="reference"):
        parse_scenario("scenario example\nsend-text ${Z}\n")


def test_the_header() -> None:
    scenario = parse_scenario(
        "scenario example\n"
        "markets A S\n"
        "config verify_hash=true queue_size=4 backlog_warning=1\n"
        "  reconnect.max_recovery_time=1.3 reconnect.jitter=false new_market=deliver\n"
        "lookup A closed winner=A2\n"
        "lookup S error\n"
        "owner-spec D2 D6\n"
        "accept\n"
        "lookup A open\n"
    )
    assert scenario.markets == ("A", "S")
    assert dict(scenario.config) == {
        "verify_hash": True,
        "queue_size": 4,
        "backlog_warning": 1.0,
        "new_market": "deliver",
    }
    assert dict(scenario.reconnect) == {"max_recovery_time": 1.3, "jitter": False}
    assert dict(scenario.lookup) == {"A": Answer("closed", "A2"), "S": Answer("error")}
    assert scenario.owner_specs == ("D2", "D6")
    # A lookup line after a step is a runner step.
    assert scenario.steps[1].action == SetLookup("A", Answer("open"))


@pytest.mark.parametrize(
    "header",
    [
        "config colour=red",
        "config url=ws://example",
        "config queue_size=4.5",
        "config verify_hash=1",
        "config new_market=keep",
        "config reconnect.attempts=3",
        "lookup A closed winner=B1",
        "lookup A sometimes",
        "owner-spec D9",
        "markets A A",
        "markets Q",
    ],
)
def test_a_header_line_the_notation_does_not_allow(header: str) -> None:
    with pytest.raises(NotationError):
        parse_scenario(f"scenario example\n{header}\naccept\n")


def test_server_steps_with_arguments() -> None:
    scenario = parse_scenario(
        "scenario example\n"
        'close 1000 "all subscribed assets resolved"\n'
        "close 1001\n"
        'expect-client-close 1000 "no subscriptions"\n'
        "silent A1 t=200 BUY:0.46:500\n"
        "send pc A t=100 A1:BUY:0.49:50 A2:SELL:0.51:0 hash=bad\n"
        "send opening (book A1 t=5 bids=0.40:1 asks=-) A2\n"
    )
    resolved, plain, client, silent, change, opening = (
        step.action for step in scenario.steps
    )
    assert resolved == Close(1000, "all subscribed assets resolved")
    assert plain == Close(1001, "")
    assert client == ExpectClientClose(1000, "no subscriptions")
    assert silent == Silent("A1", 200, Entry("A1", "BUY", "0.46", "500"))
    assert change == Send(
        PriceChangeFrame(
            "A",
            100,
            (Entry("A1", "BUY", "0.49", "50"), Entry("A2", "SELL", "0.51", "0")),
            True,
        )
    )
    assert opening == Send(Opening((BookSpec("A1", 5, (("0.40", "1"),), ()), "A2")))


@pytest.mark.parametrize(
    "step",
    [
        "send pc A t=100 B1:BUY:0.49:50",
        "send pc A t=100 A1:HOLD:0.49:50",
        "send pc A t=100 A1:BUY:0.49",
        "send pc A t=100",
        "send bba A1 t=100 hash=bad",
        "send bba S1 t=100",
        "send resolved A t=1 winner=B1",
        "send book A1 t=1 bids=-",
        "send opening (book A1 t=1 bids=-",
        "send new-market 0 t=1",
        "send-binary 0g",
        "refuse 0",
        "pong sometimes",
        "trade S 0.5",
        "wait soon",
        "expect conn nowhere",
        "expect-stats nothing=1",
        "jump",
    ],
)
def test_a_step_the_notation_does_not_allow(step: str) -> None:
    with pytest.raises(NotationError):
        parse_scenario(f"scenario example\nmarkets A\n{step}\n")


def test_an_error_names_its_line() -> None:
    with pytest.raises(NotationError, match=r"^line 42: not a step: jump"):
        parse_scenario("scenario example\nmarkets A\njump\n", first_line=40)


def test_a_block_must_sit_under_its_heading() -> None:
    document = "#### `right`\n\n```scenario\nscenario wrong\naccept\n```\n"
    with pytest.raises(NotationError, match="heading"):
        parse_document(document)
    assert parse_document(document.replace("wrong", "right"))[0].name == "right"
    with pytest.raises(NotationError, match="not closed"):
        parse_document("#### `right`\n\n```scenario\nscenario right\naccept\n")
