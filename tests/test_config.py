"""ClientConfig and ReconnectPolicy: defaults, and refusal of every invalid
value with ConfigError (spec/client.md, Configuration)."""

import math
from typing import Any

import pytest
from pydantic import ValidationError

from polymarket_market_data import ClientConfig, ConfigError, ReconnectPolicy

CONFIG_DURATIONS = [
    "ping_interval",
    "pong_timeout",
    "connect_timeout",
    "close_timeout",
    "book_timeout",
    "repeat_window",
    "hash_grace",
    "burst_quiet",
    "settlement_poll_interval",
    "settlement_confirm_timeout",
    "lookup_timeout",
]
POLICY_DURATIONS = ["base_delay", "max_delay", "max_recovery_time"]
CONFIG_COUNTS = ["queue_size", "max_message_bytes"]
POLICY_COUNTS = ["max_attempts"]
FRACTIONS = ["backlog_warning", "resume_below"]
CONFIG_FLAGS = ["verify_hash", "keep_raw"]

BAD_DURATIONS = [0, 0.0, -1, -0.5, math.nan, math.inf, -math.inf, "soon", None]
BAD_COUNTS = [0, -1, 1.5, "many", None]
# Outside (0, 1]. Values inside it that fail only against the other fraction
# are in test_resume_below_must_be_below_backlog_warning.
BAD_FRACTIONS = [0, 0.0, -0.1, 1.01, 2, math.nan, math.inf, "half", None]
BAD_FLAGS = ["maybe", 2, None]


def config(**fields: Any) -> ClientConfig:
    return ClientConfig(**fields)


def policy(**fields: Any) -> ReconnectPolicy:
    return ReconnectPolicy(**fields)


def assert_config_error(info: pytest.ExceptionInfo[ConfigError]) -> None:
    assert not isinstance(info.value, ValidationError)
    assert isinstance(info.value.__cause__, ValidationError)


def test_config_defaults() -> None:
    assert ClientConfig().model_dump() == {
        "url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        "ping_interval": 10.0,
        "pong_timeout": 20.0,
        "connect_timeout": 10.0,
        "close_timeout": 1.0,
        "reconnect": {
            "base_delay": 0.5,
            "max_delay": 30.0,
            "max_attempts": 10,
            "max_recovery_time": 300.0,
            "jitter": True,
        },
        "book_timeout": 10.0,
        "repeat_window": 2.0,
        "queue_size": 10000,
        "backlog_warning": 0.5,
        "overflow": "disconnect",
        "resume_below": 0.1,
        "verify_hash": True,
        "hash_grace": 2.0,
        "burst_quiet": 1.0,
        "settlement_poll_interval": 15.0,
        "settlement_confirm_timeout": 300.0,
        "lookup_timeout": 10.0,
        "new_market": "drop",
        "keep_raw": False,
        "max_message_bytes": 16777216,
    }
    assert ClientConfig().reconnect == ReconnectPolicy()


def test_conformance_profile_is_valid() -> None:
    # The profile of spec/conformance.md, and the overrides its scenarios
    # set, which the harness will build.
    profile: dict[str, Any] = {
        "url": "ws://127.0.0.1:8765",
        "ping_interval": 0.5,
        "pong_timeout": 2.0,
        "connect_timeout": 1.0,
        "close_timeout": 0.5,
        "reconnect": ReconnectPolicy(
            base_delay=0.1,
            max_delay=0.4,
            max_attempts=3,
            max_recovery_time=10.0,
            jitter=False,
        ),
        "book_timeout": 1.0,
        "repeat_window": 1.0,
        "queue_size": 1000,
        "backlog_warning": 0.5,
        "overflow": "disconnect",
        "resume_below": 0.1,
        "verify_hash": False,
        "hash_grace": 0.5,
        "burst_quiet": 0.1,
        "settlement_poll_interval": 0.5,
        "settlement_confirm_timeout": 3.0,
        "lookup_timeout": 1.0,
        "new_market": "drop",
        "keep_raw": False,
    }
    config(**profile)
    config(**profile | {"queue_size": 1, "backlog_warning": 1, "resume_below": 0.5})
    config(**profile | {"queue_size": 4, "backlog_warning": 0.75, "resume_below": 0.25})
    config(**profile | {"new_market": "deliver", "verify_hash": True})


@pytest.mark.parametrize("value", [1e-9, 0.001, 1, 86400.0])
@pytest.mark.parametrize("field", CONFIG_DURATIONS)
def test_config_accepts_positive_durations(field: str, value: float) -> None:
    assert getattr(config(**{field: value}), field) == value


@pytest.mark.parametrize("value", BAD_DURATIONS)
@pytest.mark.parametrize("field", CONFIG_DURATIONS)
def test_config_refuses_bad_durations(field: str, value: object) -> None:
    with pytest.raises(ConfigError) as info:
        config(**{field: value})
    assert_config_error(info)
    assert field in str(info.value)


@pytest.mark.parametrize("value", BAD_DURATIONS)
@pytest.mark.parametrize("field", POLICY_DURATIONS)
def test_policy_refuses_bad_durations(field: str, value: object) -> None:
    with pytest.raises(ConfigError) as info:
        policy(**{field: value})
    assert_config_error(info)
    assert field in str(info.value)


@pytest.mark.parametrize("value", BAD_COUNTS)
@pytest.mark.parametrize("field", CONFIG_COUNTS)
def test_config_refuses_bad_counts(field: str, value: object) -> None:
    with pytest.raises(ConfigError) as info:
        config(**{field: value})
    assert_config_error(info)


@pytest.mark.parametrize("value", BAD_COUNTS)
@pytest.mark.parametrize("field", POLICY_COUNTS)
def test_policy_refuses_bad_counts(field: str, value: object) -> None:
    with pytest.raises(ConfigError) as info:
        policy(**{field: value})
    assert_config_error(info)


def test_counts_accept_one() -> None:
    assert (
        ClientConfig(queue_size=1, backlog_warning=1, resume_below=0.5).queue_size == 1
    )
    assert ClientConfig(max_message_bytes=1).max_message_bytes == 1
    assert ReconnectPolicy(max_attempts=1).max_attempts == 1


@pytest.mark.parametrize("value", BAD_FRACTIONS)
@pytest.mark.parametrize("field", FRACTIONS)
def test_config_refuses_bad_fractions(field: str, value: object) -> None:
    with pytest.raises(ConfigError) as info:
        config(**{field: value})
    assert_config_error(info)


@pytest.mark.parametrize(
    ("backlog_warning", "resume_below"),
    [(0.5, 0.5), (0.5, 0.6), (0.1, 0.1), (0.05, 0.1), (1, 1)],
)
def test_resume_below_must_be_below_backlog_warning(
    backlog_warning: float, resume_below: float
) -> None:
    with pytest.raises(ConfigError) as info:
        ClientConfig(backlog_warning=backlog_warning, resume_below=resume_below)
    assert_config_error(info)
    assert "resume_below must be below backlog_warning" in str(info.value)


@pytest.mark.parametrize("value", ["drop", "fail", "block", "Disconnect", "", None])
def test_config_refuses_other_overflow_responses(value: object) -> None:
    with pytest.raises(ConfigError) as info:
        config(overflow=value)
    assert_config_error(info)


@pytest.mark.parametrize("value", ["deliver", "drop"])
def test_config_accepts_new_market_values(value: str) -> None:
    assert config(new_market=value).new_market == value


@pytest.mark.parametrize("value", ["keep", "DROP", "", None])
def test_config_refuses_other_new_market_values(value: object) -> None:
    with pytest.raises(ConfigError) as info:
        config(new_market=value)
    assert_config_error(info)


@pytest.mark.parametrize("value", BAD_FLAGS)
@pytest.mark.parametrize("field", CONFIG_FLAGS)
def test_config_refuses_bad_flags(field: str, value: object) -> None:
    with pytest.raises(ConfigError) as info:
        config(**{field: value})
    assert_config_error(info)


@pytest.mark.parametrize("value", BAD_FLAGS)
def test_policy_refuses_bad_jitter(value: object) -> None:
    with pytest.raises(ConfigError) as info:
        policy(jitter=value)
    assert_config_error(info)


@pytest.mark.parametrize("value", [5, None])
def test_config_refuses_bad_url(value: object) -> None:
    with pytest.raises(ConfigError) as info:
        config(url=value)
    assert_config_error(info)


@pytest.mark.parametrize(
    "value",
    [
        "fast",
        5,
        None,
        {"base_delay": 0},
        {"max_attempts": 0},
        {"max_recovery_time": math.inf},
        {"jitter": "maybe"},
        {"max_retries": 3},
    ],
)
def test_config_refuses_bad_nested_policy(value: object) -> None:
    with pytest.raises(ConfigError) as info:
        config(reconnect=value)
    assert_config_error(info)


def test_config_refuses_unvalidated_nested_policy() -> None:
    # model_copy(update=...) builds a model without validating it; the
    # config validates the nested policy again.
    unchecked = ReconnectPolicy().model_copy(update={"max_delay": -1.0})
    with pytest.raises(ConfigError) as info:
        ClientConfig(reconnect=unchecked)
    assert_config_error(info)
    assert "max_delay" in str(info.value)


def test_config_refuses_unknown_fields() -> None:
    with pytest.raises(ConfigError) as info:
        config(pong_timout=5.0)
    assert_config_error(info)
    assert "pong_timout" in str(info.value)


def test_policy_refuses_unknown_fields() -> None:
    with pytest.raises(ConfigError) as info:
        policy(max_retries=3)
    assert_config_error(info)
    assert "max_retries" in str(info.value)


def test_config_error_reports_every_invalid_field() -> None:
    with pytest.raises(ConfigError) as info:
        config(ping_interval=0, queue_size=0)
    assert_config_error(info)
    assert "ping_interval" in str(info.value)
    assert "queue_size" in str(info.value)


def test_config_is_frozen() -> None:
    frozen = ClientConfig()
    with pytest.raises(ValidationError):
        frozen.ping_interval = 5.0  # type: ignore[misc]
    with pytest.raises(ValidationError):
        frozen.reconnect.max_attempts = 5  # type: ignore[misc]


def test_configs_compare_by_value() -> None:
    assert ClientConfig(queue_size=5) == ClientConfig(queue_size=5)
    assert ClientConfig(queue_size=5) != ClientConfig()
    assert hash(ClientConfig()) == hash(ClientConfig())
