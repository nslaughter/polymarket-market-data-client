"""``ClientConfig`` and ``ReconnectPolicy`` (spec/client.md, Configuration).

Both are frozen Pydantic models (D8). Their constructors raise
``ConfigError`` for an invalid value, with Pydantic's validation error as its
``__cause__``. The check has to be in the constructor: a validator that
raised ``ConfigError`` would have it turned into a ``ValidationError``, since
``ConfigError`` is a ``ValueError``.
"""

from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from ._errors import ConfigError

# Seconds, finite and positive.
_Duration = Annotated[float, Field(gt=0, allow_inf_nan=False)]
# A fraction of queue_size, greater than 0 and at most 1.
_Fraction = Annotated[float, Field(gt=0, le=1, allow_inf_nan=False)]
_Count = Annotated[int, Field(ge=1)]


class _ConfigModel(BaseModel):
    # strict converts no value from another type, except an integer where a
    # float is expected. revalidate_instances checks a nested model again,
    # so a policy built without validation, as by model_copy(update=...),
    # is refused too.
    model_config = ConfigDict(
        frozen=True, extra="forbid", strict=True, revalidate_instances="always"
    )

    # Hidden from type checkers, so that the Pydantic plugin keeps the typed
    # signature it derives from the fields.
    if not TYPE_CHECKING:

        def __init__(self, /, **data: Any) -> None:
            try:
                super().__init__(**data)
            except ValidationError as error:
                raise ConfigError(str(error)) from error


class ReconnectPolicy(_ConfigModel):
    """The reconnect bounds and backoff (D2).

    The delay before attempt *k* is ``min(max_delay, base_delay × 2^(k − 1))``,
    multiplied by a uniform random factor in [0, 1) when ``jitter`` is on.
    """

    base_delay: _Duration = 0.5
    max_delay: _Duration = 30.0
    max_attempts: _Count = 10
    max_recovery_time: _Duration = 300.0
    jitter: bool = True


class ClientConfig(_ConfigModel):
    """The client's configuration. Each default is the contract's, and those
    marked with a decision are the default that owner specification sets."""

    url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    ping_interval: _Duration = 10.0
    pong_timeout: _Duration = 20.0  # D1
    connect_timeout: _Duration = 10.0
    close_timeout: _Duration = 1.0
    reconnect: ReconnectPolicy = Field(default_factory=ReconnectPolicy)  # D2
    book_timeout: _Duration = 10.0
    repeat_window: _Duration = 2.0
    queue_size: _Count = 10000  # D3
    backlog_warning: _Fraction = 0.5  # D3
    overflow: Literal["disconnect"] = "disconnect"  # D3
    resume_below: _Fraction = 0.1  # D3
    verify_hash: bool = True  # D4
    hash_grace: _Duration = 2.0  # D4
    burst_quiet: _Duration = 1.0  # D4
    settlement_poll_interval: _Duration = 15.0  # D6
    settlement_confirm_timeout: _Duration = 300.0  # D6
    lookup_timeout: _Duration = 10.0  # D6
    new_market: Literal["drop", "deliver"] = "drop"
    keep_raw: bool = False
    max_message_bytes: _Count = 16777216

    @model_validator(mode="after")
    def _resume_below_warning(self) -> "ClientConfig":
        if self.resume_below >= self.backlog_warning:
            raise ValueError("resume_below must be below backlog_warning")
        return self
