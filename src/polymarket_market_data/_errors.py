"""The exceptions the client raises (spec/client.md, Errors)."""


class ClientError(Exception):
    """Base class of the client's exceptions."""


class ConfigError(ClientError, ValueError):
    """A configuration value is invalid, or ``verify_hash`` is on and no
    lookup is available.

    Raised by ``ClientConfig(...)``, ``ReconnectPolicy(...)``, and
    ``MarketDataClient(...)``. When Pydantic refused the value, its
    validation error is the ``__cause__``.
    """


class ClientStateError(ClientError, RuntimeError):
    """The client was used in a way its state does not allow."""


class MarketNotFound(ClientError, LookupError):
    """The lookup found no market for the slug."""


class LookupFailed(ClientError):
    """The lookup raised or exceeded ``lookup_timeout``.

    Its ``__cause__`` is the lookup's exception, or the ``TimeoutError``.
    """


class RecoveryFailed(ClientError):
    """Reconnection exhausted its bounds (D2)."""
