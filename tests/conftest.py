"""Checks talk to no live service (docs/implementation-plan.md, 7. Settle
markets through the stream and lookup): with the SDK extra installed, a
client given no lookup uses the default one, which would call Polymarket's
REST APIs. Every HTTP request through ``httpx``, which the SDK uses, is
refused, and a test that made one fails. A test replaces the SDK's HTTP
layer with synthetic responses where it needs one.
"""

from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def no_live_http(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    try:
        import httpx
    except ImportError:  # without the SDK extra, nothing can make a request
        yield
        return
    refused: list[str] = []

    async def refuse(self: object, request: httpx.Request) -> httpx.Response:
        refused.append(f"{request.method} {request.url}")
        raise httpx.ConnectError("tests contact no live service", request=request)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", refuse)
    yield
    assert not refused, f"the test made HTTP requests: {refused}"
