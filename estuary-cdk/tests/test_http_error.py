from typing import AsyncGenerator

import pytest
import pytest_asyncio
from aiohttp import web

from estuary_cdk.http import HTTPError, HTTPMixin, Headers
from estuary_cdk.logger import init_logger

# Not valid UTF-8, so the test also shows `body` holds the bytes as sent,
# not the escaped text that goes into `message`.
BODY = b'{"error": "nope", "detail": "\xff"}'


@pytest_asyncio.fixture
async def base_url() -> AsyncGenerator[str, None]:
    async def respond(request: web.Request) -> web.Response:
        return web.Response(status=int(request.match_info["status"]), body=BODY)

    app = web.Application()
    app.router.add_get("/{status}", respond)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # pyright: ignore
    yield f"http://127.0.0.1:{port}"
    await runner.cleanup()


@pytest_asyncio.fixture
async def http() -> AsyncGenerator[HTTPMixin, None]:
    log = init_logger()
    mixin = HTTPMixin()
    await mixin._mixin_enter(log)  # pyright: ignore[reportPrivateUsage]
    yield mixin
    await mixin._mixin_exit(log)  # pyright: ignore[reportPrivateUsage]


def never_retry(status: int, headers: Headers, body: bytes, attempt: int) -> bool:
    return False


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 503])
async def test_http_error_keeps_response_body(base_url: str, http: HTTPMixin, status: int):
    with pytest.raises(HTTPError) as caught:
        await http.request(init_logger(), f"{base_url}/{status}", should_retry=never_retry)

    assert caught.value.code == status
    assert caught.value.body == BODY
    assert "Response:\n" in caught.value.message


def test_http_error_body_defaults_to_none():
    assert HTTPError("boom", 500).body is None
