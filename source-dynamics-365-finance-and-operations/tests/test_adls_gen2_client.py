import logging
import traceback
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable
from urllib.parse import parse_qsl

import pytest
from aiohttp import web

from estuary_cdk.http import HTTPError, HTTPMixin
from source_dynamics_365_finance_and_operations.adls_gen2_client import ADLSGen2Client


LOG = logging.getLogger("test-adls-gen2-client")

# A SAS token percent-encodes its signature's '+', '/' and '='. Including them
# checks how aiohttp re-encodes them.
SAS_TOKEN = (
    "sv=2022-11-02&ss=b&srt=sco&sp=rl&se=2030-01-01T00:00:00Z&spr=https"
    "&sig=SECRETsig%2Bab%2Fcd%3D"
)


@asynccontextmanager
async def adls_client(
    status: int, body: dict[str, Any]
) -> AsyncIterator[tuple[ADLSGen2Client, list[str]]]:
    """
    Yields a client whose requests go to a local server, and a list of the raw
    query strings that server receives. The server answers every request with
    `status` and `body`.
    """
    queries: list[str] = []

    async def handler(request: web.Request) -> web.Response:
        queries.append(request.rel_url.raw_query_string)
        return web.json_response(body, status=status)

    app = web.Application()
    app.router.add_get("/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]

    http = HTTPMixin()
    await http._mixin_enter(LOG)  # pyright: ignore[reportPrivateUsage]
    # The rate limiter starts by waiting a second before each request.
    http.rate_limiter.delay = 0.0
    client = ADLSGen2Client(http, "account", "filesystem", SAS_TOKEN, LOG)
    client.base_url = f"http://{host}:{port}"
    try:
        yield client, queries
    finally:
        await http._mixin_exit(LOG)  # pyright: ignore[reportPrivateUsage]
        await runner.cleanup()


@pytest.mark.asyncio
async def test_requests_carry_the_sas_token():
    async with adls_client(200, {"paths": []}) as (client, queries):
        async for _ in client.list_paths(directory="Table"):
            pass

    # aiohttp re-encodes the token, so this compares decoded values. parse_qsl
    # reads '+' as a space, and we assume Azure does too.
    assert dict(parse_qsl(queries[0])) == {
        **dict(parse_qsl(SAS_TOKEN)),
        "resource": "filesystem",
        "recursive": "false",
        "directory": "Table",
    }


@pytest.mark.parametrize(
    "send_request",
    [
        lambda client: anext(client.list_paths()),
        lambda client: client.read_file("model.json"),
        lambda client: anext(client.stream_csv("data.csv")),
    ],
    ids=["list_paths", "read_file", "stream_csv"],
)
@pytest.mark.asyncio
async def test_http_errors_leave_out_the_sas_token(
    send_request: Callable[[ADLSGen2Client], Awaitable[object]],
):
    async with adls_client(403, {"error": {"code": "AuthorizationFailure"}}) as (client, _):
        with pytest.raises(HTTPError) as exc_info:
            await send_request(client)

    err = exc_info.value
    assert err.code == 403
    assert "AuthorizationFailure" in err.message
    # The CDK logs an uncaught error's whole traceback, which includes the
    # error it was raised from.
    assert "SECRET" not in "".join(traceback.format_exception(err))
