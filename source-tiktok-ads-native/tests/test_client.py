"""Tests for how TikTok's in-body status codes reach the CDK.

TikTok answers throttling, maintenance and its own server errors with HTTP 200, and
`TikTokStatusMixin` gives those responses the status TikTok meant, so the CDK's
own retries and rate limiter handle them. That depends on the CDK's
`HTTPMixin` session and on rewriting aiohttp's response in place, so these
tests run the real connector class and the CDK's real request loop against a
local server answering like TikTok: a CDK or aiohttp upgrade that breaks the
translation fails here rather than in a capture.
"""

import asyncio
import json
import logging
import math
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from estuary_cdk.http import HTTPError
from multidict import CIMultiDict, CIMultiDictProxy
from pydantic import ValidationError
from source_tiktok_ads_native import Connector
from source_tiktok_ads_native.api import client as client_module
from source_tiktok_ads_native.api.advertisers import resolve_advertiser_ids
from source_tiktok_ads_native.api.client import (
    ENVELOPE_MAX_BYTES,
    SERVER_ERROR_ATTEMPTS,
    TikTokAPIError,
    TikTokClient,
    _UNREPLAYED_READS,
    _ReadAhead,
    _ServerErrorBudget,
    _with_tiktok_status,
    get_data,
    paginate,
)
from source_tiktok_ads_native.models import AdvertiserList, Campaign

LOG = logging.getLogger(__name__)
ADVERTISER_ID = "7694356963175268360"
# The CDK retries a 5xx forever unless told otherwise, so a broken attempt cap
# would hang a test instead of failing it.
HANG_SECONDS = 10

# A scripted reply: HTTP status, then a JSON body (or raw text), then headers.
Reply = tuple[int, Any, dict[str, str]]

# A test-only header telling `fake_tiktok` how to send a reply's body: with a
# Content-Length (the default), chunked, or gzip-compressed, as TikTok sends it.
FRAMING = "X-Test-Framing"
FRAMINGS = ["content-length", "chunked", "gzip"]


def framed(reply: Reply, framing: str) -> Reply:
    status, body, headers = reply
    return status, body, {**headers, FRAMING: framing}


def tiktok(code: int, data: Any = None) -> Reply:
    """A reply as TikTok sends it: always HTTP 200, the result in `code`."""
    body = {"code": code, "message": f"code {code}", "data": {} if data is None else data}
    return 200, body, {}


def advertisers(*ids: str) -> Reply:
    return tiktok(0, {"list": [{"advertiser_id": i} for i in ids]})


def full_page() -> Reply:
    """A listing page as large as TikTok's: 1000 documents the size of the
    sandbox's ad groups (about 3 KB each)."""
    status, body, headers = campaign_page(1, 1, [f"c{n:04}" for n in range(1000)])
    for item in body["data"]["list"]:
        item.update({f"field_{n}": "x" * 20 for n in range(100)})
    return status, body, headers


def campaign_page(page: int, total_page: int, ids: list[str]) -> Reply:
    items = [
        {"advertiser_id": ADVERTISER_ID, "campaign_id": i, "modify_time": "2026-10-01 00:00:00"}
        for i in ids
    ]
    return tiktok(0, {"list": items, "page_info": {"page": page, "total_page": total_page}})


@asynccontextmanager
async def fake_tiktok(script: dict[str, list[Reply]]) -> AsyncIterator[tuple[str, dict[str, int]]]:
    """A local server answering each path's replies in order, repeating the
    last. Yields its base URL and the number of requests per path."""
    calls: dict[str, int] = {}

    async def handler(request: web.Request) -> web.StreamResponse:
        path = request.path.removeprefix("/")
        n = calls.get(path, 0)
        calls[path] = n + 1
        status, body, headers = script[path][min(n, len(script[path]) - 1)]
        headers = dict(headers)
        framing = headers.pop(FRAMING, "content-length")
        if isinstance(body, str):
            return web.Response(status=status, text=body, headers=headers, content_type="text/html")
        if framing == "chunked":
            resp = web.StreamResponse(status=status, headers=headers)
            resp.content_type = "application/json"
            resp.enable_chunked_encoding()
            await resp.prepare(request)
            data = json.dumps(body).encode()
            for start in range(0, len(data), 2**14):
                await resp.write(data[start : start + 2**14])
            await resp.write_eof()
            return resp
        resp = web.json_response(body, status=status, headers=headers)
        if framing == "gzip":
            resp.enable_compression(web.ContentCoding.gzip)
        return resp

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    try:
        yield f"http://{host}:{port}", calls
    finally:
        await runner.cleanup()


async def _enter(connector: Connector, bases: tuple[type, ...]) -> None:
    # As the CDK does before handling any request (estuary_cdk/__init__.py).
    for base in bases:
        if enter := getattr(base, "_mixin_enter", None):
            await enter(connector, LOG)


async def _exit(connector: Connector) -> None:
    for base in reversed(Connector.__bases__):
        if exit := getattr(base, "_mixin_exit", None):
            await exit(connector, LOG)


@asynccontextmanager
async def connector_session() -> AsyncIterator[Connector]:
    """The connector's own HTTP session, entered as the CDK enters it."""
    connector = Connector()
    await _enter(connector, Connector.__bases__)
    connector.rate_limiter.delay = 0.0  # the tests don't wait between requests
    try:
        yield connector
    finally:
        await _exit(connector)


def _client(connector: Connector, base_url: str) -> TikTokClient:
    return TikTokClient(connector, base_url, math.inf)


@pytest.fixture(autouse=True)
def no_server_error_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # A lasting server error would otherwise take two minutes to fail.
    monkeypatch.setattr(client_module, "SERVER_ERROR_BACKOFF_SECONDS", 0.0)


@pytest.mark.parametrize("framing", FRAMINGS)
@pytest.mark.asyncio
async def test_a_throttle_is_retried_by_the_cdk_and_slows_its_rate_limiter(framing: str):
    script = {
        "advertiser/info/": [
            framed(tiktok(40100), framing),
            framed(tiktok(40133), framing),
            advertisers(ADVERTISER_ID),
        ]
    }
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        data = await get_data(LOG, _client(connector, base_url), "advertiser/info/", {}, AdvertiserList)

        assert [a.advertiser_id for a in data.advertisers] == [ADVERTISER_ID]
        assert calls["advertiser/info/"] == 3
        assert connector.rate_limiter.failed == 2


@pytest.mark.asyncio
async def test_maintenance_is_waited_out_without_slowing_the_rate_limiter():
    # Longer than a server error's attempt cap, which maintenance doesn't use.
    maintenance = [tiktok(60001)] * (SERVER_ERROR_ATTEMPTS + 2)
    script = {"advertiser/info/": [*maintenance, advertisers(ADVERTISER_ID)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        await asyncio.wait_for(
            get_data(LOG, _client(connector, base_url), "advertiser/info/", {}, AdvertiserList),
            HANG_SECONDS,
        )

        assert calls["advertiser/info/"] == SERVER_ERROR_ATTEMPTS + 3
        assert connector.rate_limiter.failed == 0


def test_waits_for_maintenance_double_up_to_a_minute(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(client_module, "SERVER_ERROR_BACKOFF_SECONDS", 1.0)
    budget = _ServerErrorBudget()
    body = json.dumps(tiktok(60001)[1]).encode()

    waits: list[float] = []
    # Over a day and a half of maintenance at the longest wait.
    for _ in range(2000):
        assert budget(503, CIMultiDictProxy(CIMultiDict()), body, 1)
        waits.append(budget._backoff)

    assert waits[:8] == [1, 2, 4, 8, 16, 32, 64, 64]
    assert max(waits) == 64


# Listed rather than read from SERVER_ERROR_CODES, so dropping a code fails here.
@pytest.mark.parametrize("code", [50000, 50002, 51001, 51002, 51004, 51041, 51043, 51305])
@pytest.mark.asyncio
async def test_a_server_error_is_retried_without_slowing_the_rate_limiter(code: int):
    script = {"advertiser/info/": [tiktok(code), advertisers(ADVERTISER_ID)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        await get_data(LOG, _client(connector, base_url), "advertiser/info/", {}, AdvertiserList)

        assert calls["advertiser/info/"] == 2
        assert connector.rate_limiter.failed == 0


@pytest.mark.asyncio
async def test_a_lasting_server_error_fails_after_the_attempt_cap():
    script = {"advertiser/info/": [tiktok(50000)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        with pytest.raises(HTTPError) as err:
            await asyncio.wait_for(
                get_data(LOG, _client(connector, base_url), "advertiser/info/", {}, AdvertiserList),
                HANG_SECONDS,
            )

        assert err.value.code == 503
        assert calls["advertiser/info/"] == SERVER_ERROR_ATTEMPTS


@pytest.mark.asyncio
async def test_throttles_dont_use_up_a_requests_server_error_retries():
    # The CDK's attempt count includes throttles, and a request can be throttled
    # hundreds of times while TikTok's limit resets.
    throttles = [tiktok(40100)] * SERVER_ERROR_ATTEMPTS
    script = {"advertiser/info/": [*throttles, tiktok(50002), advertisers(ADVERTISER_ID)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        data = await get_data(LOG, _client(connector, base_url), "advertiser/info/", {}, AdvertiserList)

        assert [a.advertiser_id for a in data.advertisers] == [ADVERTISER_ID]
        assert calls["advertiser/info/"] == SERVER_ERROR_ATTEMPTS + 2


@pytest.mark.asyncio
async def test_server_error_retries_back_off_exponentially(monkeypatch: pytest.MonkeyPatch):
    # The CDK retries a 5xx at once, so without a backoff a warm connector
    # spends a request's whole budget in milliseconds.
    monkeypatch.setattr(client_module, "SERVER_ERROR_BACKOFF_SECONDS", 0.01)
    script = {"advertiser/info/": [tiktok(50000)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        start = time.monotonic()
        with pytest.raises(HTTPError):
            await get_data(LOG, _client(connector, base_url), "advertiser/info/", {}, AdvertiserList)
        elapsed = time.monotonic() - start

        assert calls["advertiser/info/"] == SERVER_ERROR_ATTEMPTS
        assert elapsed >= 0.01 * (1 + 2 + 4 + 8 + 16 + 32 + 64)


@pytest.mark.asyncio
async def test_throttles_after_a_server_error_dont_repeat_its_backoff(monkeypatch: pytest.MonkeyPatch):
    # The rate limiter paces throttles.
    monkeypatch.setattr(client_module, "SERVER_ERROR_BACKOFF_SECONDS", 1.0)
    script = {"advertiser/info/": [tiktok(50002), tiktok(40100), tiktok(40100), advertisers(ADVERTISER_ID)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        await asyncio.wait_for(
            get_data(LOG, _client(connector, base_url), "advertiser/info/", {}, AdvertiserList), 2.5
        )

        assert calls["advertiser/info/"] == 4


@pytest.mark.asyncio
async def test_other_codes_reach_the_connector_without_a_retry():
    script = {"advertiser/info/": [tiktok(40001)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        with pytest.raises(TikTokAPIError) as err:
            await get_data(LOG, _client(connector, base_url), "advertiser/info/", {}, AdvertiserList)

        assert err.value.code == 40001
        assert calls["advertiser/info/"] == 1


@pytest.mark.asyncio
async def test_a_real_http_429_still_reaches_the_cdk():
    script = {"advertiser/info/": [(429, {}, {}), advertisers(ADVERTISER_ID)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        await get_data(LOG, _client(connector, base_url), "advertiser/info/", {}, AdvertiserList)

        assert calls["advertiser/info/"] == 2
        assert connector.rate_limiter.failed == 1


@pytest.mark.asyncio
async def test_a_streamed_listing_keeps_its_body_and_headers_through_a_throttle():
    # The CDK streams a success from the response, whose start the translation
    # has already read to find TikTok's code; reports read their truncation
    # header.
    first_page = campaign_page(1, 2, ["c1", "c2"])
    first_page[2]["X-Tt-Ads-Throttle"] = "seen"
    script = {"campaign/get/": [first_page, tiktok(40100), campaign_page(2, 2, ["c3"])]}
    seen_headers: list[str | None] = []
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        items = [
            item.campaign_id
            async for item in paginate(
                LOG,
                _client(connector, base_url),
                "campaign/get/",
                {"advertiser_id": ADVERTISER_ID},
                2,
                Campaign,
                lambda headers: seen_headers.append(headers.get("X-Tt-Ads-Throttle")),
            )
        ]

        assert items == ["c1", "c2", "c3"]
        assert seen_headers == ["seen", None]
        assert calls["campaign/get/"] == 3


@pytest.mark.asyncio
async def test_a_response_that_is_not_tiktok_json_reaches_the_caller_unchanged():
    script = {"advertiser/info/": [(200, "<html>gateway</html>", {})]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        client = _client(connector, base_url)

        assert await client.request(LOG, "advertiser/info/", {}) == b"<html>gateway</html>"
        with pytest.raises(ValidationError):
            await get_data(LOG, client, "advertiser/info/", {}, AdvertiserList)
        assert calls["advertiser/info/"] == 2


@pytest.mark.parametrize("framing", FRAMINGS)
@pytest.mark.asyncio
async def test_a_full_page_arrives_whole_however_tiktok_frames_it(framing: str):
    # aiohttp pauses a stream holding over 128 KiB, which once crashed the
    # translation on any page that large.
    page = full_page()
    script = {"campaign/get/": [framed(page, framing)]}
    async with fake_tiktok(script) as (base_url, _), connector_session() as connector:
        items = [
            item.campaign_id
            async for item in paginate(
                LOG, _client(connector, base_url), "campaign/get/", {"advertiser_id": ADVERTISER_ID}, 1000, Campaign
            )
        ]

    assert items == [c["campaign_id"] for c in page[1]["data"]["list"]]


@pytest.mark.parametrize("extra", [-1, 0, 1])
@pytest.mark.asyncio
async def test_a_body_either_side_of_the_read_ahead_arrives_whole(extra: int):
    status, body, headers = campaign_page(1, 1, ["c1"])
    body["data"]["list"][0]["campaign_name"] = ""
    name = "x" * (ENVELOPE_MAX_BYTES + extra - len(json.dumps(body).encode()))
    body["data"]["list"][0]["campaign_name"] = name
    script = {"campaign/get/": [(status, body, headers)]}
    async with fake_tiktok(script) as (base_url, _), connector_session() as connector:
        items = [
            item.model_dump()["campaign_name"]
            async for item in paginate(LOG, _client(connector, base_url), "campaign/get/", {}, 1, Campaign)
        ]

    assert len(json.dumps(body).encode()) == ENVELOPE_MAX_BYTES + extra
    assert items == [name]


@pytest.mark.asyncio
async def test_a_throttle_longer_than_the_read_ahead_fails_without_a_retry():
    # Never seen from TikTok, whose throttles are a few hundred bytes. Read as a
    # page, its code still fails the request.
    status, body, headers = tiktok(40100)
    body["message"] = "x" * ENVELOPE_MAX_BYTES
    script = {"advertiser/info/": [(status, body, headers)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        with pytest.raises(TikTokAPIError) as err:
            await get_data(LOG, _client(connector, base_url), "advertiser/info/", {}, AdvertiserList)

        assert err.value.code == 40100
        assert calls["advertiser/info/"] == 1


@pytest.mark.asyncio
async def test_the_translation_reads_no_more_of_a_page_than_its_read_ahead():
    # Each request in flight holds what the translation read, so that must not
    # grow with the page.
    async with fake_tiktok({"campaign/get/": [full_page()]}) as (base_url, _):
        async with aiohttp.ClientSession(middlewares=(_with_tiktok_status,)) as session:
            async with session.get(f"{base_url}/campaign/get/") as resp:
                content = resp.content
                assert isinstance(content, _ReadAhead)
                assert len(content._head) == ENVELOPE_MAX_BYTES + 1


@pytest.mark.parametrize("read", sorted(_UNREPLAYED_READS))
@pytest.mark.asyncio
async def test_a_read_that_would_skip_the_read_ahead_fails_by_name(read: str):
    # Such a read would begin past what the translation already read.
    async with fake_tiktok({"campaign/get/": [full_page()]}) as (base_url, _):
        async with aiohttp.ClientSession(middlewares=(_with_tiktok_status,)) as session:
            async with session.get(f"{base_url}/campaign/get/") as resp:
                with pytest.raises(AttributeError, match=rf"{read}\(\)"):
                    getattr(resp.content, read)


def test_every_stream_read_is_replayed_or_refused():
    # A read aiohttp adds later would otherwise begin past the read-ahead.
    reads = {n for n in dir(aiohttp.StreamReader) if n.startswith(("read", "iter"))}
    assert reads == {"read", "iter_any"} | _UNREPLAYED_READS


@pytest.mark.asyncio
async def test_a_response_that_fails_during_the_read_ahead_is_closed():
    # The CDK only releases a response the middleware returns.
    class CutOff:
        async def read(self, n: int = -1) -> bytes:
            raise aiohttp.ClientPayloadError("Response payload is not completed")

    class Response:
        status = 200
        content = CutOff()
        closed = False

        def close(self) -> None:
            self.closed = True

    resp = Response()

    async def handler(request: aiohttp.ClientRequest) -> Any:
        return resp

    with pytest.raises(aiohttp.ClientPayloadError):
        await _with_tiktok_status(None, handler)  # type: ignore[arg-type]
    assert resp.closed


@pytest.mark.asyncio
async def test_a_lasting_server_error_listing_advertisers_never_carries_the_app_secret():
    # The CDK quotes the request URL, which carries the app secret, in its error.
    app_secret = "estuary-app-s3cret"
    script = {"oauth2/advertiser/get/": [tiktok(50002)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        with pytest.raises(HTTPError) as err:
            await asyncio.wait_for(
                resolve_advertiser_ids(LOG, _client(connector, base_url), [], "app", app_secret),
                HANG_SECONDS,
            )

        assert err.value.code == 503
        assert app_secret not in str(err.value)
        assert calls["oauth2/advertiser/get/"] == SERVER_ERROR_ATTEMPTS


@pytest.mark.asyncio
async def test_a_connect_timeout_listing_advertisers_never_carries_the_app_secret(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    # aiohttp quotes the request URL, which carries the app secret, in a connect
    # timeout, and the CDK logs that message before each retry.
    app_secret = "estuary-app-s3cret"

    async def times_out(*args: Any, **kwargs: Any) -> Any:
        raise asyncio.TimeoutError()

    async with connector_session() as connector:
        assert connector.inner.connector is not None
        monkeypatch.setattr(connector.inner.connector, "connect", times_out)
        with caplog.at_level(logging.INFO, logger=LOG.name), pytest.raises(aiohttp.ConnectionTimeoutError) as err:
            await resolve_advertiser_ids(LOG, _client(connector, "http://tiktok.invalid"), [], "app", app_secret)

    connector_records = [r for r in caplog.records if r.name == LOG.name]
    assert app_secret not in str(err.value)
    assert [r for r in connector_records if "will retry" in r.getMessage()]
    assert not [r for r in connector_records if app_secret in f"{r.getMessage()} {r.args}"]


def _settings(session: aiohttp.ClientSession) -> dict[str, Any]:
    return {
        "timeout": session.timeout,
        "headers": dict(session.headers),
        "connection_limit": session.connector.limit if session.connector else None,
        "connection_limit_per_host": session.connector.limit_per_host if session.connector else None,
        "raise_for_status": session.raise_for_status,
        "auto_decompress": session.auto_decompress,
        "trust_env": session.trust_env,
        "skip_auto_headers": session.skip_auto_headers,
        "auth": session.auth,
        "trace_configs": session.trace_configs,
        "version": session.version,
    }


@pytest.mark.asyncio
async def test_the_replacement_session_keeps_every_setting_of_the_cdk_session():
    # `TikTokStatusMixin` replaces the session `HTTPMixin` opens. If the CDK starts
    # configuring that session, carry the new setting over in the mixin.
    connector = Connector()
    cdk_base, mixin = Connector.__bases__
    await _enter(connector, (cdk_base,))
    cdk_settings = _settings(connector.inner)
    await _enter(connector, (mixin,))
    try:
        assert _settings(connector.inner) == cdk_settings
    finally:
        await _exit(connector)
