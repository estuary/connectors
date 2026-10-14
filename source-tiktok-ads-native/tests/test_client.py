"""Tests for how TikTok's in-body status codes reach the CDK.

TikTok answers throttling and its own server errors with HTTP 200, and
`TikTokStatusMixin` gives those responses the status TikTok meant, so the CDK's
own retries and rate limiter handle them. That depends on the CDK's
`HTTPMixin` session and on rewriting aiohttp's response in place, so these
tests run the real connector class and the CDK's real request loop against a
local server answering like TikTok: a CDK or aiohttp upgrade that breaks the
translation fails here rather than in a capture.
"""

import asyncio
import logging
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from estuary_cdk.http import HTTPError
from pydantic import ValidationError
from source_tiktok_ads_native import Connector
from source_tiktok_ads_native.api.advertisers import resolve_advertiser_ids
from source_tiktok_ads_native.api.client import (
    SERVER_ERROR_ATTEMPTS,
    TikTokAPIError,
    TikTokClient,
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


def tiktok(code: int, data: Any = None) -> Reply:
    """A reply as TikTok sends it: always HTTP 200, the result in `code`."""
    body = {"code": code, "message": f"code {code}", "data": {} if data is None else data}
    return 200, body, {}


def advertisers(*ids: str) -> Reply:
    return tiktok(0, {"list": [{"advertiser_id": i} for i in ids]})


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

    async def handler(request: web.Request) -> web.Response:
        path = request.path.removeprefix("/")
        n = calls.get(path, 0)
        calls[path] = n + 1
        status, body, headers = script[path][min(n, len(script[path]) - 1)]
        if isinstance(body, str):
            return web.Response(status=status, text=body, headers=headers, content_type="text/html")
        return web.json_response(body, status=status, headers=headers)

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


@pytest.mark.asyncio
async def test_a_throttle_is_retried_by_the_cdk_and_slows_its_rate_limiter():
    script = {"advertiser/info/": [tiktok(40100), tiktok(40133), advertisers(ADVERTISER_ID)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        data = await get_data(_client(connector, base_url), "advertiser/info/", {}, AdvertiserList, LOG)

        assert [a.advertiser_id for a in data.advertisers] == [ADVERTISER_ID]
        assert calls["advertiser/info/"] == 3
        assert connector.rate_limiter.failed == 2


@pytest.mark.asyncio
async def test_maintenance_is_waited_out_like_a_throttle():
    script = {"advertiser/info/": [tiktok(60001), advertisers(ADVERTISER_ID)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        await get_data(_client(connector, base_url), "advertiser/info/", {}, AdvertiserList, LOG)

        assert calls["advertiser/info/"] == 2
        assert connector.rate_limiter.failed == 1


@pytest.mark.asyncio
async def test_a_server_error_is_retried_without_slowing_the_rate_limiter():
    script = {"advertiser/info/": [tiktok(50002), advertisers(ADVERTISER_ID)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        await get_data(_client(connector, base_url), "advertiser/info/", {}, AdvertiserList, LOG)

        assert calls["advertiser/info/"] == 2
        assert connector.rate_limiter.failed == 0


@pytest.mark.asyncio
async def test_a_lasting_server_error_fails_after_the_attempt_cap():
    script = {"advertiser/info/": [tiktok(50000)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        with pytest.raises(HTTPError) as err:
            await asyncio.wait_for(
                get_data(_client(connector, base_url), "advertiser/info/", {}, AdvertiserList, LOG),
                HANG_SECONDS,
            )

        assert err.value.code == 503
        assert calls["advertiser/info/"] == SERVER_ERROR_ATTEMPTS


@pytest.mark.asyncio
async def test_other_codes_reach_the_connector_without_a_retry():
    script = {"advertiser/info/": [tiktok(40001)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        with pytest.raises(TikTokAPIError) as err:
            await get_data(_client(connector, base_url), "advertiser/info/", {}, AdvertiserList, LOG)

        assert err.value.code == 40001
        assert calls["advertiser/info/"] == 1


@pytest.mark.asyncio
async def test_a_real_http_429_still_reaches_the_cdk():
    script = {"advertiser/info/": [(429, {}, {}), advertisers(ADVERTISER_ID)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        await get_data(_client(connector, base_url), "advertiser/info/", {}, AdvertiserList, LOG)

        assert calls["advertiser/info/"] == 2
        assert connector.rate_limiter.failed == 1


@pytest.mark.asyncio
async def test_a_streamed_listing_keeps_its_body_and_headers_through_a_throttle():
    # The CDK streams a success from the response, which the translation had to
    # read whole to find TikTok's code; reports read their truncation header.
    first_page = campaign_page(1, 2, ["c1", "c2"])
    first_page[2]["X-Tt-Ads-Throttle"] = "seen"
    script = {"campaign/get/": [first_page, tiktok(40100), campaign_page(2, 2, ["c3"])]}
    seen_headers: list[str | None] = []
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        items = [
            item.campaign_id
            async for item in paginate(
                _client(connector, base_url),
                "campaign/get/",
                {"advertiser_id": ADVERTISER_ID},
                2,
                Campaign,
                LOG,
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
            await get_data(client, "advertiser/info/", {}, AdvertiserList, LOG)
        assert calls["advertiser/info/"] == 2


@pytest.mark.asyncio
async def test_a_lasting_server_error_listing_advertisers_never_carries_the_app_secret():
    # The CDK quotes the request URL, which carries the app secret, in its error.
    app_secret = "estuary-app-s3cret"
    script = {"oauth2/advertiser/get/": [tiktok(50002)]}
    async with fake_tiktok(script) as (base_url, calls), connector_session() as connector:
        with pytest.raises(HTTPError) as err:
            await asyncio.wait_for(
                resolve_advertiser_ids(_client(connector, base_url), [], "app", app_secret, LOG),
                HANG_SECONDS,
            )

        assert err.value.code == 503
        assert app_secret not in str(err.value)
        assert calls["oauth2/advertiser/get/"] == SERVER_ERROR_ATTEMPTS


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
