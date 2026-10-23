import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from logging import Logger
from typing import Any

import aiohttp
from aiohttp.base_protocol import BaseProtocol
from estuary_cdk import Mixin
from estuary_cdk.http import Headers, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel, ValidationError

from ..models import (
    TikTokEnvelope,
    TikTokPageRemainder,
    TikTokResponse,
)


API = "https://business-api.tiktok.com/open_api/v1.3"
SANDBOX_API = "https://sandbox-ads.tiktok.com/open_api/v1.3"


# TikTok's rate limits belong to the developer app, per endpoint, so every
# capture using Estuary's OAuth app shares them. Apps start at 10 requests per
# second; the sandbox allows 1. Hitting a limit costs a wait that every capture
# on the app shares, so requests start no faster than the limit; the CDK's rate
# limiter slows them further when TikTok throttles.
SANDBOX_REQUESTS_PER_SECOND = 1.0
REQUESTS_PER_SECOND = 10.0

# TikTok answers throttling, maintenance and its own server errors with HTTP 200
# and a `code` in the body. `TikTokStatusMixin` gives those responses the HTTP
# status TikTok meant, so the CDK's retries and rate limiter handle them: a 429
# slows the rate limiter and is retried until TikTok accepts the request, and a
# 503 is retried up to SERVER_ERROR_ATTEMPTS. TikTok's guidance for maintenance
# is to wait, so it's treated as a throttle.
RATE_LIMITED_CODES = frozenset({40016, 40100, 40132, 40133, 60001})
SERVER_ERROR_CODES = frozenset({50000, 50002, 51305})
SERVER_ERROR_ATTEMPTS = 8


def _stream_of(body: bytes) -> aiohttp.StreamReader:
    loop = asyncio.get_running_loop()
    reader = aiohttp.StreamReader(BaseProtocol(loop), 2**16, loop=loop)
    reader.feed_data(body)
    reader.feed_eof()
    return reader


async def _with_tiktok_status(
    request: aiohttp.ClientRequest, handler: aiohttp.ClientHandlerType
) -> aiohttp.ClientResponse:
    """An aiohttp middleware giving each HTTP 200 the status TikTok meant."""
    resp = await handler(request)
    if resp.status != 200:
        return resp

    body = await resp.read()
    try:
        code = TikTokEnvelope.model_validate_json(body).code
    except ValidationError:
        code = None  # not TikTok's envelope; the caller rejects it
    if code in RATE_LIMITED_CODES:
        resp.status = 429
    elif code in SERVER_ERROR_CODES:
        resp.status = 503
    # Reading the body drained `content`, which the CDK streams a success from.
    resp.content = _stream_of(body)
    return resp


class TikTokStatusMixin(Mixin):
    """Installs `_with_tiktok_status` on the CDK's HTTP session. It must follow
    the CDK's connector base class among the connector's bases: the CDK enters
    them in order, and `HTTPMixin` opens the session this one replaces."""

    inner: aiohttp.ClientSession

    async def _mixin_enter(self, log: Logger) -> None:
        cdk_session = self.inner
        self.inner = aiohttp.ClientSession(
            timeout=cdk_session.timeout,
            headers=cdk_session.headers,
            middlewares=(_with_tiktok_status,),
        )
        await cdk_session.close()

    async def _mixin_exit(self, log: Logger) -> None:
        pass  # `HTTPMixin` closes `self.inner`


def _should_retry_server_error(
    status: int, headers: Headers, body: bytes, attempt: int
) -> bool:
    return attempt < SERVER_ERROR_ATTEMPTS


class TikTokClient:
    """One capture's access to TikTok: its HTTP session and API host, and one
    pace shared by every request, so that the subtasks of many advertisers start
    requests no faster than TikTok's rate limit together."""

    def __init__(self, http: HTTPSession, base_url: str, requests_per_second: float):
        self.http = http
        self.base_url = base_url
        self._interval = 1 / requests_per_second
        self._next_start = 0.0
        self._pacing = asyncio.Lock()

    async def _wait_for_turn(self) -> None:
        """Space request starts by the pace."""
        async with self._pacing:
            now = time.monotonic()
            if self._next_start > now:
                await asyncio.sleep(self._next_start - now)
            self._next_start = max(now, self._next_start) + self._interval

    async def request(self, log: Logger, path: str, params: dict[str, Any]) -> bytes:
        await self._wait_for_turn()
        return await self.http.request(
            log,
            f"{self.base_url}/{path}",
            params=params,
            should_retry=_should_retry_server_error,
        )

    async def request_stream(
        self, log: Logger, path: str, params: dict[str, Any]
    ) -> tuple[Headers, Callable[[], AsyncGenerator[bytes, None]]]:
        await self._wait_for_turn()
        return await self.http.request_stream(
            log,
            f"{self.base_url}/{path}",
            params=params,
            should_retry=_should_retry_server_error,
        )


def tiktok_client(http: HTTPSession, is_sandbox: bool) -> TikTokClient:
    if is_sandbox:
        return TikTokClient(http, SANDBOX_API, SANDBOX_REQUESTS_PER_SECOND)
    return TikTokClient(http, API, REQUESTS_PER_SECOND)


class TikTokAPIError(Exception):
    """A non-zero `code` in TikTok's response that isn't retried: TikTok reports
    most failures with HTTP 200, so HTTP-level error handling alone would treat
    them as success."""

    def __init__(self, code: int, message: str):
        super().__init__(f"TikTok API error {code}: {message}")
        self.code = code
        self.message = message


async def get_data[DataT: BaseModel](
    client: TikTokClient,
    path: str,
    params: dict[str, Any],
    data_model: type[DataT],
    log: Logger,
) -> DataT:
    body = await client.request(log, path, params)
    envelope = TikTokEnvelope.model_validate_json(body)
    if envelope.code != 0:
        raise TikTokAPIError(envelope.code, envelope.message)
    return TikTokResponse[data_model].model_validate_json(body).data


async def paginate[ItemT: BaseModel](
    client: TikTokClient,
    path: str,
    params: dict[str, Any],
    page_size: int,
    model: type[ItemT],
    log: Logger,
    check_headers: Callable[[Headers], None] | None = None,
) -> AsyncGenerator[ItemT, None]:
    """Every item of a paged `data.list` response. `check_headers` sees each
    page's headers before any of its items is emitted."""
    page = 1
    while True:
        headers, body = await client.request_stream(
            log, path, {**params, "page": page, "page_size": page_size}
        )
        if check_headers is not None:
            check_headers(headers)

        processor = IncrementalJsonProcessor(
            body(),
            "data.list.item",
            model,
            TikTokPageRemainder,
        )
        async for item in processor:
            yield item

        remainder = processor.get_remainder()
        if remainder.code != 0:
            raise TikTokAPIError(remainder.code, remainder.message)
        if page >= remainder.page_info().total_page:
            return
        page += 1
