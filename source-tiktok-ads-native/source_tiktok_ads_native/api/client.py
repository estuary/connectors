import asyncio
import time
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextvars import ContextVar
from logging import Logger
from typing import Any, cast

import aiohttp
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
# capture using Estuary's OAuth app shares them; some endpoints also limit each
# advertiser (`40133` in TikTok's Return codes, doc 1737172488964097). Apps start
# at 10 requests per second; the sandbox allows 1. Hitting a limit costs a wait that every capture
# on the app shares, so requests start no faster than the limit; the CDK's rate
# limiter slows them further when TikTok throttles.
SANDBOX_REQUESTS_PER_SECOND = 1.0
REQUESTS_PER_SECOND = 10.0

# TikTok answers throttling, maintenance and its own server errors with HTTP 200
# and a `code` in the body. `TikTokStatusMixin` gives those responses the HTTP
# status TikTok meant, so the CDK's retries and rate limiter handle them: a 429
# slows the rate limiter and is retried until TikTok accepts the request, and a
# 503 is retried after a backoff, until the request's SERVER_ERROR_ATTEMPTS-th
# 503, however many 429s come between them.
RATE_LIMITED_CODES = frozenset({40016, 40100, 40132, 40133})
# TikTok's documented server errors, then transient ones TikTok doesn't document
# that Airbyte's connector met in production: 51001 and 51043 "internal service
# timeout" (airbytehq/airbyte#89015), 51002 "internal error" (#85187), 51004
# "Internal Service Error." and 51041 "Page is loading, please try again later."
# (#84290).
SERVER_ERROR_CODES = frozenset(
    {50000, 50002, 51305, 51001, 51002, 51004, 51041, 51043}
)
SERVER_ERROR_ATTEMPTS = 8
# Maintenance is a 503 retried until it ends: TikTok says to wait it out, and
# Airbyte's connector warns it can last hours. As a 429 each retry would slow the
# rate limiter, which after any 429 recovers by only 0.01% per success.
MAINTENANCE = 60001
# Doubled after each server error or maintenance answer, up to 64 s: 1 s, 2 s
# ... 64 s, about two minutes over a request's server errors.
SERVER_ERROR_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_DOUBLINGS = 6


# TikTok's throttle and error responses carry `"data": {}` and a one-line
# message (a sandbox throttle is 153 bytes), so a longer body is a page of
# results and is streamed on without being read further.
ENVELOPE_MAX_BYTES = 4096

# The reads `_ReadAhead` doesn't replay its start through.
_UNREPLAYED_READS = frozenset(
    {
        "iter_chunked",
        "iter_chunks",
        "read_nowait",
        "readany",
        "readchunk",
        "readexactly",
        "readline",
        "readuntil",
    }
)


class _ReadAhead:
    """A response's `content` once `_with_tiktok_status` has read its start:
    that start, then the rest. The CDK streams a success through `iter_any()`
    and reads an error whole through `read()`. Any other read would begin past
    the start, so it fails by name; everything else is the underlying
    stream's."""

    def __init__(self, head: bytes, rest: aiohttp.StreamReader):
        self._head = head
        self._rest = rest

    async def iter_any(self) -> AsyncIterator[bytes]:
        if self._head:
            yield self._head
        async for chunk in self._rest.iter_any():
            yield chunk

    async def read(self) -> bytes:
        return self._head + await self._rest.read()

    def __getattr__(self, name: str) -> Any:
        if name in _UNREPLAYED_READS:
            raise AttributeError(
                f"_ReadAhead doesn't replay its start through {name}()"
            )
        # aiohttp itself checks and sets the stream's exception on release.
        return getattr(self._rest, name)


def _envelope_code(body: bytes) -> int | None:
    """The `code` of TikTok's envelope, or None for a body that isn't one, which
    the caller rejects."""
    try:
        return TikTokEnvelope.model_validate_json(body).code
    except ValidationError:
        return None


async def _with_tiktok_status(
    request: aiohttp.ClientRequest, handler: aiohttp.ClientHandlerType
) -> aiohttp.ClientResponse:
    """An aiohttp middleware giving each HTTP 200 the status TikTok meant, and
    backing off before an attempt that follows a server error."""
    if (budget := _request_budget.get()) is not None:
        # aiohttp's total timeout, 300 s on the CDK's session, counts this wait.
        await budget.backoff()
    try:
        resp = await handler(request)
    except aiohttp.ConnectionTimeoutError:
        # aiohttp names the whole URL, which the CDK logs before each retry, and
        # `oauth2/advertiser/get` carries the app secret in its query string.
        raise aiohttp.ConnectionTimeoutError(
            f"Connection timeout to host {request.url.with_query(None)}"
        ) from None
    if resp.status != 200:
        return resp

    head = b""
    try:
        while len(head) <= ENVELOPE_MAX_BYTES and (
            chunk := await resp.content.read(ENVELOPE_MAX_BYTES + 1 - len(head))
        ):
            head += chunk
    except BaseException:
        # The CDK only releases a response the middleware returns.
        resp.close()
        raise

    if len(head) <= ENVELOPE_MAX_BYTES:  # the whole body
        code = _envelope_code(head)
        if code in RATE_LIMITED_CODES:
            resp.status = 429
        elif code in SERVER_ERROR_CODES or code == MAINTENANCE:
            resp.status = 503
    resp.content = cast(aiohttp.StreamReader, _ReadAhead(head, resp.content))
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


class _ServerErrorBudget:
    """One request's `should_retry`. The CDK's `attempt` counts throttles too,
    and a request can be throttled hundreds of times while TikTok's limit
    resets, so the budget counts only the server errors, and never runs out
    during maintenance. The CDK retries a 5xx at once, so `_with_tiktok_status`
    waits out `backoff()` before each attempt: after a server error or
    maintenance answer, once."""

    def __init__(self) -> None:
        self.server_errors = 0
        self.maintenance_answers = 0
        self._backoff = 0.0

    def __call__(self, status: int, headers: Headers, body: bytes, attempt: int) -> bool:
        if _envelope_code(body) == MAINTENANCE:
            self.maintenance_answers += 1
            self._backoff = _backoff_seconds(self.maintenance_answers)
            return True
        self.server_errors += 1
        self._backoff = _backoff_seconds(self.server_errors)
        return self.server_errors < SERVER_ERROR_ATTEMPTS

    async def backoff(self) -> None:
        wait, self._backoff = self._backoff, 0.0
        if wait:
            await asyncio.sleep(wait)


def _backoff_seconds(retries: int) -> float:
    return SERVER_ERROR_BACKOFF_SECONDS * 2 ** min(retries - 1, MAX_BACKOFF_DOUBLINGS)


# The budget of the request in flight in this task, for the middleware, which
# sees each attempt but not the request it belongs to.
_request_budget: ContextVar[_ServerErrorBudget | None] = ContextVar(
    "_request_budget", default=None
)


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
        budget = _ServerErrorBudget()
        token = _request_budget.set(budget)
        try:
            return await self.http.request(
                log, f"{self.base_url}/{path}", params=params, should_retry=budget
            )
        finally:
            _request_budget.reset(token)

    async def request_stream(
        self, log: Logger, path: str, params: dict[str, Any]
    ) -> tuple[Headers, Callable[[], AsyncGenerator[bytes, None]]]:
        await self._wait_for_turn()
        budget = _ServerErrorBudget()
        token = _request_budget.set(budget)
        try:
            return await self.http.request_stream(
                log, f"{self.base_url}/{path}", params=params, should_retry=budget
            )
        finally:
            _request_budget.reset(token)


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
    log: Logger,
    client: TikTokClient,
    path: str,
    params: dict[str, Any],
    data_model: type[DataT],
) -> DataT:
    body = await client.request(log, path, params)
    envelope = TikTokEnvelope.model_validate_json(body)
    if envelope.code != 0:
        raise TikTokAPIError(envelope.code, envelope.message)
    return TikTokResponse[data_model].model_validate_json(body).data


async def paginate[ItemT: BaseModel](
    log: Logger,
    client: TikTokClient,
    path: str,
    params: dict[str, Any],
    page_size: int,
    model: type[ItemT],
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
