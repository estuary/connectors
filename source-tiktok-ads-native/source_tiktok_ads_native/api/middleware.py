from collections.abc import AsyncIterator
from logging import Logger
from typing import Any, cast

import aiohttp
from estuary_cdk import Mixin
from pydantic import ValidationError

from ..models import TikTokEnvelope

# TikTok answers throttling, maintenance and its own server errors with HTTP 200
# and a `code` in the body. `TikTokStatusMixin` gives those responses the HTTP
# status TikTok meant, so the CDK's retries and rate limiter handle them: a 429
# slows the rate limiter, a 503 waits out the CDK's backoff, and both are retried
# until TikTok accepts the request.
RATE_LIMITED_CODES = frozenset({40016, 40100, 40132, 40133})
# TikTok's documented server errors; maintenance (60001), which TikTok says to
# wait out and Airbyte's connector warns can last hours; then transient ones
# TikTok doesn't document that Airbyte's connector met in production: 51001 and
# 51043 "internal service timeout" (airbytehq/airbyte#89015), 51002 "internal
# error" (#85187), 51004 "Internal Service Error." and 51041 "Page is loading,
# please try again later." (#84290). Maintenance is a 503 rather than a 429
# because the CDK's rate limiter, after any 429, recovers by only 0.01% per
# success.
SERVER_ERROR_CODES = frozenset(
    {50000, 50002, 51305, 60001, 51001, 51002, 51004, 51041, 51043}
)


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


async def _strip_query_from_timeouts(
    request: aiohttp.ClientRequest, handler: aiohttp.ClientHandlerType
) -> aiohttp.ClientResponse:
    """An aiohttp middleware naming a connect timeout's URL without its query
    string. aiohttp names the whole URL, which the CDK logs before each retry,
    and `oauth2/advertiser/get` carries the app secret in its query string."""
    try:
        return await handler(request)
    except aiohttp.ConnectionTimeoutError:
        raise aiohttp.ConnectionTimeoutError(
            f"Connection timeout to host {request.url.with_query(None)}"
        ) from None


async def _with_tiktok_status(
    request: aiohttp.ClientRequest, handler: aiohttp.ClientHandlerType
) -> aiohttp.ClientResponse:
    """An aiohttp middleware giving each HTTP 200 the status TikTok meant."""
    resp = await handler(request)
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
        elif code in SERVER_ERROR_CODES:
            resp.status = 503
    resp.content = cast(aiohttp.StreamReader, _ReadAhead(head, resp.content))
    return resp


class TikTokStatusMixin(Mixin):
    """Installs `_strip_query_from_timeouts` and `_with_tiktok_status` on the
    CDK's HTTP session. It must follow the CDK's connector base class among the
    connector's bases: the CDK enters mixins in method resolution order, and
    `HTTPMixin` opens the session this one replaces."""

    inner: aiohttp.ClientSession

    async def _mixin_enter(self, log: Logger) -> None:
        cdk_session = self.inner
        self.inner = aiohttp.ClientSession(
            timeout=cdk_session.timeout,
            headers=cdk_session.headers,
            # The first wraps the second, so a timeout inside it is rewritten.
            middlewares=(_strip_query_from_timeouts, _with_tiktok_status),
        )
        await cdk_session.close()

    async def _mixin_exit(self, log: Logger) -> None:
        pass  # `HTTPMixin` closes `self.inner`
