import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from logging import Logger
from typing import Any

from estuary_cdk.http import Headers, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel

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
        return await self.http.request(log, f"{self.base_url}/{path}", params=params)

    async def request_stream(
        self, log: Logger, path: str, params: dict[str, Any]
    ) -> tuple[Headers, Callable[[], AsyncGenerator[bytes, None]]]:
        await self._wait_for_turn()
        return await self.http.request_stream(
            log, f"{self.base_url}/{path}", params=params
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
    validation_context: object | None = None,
) -> AsyncGenerator[ItemT, None]:
    """Every item of a paged `data.list` response, validated with
    `validation_context`. `check_headers` sees each page's headers before any of
    its items is emitted."""
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
            validation_context=validation_context,
        )
        async for item in processor:
            yield item

        remainder = processor.get_remainder()
        if remainder.code != 0:
            raise TikTokAPIError(remainder.code, remainder.message)
        if page >= remainder.page_info().total_page:
            return
        page += 1
