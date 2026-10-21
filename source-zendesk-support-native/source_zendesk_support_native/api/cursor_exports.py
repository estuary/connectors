import base64
from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import Headers, HeadersAndBodyGenerator, HTTPSession, HTTPError
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from ..models import (
    TimestampedResource,
    TicketsResponse,
    UsersResponse,
    INCREMENTAL_CURSOR_EXPORT_TYPES,
)

from .shared import (
    url_base,
    dt_to_s,
    s_to_dt,
    is_timestamp,
)

MAX_INCREMENTAL_EXPORT_PAGE_SIZE = 1000
MIN_PAGE_SIZE = 1


def _base64_decode(encoded: str) -> str:
    return base64.b64decode(encoded).decode("utf-8")


def _base64_encode(decoded: str) -> str:
    return base64.b64encode(decoded.encode("utf-8")).decode("utf-8")


def _is_upstream_timeout(
    status: int,
    body: str
) -> bool:
    return status == 504 and "upstream request timeout" in body


def _should_retry_incremental_cursor_export_response(
    status: int,
    headers: Headers,
    body: bytes,
    attempt: int,
) -> bool:
    # If the response has a 504 status code and a body stating
    # an upstream timeout was reached, that usually means that
    # too much data was requested and a timeout was reached before
    # the API server sent a response. To get around these timeouts,
    # the connector should make a new request for less data.
    return not _is_upstream_timeout(status, str(body))


async def _do_incremental_cursor_export_request(
    http: HTTPSession,
    url: str,
    params: dict[str, str | int],
    log: Logger,
) -> HeadersAndBodyGenerator:
    params = params.copy()

    page_size = MAX_INCREMENTAL_EXPORT_PAGE_SIZE
    while page_size >= MIN_PAGE_SIZE:
        params["per_page"] = page_size

        try:
            return await http.request_stream(log, url, params=params, should_retry=_should_retry_incremental_cursor_export_response)
        except HTTPError as err:
            if _is_upstream_timeout(err.code, err.message):
                log.debug("Received 504 upstream timeout response (will retry with a smaller page size)", {
                    "url": url,
                    "params": params,
                })
                page_size = page_size // 2
            else:
                raise

    raise Exception(f"Request to {url} failed with smallest possible page size. Query parameters were {params}")


async def _fetch_incremental_cursor_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: INCREMENTAL_CURSOR_EXPORT_TYPES,
    start_date: datetime | None,
    cursor: str | None,
    log: Logger,
    sideload_params: dict[str, str] | None = None,
) -> AsyncGenerator[TimestampedResource | str, None]:
    url = f"{url_base(subdomain)}/incremental"
    match name:
        case "tickets":
            url += "/tickets/cursor"
            response_model = TicketsResponse
        case "users":
            url += "/users/cursor"
            response_model = UsersResponse
        case _:
            raise RuntimeError(f"Unknown incremental cursor pagination resource type {name}.")

    params: dict[str, str | int] = {}

    if sideload_params:
        params.update(sideload_params)

    if cursor is None:
        assert isinstance(start_date, datetime)
        params["start_time"] = dt_to_s(start_date)
    else:
        params["cursor"] = _base64_encode(cursor)

    while True:
        _, body = await _do_incremental_cursor_export_request(http, url, params, log)
        processor = IncrementalJsonProcessor(
            body(),
            f"{name}.item",
            TimestampedResource,
            response_model,
        )

        async for resource in processor:
            yield resource

        remainder = processor.get_remainder()
        next_page_cursor, end_of_stream = remainder.after_cursor, remainder.end_of_stream
        
        if not next_page_cursor:
            return

        yield _base64_decode(next_page_cursor)

        if end_of_stream:
            return

        if "start_time" in params:
            del params["start_time"]

        params["cursor"] = next_page_cursor


async def fetch_incremental_cursor_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: INCREMENTAL_CURSOR_EXPORT_TYPES,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, tuple)
    cursor = log_cursor[0]
    assert isinstance(cursor, str)

    start_date: datetime | None = None

    if is_timestamp(cursor):
        start_date = s_to_dt(int(cursor))
        cursor = None

    generator = _fetch_incremental_cursor_export_resources(http, subdomain, name, start_date, cursor, log)

    async for result in generator:
        if isinstance(result, str):
            yield (result,)
        else:
            yield result


async def backfill_incremental_cursor_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: INCREMENTAL_CURSOR_EXPORT_TYPES,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TimestampedResource | PageCursor, None]:
    if page is not None:
        assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    generator = _fetch_incremental_cursor_export_resources(http, subdomain, name, start_date, page, log)

    async for result in generator:
        if isinstance(result, str) or result.updated_at < cutoff:
            yield result
        else:
            return
