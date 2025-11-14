from datetime import datetime
from logging import Logger
from typing import Any, AsyncGenerator
from urllib.parse import urlparse, parse_qs

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError, HTTPSession

from .models import (
    OutreachResource,
    OutreachResponse,
    CursorField,
)


DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


API = "https://api.outreach.io/api/v2"
MAX_PAGE_SIZE = 1000
MIN_PAGE_SIZE = 1


def _dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def _str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def _extract_page_cursor(links: OutreachResponse.Links | None) -> str | None:
    if links is None or links.next is None:
        return None

    parsed_url = urlparse(links.next)
    params = parse_qs(parsed_url.query)

    cursor_list = params.get('page[after]')
    return cursor_list[0] if cursor_list else None


def _should_retry_request(
    status: int,
    headers: dict[str, Any],
    body: bytes,
    attempt: int,
) -> bool:
    # If the response has a 502 status code, that could mean that
    # too much data was requested and a timeout was reached before
    # the API server sent a response. To get around these timeouts,
    # the connector should make a new request for less data.
    return status != 502


async def _do_request(
    http: HTTPSession,
    url: str,
    params: dict[str, str | int],
    log: Logger,
) -> OutreachResponse:
    # params are copied to avoid mutating the passed in dictionary.
    params = params.copy()

    page_size = MAX_PAGE_SIZE
    while page_size >= MIN_PAGE_SIZE:
        params["page[size]"] = page_size
        try:
            return OutreachResponse.model_validate_json(
                await http.request(log, url, params=params, should_retry=_should_retry_request)
            )
        except HTTPError as err:
            if err.code == 502 and "Bad Gateway" in err.message:
                log.debug("Received 502 Bad Gateway response (will retry with a smaller page size).", {
                    "url": url,
                    "params": params,
                })
                page_size = page_size // 2
            else:
                raise

    raise Exception(f"Request to {url} failed with smallest possible page size. Query parameters were {params}")


async def backfill_resources(
    http: HTTPSession,
    path: str,
    extra_params: dict[str, str | int | bool] | None,
    cursor_field: CursorField,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[OutreachResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    url = f"{API}/{path}"

    params: dict[str, str | int] = {
        "sort": cursor_field,
        "newFilterSyntax": "true",
        f"filter[{cursor_field}][gte]": _dt_to_str(start_date),
    }

    if extra_params:
        params.update(extra_params)

    if page is not None:
        assert isinstance(page, str)
        params["page[after]"] = page

    response = await _do_request(http, url, params, log)

    for resource in response.data:
        resource_dt = _str_to_dt(getattr(resource, 'attributes')[cursor_field])
        if resource_dt >= cutoff:
            return

        yield resource

    next_page_cursor = _extract_page_cursor(response.links)

    if not next_page_cursor:
        return
    else:
        yield next_page_cursor


async def fetch_resources(
    http: HTTPSession,
    path: str,
    extra_params: dict[str, str | int | bool] | None,
    cursor_field: CursorField,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[OutreachResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{API}/{path}"

    params: dict[str, str | int | bool] = {
        "sort": cursor_field,
        "newFilterSyntax": "true",
        f"filter[{cursor_field}][gte]": _dt_to_str(log_cursor),
    }

    if extra_params:
        params.update(extra_params)

    last_seen_dt = log_cursor

    while True:
        response = await _do_request(http, url, params, log)

        if (
            last_seen_dt > log_cursor
            and response.data
            and _str_to_dt(getattr(response.data[0], 'attributes')[cursor_field]) > last_seen_dt
        ):
            yield last_seen_dt

        for resource in response.data:
            resource_dt = _str_to_dt(getattr(resource, 'attributes')[cursor_field])
            if resource_dt > last_seen_dt:
                last_seen_dt = resource_dt

            if resource_dt > log_cursor:
                yield resource

        next_page_cursor = _extract_page_cursor(response.links)

        if not next_page_cursor:
            if last_seen_dt > log_cursor:
                yield last_seen_dt
            break
        else:
            params["page[after]"] = next_page_cursor
