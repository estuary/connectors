from datetime import datetime
from logging import Logger
from typing import Any, AsyncGenerator
from urllib.parse import urlparse, parse_qs

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import Headers, HTTPError, HTTPSession

from .models import (
    OutreachResource,
    OutreachResponse,
    CursorField,
)


DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


API = "https://api.outreach.io/api/v2"
MAX_PAGE_SIZE = 1000
MIN_PAGE_SIZE = 1
NEXT_PAGE_QUERY_PARAMETER = "page[after]"

def _dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def _str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def _extract_page_cursor(links: OutreachResponse.Links | None) -> str | None:
    if links is None or links.next is None:
        return None

    parsed_url = urlparse(links.next)
    params = parse_qs(parsed_url.query)

    cursor_list = params.get(NEXT_PAGE_QUERY_PARAMETER)
    return cursor_list[0] if cursor_list else None


def _should_retry_request(
    status: int,
    headers: Headers,
    body: bytes,
    attempt: int,
) -> bool:
    # If the response has a 502 status code, that could mean that
    # too much data was requested and a timeout was reached before
    # the API server sent a response. To get around these timeouts,
    # the connector should make a new request for less data.
    return status != 502


def _build_query_params(
    cursor_field: str,
    start_dt: datetime,
    extra_params: dict[str, str | int | bool] | None,
) -> dict[str, str | int | bool]:
    params: dict[str, str | int | bool] = {
        "sort": cursor_field,
        "newFilterSyntax": "true",
        f"filter[{cursor_field}][gte]": _dt_to_str(start_dt),
    }

    if extra_params:
        params.update(extra_params)

    return params


def _extract_resource_cursor(
    resource: OutreachResource,
    cursor_field: str,
) -> datetime:
    return _str_to_dt(getattr(resource, 'attributes')[cursor_field])


# _raise_if_unordered performs a sanity check to confirm
# the Outreach API returned sorted records.
def _raise_if_unordered(
    resource_id: int,
    previous_dt: datetime | None,
    resource_dt: datetime,
) -> None:
    if previous_dt and resource_dt < previous_dt:
        raise RuntimeError(f"Outreach API returned records out of order for {resource_id} with {resource_dt} < {previous_dt}.")


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

    params = _build_query_params(cursor_field, start_date, extra_params)

    if page is not None:
        assert isinstance(page, str)
        params[NEXT_PAGE_QUERY_PARAMETER] = page

    response = await _do_request(http, url, params, log)

    previous_dt: datetime | None = None

    for resource in response.data:
        resource_dt = _extract_resource_cursor(resource, cursor_field)

        _raise_if_unordered(resource.id, previous_dt, resource_dt)

        if resource_dt >= cutoff:
            return

        previous_dt = resource_dt
        yield resource

    next_page_cursor = _extract_page_cursor(response.links)

    if next_page_cursor:
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

    params = _build_query_params(cursor_field, log_cursor, extra_params)

    last_seen_dt = log_cursor

    while True:
        response = await _do_request(http, url, params, log)

        if (
            last_seen_dt > log_cursor
            and response.data
            and _extract_resource_cursor(response.data[0], cursor_field) > last_seen_dt
        ):
            yield last_seen_dt

        for resource in response.data:
            resource_dt = _extract_resource_cursor(resource, cursor_field)

            _raise_if_unordered(resource.id, last_seen_dt, resource_dt)

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
            params[NEXT_PAGE_QUERY_PARAMETER] = next_page_cursor
