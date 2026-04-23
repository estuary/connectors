from datetime import datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator
from urllib.parse import urlparse, parse_qs

from estuary_cdk.capture.common import LogCursor, PageCursor
import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.http import Headers, HTTPError, HTTPSession

from .models import (
    OutreachResource,
    OutreachResponse,
    CursorField,
)
from .shared import now


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


def _is_502_bad_gateway_response(
    status: int,
    body: str,
) -> bool:
    return status == 502 and "Bad Gateway" in body


def _is_500_network_client_error(
    status: int,
    body: str,
) -> bool:
    return status == 500 and "Network Client Error" in body


def _should_retry_request(
    status: int,
    headers: Headers,
    body: bytes,
    attempt: int,
) -> bool:
    # If the response is a specific 500 or 502 error, that could mean
    # too much data was requested and a timeout was reached before
    # the API server sent a response. To get around these timeouts,
    # the connector should make a new request for less data.
    body_str = body.decode("utf-8")
    return not (
        _is_502_bad_gateway_response(status, body_str)
        or _is_500_network_client_error(status, body_str)
    )


def _build_query_params(
    cursor_field: str,
    start_dt: datetime,
    end_dt: datetime | None,
    extra_params: dict[str, str | int | bool] | None,
) -> dict[str, str | int | bool]:
    params: dict[str, str | int | bool] = {
        "sort": cursor_field,
        "newFilterSyntax": "true",
        f"filter[{cursor_field}][gte]": _dt_to_str(start_dt),
    }

    if end_dt is not None:
        params[f"filter[{cursor_field}][lte]"] = _dt_to_str(end_dt)

    if extra_params:
        params.update(extra_params)

    return params


def _extract_resource_cursor(
    resource: OutreachResource,
    cursor_field: str,
) -> datetime:
    return _str_to_dt(getattr(resource, 'attributes')[cursor_field])


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
            if (
                _is_502_bad_gateway_response(err.code, err.message)
                or _is_500_network_client_error(err.code, err.message)
            ):
                log.debug(f"Received {err.code} response (will retry with a smaller page size).", {
                    "url": url,
                    "params": params,
                    "err.status": err.code,
                    "err.message": err.message,
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

    params = _build_query_params(cursor_field, start_date, cutoff, extra_params)

    if page is not None:
        assert isinstance(page, str)
        params[NEXT_PAGE_QUERY_PARAMETER] = page

    response = await _do_request(http, url, params, log)

    for resource in response.data:
        resource_dt = _extract_resource_cursor(resource, cursor_field)

        if resource_dt > cutoff:
            continue

        yield resource

    next_page_cursor = _extract_page_cursor(response.links)

    if next_page_cursor:
        yield next_page_cursor


async def fetch_resources(
    http: HTTPSession,
    path: str,
    extra_params: dict[str, str | int | bool] | None,
    cursor_field: CursorField,
    horizon: timedelta | None,
    window_size: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[OutreachResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    horizon_cutoff = now() - horizon if horizon else now()

    if log_cursor >= horizon_cutoff:
        return

    # window_size bounds how far forward a single invocation will advance.
    # The Outreach API sometimes returns results unsorted within a query, so
    # we process records in explicit [lower, upper] date windows and trust
    # the set of records returned for that window rather than their order.
    max_upper_bound = log_cursor + window_size
    upper_bound = min(max_upper_bound, horizon_cutoff)

    url = f"{API}/{path}"
    params = _build_query_params(cursor_field, log_cursor, upper_bound, extra_params)

    max_seen_dt = log_cursor

    while True:
        response = await _do_request(http, url, params, log)

        for resource in response.data:
            resource_dt = _extract_resource_cursor(resource, cursor_field)

            # The API sometimes ignores the server-side upper-bound filter.
            # Skip such records so they don't advance max_seen_dt past the
            # queried window; the next invocation will fetch them in its
            # own window.
            if resource_dt > upper_bound:
                log.info(f"Outreach API returned record {resource.id} with {cursor_field}={resource_dt} beyond the requested upper bound {upper_bound}.")
                continue

            if resource_dt > max_seen_dt:
                max_seen_dt = resource_dt

            if resource_dt > log_cursor and cache.should_yield(path, resource.id, resource_dt):
                yield resource

        next_page_cursor = _extract_page_cursor(response.links)

        if not next_page_cursor:
            break

        params[NEXT_PAGE_QUERY_PARAMETER] = next_page_cursor

    # Were there any records within the date window? If so, yield the
    # most recent datetime cursor from them.
    if max_seen_dt > log_cursor:
        yield max_seen_dt
    # Otherwise, did we check a max-sized date window? If so, yield
    # the upper bound to use as the lower bound next iteration to avoid
    # repeatedly checking an empty, max-sized date window.
    elif upper_bound >= max_upper_bound:
        yield upper_bound
