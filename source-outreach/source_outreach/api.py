from datetime import datetime
from logging import Logger
from typing import AsyncGenerator
from urllib.parse import urlparse, parse_qs

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .models import (
    OutreachResource,
    OutreachResponse,
    CursorField,
)


DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


API = "https://api.outreach.io/api/v2"
MAX_PAGE_SIZE = 1000


def _dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def _str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def _extract_page_cursor(links: OutreachResponse.Links | None) -> str | None:
    if links is None or links.next is None:
        return None

    parsed_url = urlparse(links.next)
    params: dict[str, str] = parse_qs(parsed_url.query) #type: ignore

    return params.get('page[after]')


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
        "page[size]": MAX_PAGE_SIZE,
    }

    if extra_params:
        params.update(extra_params)

    if page is not None:
        assert isinstance(page, str)
        params["page[after]"] = page

    response = OutreachResponse.model_validate_json(
        await http.request(log, url, params=params)
    )

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
        "page[size]": MAX_PAGE_SIZE,
    }

    if extra_params:
        params.update(extra_params)

    last_seen_dt = log_cursor

    while True:
        response = OutreachResponse.model_validate_json(
            await http.request(log, url, params=params)
        )

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
