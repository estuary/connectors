from datetime import datetime, UTC
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    TimestampedResource,
    ClientSideIncrementalOffsetPaginatedResponse,
    ClientSideIncrementalCursorPaginatedResponse,
)

from .shared import (
    CURSOR_PAGINATION_PAGE_SIZE,
    INCREMENTAL_LAG,
    url_base,
)


async def fetch_client_side_incremental_offset_paginated_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[ClientSideIncrementalOffsetPaginatedResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{url_base(subdomain)}/{path}"
    page_num = 1
    params: dict[str, str | int] = {
        "per_page": CURSOR_PAGINATION_PAGE_SIZE,
        "page": page_num,
    }

    last_seen = log_cursor
    horizon = datetime.now(tz=UTC) - INCREMENTAL_LAG

    if horizon <= log_cursor:
        return

    while True:
        response = response_model.model_validate_json(
            await http.request(log, url, params=params)
        )

        for resource in response.resources:
            # Skip records Zendesk may not have finished making visible.
            if resource.updated_at >= horizon:
                continue

            if resource.updated_at > log_cursor:
                yield resource

            if resource.updated_at > last_seen:
                last_seen = resource.updated_at

        if not response.next_page:
            break

        page_num += 1
        params["page"] = page_num

    if last_seen > log_cursor:
        yield last_seen


async def fetch_client_side_incremental_cursor_paginated_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    additional_query_params: dict[str, Any] | None,
    response_model: type[ClientSideIncrementalCursorPaginatedResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{url_base(subdomain)}/{path}"

    params: dict[str, str | int] = {
        "page[size]": CURSOR_PAGINATION_PAGE_SIZE,
    }

    if additional_query_params:
        params.update(additional_query_params)

    last_seen = log_cursor
    horizon = datetime.now(tz=UTC) - INCREMENTAL_LAG

    if horizon <= log_cursor:
        return

    while True:
        response = response_model.model_validate_json(
            await http.request(log, url, params=params)
        )

        for resource in response.resources:
            # Skip records Zendesk may not have finished making visible.
            if resource.updated_at >= horizon:
                continue

            if resource.updated_at > log_cursor:
                yield resource

            if resource.updated_at > last_seen:
                last_seen = resource.updated_at

        if not response.meta.has_more:
            break

        if response.meta.after_cursor:
            params["page[after]"] = response.meta.after_cursor

    if last_seen > log_cursor:
        yield last_seen
