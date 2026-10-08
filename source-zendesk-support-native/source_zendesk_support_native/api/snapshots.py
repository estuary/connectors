from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.http import HTTPSession

from ..models import (
    FullRefreshResource,
    FullRefreshResponse,
    FullRefreshOffsetPaginatedResponse,
    FullRefreshCursorPaginatedResponse,
)

from .shared import (
    CURSOR_PAGINATION_PAGE_SIZE,
    url_base,
)


async def snapshot_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[FullRefreshResponse],
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(subdomain)}/{path}"

    response = response_model.model_validate_json(
        await http.request(log, url)
    )

    for resource in response.resources:
        yield resource


async def snapshot_offset_paginated_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[FullRefreshOffsetPaginatedResponse],
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(subdomain)}/{path}"
    page_num = 1
    params: dict[str, str | int] = {
        "per_page": CURSOR_PAGINATION_PAGE_SIZE,
        "page": page_num,
    }

    while True:
        response = response_model.model_validate_json(
            await http.request(log, url, params=params)
        )

        for resource in response.resources:
            yield resource

        if not response.next_page:
            return

        page_num += 1

        params["page"] = page_num


async def snapshot_cursor_paginated_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[FullRefreshCursorPaginatedResponse],
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(subdomain)}/{path}"
    params: dict[str, str | int] = {
        "page[size]": CURSOR_PAGINATION_PAGE_SIZE,
    }

    while True:
        response = response_model.model_validate_json(
            await http.request(log, url, params=params)
        )

        for resource in response.resources:
            yield resource

        if not response.meta.has_more:
            return

        if response.meta.after_cursor:
            params["page[after]"] = response.meta.after_cursor
