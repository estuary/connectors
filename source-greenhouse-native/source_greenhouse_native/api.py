from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    GreenhouseResource,
)

from .shared import (
    extract_next_cursor,
    updated_at_param,
    now,
)

GREENHOUSE_BASE_URL = "https://harvest.greenhouse.io/v3"

EVENTUAL_CONSISTENCY_LAG = timedelta(minutes=5)
MAX_LIMIT = 500


async def _fetch_page(
    http: HTTPSession,
    model: type[GreenhouseResource],
    log: Logger,
    url: str,
    params: dict[str, str | int],
) -> tuple[AsyncGenerator[GreenhouseResource, None], str | None]:
    """Fetch a single page of results from a Greenhouse v3 endpoint.

    Returns a streaming async generator of resources and the next cursor
    (or None if no more pages)."""
    headers, body = await http.request_stream(log, url, params=params)
    next_cursor = extract_next_cursor(headers)

    async def docs() -> AsyncGenerator[GreenhouseResource, None]:
        async for doc in IncrementalJsonProcessor(body(), "item", model):
            yield doc

    return docs(), next_cursor


async def _paginate_greenhouse_resources(
    http: HTTPSession,
    model: type[GreenhouseResource],
    log: Logger,
    params: dict[str, str | int],
) -> AsyncGenerator[GreenhouseResource, None]:
    """Paginate through all pages of a Greenhouse v3 endpoint."""
    url = f"{GREENHOUSE_BASE_URL}/{model.path}"

    while True:
        docs, next_cursor = await _fetch_page(http, model, log, url, params)

        async for doc in docs:
            yield doc

        if not next_cursor:
            return

        # Greenhouse API documentation states that no other parameters
        # should be used when using the "cursor" parameter.
        params = {"cursor": next_cursor}


async def fetch_greenhouse_resources(
    http: HTTPSession,
    model: type[GreenhouseResource],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[GreenhouseResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    start = log_cursor
    end = now() - EVENTUAL_CONSISTENCY_LAG

    if start >= end:
        return

    params: dict[str, str | int] = {
        "updated_at": updated_at_param(gt=start, lte=end),
        "per_page": MAX_LIMIT,
    }

    most_recent_cursor = log_cursor

    async for resource in _paginate_greenhouse_resources(http, model, log, params):
        if (
            resource.updated_at <= start or
            resource.updated_at > end
        ):
            continue

        yield resource
        most_recent_cursor = max(most_recent_cursor, resource.updated_at)

    if most_recent_cursor > log_cursor:
        yield most_recent_cursor


async def backfill_greenhouse_resources(
    http: HTTPSession,
    model: type[GreenhouseResource],
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[GreenhouseResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    url = f"{GREENHOUSE_BASE_URL}/{model.path}"
    params: dict[str, str | int] = {}

    if page is not None:
        assert isinstance(page, str)
        # Greenhouse API documentation states that no other parameters
        # should be used when using the "cursor" parameter.
        params["cursor"] = page
    else:
        params["updated_at"] = updated_at_param(gte=start_date, lte=cutoff)
        params["per_page"] = MAX_LIMIT

    docs, next_cursor = await _fetch_page(http, model, log, url, params)

    async for doc in docs:
        yield doc

    if next_cursor:
        yield next_cursor
