from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator
from urllib.parse import urlparse, parse_qs

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    AdaResource,
    ExportResponse,
    AdaResponse,
    TExportResource,
    EXPORT_RETENTION_LIMIT,
)

from .shared import (
    dt_to_str,
    str_to_dt,
    now
)

# The maximum export window size Ada supports is actually 60 days, but the connector
# uses a slightly smaller as it's max size so cursor management is a little easier to
# reason about.
MAX_EXPORT_WINDOW_SIZE = timedelta(days=59)
# Data from Ada's export APIs can take up to 48 hours to settle. We avoid
# capturing data from the past two days to avoid missing any due to this
# eventual consistency.
EVENTUAL_CONSISTENCY_LAG = timedelta(hours=48)


def base_url(bot_handle: str):
    return f"https://{bot_handle}.ada.support/api/v2"


def _extract_query_param(url: str, param_name: str) -> str:
    query_params = parse_qs(urlparse(url).query)

    param = query_params.get(param_name, None)
    if param is None:
        msg = f"Did not find a {param_name} parameter in URL: {url}"
        raise RuntimeError(msg)

    return param[0]


async def snapshot_resources(
    http: HTTPSession,
    bot_handle: str,
    model: type[AdaResource],
    log: Logger,
) -> AsyncGenerator[AdaResource, None]:
    url = f"{base_url(bot_handle)}/{model.path}"
    params: dict[str, str] = {}

    while True:
        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(),
            "data.item",
            AdaResource,
            AdaResponse,
        )

        async for doc in processor:
            yield doc

        remainder = processor.get_remainder()

        if not remainder.meta or not remainder.meta.next_page_uri:
            return

        cursor = _extract_query_param(remainder.meta.next_page_uri, "cursor")

        if not cursor:
            return

        params["cursor"] = cursor


async def _paginate_through_export_resources(
    http: HTTPSession,
    bot_handle: str,
    model: type[TExportResource],
    start: datetime,
    end: datetime,
    log: Logger,
) -> AsyncGenerator[TExportResource, None]:
    """Paginates through resources with cursor values between start (inclusive) and end (exclusive)."""
    url = f"{base_url(bot_handle)}/export/{model.path}"

    # While there is no explicit "sort" query parameter, results are sorted chronologically by either
    # "date_created" or "date_updated" depending on what lower & upper bound query parameters are provided.
    # 
    # Using "created_since" and "created_to" cause records to be sorted by "date_created".
    # Using "updated_since" and "updated_to" cause records to be sorted by "date_updated".
    params: dict[str, str] = {
        # The Ada API returns data inclusive of the lower bound and exclusive of the upper bound.
        model.lower_bound_param: dt_to_str(start),
        model.upper_bound_param: dt_to_str(end),
    }

    while True:
        _, body = await http.request_stream(log, url, params=params)
        processor = IncrementalJsonProcessor(
            body(),
            "data.item",
            model,
            ExportResponse[model],
        )

        async for item in processor:
            yield item

        remainder = processor.get_remainder()

        if not remainder.meta or not remainder.meta.next_page_uri:
            break

        next_lower_bound = _extract_query_param(remainder.meta.next_page_uri, model.lower_bound_param)

        params[model.lower_bound_param] = next_lower_bound


async def fetch_export_resources(
    http: HTTPSession,
    bot_handle: str,
    model: type[TExportResource],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TExportResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    start = log_cursor
    end = min(
        now() - EVENTUAL_CONSISTENCY_LAG,
        start + MAX_EXPORT_WINDOW_SIZE
    )

    if end <= start:
        return

    most_recent_cursor = log_cursor

    async for resource in _paginate_through_export_resources(
        http,
        bot_handle,
        model,
        start,
        # The API's upper query bound is exclusive. Add a second to ensure
        # records at `end` are included in the response.
        end + timedelta(seconds=1),
        log,
    ):
        # When querying the API, the lower bound is inclusive, so records
        # with cursor values at the log_cursor will be re-fetched. We ignore
        # these duplicates.
        if resource.cursor_date <= log_cursor:
            continue

        yield resource
        most_recent_cursor = max(most_recent_cursor, resource.cursor_date)

    if most_recent_cursor > log_cursor:
        yield most_recent_cursor
    elif end == start + MAX_EXPORT_WINDOW_SIZE:
        yield end


async def backfill_export_resources(
    http: HTTPSession,
    bot_handle: str,
    model: type[TExportResource],
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TExportResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    if page is not None:
        assert isinstance(page, str)
        start = str_to_dt(page)
    else:
        start = start_date

    earliest_allowed_start = now() - EXPORT_RETENTION_LIMIT
    if start < earliest_allowed_start:
        log.info(f"Start date is beyond the Ada API's retention limit of 12 months. Pulling start date up to 12 months ago.", {
            "start": start,
            "earliest_allowed_start": earliest_allowed_start,
        })

        start = earliest_allowed_start

    if start >= cutoff:
        return

    end = min(
        cutoff,
        start + MAX_EXPORT_WINDOW_SIZE,
    )

    most_recent_cursor = start

    async for resource in _paginate_through_export_resources(
        http,
        bot_handle,
        model,
        start,
        # The API's upper query bound is exclusive. Add a second to ensure
        # records at `end` are included in the response.
        end + timedelta(seconds=1),
        log,
    ):
        # When querying the API, the lower bound is inclusive, so records
        # with cursor values at `start` will be re-fetched. We ignore
        # these duplicates.
        if resource.cursor_date <= start:
            continue

        yield resource
        most_recent_cursor = max(most_recent_cursor, resource.cursor_date)

    if most_recent_cursor > start:
        yield dt_to_str(most_recent_cursor)
    else:
        yield dt_to_str(end)
