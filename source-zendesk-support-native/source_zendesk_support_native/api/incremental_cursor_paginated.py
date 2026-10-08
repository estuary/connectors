from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    ZendeskResource,
    IncrementalCursorPaginatedResponse,
    FilterParam,
)

from .shared import (
    CURSOR_PAGINATION_PAGE_SIZE,
    url_base,
    _dt_to_s,
    _dt_to_str,
    _str_to_dt,
)

# Zendesk can record ticket metric events many minutes after the ticket change that
# produced them, stamped with that change's time. ticket_metric_events trails further
# behind the present so these late events surface before its cursor passes them.
TICKET_METRIC_EVENTS_LAG = timedelta(minutes=30)


def _convert_log_cursor_for_filter_param(
    cursor: datetime,
    filter_param: FilterParam,
) -> str | int:
    match filter_param:
        case FilterParam.START_TIME:
            return _dt_to_s(cursor)
        case FilterParam.SINCE:
            return _dt_to_str(cursor)
        case _:
            raise RuntimeError(f"Unknown filter parameter type {filter}.")


async def fetch_incremental_cursor_paginated_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    filter_param: FilterParam,
    cursor_field: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    lag: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{url_base(subdomain)}/{path}"

    params: dict[str, str | int] = {
        filter_param: _convert_log_cursor_for_filter_param(log_cursor, filter_param),
        "page[size]": 1000 if "ticket_metric_events" in path else CURSOR_PAGINATION_PAGE_SIZE,
    }

    last_seen_dt = log_cursor
    last_checkpointed = log_cursor
    horizon = datetime.now(tz=UTC) - lag

    if horizon <= log_cursor:
        return

    while True:
        response = response_model.model_validate_json(
            await http.request(log, url, params=params)
        )

        if (
            last_seen_dt > last_checkpointed
            and response.resources
            and _str_to_dt(getattr(response.resources[0], cursor_field)) > last_seen_dt
        ):
            yield last_seen_dt
            last_checkpointed = last_seen_dt


        for resource in response.resources:
            resource_dt = _str_to_dt(getattr(resource, cursor_field))
            # Skip records Zendesk may not have finished making visible.
            if resource_dt >= horizon:
                continue

            if resource_dt > last_seen_dt:
                last_seen_dt = resource_dt

            if resource_dt > log_cursor:
                yield resource

        if not response.meta.has_more:
            if last_seen_dt > last_checkpointed:
                yield last_seen_dt
            break

        if response.meta.after_cursor:
            params["page[after]"] = response.meta.after_cursor


async def backfill_incremental_cursor_paginated_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    filter_param: FilterParam,
    cursor_field: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ZendeskResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    url = f"{url_base(subdomain)}/{path}"

    params: dict[str, str | int] = {
        filter_param: _convert_log_cursor_for_filter_param(start_date, filter_param),
        "page[size]": 1000 if "ticket_metric_events" in path else CURSOR_PAGINATION_PAGE_SIZE,
    }

    if page is not None:
        assert isinstance(page, str)
        params["page[after]"] = page

    response = response_model.model_validate_json(
        await http.request(log, url, params=params)
    )

    for resource in response.resources:
        resource_dt = _str_to_dt(getattr(resource, cursor_field))
        if resource_dt >= cutoff:
            return

        yield resource

    if not response.meta.has_more:
        return

    if response.meta.after_cursor:
        yield response.meta.after_cursor
