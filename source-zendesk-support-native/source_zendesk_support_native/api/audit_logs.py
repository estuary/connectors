from datetime import datetime, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    AuditLog,
    AuditLogsResponse,
)

from .shared import (
    CURSOR_PAGINATION_PAGE_SIZE,
    INCREMENTAL_LAG,
    url_base,
    _dt_to_str,
)


async def fetch_audit_logs(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[AuditLog | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{url_base(subdomain)}/audit_logs"

    horizon = datetime.now(tz=UTC) - INCREMENTAL_LAG

    if horizon <= log_cursor:
        return

    start = _dt_to_str(log_cursor)
    end = _dt_to_str(horizon)

    params = {
        "page[size]": CURSOR_PAGINATION_PAGE_SIZE,
        "sort": "created_at",
        "filter[created_at][]": [start, end],
    }

    last_seen_dt = log_cursor

    while True:
        response = AuditLogsResponse.model_validate_json(
            await http.request(log, url, params=params)
        )

        if (
            last_seen_dt > log_cursor
            and response.resources
            and response.resources[0].created_at > last_seen_dt
        ):
            yield last_seen_dt


        for audit_log in response.resources:
            if audit_log.created_at > last_seen_dt:
                last_seen_dt = audit_log.created_at
            
            if audit_log.created_at > log_cursor:
                yield audit_log

        if not response.meta.has_more:
            if last_seen_dt > log_cursor:
                yield last_seen_dt
            break

        if response.meta.after_cursor:
            params["page[after]"] = response.meta.after_cursor


async def backfill_audit_logs(
    http: HTTPSession,
    subdomain: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[AuditLog | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    url = f"{url_base(subdomain)}/audit_logs"

    start = _dt_to_str(start_date)
    end = _dt_to_str(cutoff)

    params = {
        "page[size]": CURSOR_PAGINATION_PAGE_SIZE,
        "sort": "created_at",
        "filter[created_at][]": [start, end],
    }

    if page is not None:
        assert isinstance(page, str)
        params["page[after]"] = page

    response = AuditLogsResponse.model_validate_json(
        await http.request(log, url, params=params)
    )

    for audit_log in response.resources:
        yield audit_log

    if not response.meta.has_more:
        return

    if response.meta.after_cursor:
        yield response.meta.after_cursor
