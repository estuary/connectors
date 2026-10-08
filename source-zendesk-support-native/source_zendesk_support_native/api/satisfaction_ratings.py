from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    ZendeskResource,
    SatisfactionRatingsResponse,
)

from .shared import (
    CURSOR_PAGINATION_PAGE_SIZE,
    INCREMENTAL_LAG,
    url_base,
    dt_to_s,
)

MAX_SATISFACTION_RATINGS_WINDOW_SIZE = timedelta(days=30)


async def _fetch_satisfaction_ratings_between(
    http: HTTPSession,
    subdomain: str,
    start: int,
    end: int,
    log: Logger,
) -> AsyncGenerator[ZendeskResource, None]:
    url = f"{url_base(subdomain)}/satisfaction_ratings"

    params: dict[str, str | int] = {
        "start_time": start,
        "end_time": end,
        "page[size]": CURSOR_PAGINATION_PAGE_SIZE,
    }

    while True:
        response = SatisfactionRatingsResponse.model_validate_json(
            await http.request(log, url, params=params)
        )

        for satisfaction_rating in response.resources:
            yield satisfaction_rating

        if not response.meta.has_more:
            break

        if response.meta.after_cursor:
            params["page[after]"] = response.meta.after_cursor


async def fetch_satisfaction_ratings(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    end = min(
        datetime.now(tz=UTC) - INCREMENTAL_LAG,
        log_cursor + MAX_SATISFACTION_RATINGS_WINDOW_SIZE,
    )

    if end <= log_cursor:
        return

    generator = _fetch_satisfaction_ratings_between(
        http=http,
        subdomain=subdomain,
        start=dt_to_s(log_cursor),
        end=dt_to_s(end),
        log=log,
    )

    async for satisfaction_rating in generator:
        yield satisfaction_rating

    yield end


async def backfill_satisfaction_ratings(
    http: HTTPSession,
    subdomain: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ZendeskResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    cutoff_ts = dt_to_s(cutoff)

    if page is None:
        start = dt_to_s(start_date)
    else:
        assert isinstance(page, int)
        start = page

    if start >= cutoff_ts:
        return

    end = min(cutoff_ts, start + int(MAX_SATISFACTION_RATINGS_WINDOW_SIZE.total_seconds()))

    generator = _fetch_satisfaction_ratings_between(
        http=http,
        subdomain=subdomain,
        start=start,
        end=end,
        log=log,
    )

    async for satisfaction_rating in generator:
        yield satisfaction_rating

    yield end
