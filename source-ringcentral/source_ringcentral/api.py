from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator, Generic, NamedTuple, TypeVar

from estuary_cdk import emitted_changes_cache as cache
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPMixin

from .constants import (
    API_BASE_URL,
    DETAILED_VIEW,
    PAGE_SIZE,
    CursorField,
    QueryParam,
    RateGroup,
    Scope,
)
from .models import (
    FullRefreshResource,
    IncrementalResource,
    JWTCredentials,
    PaginatedResponse,
    RingCentralResource,
)
from .rate_limiter import get_rate_limiter

TResource = TypeVar("TResource", bound=RingCentralResource)


class FetchResult(NamedTuple, Generic[TResource]):
    records: list[TResource]
    has_more: bool


async def _fetch_paginated(
    http: HTTPMixin,
    log: Logger,
    url: str,
    params: dict[str, str],
    rate_group: RateGroup,
    response_model: type[PaginatedResponse[TResource]],
) -> FetchResult[TResource]:
    # Throttle before request to stay within rate limits.
    # This is to prevent penalties when exceeding the API rate limits
    # for an API group. See README.md for more details.
    await get_rate_limiter().throttle(rate_group, log)

    response = response_model.model_validate_json(
        await http.request(log, url, params=params)
    )

    has_more = False
    if response.navigation and response.navigation.nextPage:
        has_more = True

    return FetchResult(response.records, has_more)


async def fetch_snapshot(
    http: HTTPMixin,
    log: Logger,
    model: type[FullRefreshResource],
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{API_BASE_URL}{model.ENDPOINT}"
    page = 1

    while True:
        params: dict[str, str] = {
            QueryParam.PAGE: str(page),
            QueryParam.PER_PAGE: str(PAGE_SIZE),
            QueryParam.VIEW: DETAILED_VIEW,
        }

        result = await _fetch_paginated(
            http=http,
            log=log,
            url=url,
            params=params,
            rate_group=model.RATE_GROUP,
            response_model=PaginatedResponse[model],
        )

        for record in result.records:
            yield record

        if result.has_more:
            log.debug(
                f"Resource {model.NAME} has more pages. Current page: {page}"
            )
            page += 1
        else:
            break


async def fetch_incremental_changes(
    http: HTTPMixin,
    log: Logger,
    log_cursor: LogCursor,
    model: type[IncrementalResource],
    lookback_window: int,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{API_BASE_URL}{model.ENDPOINT}"
    page = 1
    now = datetime.now(UTC)

    fetch_from = log_cursor
    max_cursor = log_cursor

    should_apply_lookback = (
        model.REQUIRES_LOOKBACK
        and lookback_window
        and log_cursor.date() < now.date()
    )

    if should_apply_lookback:
        fetch_from = log_cursor - timedelta(hours=lookback_window)
        log.info(
            f"Applying {lookback_window}h lookback for {model.NAME}: "
            f"fetching from {fetch_from.isoformat()} (cursor was {log_cursor.isoformat()})"
        )

    while True:
        params: dict[str, str] = {
            QueryParam.DATE_FROM: fetch_from.isoformat(),
            QueryParam.DATE_TO: now.isoformat(),
            QueryParam.PAGE: str(page),
            QueryParam.PER_PAGE: str(PAGE_SIZE),
            QueryParam.VIEW: DETAILED_VIEW,
        }

        result = await _fetch_paginated(
            http=http,
            log=log,
            url=url,
            params=params,
            rate_group=model.RATE_GROUP,
            response_model=PaginatedResponse[model],
        )

        for record in result.records:
            if cache.should_yield(
                model.NAME, CursorField.LAST_MODIFIED_TIME, record.cursor_value
            ):
                yield record
                if record.cursor_value > max_cursor:
                    max_cursor = record.cursor_value

        if result.has_more:
            log.debug(
                f"Resource {model.NAME} has more pages. Current page: {page}"
            )
            page += 1
        else:
            break

    if max_cursor > log_cursor:
        yield max_cursor
    else:
        # No records found since last fetch - move cursor forward to now
        yield now


async def fetch_incremental_page(
    http: HTTPMixin,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
    model: type[IncrementalResource],
    start_date: datetime,
) -> AsyncGenerator[IncrementalResource | PageCursor, None]:
    assert page is None or isinstance(page, int)
    assert isinstance(cutoff, datetime)

    if page is None:
        page = 1

    url = f"{API_BASE_URL}{model.ENDPOINT}"
    params: dict[str, str] = {
        QueryParam.DATE_FROM: start_date.isoformat(),
        QueryParam.DATE_TO: cutoff.isoformat(),
        QueryParam.PAGE: str(page),
        QueryParam.PER_PAGE: str(PAGE_SIZE),
        QueryParam.VIEW: DETAILED_VIEW,
    }

    result = await _fetch_paginated(
        http=http,
        log=log,
        url=url,
        params=params,
        rate_group=model.RATE_GROUP,
        response_model=PaginatedResponse[model],
    )

    for record in result.records:
        if record.lastModifiedTime and record.lastModifiedTime >= cutoff:
            continue
        yield record

    if result.has_more:
        log.debug(f"Resource {model.NAME} has more pages. Current page: {page}")
        yield page + 1


async def validate_credentials(
    http: HTTPMixin,
    log: Logger,
) -> None:
    try:
        url = f"{API_BASE_URL}/account/~"
        await http.request(log, url)
    except Exception as e:
        raise ValidationError(
            [
                f"Invalid credentials. Please confirm the provided credentials are correct.{e}"
            ]
        )


def validate_scopes(
    credentials: JWTCredentials,
    required_scopes: set[Scope],
) -> None:
    granted_scopes = credentials.get_scopes()
    missing_scopes = {str(scope) for scope in required_scopes} - granted_scopes
    if missing_scopes:
        raise ValidationError(
            [
                f"JWT credentials are missing required scopes for enabled bindings. Required scopes: "
                f"{', '.join(sorted(missing_scopes))}. "
                f"Granted scopes: {', '.join(sorted(granted_scopes)) or 'none'}."
            ]
        )
