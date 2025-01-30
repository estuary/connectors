import base64
from datetime import datetime, timedelta, UTC
import json
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from .models import (
    FullRefreshResource,
    FullRefreshCursorPaginatedResponse,
    ZendeskResource,
    TimestampedResource,
    IncrementalCursorExportResponse,
    TicketsResponse,
    UsersResponse,
    ClientSideIncrementalCursorPaginatedResponse,
    IncrementalCursorPaginatedResponse,
    SatisfactionRatingsResponse,
    AuditLog,
    AuditLogsResponse,
    INCREMENTAL_CURSOR_EXPORT_TYPES,
)

CURSOR_PAGINATION_PAGE_SIZE = 100
MAX_SATISFACTION_RATINGS_WINDOW_SIZE = timedelta(days=30)
# Zendesk errors out if a start or end time parameter is more recent than 60 seconds in the past.
TIME_PARAMETER_DELAY = timedelta(seconds=60)

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

def url_base(subdomain: str) -> str:
    return f"https://{subdomain}.zendesk.com/api/v2"


def _base64_decode(encoded: str) -> str:
    return base64.b64decode(encoded).decode("utf-8")


def _base64_encode(decoded: str) -> str:
    return base64.b64encode(decoded.encode("utf-8")).decode("utf-8")


def _dt_to_s(dt: datetime) -> int:
    return int(dt.timestamp())


def _s_to_dt(s: int) -> datetime:
    return datetime.fromtimestamp(s, tz=UTC)


def _dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def _str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def _is_timestamp(string: str) -> bool:
    try:
        _s_to_dt(int(string))
        return True
    except ValueError:
        return False


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

    while True:
        response = response_model.model_validate_json(
            await http.request(log, url, params=params)
        )

        for resource in response.resources:
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


async def fetch_incremental_cursor_paginated_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    cursor_field: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{url_base(subdomain)}/{path}"

    params: dict[str, str | int] = {
        "start_time": _dt_to_s(log_cursor),
        "page[size]": 1000 if "ticket_metric_events" in path else CURSOR_PAGINATION_PAGE_SIZE,
    }

    last_seen_dt = log_cursor

    while True:
        response = response_model.model_validate_json(
            await http.request(log, url, params=params)
        )

        if (
            last_seen_dt > log_cursor
            and response.resources
            and _str_to_dt(getattr(response.resources[0], cursor_field)) > last_seen_dt
        ):
            yield last_seen_dt


        for resource in response.resources:
            resource_dt = _str_to_dt(getattr(response.resources[0], cursor_field))
            if resource_dt > last_seen_dt:
                last_seen_dt = resource_dt

            if resource_dt > log_cursor:
                yield resource

        if not response.meta.has_more:
            if last_seen_dt > log_cursor:
                yield last_seen_dt
            break

        if response.meta.after_cursor:
            params["page[after]"] = response.meta.after_cursor


async def backfill_incremental_cursor_paginated_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
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
        "start_time": _dt_to_s(start_date),
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

    end = min(datetime.now(tz=UTC) - TIME_PARAMETER_DELAY, log_cursor + MAX_SATISFACTION_RATINGS_WINDOW_SIZE)

    generator = _fetch_satisfaction_ratings_between(
        http=http,
        subdomain=subdomain,
        start=_dt_to_s(log_cursor),
        end=_dt_to_s(end),
        log=log,
    )

    async for satisfaction_rating in generator:
        yield satisfaction_rating

    yield end


async def backfill_satisfaction_ratings(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ZendeskResource | PageCursor, None]:
    assert isinstance(page, int)
    assert isinstance(cutoff, datetime)
    cutoff_ts = _dt_to_s(cutoff)

    if page >= cutoff_ts:
        return

    end = min(cutoff_ts, page + int(MAX_SATISFACTION_RATINGS_WINDOW_SIZE.total_seconds()))

    generator = _fetch_satisfaction_ratings_between(
        http=http,
        subdomain=subdomain,
        start=page,
        end=end,
        log=log,
    )

    async for satisfaction_rating in generator:
        yield satisfaction_rating

    yield end


async def _fetch_top_level_fields(
    http: HTTPSession,
    url: str,
    params: dict[str, str | int],
    response_model: type[IncrementalCursorExportResponse],
    log: Logger,
) -> tuple[str | None, bool]:
    # Instead of using Pydantic's model_validate_json that uses json.loads internally,
    # use json.JSONDecoder().raw_decode to reduce memory overhead when processing the response.
    raw_response_bytes = await http.request(log, url, params=params)
    obj, _ = json.JSONDecoder().raw_decode(raw_response_bytes.decode('utf-8'))
    # model_construct is used to avoid validating & transforming all resources within large response bodies at once
    # to reduce memory overhead. Instead, resources are validated & transformed one-by-one as they are yielded.
    response =  response_model.model_construct(**obj)
    return (response.after_cursor, response.end_of_stream)


async def _fetch_incremental_cursor_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: INCREMENTAL_CURSOR_EXPORT_TYPES,
    start_date: datetime | None,
    cursor: str | None,
    log: Logger,
    sideload_params: dict[str, str] | None = None
) -> AsyncGenerator[TimestampedResource | str, None]:
    url = f"{url_base(subdomain)}/incremental"
    match name:
        case "tickets":
            url += "/tickets/cursor"
            response_model = TicketsResponse
        case "users":
            url += "/users/cursor"
            response_model = UsersResponse
        case _:
            raise RuntimeError(f"Unknown incremental cursor pagination resource type {name}.")

    params: dict[str, str | int] = {}
    if sideload_params:
        params.update(sideload_params)

    if cursor is None:
        assert isinstance(start_date, datetime)
        params["start_time"] = _dt_to_s(start_date)
    else:
        params["cursor"] = _base64_encode(cursor)

    while True:
        next_page_cursor, end_of_stream = await _fetch_top_level_fields(http, url, params, response_model, log)

        if next_page_cursor is None:
            return

        async for resource in http.request_object_stream(log, TimestampedResource, f"{name}.item", url, params=params):
            yield resource

        yield _base64_decode(next_page_cursor)

        if end_of_stream:
            return

        if "start_time" in params:
            del params["start_time"]

        params["cursor"] = next_page_cursor


async def fetch_incremental_cursor_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: INCREMENTAL_CURSOR_EXPORT_TYPES,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, tuple)
    cursor = log_cursor[0]
    assert isinstance(cursor, str)

    start_date: datetime | None = None

    if _is_timestamp(cursor):
        start_date = _s_to_dt(int(cursor))
        cursor = None

    generator = _fetch_incremental_cursor_export_resources(http, subdomain, name, start_date, cursor, log)

    async for result in generator:
        if isinstance(result, str):
            yield (result,)
        else:
            yield result


async def backfill_incremental_cursor_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: INCREMENTAL_CURSOR_EXPORT_TYPES,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TimestampedResource | PageCursor, None]:
    if page is not None:
        assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    generator = _fetch_incremental_cursor_export_resources(http, subdomain, name, start_date, page, log)

    async for result in generator:
        if isinstance(result, str) or result.updated_at < cutoff:
            yield result
        else:
            return


async def _fetch_ticket_child_resource(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    ticket_id: int,
    log: Logger,
) -> AsyncGenerator[ZendeskResource, None]:
        url = f"{url_base(subdomain)}/tickets/{ticket_id}/{path}"
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
                break

            if response.meta.after_cursor:
                params["page[after]"] = response.meta.after_cursor


async def fetch_ticket_child_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, tuple)
    cursor = log_cursor[0]
    assert isinstance(cursor, str)

    start_date: datetime | None = None

    if _is_timestamp(cursor):
        start_date = _s_to_dt(int(cursor))
        cursor = None

    tickets_generator = _fetch_incremental_cursor_export_resources(http, subdomain, "tickets", start_date, cursor, log)

    async for result in tickets_generator:
        if isinstance(result, str):
            yield (result,)
        else:
            status = getattr(result, "status")
            if status == "deleted":
                continue

            async for child_resource in _fetch_ticket_child_resource(http, subdomain, path, response_model, result.id, log):
                yield child_resource


async def backfill_ticket_child_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ZendeskResource | PageCursor, None]:
    if page is not None:
        assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    generator = _fetch_incremental_cursor_export_resources(http, subdomain, "tickets", start_date, page, log)

    async for result in generator:
        if isinstance(result, str):
            yield result
        elif result.updated_at < cutoff:
            status = getattr(result, "status")
            if status == "deleted":
                continue

            async for child_resource in _fetch_ticket_child_resource(http, subdomain, path, response_model, result.id, log):
                yield child_resource
        else:
            return


async def fetch_audit_logs(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[AuditLog | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{url_base(subdomain)}/audit_logs"

    start = _dt_to_str(log_cursor)
    end = _dt_to_str(datetime.now(tz=UTC))

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


async def fetch_ticket_metrics(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, tuple)
    cursor = log_cursor[0]
    assert isinstance(cursor, str)

    start_date: datetime | None = None

    if _is_timestamp(cursor):
        start_date = _s_to_dt(int(cursor))
        cursor = None

    sideload_params = {
        "include": "metric_sets"
    }

    generator = _fetch_incremental_cursor_export_resources(http, subdomain, "tickets", start_date, cursor, log, sideload_params)

    async for result in generator:
        if isinstance(result, str):
            yield (result,)
        else:
            metrics = getattr(result, "metric_set")
            # Deleted tickets have no metrics, so we have to check that the metric set exists before yielding it.
            if metrics is not None:
                yield ZendeskResource.model_validate(metrics)


async def backfill_ticket_metrics(
    http: HTTPSession,
    subdomain: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ZendeskResource | PageCursor, None]:
    if page is not None:
        assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    sideload_params = {
        "include": "metric_sets"
    }

    generator = _fetch_incremental_cursor_export_resources(http, subdomain, "tickets", start_date, page, log, sideload_params)

    async for result in generator:

        if isinstance(result, str):
            yield result
        elif result.updated_at < cutoff:
            metrics = getattr(result, "metric_set")
            # Deleted tickets have no metrics, so we have to check that the metric set exists before yielding it.
            if metrics is not None:
                yield ZendeskResource.model_validate(metrics)
        else:
            return
