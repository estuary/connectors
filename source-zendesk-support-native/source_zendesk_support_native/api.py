import asyncio
import base64
from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import Headers, HeadersAndBodyGenerator, HTTPSession, HTTPError
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    FullRefreshResource,
    FullRefreshResponse,
    FullRefreshOffsetPaginatedResponse,
    FullRefreshCursorPaginatedResponse,
    ZendeskResource,
    TimestampedResource,
    AbbreviatedTicket,
    IncrementalTimeExportResponse,
    TalkIncrementalExportResponse,
    TicketsResponse,
    UsersResponse,
    ClientSideIncrementalOffsetPaginatedResponse,
    ClientSideIncrementalCursorPaginatedResponse,
    IncrementalCursorPaginatedResponse,
    SatisfactionRatingsResponse,
    AuditLog,
    AuditLogsResponse,
    Post,
    PostsResponse,
    PostComment,
    PostCommentsResponse,
    PostCommentVotesResponse,
    INCREMENTAL_CURSOR_EXPORT_TYPES,
    FilterParam,
    TicketChildResourceValidationContext,
)

CHECKPOINT_INTERVAL = 1000
CURSOR_PAGINATION_PAGE_SIZE = 100
MAX_INCREMENTAL_EXPORT_PAGE_SIZE = 1000
MAX_SATISFACTION_RATINGS_WINDOW_SIZE = timedelta(days=30)
MIN_PAGE_SIZE = 1
# Zendesk errors out if a start or end time parameter is 60 seconds or less in the past. 
TIME_PARAMETER_DELAY = timedelta(seconds=61)

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
INCREMENTAL_TIME_EXPORT_REQ_PER_MIN_LIMIT = 10

incremental_time_export_api_lock = asyncio.Lock()


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

    while True:
        response = response_model.model_validate_json(
            await http.request(log, url, params=params)
        )

        for resource in response.resources:
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


async def _fetch_incremental_time_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[IncrementalTimeExportResponse],
    start_date: datetime,
    log: Logger,
) -> AsyncGenerator[TimestampedResource | datetime, None]:
    # Docs: https://developer.zendesk.com/documentation/ticketing/managing-tickets/using-the-incremental-export-api/#time-based-incremental-exports
    # Incremental time export streams use timestamps for pagination that correlate to the updated_at timestamp for each record.
    # The end_time returned in the response for fetching the next page is *always* the updated_at timestamp of the last
    # record in the current response. This means that we'll always get at least one duplicate result when paginating. This also means
    # that the stream could get stuck looping & making the same request if more than 1,000 results are updated at the same time, but
    # an error should be raised if we detect that.
    url = f"{url_base(subdomain)}/incremental/{path}"

    params = {"start_time": _dt_to_s(start_date)}

    last_seen_dt = start_date
    count = 0

    while True:
        async with incremental_time_export_api_lock:
            _, body = await http.request_stream(log, url, params=params)
            processor = IncrementalJsonProcessor(
                body(),
                f"{name}.item",
                TimestampedResource,
                response_model,
            )

            async for resource in processor:
                # Ignore duplicate results that were yielded on the previous sweep.
                if resource.updated_at <= start_date:
                    continue

                # Checkpoint previously yielded documents if we see a new updated_at value.
                if (
                    resource.updated_at > last_seen_dt and 
                    last_seen_dt != start_date and
                    count >= CHECKPOINT_INTERVAL
                ):
                    yield last_seen_dt
                    count = 0

                yield resource
                count += 1
                last_seen_dt = resource.updated_at

            remainder = processor.get_remainder()

            # Handle empty responses. Since the end_time used to get the next page always overlaps with at least one
            # record on the previous page, we should only see empty responses if users don't have any organizations updated
            # on or afterthe start date.
            if remainder.count == 0 or remainder.end_time is None:
                return

            # Error if 1000+ organizations have the same updated_at value. This stops the stream from
            # looping & making the same request endlessly. If this happens, we can evaluate different strategies
            # for users that hit this issue.
            if params["start_time"] == remainder.end_time and remainder.count >= 1000:
                raise RuntimeError(f"At least 1,000 organizations were updated at {remainder.end_time}, and this stream cannot progress without potentially missing data. Contact Estuary Support for help resolving this issue.")

            if remainder.end_of_stream:
                # Checkpoint the last document(s) if there were any updated records in this sweep.
                if last_seen_dt > start_date:
                    yield last_seen_dt

                return

            params["start_time"] = remainder.end_time

            # Sleep to avoid excessively hitting this endpoint's more restricting 10 req/min limit.
            await asyncio.sleep(60 / INCREMENTAL_TIME_EXPORT_REQ_PER_MIN_LIMIT)


async def fetch_incremental_time_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[IncrementalTimeExportResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    generator = _fetch_incremental_time_export_resources(http, subdomain, name, path, response_model, log_cursor, log)

    async for result in generator:
        yield result


async def backfill_incremental_time_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[IncrementalTimeExportResponse],
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[TimestampedResource | PageCursor, None]:
    assert isinstance(page, int)
    assert isinstance(cutoff, datetime)

    generator = _fetch_incremental_time_export_resources(http, subdomain, name, path, response_model, _s_to_dt(page), log)

    async for result in generator:
        if isinstance(result, datetime):
            yield _dt_to_s(result)
        elif result.updated_at > cutoff:
            return
        else:
            yield result


# Talk API incremental exports (calls, legs) have a different response structure than other incremental exports.
# They don't return an end_of_stream field - instead we check if end_time == start_time.
# See: https://developer.zendesk.com/api-reference/voice/talk-api/incremental_exports/
talk_incremental_export_api_lock = asyncio.Lock()


async def _fetch_talk_incremental_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[TalkIncrementalExportResponse],
    start_date: datetime,
    log: Logger,
) -> AsyncGenerator[TimestampedResource | datetime, None]:
    # Talk API incremental exports use timestamps for pagination similar to other time-based exports.
    # The end_time returned in the response is the updated_at timestamp of the last record.
    # This means we'll get at least one duplicate when paginating (records with updated_at == end_time).
    # Unlike other incremental exports, Talk API doesn't have end_of_stream - we detect end by
    # checking if end_time == start_time (no newer records exist).
    url = f"{url_base(subdomain)}/channels/voice/stats/incremental/{path}"

    params = {"start_time": _dt_to_s(start_date)}

    last_seen_dt = start_date
    count = 0

    while True:
        async with talk_incremental_export_api_lock:
            _, body = await http.request_stream(log, url, params=params)
            processor = IncrementalJsonProcessor(
                body(),
                f"{path}.item",
                TimestampedResource,
                response_model,
            )

            async for resource in processor:
                # Ignore duplicate results that were yielded on the previous sweep.
                if resource.updated_at <= start_date:
                    continue

                # Checkpoint previously yielded documents if we see a new updated_at value.
                if (
                    resource.updated_at > last_seen_dt and
                    last_seen_dt != start_date and
                    count >= CHECKPOINT_INTERVAL
                ):
                    yield last_seen_dt
                    count = 0

                yield resource
                count += 1
                last_seen_dt = resource.updated_at

            remainder = processor.get_remainder()

            if remainder.count == 0 or remainder.end_time is None:
                return

            # Error if 1000+ records have the same updated_at value. This stops the stream from
            # looping & making the same request endlessly.
            if params["start_time"] == remainder.end_time and remainder.count >= 1000:
                raise RuntimeError(
                    f"At least 1,000 {name} were updated at {remainder.end_time}, and this stream "
                    f"cannot progress without potentially missing data. Contact Estuary Support for help."
                )

            # Talk API responses don't have an end_of_stream field for detecting the last page,
            # so we detect the last page by checking if end_time == start_time.
            # This means no newer records exist beyond the current page.
            if params["start_time"] == remainder.end_time:
                if last_seen_dt > start_date:
                    yield last_seen_dt
                return

            params["start_time"] = remainder.end_time

            # Sleep to avoid excessively hitting this endpoint's 10 req/min limit.
            await asyncio.sleep(60 / INCREMENTAL_TIME_EXPORT_REQ_PER_MIN_LIMIT)


async def fetch_talk_incremental_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[TalkIncrementalExportResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    generator = _fetch_talk_incremental_export_resources(http, subdomain, name, path, response_model, log_cursor, log)

    async for result in generator:
        yield result


async def backfill_talk_incremental_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[TalkIncrementalExportResponse],
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[TimestampedResource | PageCursor, None]:
    assert isinstance(page, int)
    assert isinstance(cutoff, datetime)

    generator = _fetch_talk_incremental_export_resources(http, subdomain, name, path, response_model, _s_to_dt(page), log)

    async for result in generator:
        if isinstance(result, datetime):
            yield _dt_to_s(result)
        elif result.updated_at > cutoff:
            return
        else:
            yield result


def _is_upstream_timeout(
    status: int,
    body: str
) -> bool:
    return status == 504 and "upstream request timeout" in body


def _should_retry_incremental_cursor_export_response(
    status: int,
    headers: Headers,
    body: bytes,
    attempt: int,
) -> bool:
    # If the response has a 504 status code and a body stating
    # an upstream timeout was reached, that usually means that
    # too much data was requested and a timeout was reached before
    # the API server sent a response. To get around these timeouts,
    # the connector should make a new request for less data.
    return not _is_upstream_timeout(status, str(body))


async def _do_incremental_cursor_export_request(
    http: HTTPSession,
    url: str,
    params: dict[str, str | int],
    log: Logger,
) -> HeadersAndBodyGenerator:
    params = params.copy()

    page_size = MAX_INCREMENTAL_EXPORT_PAGE_SIZE
    while page_size >= MIN_PAGE_SIZE:
        params["per_page"] = page_size

        try:
            return await http.request_stream(log, url, params=params, should_retry=_should_retry_incremental_cursor_export_response)
        except HTTPError as err:
            if _is_upstream_timeout(err.code, err.message):
                log.debug("Received 504 upstream timeout response (will retry with a smaller page size)", {
                    "url": url,
                    "params": params,
                })
                page_size = page_size // 2
            else:
                raise

    raise Exception(f"Request to {url} failed with smallest possible page size. Query parameters were {params}")


async def _fetch_incremental_cursor_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: INCREMENTAL_CURSOR_EXPORT_TYPES,
    start_date: datetime | None,
    cursor: str | None,
    log: Logger,
    sideload_params: dict[str, str] | None = None,
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
        _, body = await _do_incremental_cursor_export_request(http, url, params, log)
        processor = IncrementalJsonProcessor(
            body(),
            f"{name}.item",
            TimestampedResource,
            response_model,
        )

        async for resource in processor:
            yield resource

        remainder = processor.get_remainder()
        next_page_cursor, end_of_stream = remainder.after_cursor, remainder.end_of_stream
        
        if not next_page_cursor:
            return

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


async def _fetch_ticket_child_resources(
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
        context = TicketChildResourceValidationContext(ticket_id=ticket_id)

        while True:
            response = response_model.model_validate_json(
                await http.request(log, url, params=params),
                context=context
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

    tickets: list[AbbreviatedTicket] = []

    while True:
        # Fetching comments for each ticket as we're streaming a tickets response often triggers aiohttp's TimeoutError.
        # To avoid these TimeoutErrors, we fetch all ticket ids in a single response, then fetch the child resources for 
        # those ticket ids.

        next_page_cursor: str | None = None
        async for result in tickets_generator:
            if isinstance(result, TimestampedResource):
                tickets.append(AbbreviatedTicket(
                    id=result.id, 
                    status=getattr(result, "status"),
                    updated_at=result.updated_at
                ))
            elif isinstance(result, str):
                next_page_cursor = result
                break

        if len(tickets) > 0 and next_page_cursor:
            for ticket in tickets:
                if ticket.status == 'deleted':
                    continue

                async for child_resource in _fetch_ticket_child_resources(http, subdomain, path, response_model, ticket.id, log):
                    yield child_resource

            yield (next_page_cursor,)
            tickets = []
        elif not next_page_cursor:
            break


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

    tickets_generator = _fetch_incremental_cursor_export_resources(http, subdomain, "tickets", start_date, page, log)

    tickets: list[AbbreviatedTicket] = []

    while True:
        next_page_cursor: str | None = None
        async for result in tickets_generator:
            if isinstance(result, TimestampedResource):
                tickets.append(AbbreviatedTicket(
                    id=result.id, 
                    status=getattr(result, "status"),
                    updated_at=result.updated_at
                ))
            elif isinstance(result, str):
                next_page_cursor = result
                break

        if len(tickets) > 0 and next_page_cursor:
            for ticket in tickets:
                if ticket.updated_at >= cutoff:
                    return
                if ticket.status == 'deleted':
                    continue

                async for child_resource in _fetch_ticket_child_resources(http, subdomain, path, response_model, ticket.id, log):
                    yield child_resource

            yield next_page_cursor
            tickets = []
        elif not next_page_cursor:
            break


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


async def fetch_post_child_resources(
    http: HTTPSession,
    subdomain: str,
    path_segment: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    posts_generator = fetch_client_side_incremental_cursor_paginated_resources(http, subdomain, "community/posts", None, PostsResponse, log, log_cursor)

    async for result in posts_generator:
        if isinstance(result, TimestampedResource):
            post = Post.model_validate(result)

            if (
                (path_segment == "votes" and post.vote_count == 0) or 
                (path_segment == "comments" and post.comment_count == 0)
            ):
                continue

            path = f"community/posts/{post.id}/{path_segment}"

            async for child_resource in snapshot_cursor_paginated_resources(http, subdomain, path, response_model, log):
                yield ZendeskResource.model_validate({
                    "post_id": post.id,
                    **child_resource.model_dump(exclude={"meta_"}),
                })

        else:
            yield result


async def fetch_post_comment_votes(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    post_comments_generator = fetch_post_child_resources(http, subdomain, "comments", PostCommentsResponse, log, log_cursor)

    async for result in post_comments_generator:

        if isinstance(result, ZendeskResource):
            post_comment = PostComment.model_validate(result.model_dump())  

            if post_comment.vote_count == 0:
                continue

            path = f"community/posts/{post_comment.post_id}/comments/{post_comment.id}/votes"

            async for child_resource in snapshot_cursor_paginated_resources(http, subdomain, path, PostCommentVotesResponse, log):
                yield ZendeskResource.model_validate({
                    "post_id": post_comment.post_id,
                    **child_resource.model_dump(exclude={"meta_"}),
                })

        else:
            yield result
