from datetime import datetime, UTC, timedelta
from logging import Logger
from math import trunc
from typing import AsyncGenerator
from urllib.parse import urlparse, parse_qs, quote
from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession

from .models import (
    FrontResource,
    Response,
)


API = "https://api2.frontapp.com"
MAX_DATE_WINDOW_SIZE = 15
CONVERSATIONS_LAG = 2

def _dt_to_s(dt: datetime) -> float:
    timestamp = dt.timestamp()
    # Front timestamps can have up to 3 decimal places, so we only keep the last 3 decimals.
    return trunc(timestamp * 1000) / 1000

def _s_to_dt(s: float) -> datetime:
    return datetime.fromtimestamp(s, tz=UTC)

def _extract_page_token(url: str | None) -> str | None:
    if url is None:
        return None

    parsed_url = urlparse(url)
    params: dict[str, str] = parse_qs(parsed_url.query) #type: ignore

    return params.get("page_token")


async def snapshot_resources(
        http: HTTPSession,
        path: str,
        log: Logger,
) -> AsyncGenerator[FrontResource, None]:
    url = f"{API}/{path}"

    params: dict[str, str | int] = {
        "limit": 100,
    }

    while True:
        response = Response.model_validate_json(
            await http.request(log, url, params=params)
        )

        resources = response.results

        for resource in resources:
            yield resource

        next_page_token = _extract_page_token(response.pagination.next)

        if next_page_token is None:
            break

        params["page_token"] = next_page_token


async def _fetch_conversations_between(
        http: HTTPSession,
        log: Logger,
        start: datetime,
        end: datetime,
) -> AsyncGenerator[FrontResource, None]:
    query_path = f"after:{_dt_to_s(start)} before:{_dt_to_s(end)}"
    url = f"{API}/conversations/search/{quote(query_path)}"

    params: dict[str, str | int] = {
        "limit": 100,
    }

    while True:
        response = Response.model_validate_json(
            await http.request(log, url, params=params)
        )

        records = response.results

        for record in records:
            yield record

        next_page_token = _extract_page_token(response.pagination.next)

        if next_page_token is None:
            return

        params["page_token"] = next_page_token


async def fetch_conversations(
        http: HTTPSession,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[FrontResource | LogCursor, None]:
    # Conversations do not contain a field that indicate when they were updated, so there's no way of determining
    # when a conversation was updated. The /conversations/search/:query endpoint lets us filter conversations
    # to only those who contain messages/comments created between timestamps. 
    assert isinstance(log_cursor, datetime)

    # Move the cutoff a couple minutes in the past to *hopefully* avoid potential distributed clocks issues.
    cutoff = datetime.now(tz=UTC) - timedelta(minutes=CONVERSATIONS_LAG)

    def _determine_next_end(dt: datetime) -> datetime:
        return min(dt + timedelta(days=MAX_DATE_WINDOW_SIZE), cutoff)

    def _determine_next_start(dt: datetime) -> datetime:
        # The after and before filters are inclusive per the Front docs, so the new start needs to be
        # 1 millisecond (the smallest time increment in Front) than the previous end.
        return dt + timedelta(milliseconds=1)

    start = _determine_next_start(log_cursor)
    end = _determine_next_end(start)
    while start < cutoff:
        count = 0
        async for record in _fetch_conversations_between(http, log, start, end):
            yield record
            count += 1

        # Checkpoint after completing a date window.
        yield end
        log.info(f"Fetched {count} conversations between {start} and {end}.")

        start = _determine_next_start(end)
        end = _determine_next_end(start)


async def fetch_resources_with_cursor_fields(
    http: HTTPSession,
    path: str,
    q_filter: str,
    sort_by: str,
    model: type[FrontResource],
    cursor_field: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[FrontResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    url = f"{API}/{path}"
    previous_cursor_ts = _dt_to_s(log_cursor)
    last_seen_ts = previous_cursor_ts
    MIN_CHECKPOINT_INTERVAL = 100

    params = {
        f"q[{q_filter}]": previous_cursor_ts,
        "limit": 100,
        "sort_by": sort_by,
        "sort_order": "asc",
    }

    count = 0

    while True:
        response = Response.model_validate_json(
            await http.request(log, url, params=params)
        )

        records = response.results

        for record in records:
            document = model.model_validate(record.model_dump())
            ts: float = getattr(document, cursor_field)
            if ts > last_seen_ts:
                # Checkpoint the last timestamp we saw in this sweep after MIN_CHECKPOINT_INTERVAL documents are yielded.
                if count >= MIN_CHECKPOINT_INTERVAL:
                    dt = _s_to_dt(last_seen_ts)
                    log.info(f"Updating checkpoint to {dt}.")
                    yield dt
                    count = 0

                last_seen_ts = ts
            if ts > previous_cursor_ts:
                yield document
                count += 1

        next_page_token = _extract_page_token(response.pagination.next)

        if next_page_token is None:
            break

        params["page_token"] = next_page_token

    if last_seen_ts > previous_cursor_ts:
        yield _s_to_dt(last_seen_ts)
