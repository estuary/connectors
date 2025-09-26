from __future__ import annotations

from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, Type, TypeVar

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel
from requests.utils import parse_header_links

from .models import (
    Issue,
    SentryEntity,
)


class HeaderLink(BaseModel):
    has_more: bool
    next_url: str

    @classmethod
    def from_headers(cls, raw_headers: dict[str, Any]) -> HeaderLink:
        next_header_link = next(
            (
                link
                for link in parse_header_links(raw_headers.get("Link", ""))
                if link["rel"] == "next"
            ),
            {},
        )

        return HeaderLink(
            has_more=next_header_link.get("results", "missing") == "true",
            next_url=next_header_link.get("url", ""),
        )


T = TypeVar("T", bound=SentryEntity)


async def list_entity(
    model: Type[T],
    http: HTTPSession,
    organization: str,
    log: Logger,
    extra_params: dict[str, Any] | None = None,
) -> AsyncGenerator[T, None]:
    url = f"https://sentry.io/api/0/organizations/{organization}/{model.resource_path}/"
    params = {}
    params.update(model.query_params)
    params.update(extra_params or {})

    while True:
        headers, body = await http.request_stream(log, url, params=params)

        async for item in IncrementalJsonProcessor(body(), "item", model):
            yield item

        header_link = HeaderLink.from_headers(headers)

        if not header_link.has_more:
            break

        url = header_link.next_url


async def list_time_ranged_entity(
    model: Type[T],
    start_date: datetime,
    end_date: datetime,
    http: HTTPSession,
    organization: str,
    log: Logger,
    extra_params: dict[str, Any] | None = None,
) -> AsyncGenerator[T, None]:
    params = {}
    params.update(extra_params or {})
    params.update(
        {
            "start": start_date.isoformat(timespec="seconds"),
            "end": end_date.isoformat(timespec="seconds"),
        }  # WARN: Matching the start timestamp will cause an HTTP 400 error
    )

    async for item in list_entity(model, http, organization, log, extra_params):
        yield item


async def backfill_issues(
    http: HTTPSession,
    organization: str,
    window_size: timedelta,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Issue | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    page_ts = datetime.fromisoformat(page)
    max_date_to_fetch = min(page_ts + window_size, cutoff)

    log.info(
        "Initiating backfill fetch",
        {
            "page_ts": page_ts,
            "max_date_to_fetch": max_date_to_fetch,
            "window_size": window_size,
            "cutoff": cutoff,
        },
    )

    if page_ts >= cutoff:
        log.info(
            "Page cursor exceeds the cutoff, ending backfill",
            {"page_ts": page_ts, "cutoff": cutoff},
        )
        return

    async for issue in list_time_ranged_entity(
        Issue, page_ts, max_date_to_fetch, http, organization, log
    ):
        yield issue

    yield max_date_to_fetch.isoformat(timespec="seconds")


async def fetch_issues(
    http: HTTPSession, organization: str, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[Issue | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    truncated_log_cursor = log_cursor.replace(microsecond=0)
    now = datetime.now(tz=UTC).replace(microsecond=0)

    # Sentry will respond with an HTTP 400 if the start and end timestamps match
    if now == truncated_log_cursor:
        return

    log.info(
        "Starting incremental fetch",
        {"log_cursor": log_cursor, "now": now},
    )

    new_log_cursor = log_cursor
    async for issue in list_time_ranged_entity(
        Issue, log_cursor, now, http, organization, log
    ):
        if issue.lastSeen == log_cursor:
            continue  # Make the start time bound exclusive to tolerate multiple runs with the same log_cursor

        new_log_cursor = max(issue.lastSeen, new_log_cursor)
        yield issue

    if new_log_cursor != log_cursor:
        yield new_log_cursor
