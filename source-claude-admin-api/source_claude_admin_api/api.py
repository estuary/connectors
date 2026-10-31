"""Claude Admin API client functions.

Fetch shapes are generic over the model type and read each model's `api_path`
ClassVar — no hardcoded URLs:

1. fetch_singleton      — single GET, no pagination (Organization).
2. fetch_cursor_list    — ID-cursor pagination (limit / after_id -> has_more / last_id), Users.
3. fetch_claude_code_usage — opaque page cursor WITHIN a single UTC day
                          (page -> next_page / has_more) plus an outer day loop with a
                          freshness gate. Bespoke: the freshness gate isn't generalizable,
                          but it still reads ClaudeCodeUsageRecord.api_path.
"""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import TypeVar

from estuary_cdk.capture.common import BaseDocument, LogCursor
from estuary_cdk.http import HTTPSession

from .models import (
    ANTHROPIC_VERSION_HEADERS,
    BaseClaudeEntity,
    ClaudeCodeUsageRecord,
    CursorPage,
    OpaquePage,
)

EntityT = TypeVar("EntityT", bound=BaseClaudeEntity)

# The usage report API only returns data older than ~1 hour. We wait this long past
# midnight of the *next* day before processing a day, so a day is never advanced past
# while its data may still be incomplete (late-arriving rows). 2h = the ~1h API lag
# plus 1h of deliberate slack.
USAGE_FRESHNESS_LAG = timedelta(hours=2)


def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _midnight(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def _entity_url(base_url: str, model: type[BaseClaudeEntity]) -> str:
    return f"{base_url.rstrip('/')}/{model.api_path}"


async def fetch_singleton(
    model: type[EntityT],
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[EntityT, None]:
    doc = model.model_validate_json(
        await http.request(log, _entity_url(base_url, model), headers=ANTHROPIC_VERSION_HEADERS)
    )
    yield doc


async def fetch_cursor_list(
    model: type[EntityT],
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[EntityT, None]:
    url = _entity_url(base_url, model)
    params: dict[str, str | int] = {"limit": model.page_limit}

    while True:
        page = CursorPage[model].model_validate_json(
            await http.request(log, url, params=params, headers=ANTHROPIC_VERSION_HEADERS)
        )

        for item in page.data:
            yield item

        if not page.has_more or page.last_id is None:
            break

        params["after_id"] = page.last_id


async def _fetch_usage_day(
    http: HTTPSession,
    base_url: str,
    log: Logger,
    day: datetime,
) -> AsyncGenerator[ClaudeCodeUsageRecord, None]:
    url = _entity_url(base_url, ClaudeCodeUsageRecord)
    params: dict[str, str | int] = {
        "starting_at": _midnight(day).strftime("%Y-%m-%d"),
        "limit": ClaudeCodeUsageRecord.page_limit,
    }

    while True:
        report = OpaquePage[ClaudeCodeUsageRecord].model_validate_json(
            await http.request(log, url, params=params, headers=ANTHROPIC_VERSION_HEADERS)
        )

        for record in report.data:
            yield record

        if not report.has_more or report.next_page is None:
            break

        params["page"] = report.next_page


async def fetch_claude_code_usage(
    http: HTTPSession,
    base_url: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ClaudeCodeUsageRecord | LogCursor, None]:
    # The usage report is daily-aggregated: one record per actor per UTC day. The
    # API has no range query, so we iterate one day at a time. The cursor is the
    # next UTC day (midnight) we still need to fetch.
    assert isinstance(log_cursor, datetime)

    day = _midnight(log_cursor)

    # Freshness gate: only process day D once we are safely past the point where its
    # data is complete. Day D's data is "done" at midnight(D+1); add a safety lag
    # because the API only returns data older than ~1 hour.
    def _is_ready(d: datetime) -> bool:
        return now_utc() >= _midnight(d) + timedelta(days=1) + USAGE_FRESHNESS_LAG

    while _is_ready(day):
        count = 0
        async for record in _fetch_usage_day(http, base_url, log, day):
            record.meta_ = BaseDocument.Meta(op="c")
            yield record
            count += 1

        next_day = day + timedelta(days=1)
        # Checkpoint only after the day is fully drained, advancing to the next day.
        yield next_day
        log.info(
            f"Fetched {count} Claude Code usage records for {day.strftime('%Y-%m-%d')}."
        )
        day = next_day
