"""Claude Admin API client functions.

Three distinct fetch shapes — deliberately NOT unified behind one helper:

1. Organization      — single GET, no pagination (snapshot singleton).
2. Users             — ID-cursor pagination (limit / after_id -> has_more / last_id),
                       full re-list each poll (snapshot).
3. ClaudeCodeUsageReport — opaque page cursor WITHIN a single UTC day
                       (page -> next_page / has_more) plus an outer day loop with a
                       freshness gate (mirrors source-front::fetch_conversations).
"""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import BaseDocument, LogCursor
from estuary_cdk.http import HTTPSession

from .models import (
    ANTHROPIC_VERSION_HEADERS,
    ClaudeCodeUsageRecord,
    ClaudeCodeUsageReport,
    Organization,
    User,
    UserList,
)

USERS_PAGE_LIMIT = 1000
USAGE_PAGE_LIMIT = 1000

# The usage report API only returns data older than ~1 hour. We wait this long past
# midnight of the *next* day before processing a day, so a day is never advanced past
# while its data may still be incomplete (late-arriving rows). 2h = the ~1h API lag
# plus 1h of deliberate slack.
USAGE_FRESHNESS_LAG = timedelta(hours=2)


def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _midnight(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


async def fetch_organization(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[Organization, None]:
    url = f"{base_url}/v1/organizations/me"
    org = Organization.model_validate_json(
        await http.request(log, url, headers=ANTHROPIC_VERSION_HEADERS)
    )
    yield org


async def fetch_users(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[User, None]:
    url = f"{base_url}/v1/organizations/users"
    params: dict[str, str | int] = {"limit": USERS_PAGE_LIMIT}

    while True:
        page = UserList.model_validate_json(
            await http.request(
                log, url, params=params, headers=ANTHROPIC_VERSION_HEADERS
            )
        )

        for user in page.data:
            yield user

        if not page.has_more or page.last_id is None:
            break

        params["after_id"] = page.last_id


async def _fetch_usage_day(
    http: HTTPSession,
    base_url: str,
    log: Logger,
    day: datetime,
) -> AsyncGenerator[ClaudeCodeUsageRecord, None]:
    url = f"{base_url}/v1/organizations/usage_report/claude_code"
    starting_at = _midnight(day).strftime("%Y-%m-%d")
    params: dict[str, str | int] = {
        "starting_at": starting_at,
        "limit": USAGE_PAGE_LIMIT,
    }

    while True:
        report = ClaudeCodeUsageReport.model_validate_json(
            await http.request(
                log, url, params=params, headers=ANTHROPIC_VERSION_HEADERS
            )
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

    # Freshness gate: only process day D once we are safely past the point where
    # its data is complete. Day D's data is "done" at midnight(D+1); add a safety
    # lag because the API only returns data older than ~1 hour.
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
