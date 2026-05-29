from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, Callable, Type, TypeVar

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel
from requests.utils import parse_header_links

from .models import (
    Dataset,
    ExploreResponse,
    ExploreQuery,
    ExploreRow,
    Issue,
    SentryEntity,
    dataset_row_model,
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
        # next_url already contains the query params necessary to continue paginating through
        # our query, so we don't need to continue passing any parameters in the params dict.
        params = {}


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

    async for item in list_entity(model, http, organization, log, params):
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


# 30d is the largest relative window Sentry serves at full fidelity across all
# datasets, including spans - larger windows are downsampled. Incremental polls
# query within this rolling window, so a cursor that has fallen more than this
# far behind "now" has gaps that statsPeriod can no longer reach.
MAX_EXPLORE_FULL_FIDELITY_WINDOW = timedelta(days=30)
_EXPLORE_STATS_PERIOD = f"{MAX_EXPLORE_FULL_FIDELITY_WINDOW.days}d"
# Explore offset pagination cannot exceed 10,000 rows per query.
_MAX_EXPLORE_ROWS_PER_QUERY = 10_000
_MAX_EXPLORE_PER_PAGE = 100

# Hold incremental polls this far back from "now" so Sentry's ingestion has time
# to settle. `timestamp` is event time, not index time, so very recent rows may
# still be arriving. Querying only up to (now - _EXPLORE_INGESTION_LAG) avoids
# advancing the cursor past seconds that aren't fully populated yet.
_EXPLORE_INGESTION_LAG = timedelta(minutes=10)

_EXPLORE_BACKFILL_WINDOW = timedelta(days=1)


@dataclass(frozen=True)
class _QuerySummary:
    # Yielded as the final item of a query, carrying the query-level signals
    # that can't be derived from the rows themselves because they come from
    # the response headers/meta.
    #
    # True when the 10,000-row pagination cap was hit with more rows available.
    saturated: bool
    # True if any page reported dataScanned: "partial", indicating sampled data.
    partial: bool


def _explore_url(organization: str) -> str:
    return f"https://sentry.io/api/0/organizations/{organization}/events/"


def _base_explore_params(explore_query: ExploreQuery) -> dict[str, Any]:
    return {
        "dataset": explore_query.dataset.value,
        "field": explore_query.field_list,
        "sort": "timestamp",
        "per_page": _MAX_EXPLORE_PER_PAGE,
        "project": explore_query.project_ids or [-1],  # -1 means all projects
    }


def _timestamp_window_query(lower: datetime, upper: datetime, user_query: str) -> str:
    clauses = [
        f"timestamp:>={lower.isoformat(timespec='seconds')}",
        f"timestamp:<{upper.isoformat(timespec='seconds')}",
    ]
    if user_query:
        clauses.append(user_query)
    return " ".join(clauses)


def _incremental_explore_params(explore_query: ExploreQuery, lower: datetime, upper: datetime) -> dict[str, Any]:
    params = _base_explore_params(explore_query)
    params["statsPeriod"] = _EXPLORE_STATS_PERIOD
    params["query"] = _timestamp_window_query(lower, upper, explore_query.query)
    return params


def _backfill_explore_params(explore_query: ExploreQuery, lower: datetime, upper: datetime) -> dict[str, Any]:
    params = _base_explore_params(explore_query)
    params["start"] = lower.isoformat(timespec="seconds")
    params["end"] = upper.isoformat(timespec="seconds")
    if explore_query.query:
        params["query"] = explore_query.query
    return params


async def _run_explore_query(
    http: HTTPSession,
    organization: str,
    explore_query: ExploreQuery,
    log: Logger,
    params: dict[str, Any],
) -> AsyncGenerator[ExploreRow | _QuerySummary, None]:
    """Stream rows from one Explore query, following Link pagination until the
    result set is exhausted or the 10,000-row offset cap is hit. Rows are yielded
    as they arrive (never buffered); a single `_QuerySummary` is yielded last,
    carrying the pass-level signals."""
    model = dataset_row_model(explore_query.dataset)
    url = _explore_url(organization)
    next_params: dict[str, Any] = params
    count = 0
    partial = False
    saturated = False

    while True:
        headers, body = await http.request_stream(log, url, params=next_params)
        processor = IncrementalJsonProcessor(
            body(), "data.item", model, ExploreResponse
        )
        async for row in processor:
            count += 1
            yield row

        if processor.get_remainder().meta.dataScanned == "partial":
            partial = True

        link = HeaderLink.from_headers(headers)
        if not link.has_more:
            break
        if count >= _MAX_EXPLORE_ROWS_PER_QUERY:
            saturated = True
            break

        url = link.next_url
        # next_url already carries the full query; don't re-send params.
        next_params = {}

    yield _QuerySummary(saturated=saturated, partial=partial)


async def _read_explore_range(
    http: HTTPSession,
    organization: str,
    explore_query: ExploreQuery,
    log: Logger,
    *,
    lower_bound: datetime,
    upper_bound: datetime,
    build_params: Callable[[ExploreQuery, datetime, datetime], dict[str, Any]],
    expect_full: bool,
) -> AsyncGenerator[ExploreRow, None]:
    """Emit every row in [lower, upper) at the dataset's available fidelity,
    advancing a 1-second-resolution cursor. A saturated pass that crosses a
    second boundary advances the cursor to the highest timestamp seen and
    resumes; a saturated pass confined to a single second (>10,000 rows in that
    second — unreachable via Sentry's API) is logged and skipped so the walk
    never loops."""
    cursor = lower_bound
    while cursor < upper_bound:
        summary: _QuerySummary | None = None
        max_timestamp: datetime | None = None
        params = build_params(explore_query, cursor, upper_bound)
        async for item in _run_explore_query(
            http, organization, explore_query, log, params
        ):
            if isinstance(item, _QuerySummary):
                summary = item  # terminal item; the loop ends right after this
            else:
                if max_timestamp is None or item.timestamp > max_timestamp:
                    max_timestamp = item.timestamp
                yield item
        assert summary is not None  # a query always yields a terminal _QuerySummary

        if summary.partial and expect_full:
            log.warning(
                "Explore returned sampled data (dataScanned=partial) on a full-fidelity query; org cache routing may have changed",
                {"explore_query": explore_query.name, "dataset": explore_query.dataset.value, "params": params},
            )

        if not summary.saturated:
            break

        assert max_timestamp is not None  # saturated => at least 10000 rows read
        if max_timestamp > cursor:
            cursor = max_timestamp  # re-read this second next pass (it may be cut off)
        else:
            log.warning(
                "More than 10,000 rows in a single second; rows beyond the pagination cap are unreachable via Sentry's API and were skipped",
                {
                    "explore_query": explore_query.name,
                    "dataset": explore_query.dataset.value,
                    "second": cursor.isoformat(timespec="seconds"),
                },
            )
            cursor = cursor + timedelta(seconds=1)


async def fetch_explore_query(
    http: HTTPSession,
    organization: str,
    explore_query: ExploreQuery,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ExploreRow | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    now = datetime.now(tz=UTC).replace(microsecond=0)
    # Hold back from the live edge so Sentry's ingestion can settle; only query
    # timestamps that have aged past the lag (see _EXPLORE_INGESTION_LAG).
    upper = now - _EXPLORE_INGESTION_LAG
    if upper <= log_cursor:
        return

    window_start = now - MAX_EXPLORE_FULL_FIDELITY_WINDOW
    if log_cursor < window_start:
        # statsPeriod only reaches back to window_start, so rows in
        # (log_cursor, window_start) can't be fetched and the cursor will jump
        # forward past them. Surface the gap; recovering it requires resetting
        # the backfill, and only if it's still within Sentry's retention.
        log.warning(
            "Cursor predates Sentry's full-fidelity window; rows between the cursor and the window start were not captured",
            {
                "explore_query": explore_query.name,
                "log_cursor": log_cursor,
                "window_start": window_start,
                "stats_period": _EXPLORE_STATS_PERIOD,
            },
        )

    new_log_cursor = log_cursor
    async for row in _read_explore_range(
        http,
        organization,
        explore_query,
        log,
        lower_bound=log_cursor,
        upper_bound=upper,
        build_params=_incremental_explore_params,
        expect_full=True,
    ):
        # The query lower bound is inclusive (for saturation drain), so the
        # boundary second can re-surface rows already emitted in a prior poll.
        # Skipping them keeps the cursor strictly advancing whenever we emit, so
        # we never yield a cursor <= log_cursor and always yield a new cursor
        # after emitting documents.
        if row.timestamp <= log_cursor:
            continue
        new_log_cursor = max(row.timestamp, new_log_cursor)
        yield row

    if new_log_cursor != log_cursor:
        yield new_log_cursor


async def backfill_explore_query(
    http: HTTPSession,
    organization: str,
    explore_query: ExploreQuery,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ExploreRow | PageCursor, None]:
    """Backfill the pre-30d tail (older than the rolling full-fidelity window)
    one explicit start/end window at a time. errors/transactions are full;
    spans are sampled (Sentry downsamples on explicit ranges)."""
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    page_ts = datetime.fromisoformat(page)
    if page_ts >= cutoff:
        return

    window_end = min(page_ts + _EXPLORE_BACKFILL_WINDOW, cutoff)

    async for row in _read_explore_range(
        http,
        organization,
        explore_query,
        log,
        lower_bound=page_ts,
        upper_bound=window_end,
        build_params=_backfill_explore_params,
        expect_full=explore_query.dataset is not Dataset.SPANS,
    ):
        yield row

    yield window_end.isoformat(timespec="seconds")


async def validate_explore_query(
    http: HTTPSession,
    organization: str,
    explore_query: ExploreQuery,
    log: Logger,
) -> None:
    """Issue one minimal Explore request to confirm Sentry accepts the
    configured dataset, fields, and query."""
    now = datetime.now(tz=UTC)
    params = _incremental_explore_params(explore_query, now - timedelta(minutes=1), now)
    params["per_page"] = 1

    # request() returns the full body and raises HTTPError on a non-2xx status;
    # we only care that the query was accepted, so the response is discarded.
    await http.request(log, _explore_url(organization), params=params)
