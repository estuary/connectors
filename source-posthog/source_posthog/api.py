"""
PostHog API client functions.
"""

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import wraps
from logging import Logger
from typing import Callable, ParamSpec
from urllib.parse import urljoin

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    EndpointConfig,
    Event,
    FeatureFlag,
    HogQLEntity,
    HogQLResponseMeta,
    HogQLRow,
    Person,
    PersonalApiKeyInfo,
    PostHogEntity,
    Project,
    ProjectEntity,
    ProjectIdValidationContext,
    RestResponseMeta,
    Session,
)

P = ParamSpec("P")

HOGQL_PAGE_SIZE = 50_000
BACKFILL_TIMEOUT_PERIOD = timedelta(minutes=5)

# PostHog events eventual consistency horizon.
# Events may be delayed in appearing in query results due to ClickHouse replication
# and Kafka processing. The lookback stream trails behind to capture delayed events.
EVENTS_EVENTUAL_CONSISTENCY_HORIZON = timedelta(hours=1)

# How far before the start of the cursor's hour each Persons poll reads again.
# PostHog rounds `last_seen_at` down to the hour, so the cursor's own hour has
# to be read again in any case. The extra hour catches persons that PostHog
# writes late. Persons written more than about an hour late are still missed.
PERSONS_REREAD = timedelta(hours=1)

# How far a session's ingestion (`max_inserted_at`) may trail its last event
# (`$end_timestamp`) and still be captured. The query bounds `$end_timestamp`
# by it so PostHog's backend can skip old data, which the computed cursor can't
# do. Sessions whose events arrive later than this are not captured. PostHog's
# own SESSIONS_LOOKBACK_DAYS is 3 days, with the note "While testing, 3 days
# catched almost all sessions"; we allow 5 to cover more of the tail.
SESSIONS_ALLOWED_LATENESS = timedelta(days=5)

# One unit of the cursor's resolution; `Session.cursor_expression` fixes it at
# a second.
SESSIONS_CURSOR_TICK = timedelta(seconds=1)
SESSIONS_REALTIME_LAG = timedelta(minutes=5)
SESSIONS_LOOKBACK_LAG = timedelta(hours=1)


# Cache for project IDs per organization (avoids re-fetching on retry).
_project_ids_cache = None


async def fetch_project_ids(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> list[int]:
    global _project_ids_cache

    if _project_ids_cache is None:
        _project_ids_cache = [
            project.id async for project in fetch_entity(Project, http, config, log)
        ]

    return _project_ids_cache


async def fetch_token_scopes(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> set[str]:
    url = urljoin(config.advanced.base_url, "api/personal_api_keys/@current")

    response = await http.request(log, url)
    api_key_info = PersonalApiKeyInfo.model_validate_json(response)

    log.info(f"Token scopes: {api_key_info.scopes}")
    return api_key_info.scopes


async def _fetch_from_url[
    T: PostHogEntity[str] | PostHogEntity[int] | ProjectEntity[str] | ProjectEntity[int]
](
    url: str,
    model: type[T],
    http: HTTPSession,
    log: Logger,
    validation_context: object | None = None,
) -> AsyncGenerator[T, None]:
    current_url: str | None = url

    while current_url is not None:
        _, body = await http.request_stream(log, current_url)
        processor: IncrementalJsonProcessor[T, RestResponseMeta] = (
            IncrementalJsonProcessor(
                body(),
                "results.item",
                model,
                remainder_cls=RestResponseMeta,
                validation_context=validation_context,
            )
        )

        async for item in processor:
            yield item

        remainder = processor.get_remainder()
        current_url = remainder.next if remainder else None


async def fetch_entity[T: PostHogEntity[str] | PostHogEntity[int]](
    model: type[T],
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[T, None]:
    url = model.get_api_endpoint_url(config)
    count = 0

    async for item in _fetch_from_url(url, model, http, log):
        yield item
        count += 1

    log.info(f"Fetched {count} {model.resource_name}")


async def fetch_project_entity[T: ProjectEntity[str] | ProjectEntity[int]](
    model: type[T],
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[T, None]:
    total_count = 0

    async for project in fetch_entity(Project, http, config, log):
        url = model.get_api_endpoint_url(config.advanced.base_url, project.id)
        count = 0

        async for item in _fetch_from_url(url, model, http, log):
            yield item
            count += 1

        log.info(f"Fetched {count} {model.resource_name} from project {project.id}")
        total_count += count

    log.info(f"Fetched {total_count} total {model.resource_name}")


async def _get_hogql_columns(
    model: type[HogQLEntity[str] | HogQLEntity[int]],
    base_url: str,
    project_id: int,
    http: HTTPSession,
    log: Logger,
) -> list[str]:
    url = model.get_api_endpoint_url(base_url, project_id)
    payload = {
        "query": {
            "kind": "HogQLQuery",
            "query": f"SELECT * FROM {model.table_name} LIMIT 0",
        },
    }
    response = await http.request(log, url, method="POST", json=payload)
    return HogQLResponseMeta.model_validate_json(response).columns


def _hogql_datetime(when: datetime) -> str:
    """Write `when` as a HogQL UTC timestamp with microsecond precision."""
    serialized = when.astimezone(UTC).replace(tzinfo=None).isoformat()
    return f"toDateTime64('{serialized}', 6, 'UTC')"


def _hogql_cursor(model: type[HogQLEntity[str]] | type[HogQLEntity[int]]) -> str:
    # A bare column lets PostHog use it to skip data, which PostHog's docs
    # recommend for paging `events` by `timestamp`.
    if len(model.cursor_columns) == 1:
        return model.cursor_columns[0]
    return f"COALESCE({', '.join(model.cursor_columns)})"


async def _query_hogql[T: HogQLEntity[str] | HogQLEntity[int]](
    model: type[T],
    column_names: list[str],
    start: datetime,
    inclusive_start: bool,
    after_id: str | int | None,
    end: datetime | None,
    base_url: str,
    project_id: int,
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[T]:
    """Yield one page of rows ordered by (cursor, id), up to `end` inclusive.

    Without `after_id`, the page opens at `start`. With it, the page opens
    after the row (`start`, `after_id`), the last row of the previous page.
    """
    log.debug(
        "Querying HogQL",
        {
            "table": model.table_name,
            "project_id": project_id,
            "start": start,
            "after_id": after_id,
            "end": end,
        },
    )

    url = model.get_api_endpoint_url(base_url, project_id)
    cursor = _hogql_cursor(model)
    # Compare as strings: the ORDER BY uses the same expression, so the
    # comparison and the sort always agree, whatever the column's type.
    tiebreak = f"toString({model.tiebreak_column})"

    if after_id is not None:
        lower_bound = (
            f"({cursor} > {_hogql_datetime(start)} OR "
            + f"({cursor} = {_hogql_datetime(start)} "
            + f"AND {tiebreak} > {_hogql_string(str(after_id))}))"
        )
    elif inclusive_start:
        lower_bound = f"{cursor} >= {_hogql_datetime(start)}"
    else:
        lower_bound = f"{cursor} > {_hogql_datetime(start)}"

    upper_bound = (
        f"AND {cursor} <= {_hogql_datetime(end)} " if end is not None else ""
    )

    payload = {
        "query": {
            "kind": "HogQLQuery",
            "query": f"SELECT {", ".join(column_names)} "
            + f"FROM {model.table_name} "
            + f"WHERE {lower_bound} "
            + upper_bound
            + f"ORDER BY {cursor} ASC, {tiebreak} ASC "
            + f"LIMIT {HOGQL_PAGE_SIZE}",
        },
    }

    _, body = await http.request_stream(
        log,
        url,
        method="POST",
        json=payload,
    )
    processor = IncrementalJsonProcessor(body(), "results.item", HogQLRow)

    async for row in processor:
        yield model.model_validate(
            dict(zip(column_names, row.root, strict=True)),
            context=ProjectIdValidationContext(project_id),
        )


async def _fetch_hogql_between[T: HogQLEntity[str] | HogQLEntity[int]](
    model: type[T],
    start: datetime,
    end: datetime | None,
    base_url: str,
    project_id: int,
    http: HTTPSession,
    log: Logger,
    inclusive_start: bool = False,
) -> AsyncGenerator[T]:
    """Yield every row with a cursor after `start` and at or before `end`.

    With `inclusive_start`, rows at `start` are included too. A missing `end`
    means no upper bound. Rows come in (cursor, id) order, and each row comes
    once, even when many rows share one cursor value.
    """
    column_names = await _get_hogql_columns(model, base_url, project_id, http, log)
    reached = start
    after_id: str | int | None = None

    while True:
        batch_count = 0
        page_start = (reached, after_id)

        async for item in _query_hogql(
            model,
            column_names,
            reached,
            inclusive_start,
            after_id,
            end,
            base_url,
            project_id,
            http,
            log,
        ):
            batch_count += 1
            # Both halves of the position come from the same row, the last one
            # the server sent, so the next page opens exactly where this ended.
            reached = item.get_cursor()
            after_id = item.id
            yield item

        if batch_count < HOGQL_PAGE_SIZE:
            break

        # A full page that does not move the position would repeat forever.
        if (reached, after_id) == page_start:
            raise RuntimeError(
                f"HogQL paging on {model.table_name} did not advance past "
                + f"({reached.isoformat()}, {after_id}) in project {project_id}"
            )


async def backfill_feature_flags(
    http: HTTPSession,
    config: EndpointConfig,
    project_id: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[FeatureFlag | PageCursor, None]:
    assert isinstance(page, str | None)
    assert isinstance(cutoff, datetime)

    start_date = datetime.fromisoformat(page) if page is not None else config.start_date

    if start_date >= cutoff:
        return

    base_url = config.advanced.base_url
    url = FeatureFlag.get_api_endpoint_url(base_url, project_id)
    ctx = ProjectIdValidationContext(project_id=project_id)

    new_cursor = cutoff
    doc_count = 0

    async for item in _fetch_from_url(
        url, FeatureFlag, http, log, validation_context=ctx
    ):
        item_cursor = item.get_cursor()

        if item_cursor >= cutoff:
            continue

        if item_cursor <= start_date:
            break

        new_cursor = min(new_cursor, item_cursor)
        doc_count += 1
        yield item

    log.info(f"Backfilled {doc_count} feature flags from project {project_id}")


async def fetch_feature_flags(
    http: HTTPSession,
    config: EndpointConfig,
    project_id: int,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[FeatureFlag | LogCursor, None]:
    assert isinstance(cursor, datetime)

    base_url = config.advanced.base_url
    url = FeatureFlag.get_api_endpoint_url(base_url, project_id)
    ctx = ProjectIdValidationContext(project_id=project_id)

    new_cursor = cursor
    doc_count = 0

    async for item in _fetch_from_url(
        url, FeatureFlag, http, log, validation_context=ctx
    ):
        item_cursor = item.get_cursor()

        if item_cursor <= cursor:
            break

        new_cursor = max(new_cursor, item_cursor)
        doc_count += 1
        yield item

    log.info(f"Fetched {doc_count} feature flag changes from project {project_id}")

    if new_cursor > cursor:
        yield new_cursor


def backfill_timeout(timeout_period: timedelta):
    """
    Exposes a timeout parameter for our `backfill_timeout` decorator
    by using the decorator factory pattern.

    (https://stackoverflow.com/questions/5929107/decorators-with-parameters)

    These decorators may only be used for documents with timestamps in ascending order.
    Once the allowed time has elapsed, they continue to process documents until they have
    completed the discrete unit of time, guaranteeing the end timestamp to be inclusive.
    This means that any remaining documents will be discarded.
    """

    def decorator[**P](
        backfill_fn: Callable[
            P, AsyncGenerator[HogQLEntity[str] | HogQLEntity[int] | PageCursor]
        ],
    ) -> Callable[P, AsyncGenerator[HogQLEntity[str] | HogQLEntity[int] | PageCursor]]:

        @wraps(backfill_fn)
        async def wrapper(
            *args: P.args, **kwargs: P.kwargs
        ) -> AsyncGenerator[HogQLEntity[str] | HogQLEntity[int] | PageCursor, None]:
            """Wraps the real `backfill_fn` in our timeout logic"""

            timeout = datetime.now(tz=UTC) + timeout_period
            last_doc_ts_found_at_timeout = None

            async for item in backfill_fn(*args, **kwargs):
                if not isinstance(item, HogQLEntity):
                    yield item
                    continue

                now = datetime.now(tz=UTC)

                if last_doc_ts_found_at_timeout is None and now > timeout:
                    last_doc_ts_found_at_timeout = item.get_cursor()

                if (
                    last_doc_ts_found_at_timeout is not None
                    and item.get_cursor() > last_doc_ts_found_at_timeout
                ):
                    yield last_doc_ts_found_at_timeout.isoformat()
                    return

                yield item

        return wrapper

    return decorator


@dataclass(frozen=True, slots=True)
class _SessionsQuery:
    """The SELECT shape for one project's sessions table, probed once per sweep.

    `column_names` positionally matches `selected`; the row zip relies on it.
    """

    cursor: str
    selected: list[str]
    column_names: list[str]


async def _build_sessions_query(
    base_url: str,
    project_id: int,
    http: HTTPSession,
    log: Logger,
) -> _SessionsQuery:
    discovered = await _get_hogql_columns(Session, base_url, project_id, http, log)
    # `$`-prefixed names are aliased bare so documents read like the rest of the
    # connector. `team_id` is left alone: HogQL rejects it as an alias.
    return _SessionsQuery(
        cursor=Session.cursor_expression(discovered),
        selected=[
            f"{column} AS {column.removeprefix('$')}"
            if column.startswith("$")
            else column
            for column in discovered
        ]
        + list(Session.extra_columns.values()),
        column_names=[column.removeprefix("$") for column in discovered]
        + list(Session.extra_columns),
    )


def _hogql_string(value: str) -> str:
    """Quote `value` as a HogQL string literal."""
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


async def _query_sessions(
    query: _SessionsQuery,
    start: datetime,
    after_id: str | None,
    end: datetime,
    base_url: str,
    project_id: int,
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Session, None]:
    """Yield one page of sessions from [start, end), ordered by (cursor, id)."""
    url = Session.get_api_endpoint_url(base_url, project_id)
    ctx = ProjectIdValidationContext(project_id=project_id)
    cursor = query.cursor
    scan_column = Session.scan_column
    tiebreak = Session.tiebreak_column

    def literal(when: datetime) -> str:
        serialized = when.astimezone(UTC).replace(tzinfo=None).isoformat()
        return f"toDateTime64('{serialized}', 6, 'UTC')"

    if after_id is None:
        lower_bound = f"{cursor} >= {literal(start)}"
    else:
        lower_bound = (
            f"({cursor} > {literal(start)} OR "
            + f"({cursor} = {literal(start)} "
            + f"AND {tiebreak} > {_hogql_string(after_id)}))"
        )

    payload = {
        "query": {
            "kind": "HogQLQuery",
            "query": f"SELECT {', '.join(query.selected)} "
            + f"FROM {Session.table_name} "
            + f"WHERE {lower_bound} "
            + f"AND {cursor} < {literal(end)} "
            # Present so PostHog's backend can skip old data: the cursor is
            # computed from two columns, so it cannot be used to narrow the
            # scan, but this stored column can. The upper bound is implied by
            # the cursor and costs nothing. The lower bound is not free — it
            # drops a session whose events arrive more than
            # SESSIONS_ALLOWED_LATENESS after the session ended.
            + f"AND {scan_column} >= {literal(start - SESSIONS_ALLOWED_LATENESS)} "
            + f"AND {scan_column} < {literal(end)} "
            + f"ORDER BY {cursor} ASC, {tiebreak} ASC "
            + f"LIMIT {HOGQL_PAGE_SIZE}",
        },
    }

    _, body = await http.request_stream(log, url, method="POST", json=payload)
    processor = IncrementalJsonProcessor(body(), "results.item", HogQLRow)

    async for row in processor:
        yield Session.model_validate(
            dict(zip(query.column_names, row.root, strict=True)),
            context=ctx,
        )


async def _fetch_sessions_between(
    http: HTTPSession,
    base_url: str,
    project_id: int,
    log: Logger,
    start: datetime,
    end: datetime,
) -> AsyncGenerator[Session, None]:
    """Page through sessions in [start, end), yielding every row the API returns."""
    query = await _build_sessions_query(base_url, project_id, http, log)
    reached = start
    after_id: str | None = None

    while True:
        batch_count = 0

        async for item in _query_sessions(
            query,
            reached,
            after_id,
            end,
            base_url,
            project_id,
            http,
            log,
        ):
            batch_count += 1
            # Both halves of the page key come from the same row — the last one
            # the server sent — so the next page opens exactly where this ended.
            reached = item.get_cursor()
            after_id = item.id
            yield item

        if batch_count < HOGQL_PAGE_SIZE:
            break


async def fetch_sessions(
    http: HTTPSession,
    base_url: str,
    project_id: int,
    lag: timedelta,
    should_evict_emitted_changes: bool,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[Session | LogCursor, None]:
    """Emit sessions whose ingestion time landed since the last poll.

                cursor        horizon - 1s   horizon = now - lag
    ───────────────┼───────────────┼──────────────┼─────▶ time (1s ticks)
                   │               │              │
    cursor_expr ───[═══════════════╪══════════════)
    scan_column ═══╪═══════════════╪══════════════)
    emitted ───────(═══════════════]              │
                   │               │              └─ excluded; rows this
                   │               │                 recent may not be
                   │               │                 queryable yet
                   │               └─ last second collected
                   └─ swept but not emitted: rows on this instant were emitted
                      by the poll that checkpointed here

    `lag` is how far this subtask trails the present: SESSIONS_REALTIME_LAG for
    the realtime subtask, SESSIONS_LOOKBACK_LAG for the one that re-walks the
    same span later to pick up rows that were not yet queryable. Only the
    trailing subtask sets `should_evict_emitted_changes`.
    """
    assert isinstance(cursor, datetime)

    horizon = datetime.now(tz=UTC).replace(microsecond=0) - lag
    if horizon <= cursor:
        return

    # Namespaced per project so one project's pruning cannot evict another's
    # entries; the subtasks of a single project share a cursor timeline.
    cache_name = f"sessions/{project_id}"
    last_cursor = cursor
    doc_count = 0

    async for item in _fetch_sessions_between(
        http, base_url, project_id, log, cursor, horizon
    ):
        item_cursor = item.get_cursor()

        if item_cursor <= cursor:
            continue

        last_cursor = max(last_cursor, item_cursor)

        # The two subtasks re-walk the same span an hour apart, so most of what
        # the lookback subtask reads the realtime one has already emitted.
        if cache.should_yield(cache_name, item.id, item_cursor):
            doc_count += 1
            yield item

    if should_evict_emitted_changes:
        # Everything at or before this poll's start has now been walked by the
        # trailing subtask too, so it can no longer suppress anything. Only the
        # trailing subtask may evict: the realtime one runs ahead of entries the
        # lookback one still needs.
        cache.cleanup(cache_name, cursor)

    log.info(f"Fetched {doc_count} session changes from project {project_id}")

    if last_cursor > cursor:
        yield last_cursor


@backfill_timeout(BACKFILL_TIMEOUT_PERIOD)
async def backfill_sessions(
    http: HTTPSession,
    base_url: str,
    start_date: datetime,
    project_id: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Session | PageCursor, None]:
    """Walk sessions from the configured start date up to the incremental cutoff.

             start_date      cutoff - 1s    cutoff = incremental hand-off
    ───────────────┼───────────────┼──────────────┼─────▶ time (1s ticks)
                   │               │              │
    cursor_expr ───[═══════════════╪══════════════)
    scan_column ═══╪═══════════════╪══════════════)
    emitted ───────[═══════════════]              │
                   │               │              └─ belongs to the next stage:
                   │               │                 incremental is seeded one
                   │               │                 tick back so it opens here
                   │               └─ last second collected
                   └─ emitted on the first pass; on a resume this instant was
                      already emitted before the checkpoint was written

    Resumes from the reached instant rather than an offset, so a row that is
    deleted mid-walk renumbers nothing.
    """
    assert isinstance(page, str | None)
    assert isinstance(cutoff, datetime)

    resuming = page is not None
    start = datetime.fromisoformat(page) if resuming else start_date

    if start >= cutoff:
        return

    cache_name = f"sessions/{project_id}"
    last_cursor = start
    doc_count = 0

    async for item in _fetch_sessions_between(
        http, base_url, project_id, log, start, cutoff
    ):
        item_cursor = item.get_cursor()

        if resuming and item_cursor <= start:
            continue

        last_cursor = max(last_cursor, item_cursor)

        # The lookback subtask opens an hour before this cutoff, so the tail of
        # the backfill's range is walked by both.
        if cache.should_yield(cache_name, item.id, item_cursor):
            doc_count += 1
            yield item

    log.info(f"Backfilled {doc_count} sessions from project {project_id}")

    if last_cursor > start:
        yield last_cursor.isoformat()


@backfill_timeout(BACKFILL_TIMEOUT_PERIOD)
async def backfill_project_events(
    http: HTTPSession,
    config: EndpointConfig,
    project_id: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Event | PageCursor, None]:
    assert isinstance(page, str | None)
    assert isinstance(cutoff, datetime)

    start_date = datetime.fromisoformat(page) if page is not None else config.start_date

    if start_date >= cutoff:
        return

    base_url = config.advanced.base_url
    cache_name = f"events/{project_id}"
    # The lookback incremental starts this far before the cutoff, so only
    # events after this instant can be read by both. Caching only those keeps
    # the cache small: a full history would not fit in memory.
    overlap_start = cutoff - EVENTS_EVENTUAL_CONSISTENCY_HORIZON
    new_cursor = start_date
    doc_count = 0

    async for item in _fetch_hogql_between(
        Event, start_date, cutoff, base_url, project_id, http, log
    ):
        item_cursor = item.get_cursor()
        new_cursor = max(new_cursor, item_cursor)

        if item_cursor > overlap_start and not cache.should_yield(
            cache_name, item.id, item_cursor
        ):
            continue

        doc_count += 1
        yield item

    log.info(f"Backfilled {doc_count} events from project {project_id}")

    if new_cursor > start_date:
        yield new_cursor.isoformat()


async def fetch_project_events(
    http: HTTPSession,
    config: EndpointConfig,
    project_id: int,
    horizon: timedelta | None,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[Event | LogCursor, None]:
    assert isinstance(cursor, datetime)

    base_url = config.advanced.base_url
    now = datetime.now(tz=UTC)
    upper_bound = now - horizon if horizon else None

    # One cache per project, so one project's cleanup cannot remove another
    # project's entries.
    cache_name = f"events/{project_id}"
    new_cursor = cursor
    doc_count = 0

    async for item in _fetch_hogql_between(
        Event, cursor, upper_bound, base_url, project_id, http, log
    ):
        item_cursor = item.get_cursor()
        new_cursor = max(new_cursor, item_cursor)

        if cache.should_yield(cache_name, item.id, item_cursor):
            doc_count += 1
            yield item

    if horizon is not None:
        # Only the lookback subtask (the one with a horizon) cleans up. It
        # trails the realtime subtask, so no subtask reads at or before its
        # starting cursor again.
        cache.cleanup(cache_name, cursor)

    log.info(f"Fetched {doc_count} events from project {project_id}")

    if new_cursor > cursor:
        yield new_cursor


@backfill_timeout(BACKFILL_TIMEOUT_PERIOD)
async def backfill_persons(
    http: HTTPSession,
    config: EndpointConfig,
    project_id: int,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Person | PageCursor, None]:
    assert isinstance(page, str | None)
    assert isinstance(cutoff, datetime)

    start_date = datetime.fromisoformat(page) if page is not None else config.start_date

    if start_date >= cutoff:
        return

    base_url = config.advanced.base_url
    new_cursor = start_date
    doc_count = 0

    async for item in _fetch_hogql_between(
        Person, start_date, cutoff, base_url, project_id, http, log
    ):
        doc_count += 1
        new_cursor = max(new_cursor, item.get_cursor())
        yield item

    log.info(f"Backfilled {doc_count} persons from project {project_id}")

    if new_cursor > start_date:
        yield new_cursor.isoformat()


async def fetch_persons(
    http: HTTPSession,
    config: EndpointConfig,
    project_id: int,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[Person | LogCursor, None]:
    """Emit persons added or seen since the last poll.

    PostHog writes `last_seen_at` as the start of the hour, so persons added
    after the cursor moved can still carry the cursor's own value. Each poll
    reads again from the hour before the cursor's hour, which also catches
    persons written up to an hour late. A person is emitted again only when
    its cursor value changes.
    """
    assert isinstance(cursor, datetime)

    base_url = config.advanced.base_url
    # From the start of the cursor's hour, so the small step added to the
    # cursor below never moves the re-read window.
    start = cursor.replace(minute=0, second=0, microsecond=0) - PERSONS_REREAD
    # One cache per project, so one project's cleanup cannot remove another
    # project's entries.
    cache_name = f"persons/{project_id}"
    new_cursor = cursor
    doc_count = 0

    async for item in _fetch_hogql_between(
        Person, start, None, base_url, project_id, http, log, inclusive_start=True
    ):
        item_cursor = item.get_cursor()
        new_cursor = max(new_cursor, item_cursor)

        if cache.should_yield(cache_name, item.id, item_cursor):
            doc_count += 1
            yield item

    # The CDK requires a strictly greater cursor after any documents. A new
    # person in the cursor's own hour has the same value as the cursor, so
    # step forward by the smallest amount.
    if doc_count > 0:
        new_cursor = max(new_cursor, cursor + timedelta(microseconds=1))

    # The next poll reads from `next_start` (inclusive), so it cannot need
    # entries from before it. `cleanup` removes entries at or before its
    # argument.
    next_start = (
        new_cursor.replace(minute=0, second=0, microsecond=0) - PERSONS_REREAD
    )
    cache.cleanup(cache_name, next_start - timedelta(microseconds=1))

    log.info(f"Fetched {doc_count} persons from project {project_id}")

    if new_cursor > cursor:
        yield new_cursor
