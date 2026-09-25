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

# 3 days is PostHog's own SESSIONS_LOOKBACK_DAYS, from the sessions model in
# their batch exporter:
# products/batch_exports/backend/temporal/sql/sessions.py
# Their note on choosing it: "While testing, 3 days catched almost all sessions."
SESSIONS_LOOKBACK = timedelta(days=3)

# One unit of the Sessions cursor's resolution, which `toStartOfSecond` in
# `Session.cursor_expression` fixes at one second.
SESSIONS_CURSOR_TICK = timedelta(seconds=1)


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


async def _query_hogql[T: HogQLEntity[str] | HogQLEntity[int]](
    model: type[T],
    start_date: datetime,
    end_date: datetime | None,
    base_url: str,
    project_id: int,
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[T]:
    log.debug(
        "Querying HogQL",
        {
            "table": model.table_name,
            "project_id": project_id,
            "start": start_date,
            "end": end_date,
        },
    )

    url = model.get_api_endpoint_url(base_url, project_id)
    column_names = await _get_hogql_columns(model, base_url, project_id, http, log)

    coalesced_cursor_fields = f'COALESCE({",".join(model.cursor_columns)})'

    serialized_start_date = start_date.astimezone(UTC).replace(tzinfo=None).isoformat()
    serialized_end_date = (
        end_date.astimezone(UTC).replace(tzinfo=None).isoformat()
        if end_date is not None
        else None
    )

    start_date_clause = (
        f"WHERE {coalesced_cursor_fields} > "
        + f"toDateTime64('{serialized_start_date}', 6, 'UTC') "
    )
    end_date_clause = (
        f"AND {coalesced_cursor_fields} <= toDateTime64('{serialized_end_date}', 6, 'UTC') "
        if end_date is not None
        else ""
    )

    payload = {
        "query": {
            "kind": "HogQLQuery",
            "query": f"SELECT {", ".join(column_names)} "
            + f"FROM {model.table_name} "
            + start_date_clause
            + end_date_clause
            + f"ORDER BY {coalesced_cursor_fields} ASC "
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

    `cursor` is an expression rather than a column, so it is repeated in the
    WHERE and ORDER BY clauses rather than referenced by alias. `column_names`
    positionally matches `selected`, which is what the row zip relies on.
    """

    cursor: str
    selected: list[str]
    column_names: list[str]


async def _sessions_query(
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
            f"{column} AS {column.lstrip('$')}" if column.startswith("$") else column
            for column in discovered
        ]
        + Session.extra_columns,
        column_names=[column.lstrip("$") for column in discovered]
        + [
            "session_id_v7",
            "team_id",
            "duration",
        ],
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
    lookback: timedelta,
    base_url: str,
    project_id: int,
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Session, None]:
    """Yield one page of sessions from [start, end), ordered by (cursor, id).

    With `after_id` set the page opens strictly after that row in the ordering,
    so paging cannot stall on a run of rows sharing one cursor instant. With it
    unset the lower bound is inclusive instead, so a row sitting on a
    checkpointed instant is re-read rather than skipped; the collection key
    collapses the duplicate.

    Sessions needs its own query rather than `_query_hogql` because its cursor
    is an expression rather than a COALESCE of columns, it carries a second
    predicate for partition pruning, and it pages by key rather than by cursor
    alone.
    """
    url = Session.get_api_endpoint_url(base_url, project_id)
    ctx = ProjectIdValidationContext(project_id=project_id)
    cursor = query.cursor
    prune = Session.prune_column
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
            + f"AND {prune} >= {literal(start - lookback)} "
            + f"AND {prune} < {literal(end)} "
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


async def _sweep_sessions(
    http: HTTPSession,
    config: EndpointConfig,
    project_id: int,
    log: Logger,
    start: datetime,
    end: datetime,
) -> AsyncGenerator[Session | datetime, None]:
    """Walk [start, end) by advancing the cursor, yielding the reached instant last.

    The cursor advances only past rows actually read. A session whose
    `$end_timestamp` is in the future — a client with a fast clock — sits outside
    `end` and is therefore never read, so it cannot drag the cursor past itself
    and is picked up by a later sweep instead.

    Only the instant is checkpointed, never the tie-break id: resuming a sweep
    re-opens the instant inclusively and pages through its rows again, so the
    persisted state stays a plain value watermark.
    """
    base_url = config.advanced.base_url
    query = await _sessions_query(base_url, project_id, http, log)
    reached = start
    after_id: str | None = None
    doc_count = 0

    while True:
        batch_count = 0

        async for item in _query_sessions(
            query,
            reached,
            after_id,
            end,
            SESSIONS_LOOKBACK,
            base_url,
            project_id,
            http,
            log,
        ):
            item_cursor = item.get_cursor()
            batch_count += 1
            # Both halves of the key come from the same row — the last one the
            # server sent — so the next page opens exactly where this one ended.
            reached = item_cursor
            after_id = item.id

            if cache.should_yield("sessions", f"{project_id}/{item.id}", item_cursor):
                doc_count += 1
                yield item

        if batch_count < HOGQL_PAGE_SIZE:
            break

    # Evict below the window's lower bound, never at or above it. The rows
    # sitting exactly on `start` are the ones the next sweep re-reads through
    # its inclusive bound, and they have to stay cached for that re-read to be
    # suppressed — hence the one-tick offset, since `cleanup` keeps only
    # entries strictly newer than its cutoff. Evicting on `reached` instead
    # would drop the whole tail the next sweep re-opens. Anything older than
    # this can no longer be reached by either task's window and is dead weight.
    evicted = cache.cleanup("sessions", start - SESSIONS_CURSOR_TICK)

    log.info(
        f"Swept {doc_count} sessions from project {project_id} "
        + f"(evicted {evicted} cache entries, {cache.count_for('sessions')} remain)"
    )

    if reached > start:
        yield reached


async def fetch_sessions(
    http: HTTPSession,
    config: EndpointConfig,
    project_id: int,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[Session | LogCursor, None]:
    """Emit sessions whose ingestion time landed since the last poll.

                cursor        horizon - 1s   horizon = last elapsed second
    ───────────────┼───────────────┼──────────────┼─────▶ time (1s ticks)
                   │               │              │
    cursor_expr ───[═══════════════╪══════════════)
    prune_column ══╪═══════════════╪══════════════)
    emitted ───────[═══════════════]              │
                   │               │              └─ excluded; the window is
                   │               │                 half-open, so it opens
                   │               │                 the next poll
                   │               └─ last second collected
                   └─ re-read on purpose: a row sharing this exact instant
                      must not be skipped at a page boundary

    The cursor bound is inclusive and the horizon exclusive, so a row is
    re-read rather than lost when several share one instant; the collection
    key collapses the duplicate. `prune_column` trails by SESSIONS_LOOKBACK.
    """
    assert isinstance(cursor, datetime)

    horizon = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(seconds=1)
    if horizon <= cursor:
        return

    async for item in _sweep_sessions(http, config, project_id, log, cursor, horizon):
        yield item


@backfill_timeout(BACKFILL_TIMEOUT_PERIOD)
async def backfill_sessions(
    http: HTTPSession,
    config: EndpointConfig,
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
    prune_column ══╪═══════════════╪══════════════)
    emitted ───────[═══════════════]              │
                   │               │              └─ belongs to the next
                   │               │                 stage: incremental opens
                   │               │                 its first window here
                   │               └─ last second collected
                   └─ queried as-is; the boundary instant falls out

    Resumes from the reached instant rather than an offset, so a row that is
    deleted mid-walk renumbers nothing.
    """
    assert isinstance(page, str | None)
    assert isinstance(cutoff, datetime)

    start = datetime.fromisoformat(page) if page is not None else config.start_date

    if start >= cutoff:
        return

    async for item in _sweep_sessions(http, config, project_id, log, start, cutoff):
        if isinstance(item, datetime):
            yield item.isoformat()
        else:
            yield item


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
    new_cursor = start_date
    doc_count = 0

    while True:
        batch_count = 0

        async for item in _query_hogql(
            Event,
            new_cursor,
            cutoff,
            base_url,
            project_id,
            http,
            log,
        ):
            item_cursor = item.get_cursor()
            batch_count += 1
            new_cursor = max(new_cursor, item_cursor)

            if cache.should_yield("events", f"{project_id}/{item.id}", item_cursor):
                doc_count += 1
                yield item

        if batch_count < HOGQL_PAGE_SIZE:
            break

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

    new_cursor = cursor
    doc_count = 0

    while True:
        batch_count = 0

        async for item in _query_hogql(
            Event,
            new_cursor,
            upper_bound,
            base_url,
            project_id,
            http,
            log,
        ):
            item_cursor = item.get_cursor()
            batch_count += 1

            new_cursor = max(new_cursor, item_cursor)

            if cache.should_yield("events", f"{project_id}/{item.id}", item_cursor):
                doc_count += 1
                yield item

        if batch_count < HOGQL_PAGE_SIZE:
            break

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

    while True:
        batch_count = 0

        async for item in _query_hogql(
            Person,
            new_cursor,
            cutoff,
            base_url,
            project_id,
            http,
            log,
        ):
            batch_count += 1
            doc_count += 1

            new_cursor = max(new_cursor, item.get_cursor())

            yield item

        if batch_count < HOGQL_PAGE_SIZE:
            break

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
    assert isinstance(cursor, datetime)

    base_url = config.advanced.base_url
    new_cursor = cursor
    doc_count = 0

    while True:
        batch_count = 0

        async for item in _query_hogql(
            Person,
            new_cursor,
            None,
            base_url,
            project_id,
            http,
            log,
        ):
            batch_count += 1
            doc_count += 1
            item_cursor = item.get_cursor()
            new_cursor = max(new_cursor, item_cursor)

            yield item

        if batch_count < HOGQL_PAGE_SIZE:
            break

    log.info(f"Fetched {doc_count} persons from project {project_id}")

    if new_cursor > cursor:
        yield new_cursor
