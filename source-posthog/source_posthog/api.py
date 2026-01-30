"""
PostHog API client functions.
"""

import functools
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger
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
)

HOGQL_PAGE_SIZE = 50_000


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
    url = model.get_api_endpoint_url(base_url, project_id)
    column_names = await _get_hogql_columns(model, base_url, project_id, http, log)

    serialized_start_date = start_date.astimezone(UTC).replace(tzinfo=None).isoformat()
    serialized_end_date = (
        end_date.astimezone(UTC).replace(tzinfo=None).isoformat()
        if end_date is not None
        else None
    )

    start_date_clause = (
        f"WHERE {model.cursor_column} > "
        + f"toDateTime64('{serialized_start_date}', 6, 'UTC') "
    )
    end_date_clause = (
        f"AND {model.cursor_column} <= toDateTime64('{serialized_end_date}', 6, 'UTC') "
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
            + f"ORDER BY {model.cursor_column} DESC "
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
    ctx = ProjectIdValidationContext(project_id=project_id)
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
    ctx = ProjectIdValidationContext(project_id=project_id)
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


async def snapshot_persons(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[Person, None]:
    base_url = config.advanced.base_url
    project_ids = await fetch_project_ids(http, config, log)

    total_doc_count = 0

    for project_id in project_ids:
        doc_count = 0
        project_cursor = datetime.min.replace(tzinfo=UTC)

        while True:
            async for item in _query_hogql(
                Person, project_cursor, None, base_url, project_id, http, log
            ):
                doc_count += 1
                project_cursor = item.get_cursor()

                yield item

            # If we got fewer rows than page_size, we've reached the end
            if doc_count < HOGQL_PAGE_SIZE:
                break

        log.info(f"Fetched {doc_count} persons from project {project_id}")
        total_doc_count += doc_count

    log.info(f"Fetched {total_doc_count} total persons across all projects")
