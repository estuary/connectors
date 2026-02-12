"""
PostHog API client functions.
"""

import json
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime
from logging import Logger
from typing import Any

from estuary_cdk.capture.common import BaseDocument
from estuary_cdk.http import HTTPMixin, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    Annotation,
    Cohort,
    EndpointConfig,
    Event,
    FeatureFlag,
    Organization,
    Person,
    PostHogEntity,
    Project,
    ProjectEntity,
    RestResponseMeta,
)


def _parse_timestamp(value: str | datetime | None) -> datetime | None:
    """Parse timestamp from HogQL response, handling Z suffix."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _parse_json_properties(value: str | dict | None) -> dict:
    """Parse properties from HogQL response (may be JSON string or dict)."""
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    return json.loads(value)


@dataclass
class ValidationResult:
    """Result of credential/organization validation."""

    valid: bool
    error: str | None = None
    organization_id: str | None = None
    organization_name: str | None = None
    project_ids: list[int] | None = None


async def validate_credentials(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> ValidationResult:
    """
    Validate API credentials and organization access.

    Checks:
    1. API key can access the organizations endpoint
    2. API key has access to the specified organization
    3. Organization has at least one project

    Returns ValidationResult with project IDs if validation succeeds.
    """
    organization_id = config.organization_id

    try:
        orgs: list[Organization] = []
        async for org in fetch_entity(Organization, http, config, log):
            orgs.append(org)
    except Exception as e:
        return ValidationResult(
            valid=False,
            error=f"Failed to fetch organizations: {e}",
        )

    if not orgs:
        return ValidationResult(
            valid=False,
            error="API key has no access to any organizations",
        )

    # Find the specified organization
    target_org = None
    accessible_names = []
    for org in orgs:
        # Access name via model_extra since it's not an explicit field
        org_data = org.model_dump()
        org_name = org_data.get("name", org.id)
        accessible_names.append(f"{org_name} ({org.id})")
        if org.id == organization_id:
            target_org = org
            break

    if not target_org:
        return ValidationResult(
            valid=False,
            error=f"API key does not have access to organization '{organization_id}'. "
            f"Accessible organizations: {accessible_names}",
        )

    # Extract project IDs from the organization (projects is in extra fields)
    org_data = target_org.model_dump()
    projects = org_data.get("projects", [])
    if not projects:
        return ValidationResult(
            valid=False,
            error=f"Organization '{organization_id}' has no projects",
        )

    project_ids = [proj.get("id") for proj in projects]
    org_name = org_data.get("name")

    log.info(
        f"Validated organization '{org_name}' ({organization_id}) "
        f"with {len(project_ids)} projects: {project_ids}"
    )

    return ValidationResult(
        valid=True,
        organization_id=organization_id,
        organization_name=org_name,
        project_ids=project_ids,
    )


# Cache for project IDs per organization (avoids re-fetching on retry).
# Note: @functools.cache doesn't work with async - it caches the coroutine
# object, not the result, causing "cannot reuse already awaited coroutine".
_project_ids_cache: dict[str, list[int]] = {}


async def fetch_project_ids(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> list[int]:
    """
    Fetch all project IDs for an organization.

    Returns list of project IDs belonging to the specified organization.
    Results are cached per organization_id for retry safety.
    """
    org_id = config.organization_id
    if org_id in _project_ids_cache:
        return _project_ids_cache[org_id]

    result = await validate_credentials(http, config, log)
    project_ids = result.project_ids if result.valid and result.project_ids else []
    _project_ids_cache[org_id] = project_ids
    return project_ids


# =============================================================================
# Generic REST API Fetchers (using IncrementalJsonProcessor)
# =============================================================================


async def _fetch_from_url[T: PostHogEntity | ProjectEntity](
    url: str,
    model: type[T],
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[T, None]:
    """
    Fetch paginated results from a URL using IncrementalJsonProcessor.

    Handles pagination automatically via the 'next' field in response metadata.
    Yields validated model instances.
    """
    current_url: str | None = url

    while current_url is not None:
        _, body = await http.request_stream(log, current_url)
        processor: IncrementalJsonProcessor[T, RestResponseMeta] = IncrementalJsonProcessor(
            body(), "results.item", model, remainder_cls=RestResponseMeta
        )

        async for item in processor:
            yield item

        remainder = processor.get_remainder()
        current_url = remainder.next if remainder else None


async def fetch_entity[T: PostHogEntity](
    model: type[T],
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[T, None]:
    """
    Fetch all instances of a PostHogEntity from the REST API.

    Uses the model's api_path to construct the URL.
    """
    url = model.get_api_endpoint_url(config.advanced.base_url)
    count = 0

    async for item in _fetch_from_url(url, model, http, log):
        yield item
        count += 1

    log.info(f"Fetched {count} {model.resource_name}")


async def fetch_project_entity[T: ProjectEntity](
    model: type[T],
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[T, None]:
    """
    Fetch all instances of a ProjectEntity across all projects.

    Iterates over projects fetched via fetch_entity and fetches from each project's endpoint.
    """
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


# =============================================================================
# Snapshot Functions (REST API)
# =============================================================================


async def snapshot_organizations(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[Organization, None]:
    """Fetch all organizations from PostHog."""
    async for item in fetch_entity(Organization, http, config, log):
        yield item


async def snapshot_projects(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[Project, None]:
    """Fetch all projects from PostHog."""
    async for item in fetch_entity(Project, http, config, log):
        yield item


async def _query_hogql(
    http: HTTPMixin,
    base_url: str,
    project_id: int,
    query: str,
    log: Logger,
    name: str = "connector_query",
) -> dict[str, Any]:
    """
    Execute a HogQL query via the Query API.

    Returns the full response including results, columns, and metadata.
    """
    url = f"{base_url}/api/projects/{project_id}/query/"
    payload = {
        "query": {
            "kind": "HogQLQuery",
            "query": query,
        },
        "name": name,
    }

    response = await http.request(
        log,
        url,
        method="POST",
        json=payload,
    )
    return json.loads(response)


def _format_cursor(cursor: datetime | None) -> str:
    """Format datetime cursor for HogQL WHERE clause."""
    if cursor is None:
        return ""
    return cursor.strftime("%Y-%m-%d %H:%M:%S")


async def fetch_events(
    http: HTTPMixin,
    config: EndpointConfig,
    log: Logger,
    cursor: datetime,
) -> AsyncGenerator[Event | datetime, None]:
    """
    Fetch events from PostHog across all projects using HogQL Query API.

    Uses timestamp-based pagination for efficient querying of large datasets.
    Yields Event objects (with project_id) and finally yields the new cursor datetime.
    """
    base_url = config.advanced.base_url.rstrip("/")
    page_size = config.advanced.page_size
    project_ids = await fetch_project_ids(http, config, log)

    log.info(
        f"fetch_events called with cursor: {cursor.isoformat()} for {len(project_ids)} projects"
    )

    last_timestamp = cursor
    total_doc_count = 0

    for project_id in project_ids:
        doc_count = 0
        page_cursor = cursor

        while True:
            # HogQL query with timestamp-based pagination
            query = f"""
                SELECT
                    uuid as id,
                    event,
                    timestamp,
                    distinct_id,
                    properties,
                    elements_chain
                FROM {Event.table_name}
                WHERE timestamp > toDateTime('{_format_cursor(page_cursor)}')
                ORDER BY timestamp ASC
                LIMIT {page_size}
            """

            result = await _query_hogql(http, base_url, project_id, query, log, name="fetch_events")

            columns = result.get("columns", [])
            rows = result.get("results", [])

            if not rows:
                break

            for row in rows:
                row_dict = dict(zip(columns, row, strict=False))
                ts = _parse_timestamp(row_dict.pop("timestamp"))
                props = _parse_json_properties(row_dict.pop("properties"))

                event = Event(
                    **row_dict,
                    timestamp=ts,
                    properties=props,
                    meta_=BaseDocument.Meta(op="c"),
                    project_id=project_id,
                )
                yield event
                doc_count += 1

                if ts and ts > last_timestamp:
                    last_timestamp = ts

            # Timestamp-based pagination: use the last event's timestamp
            last_row = dict(zip(columns, rows[-1], strict=False))
            page_cursor = _parse_timestamp(last_row.get("timestamp"))

            # If we got fewer rows than page_size, we've reached the end
            if len(rows) < page_size:
                break

        log.info(f"Fetched {doc_count} events from project {project_id}")
        total_doc_count += doc_count

    log.info(f"Fetched {total_doc_count} total events across all projects")
    # CDK requires cursor to always advance beyond the previous cursor.
    # Use current time when no events found, or when last_timestamp hasn't advanced.
    new_cursor = datetime.now(tz=UTC)
    if total_doc_count > 0 and last_timestamp > cursor:
        new_cursor = last_timestamp
    yield new_cursor


async def snapshot_persons(
    http: HTTPMixin,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[Person, None]:
    """
    Fetch all persons from PostHog across all projects using HogQL Query API.

    Uses created_at-based pagination for efficient querying.
    Yields Person objects with project_id. No cursor needed - CDK handles snapshot state.

    Note: HogQL persons table doesn't include distinct_ids directly.
    We query person_distinct_ids separately if needed.
    """
    base_url = config.advanced.base_url.rstrip("/")
    page_size = config.advanced.page_size
    project_ids = await fetch_project_ids(http, config, log)

    total_doc_count = 0

    for project_id in project_ids:
        doc_count = 0
        last_created_at: datetime | None = None

        while True:
            # Build WHERE clause for pagination
            where_clause = ""
            if last_created_at:
                where_clause = f"WHERE created_at > toDateTime('{_format_cursor(last_created_at)}')"

            # HogQL query for persons with distinct_ids via subquery
            query = f"""
                SELECT
                    p.id,
                    p.created_at,
                    p.properties,
                    p.is_identified,
                    groupArray(pdi.distinct_id) as distinct_ids
                FROM {Person.table_name} p
                LEFT JOIN person_distinct_ids pdi ON p.id = pdi.person_id
                {where_clause}
                GROUP BY p.id, p.created_at, p.properties, p.is_identified
                ORDER BY p.created_at ASC
                LIMIT {page_size}
            """

            result = await _query_hogql(
                http, base_url, project_id, query, log, name="snapshot_persons"
            )

            columns = result.get("columns", [])
            rows = result.get("results", [])

            if not rows:
                break

            for row in rows:
                row_dict = dict(zip(columns, row, strict=False))
                created = _parse_timestamp(row_dict.pop("created_at"))
                props = _parse_json_properties(row_dict.pop("properties"))
                is_id = bool(row_dict.pop("is_identified", 0))

                person = Person(
                    **row_dict,
                    created_at=created,
                    properties=props,
                    is_identified=is_id,
                    project_id=project_id,
                )
                yield person
                doc_count += 1

            # Pagination: use last person's created_at
            last_row = dict(zip(columns, rows[-1], strict=False))
            last_created_at = _parse_timestamp(last_row.get("created_at"))

            # If we got fewer rows than page_size, we've reached the end
            if len(rows) < page_size:
                break

        log.info(f"Fetched {doc_count} persons from project {project_id}")
        total_doc_count += doc_count

    log.info(f"Fetched {total_doc_count} total persons across all projects")


async def snapshot_cohorts(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[Cohort, None]:
    """Fetch all cohorts from PostHog across all projects."""
    async for item in fetch_project_entity(Cohort, http, config, log):
        yield item


async def snapshot_feature_flags(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[FeatureFlag, None]:
    """Fetch all feature flags from PostHog across all projects."""
    async for item in fetch_project_entity(FeatureFlag, http, config, log):
        yield item


async def snapshot_annotations(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[Annotation, None]:
    """Fetch all annotations from PostHog across all projects."""
    async for item in fetch_project_entity(Annotation, http, config, log):
        yield item
