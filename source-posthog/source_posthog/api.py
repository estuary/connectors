"""
PostHog API client functions.
"""

import json
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any
from urllib.parse import urljoin

from estuary_cdk.capture.common import BaseDocument
from estuary_cdk.http import HTTPSession

from .models import (
    Annotation,
    Cohort,
    EndpointConfig,
    Event,
    FeatureFlag,
    Organization,
    Person,
    Project,
)


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
    url = urljoin(config.advanced.base_url, "api/organizations/")

    try:
        response = await http.request(log, url)
        data = json.loads(response)
        results = data.get("results", [])
    except Exception as e:
        return ValidationResult(
            valid=False,
            error=f"Failed to fetch organizations: {e}",
        )

    if not results:
        return ValidationResult(
            valid=False,
            error="API key has no access to any organizations",
        )

    # Find the specified organization
    target_org = None
    accessible_names = []
    for org in results:
        accessible_names.append(f"{org.get('name')} ({org.get('id')})")
        if org.get("id") == organization_id:
            target_org = org
            break

    if not target_org:
        return ValidationResult(
            valid=False,
            error=f"API key does not have access to organization '{organization_id}'. "
            f"Accessible organizations: {accessible_names}",
        )

    # Extract project IDs from the organization
    projects = target_org.get("projects", [])
    if not projects:
        return ValidationResult(
            valid=False,
            error=f"Organization '{organization_id}' has no projects",
        )

    project_ids = [proj.get("id") for proj in projects]
    org_name = target_org.get("name")

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


async def fetch_project_ids(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> list[int]:
    """
    Fetch all project IDs for an organization.

    Returns list of project IDs belonging to the specified organization.
    """
    organization_id = config.organization_id
    url = urljoin(config.advanced.base_url, "api/organizations/")

    response = await http.request(log, url)
    data = json.loads(response)
    results = data.get("results", [])

    for org in results:
        if org.get("id") == organization_id:
            projects = org.get("projects", [])
            project_ids = [proj.get("id") for proj in projects]
            log.info(f"Found {len(project_ids)} projects in org {organization_id}")
            return project_ids

    log.warning(f"Organization {organization_id} not found")
    return []


async def snapshot_organizations(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[Organization, None]:
    """
    Fetch all organizations from PostHog (snapshot resource).

    Yields Organization objects. No cursor needed - CDK handles snapshot state.
    """
    url = urljoin(config.advanced.base_url, "api/organizations/")

    response = await http.request(log, url)
    data = json.loads(response)
    results = data.get("results", [])

    for item in results:
        org = Organization.model_validate(item)
        yield org

    log.info(f"Fetched {len(results)} organizations")


async def snapshot_projects(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[Project, None]:
    """
    Fetch all projects from PostHog (snapshot resource).

    Yields Project objects. No cursor needed - CDK handles snapshot state.
    """
    url = urljoin(config.advanced.base_url, "api/projects/")

    response = await http.request(log, url)
    data = json.loads(response)
    results = data.get("results", [])

    for item in results:
        project = Project.model_validate(item)
        yield project

    log.info(f"Fetched {len(results)} projects")


async def fetch_events(
    http: HTTPSession,
    config: EndpointConfig,
    project_ids: list[int],
    log: Logger,
    cursor: datetime,
) -> AsyncGenerator[Event | datetime, None]:
    """
    Fetch events from PostHog across all projects in the organization.

    Yields Event objects (with project_id) and finally yields the new cursor datetime.
    """
    log.info(
        f"fetch_events called with cursor: {cursor.isoformat()} for {len(project_ids)} projects"
    )

    last_timestamp = cursor
    total_doc_count = 0

    for project_id in project_ids:
        url = urljoin(config.advanced.base_url, f"api/projects/{project_id}/events/")
        params: dict[str, Any] = {
            "after": cursor.isoformat(),
            "limit": 100,
        }

        doc_count = 0

        while True:
            response = await http.request(log, url, params=params)
            data = json.loads(response)
            results = data.get("results", [])

            if not results:
                break

            for item in results:
                item["project_id"] = project_id
                event = Event.model_validate(item)
                event.meta_ = BaseDocument.Meta(op="c")
                yield event
                doc_count += 1

                if event.timestamp > last_timestamp:
                    last_timestamp = event.timestamp

            # Check for pagination
            next_url = data.get("next")
            if not next_url:
                break

            # Update params for next page
            params = {}
            url = next_url

        log.info(f"Fetched {doc_count} events from project {project_id}")
        total_doc_count += doc_count

    log.info(f"Fetched {total_doc_count} total events across all projects")

    if total_doc_count == 0:
        # If there were no events, we may need to move the cursor forward to keep it from going
        # stale. Use a conservative time horizon to account for any API delays in event ingestion.
        last_timestamp = datetime.now(tz=UTC) - timedelta(minutes=5)

        if last_timestamp <= cursor:
            # Common case: caught up with frequent events, most recent event is newer than our
            # conservative horizon. No cursor update needed.
            log.debug(
                f"not updating cursor since last_timestamp ({last_timestamp}) <= cursor ({cursor})"
            )
            return
        elif last_timestamp - cursor < timedelta(hours=6):
            # Only emit an updated checkpoint when there are no documents if the current cursor
            # is sufficiently old. Emitting a checkpoint with no documents will trigger an
            # immediate re-invocation of this task, so avoid doing it too frequently.
            log.debug(
                f"not updating cursor since it's less than 6 hours newer than prior "
                f"(new: {last_timestamp} vs prior: {cursor})"
            )
            return
    else:
        # Bump cursor by 1ms to avoid re-fetching the last event on the next round.
        # Assumes all events with the same timestamp were retrieved together.
        last_timestamp += timedelta(milliseconds=1)

    yield last_timestamp


async def snapshot_persons(
    http: HTTPSession,
    config: EndpointConfig,
    project_ids: list[int],
    log: Logger,
) -> AsyncGenerator[Person, None]:
    """
    Fetch all persons from PostHog across all projects (snapshot resource).

    Yields Person objects with project_id. No cursor needed - CDK handles snapshot state.
    """
    total_doc_count = 0

    for project_id in project_ids:
        url = urljoin(config.advanced.base_url, f"api/projects/{project_id}/persons/")
        params: dict[str, Any] = {
            "limit": 100,
        }

        doc_count = 0

        while True:
            response = await http.request(log, url, params=params)
            data = json.loads(response)
            results = data.get("results", [])

            if not results:
                break

            for item in results:
                item["project_id"] = project_id
                person = Person.model_validate(item)
                yield person
                doc_count += 1

            # Check for pagination
            next_url = data.get("next")
            if not next_url:
                break

            params = {}
            url = next_url

        log.info(f"Fetched {doc_count} persons from project {project_id}")
        total_doc_count += doc_count

    log.info(f"Fetched {total_doc_count} total persons across all projects")


async def snapshot_cohorts(
    http: HTTPSession,
    config: EndpointConfig,
    project_ids: list[int],
    log: Logger,
) -> AsyncGenerator[Cohort, None]:
    """
    Fetch all cohorts from PostHog across all projects (snapshot resource).

    Yields Cohort objects with project_id. No cursor needed - CDK handles snapshot state.
    """
    total_count = 0

    for project_id in project_ids:
        url = urljoin(config.advanced.base_url, f"api/projects/{project_id}/cohorts/")

        response = await http.request(log, url)
        data = json.loads(response)
        results = data.get("results", [])

        for item in results:
            item["project_id"] = project_id
            cohort = Cohort.model_validate(item)
            yield cohort

        log.info(f"Fetched {len(results)} cohorts from project {project_id}")
        total_count += len(results)

    log.info(f"Fetched {total_count} total cohorts across all projects")


async def snapshot_feature_flags(
    http: HTTPSession,
    config: EndpointConfig,
    project_ids: list[int],
    log: Logger,
) -> AsyncGenerator[FeatureFlag, None]:
    """
    Fetch all feature flags from PostHog across all projects (snapshot resource).

    Yields FeatureFlag objects with project_id. No cursor needed - CDK handles snapshot state.
    """
    total_count = 0

    for project_id in project_ids:
        url = urljoin(config.advanced.base_url, f"api/projects/{project_id}/feature_flags/")

        response = await http.request(log, url)
        data = json.loads(response)
        results = data.get("results", [])

        for item in results:
            item["project_id"] = project_id
            flag = FeatureFlag.model_validate(item)
            yield flag

        log.info(f"Fetched {len(results)} feature flags from project {project_id}")
        total_count += len(results)

    log.info(f"Fetched {total_count} total feature flags across all projects")


async def snapshot_annotations(
    http: HTTPSession,
    config: EndpointConfig,
    project_ids: list[int],
    log: Logger,
) -> AsyncGenerator[Annotation, None]:
    """
    Fetch all annotations from PostHog across all projects (snapshot resource).

    Yields Annotation objects with project_id. No cursor needed - CDK handles snapshot state.
    """
    total_count = 0

    for project_id in project_ids:
        url = urljoin(config.advanced.base_url, f"api/projects/{project_id}/annotations/")

        response = await http.request(log, url)
        data = json.loads(response)
        results = data.get("results", [])

        for item in results:
            item["project_id"] = project_id
            annotation = Annotation.model_validate(item)
            yield annotation

        log.info(f"Fetched {len(results)} annotations from project {project_id}")
        total_count += len(results)

    log.info(f"Fetched {total_count} total annotations across all projects")
