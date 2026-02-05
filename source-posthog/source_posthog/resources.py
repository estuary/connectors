"""Resource definitions for PostHog connector."""

import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import BaseDocument
from estuary_cdk.http import HTTPSession

from .api import (
    fetch_events,
    fetch_project_ids,
    snapshot_annotations,
    snapshot_cohorts,
    snapshot_feature_flags,
    snapshot_organizations,
    snapshot_persons,
    snapshot_projects,
)
from .models import (
    Annotation,
    BasePostHogEntity,
    Cohort,
    EndpointConfig,
    Event,
    FeatureFlag,
    Organization,
    Person,
    Project,
    ResourceConfig,
    ResourceState,
    default_start_date,
)

# Standard tombstone for snapshot resources (CDK convention)
TOMBSTONE = BaseDocument(_meta=BaseDocument.Meta(op="d"))

# Snapshot resources: (resource_type, model_class)
SNAPSHOT_RESOURCES: list[tuple[str, type[BasePostHogEntity]]] = [
    ("organizations", Organization),
    ("projects", Project),
    ("persons", Person),
    ("cohorts", Cohort),
    ("feature_flags", FeatureFlag),
    ("annotations", Annotation),
]

# Incremental resources: (resource_type, model_class)
INCREMENTAL_RESOURCES: list[tuple[str, type[BasePostHogEntity]]] = [
    ("events", Event),
]


async def all_resources(
    log: Logger,
    http: HTTPSession,
    config: EndpointConfig,
) -> list[common.Resource]:
    """Build list of all available PostHog resources."""
    project_ids = await fetch_project_ids(http, config, log)
    log.info(f"Capturing data from {len(project_ids)} projects in org {config.organization_id}")

    start_date = (
        datetime.fromisoformat(config.advanced.start_date).replace(tzinfo=UTC)
        if config.advanced.start_date
        else default_start_date()
    )

    # Map resource types to fetch functions
    snapshot_fetchers = {
        "organizations": functools.partial(snapshot_organizations, http, config),
        "projects": functools.partial(snapshot_projects, http, config),
        "persons": functools.partial(snapshot_persons, http, config),
        "cohorts": functools.partial(snapshot_cohorts, http, config),
        "feature_flags": functools.partial(snapshot_feature_flags, http, config),
        "annotations": functools.partial(snapshot_annotations, http, config),
    }

    incremental_fetchers = {
        "events": functools.partial(fetch_events, http, config),
    }

    def open_snapshot_binding(
        resource_type: str,
        binding: Any,
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: Any,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot_fetchers[resource_type],
            tombstone=TOMBSTONE,
        )

    def open_incremental_binding(
        resource_type: str,
        binding: Any,
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: Any,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=incremental_fetchers[resource_type],
        )

    resources = []

    # Add snapshot resources
    for resource_type, model_class in SNAPSHOT_RESOURCES:
        resources.append(
            common.Resource(
                name=model_class.resource_name,
                key=["/id"],
                model=model_class,
                open=functools.partial(open_snapshot_binding, resource_type),
                initial_state=ResourceState(
                    snapshot=ResourceState.Snapshot(
                        updated_at=datetime.min.replace(tzinfo=UTC),
                        last_count=0,
                        last_digest="",
                    )
                ),
                initial_config=ResourceConfig(
                    name=model_class.resource_name,
                    interval=timedelta(minutes=5),
                ),
                schema_inference=True,
            )
        )

    # Add incremental resources
    for resource_type, model_class in INCREMENTAL_RESOURCES:
        resources.append(
            common.Resource(
                name=model_class.resource_name,
                key=["/id"],
                model=model_class,
                open=functools.partial(open_incremental_binding, resource_type),
                initial_state=ResourceState(inc=ResourceState.Incremental(cursor=start_date)),
                initial_config=ResourceConfig(
                    name=model_class.resource_name,
                    interval=timedelta(minutes=5),
                ),
                schema_inference=True,
            )
        )

    return resources
