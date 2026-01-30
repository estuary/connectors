"""Resource definitions for PostHog connector."""

import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding
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
    EndpointConfig,
    INCREMENTAL_RESOURCES,
    ResourceConfig,
    ResourceState,
    SNAPSHOT_RESOURCES,
    default_start_date,
)


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
        "persons": functools.partial(snapshot_persons, http, config, project_ids),
        "cohorts": functools.partial(snapshot_cohorts, http, config, project_ids),
        "feature_flags": functools.partial(snapshot_feature_flags, http, config, project_ids),
        "annotations": functools.partial(snapshot_annotations, http, config, project_ids),
    }

    incremental_fetchers = {
        "events": functools.partial(fetch_events, http, config, project_ids),
    }

    def open_snapshot_binding(
        resource_type: str,
        tombstone: object,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: list[CaptureBinding[ResourceConfig]],
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot_fetchers[resource_type],
            tombstone=tombstone,
        )

    def open_incremental_binding(
        resource_type: str,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: list[CaptureBinding[ResourceConfig]],
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
    for resource_type, model_class, key_field, name, tombstone in SNAPSHOT_RESOURCES:
        resources.append(
            common.Resource(
                name=name,
                key=[key_field],
                model=model_class,
                open=functools.partial(open_snapshot_binding, resource_type, tombstone),
                initial_state=ResourceState(
                    snapshot=ResourceState.Snapshot(
                        updated_at=datetime.min.replace(tzinfo=UTC),
                        last_count=0,
                        last_digest="",
                    )
                ),
                initial_config=ResourceConfig(
                    name=name,
                    interval=timedelta(seconds=30),
                ),
                schema_inference=True,
            )
        )

    # Add incremental resources
    for resource_type, model_class, key_field, name in INCREMENTAL_RESOURCES:
        resources.append(
            common.Resource(
                name=name,
                key=[key_field],
                model=model_class,
                open=functools.partial(open_incremental_binding, resource_type),
                initial_state=ResourceState(inc=ResourceState.Incremental(cursor=start_date)),
                initial_config=ResourceConfig(
                    name=name,
                    interval=timedelta(seconds=30),
                ),
                schema_inference=True,
            )
        )

    return resources
