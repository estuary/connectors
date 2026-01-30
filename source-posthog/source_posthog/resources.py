"""Resource definitions for PostHog connector."""

import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import BaseDocument, Resource, open_binding
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    backfill_feature_flags,
    backfill_project_events,
    fetch_entity,
    fetch_feature_flags,
    fetch_project_entity,
    fetch_project_events,
    fetch_project_ids,
    fetch_token_scopes,
    snapshot_persons,
)
from .models import (
    Annotation,
    Cohort,
    EndpointConfig,
    Event,
    FeatureFlag,
    Organization,
    Person,
    Project,
    ResourceConfig,
    ResourceState,
)

# Standard tombstone for snapshot resources (CDK convention)
TOMBSTONE = BaseDocument(_meta=BaseDocument.Meta(op="d"))

REALTIME = "realtime"
LOOKBACK = "lookback"

# PostHog events eventual consistency horizon.
# Events may be delayed in appearing in query results due to ClickHouse replication
# and Kafka processing. The lookback stream trails behind to capture delayed events.
EVENTS_EVENTUAL_CONSISTENCY_HORIZON = timedelta(hours=1)

RESOURCE_REQUIRED_SCOPES: dict[str, str] = {
    "Cohorts": "cohort:read",
    "FeatureFlags": "feature_flag:read",
    "Annotations": "annotation:read",
    "Events": "query:read",
    "Persons": "query:read",
}


SNAPSHOT_RESOURCES = [
    Annotation,
    Cohort,
    Organization,
    Person,
    Project,
]


async def validate_credentials(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
):
    """
    Validate API credentials and organization access.

    Checks:
    1. API key can access the organizations endpoint
    2. API key has access to the specified organization
    3. Organization has at least one project

    Returns ValidationResult with project IDs if validation succeeds.
    """
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    organization_id = config.organization_id

    try:
        orgs = {
            org.id: org.display_name
            async for org in fetch_entity(Organization, http, config, log)
        }
    except HTTPError as err:
        msg = f"Encountered error validating credentials.\n\n{err.message}"
        if err.code == 401:
            msg = (
                "Invalid credentials. "
                + f"Please confirm the provided credentials are correct.\n\n{err.message}"
            )

        raise ValidationError([msg]) from err

    if not orgs:
        raise ValidationError(["API key has no access to any organizations"])

    if organization_id not in orgs:
        raise ValidationError(
            [
                f"API key does not have access to organization '{organization_id}'. "
                + f"Accessible organizations: {list(orgs.values())}"
            ]
        )


async def filter_resources_by_scopes(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
    resources: list[Resource],
) -> list[Resource]:
    scopes = await fetch_token_scopes(http, config, log)

    if "*" in scopes:
        return resources

    def _is_resource_in_scopes(resource):
        required_scope = RESOURCE_REQUIRED_SCOPES.get(resource.name)

        return required_scope is None or required_scope in scopes

    return list(filter(_is_resource_in_scopes, resources))


def snapshot_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    """Return Resource objects for all snapshot (full-refresh) resources."""

    snapshot_fetchers = {
        "Organizations": functools.partial(fetch_entity, Organization, http, config),
        "Projects": functools.partial(fetch_entity, Project, http, config),
        "Persons": functools.partial(snapshot_persons, http, config),
        "Cohorts": functools.partial(fetch_project_entity, Cohort, http, config),
        "Annotations": functools.partial(
            fetch_project_entity, Annotation, http, config
        ),
    }

    def open(
        resource_type: str,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=snapshot_fetchers[resource_type],
            tombstone=TOMBSTONE,
        )

    return [
        Resource(
            name=model.resource_name,
            key=["/_meta/row_id"],
            model=model,
            open=functools.partial(open, model.resource_name),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=model.resource_name,
                interval=timedelta(minutes=5),
            ),
            schema_inference=True,
        )
        for model in SNAPSHOT_RESOURCES
    ]


async def events(log: Logger, http: HTTPMixin, config: EndpointConfig) -> Resource:
    project_ids = await fetch_project_ids(http, config, log)

    cutoff = datetime.now(tz=UTC)
    lookback_cutoff = cutoff - EVENTS_EVENTUAL_CONSISTENCY_HORIZON

    # Incremental fetchers (for fetch_changes) - called with (log, cursor)
    incremental_fetchers = {
        **{
            f"{project_id}_{REALTIME}": functools.partial(
                fetch_project_events, http, config, project_id, None
            )
            for project_id in project_ids
        },
        **{
            f"{project_id}_{LOOKBACK}": functools.partial(
                fetch_project_events,
                http,
                config,
                project_id,
                EVENTS_EVENTUAL_CONSISTENCY_HORIZON,
            )
            for project_id in project_ids
        },
    }

    # Backfill fetchers (for fetch_page) - called with (log, page, cutoff)
    backfill_fetchers = {
        **{
            f"{project_id}_{REALTIME}": functools.partial(
                backfill_project_events, http, config, project_id
            )
            for project_id in project_ids
        },
        **{
            f"{project_id}_{LOOKBACK}": functools.partial(
                backfill_project_events, http, config, project_id
            )
            for project_id in project_ids
        },
    }

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=incremental_fetchers,
            fetch_page=backfill_fetchers,
        )

    return Resource(
        name=Event.resource_name,
        key=["/_meta/project_id", "/uuid"],
        model=Event,
        open=open,
        initial_state=ResourceState(
            backfill={
                **{
                    f"{project_id}_{REALTIME}": ResourceState.Backfill(
                        cutoff=cutoff, next_page=None
                    )
                    for project_id in project_ids
                },
                **{
                    f"{project_id}_{LOOKBACK}": ResourceState.Backfill(
                        cutoff=lookback_cutoff, next_page=None
                    )
                    for project_id in project_ids
                },
            },
            inc={
                **{
                    f"{project_id}_{REALTIME}": ResourceState.Incremental(cursor=cutoff)
                    for project_id in project_ids
                },
                **{
                    f"{project_id}_{LOOKBACK}": ResourceState.Incremental(
                        cursor=lookback_cutoff
                    )
                    for project_id in project_ids
                },
            },
        ),
        initial_config=ResourceConfig(
            name=Event.resource_name,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


async def feature_flags(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> Resource:
    """Return Resource for incremental feature flags capture."""
    project_ids = await fetch_project_ids(http, config, log)
    cutoff = datetime.now(tz=UTC)

    # Incremental fetchers (for fetch_changes) - called with (log, cursor)
    incremental_fetchers = {
        f"{project_id}": functools.partial(
            fetch_feature_flags, http, config, project_id
        )
        for project_id in project_ids
    }

    # Backfill fetchers (for fetch_page) - called with (log, page, cutoff)
    backfill_fetchers = {
        f"{project_id}": functools.partial(
            backfill_feature_flags, http, config, project_id
        )
        for project_id in project_ids
    }

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=incremental_fetchers,
            fetch_page=backfill_fetchers,
        )

    return Resource(
        name=FeatureFlag.resource_name,
        key=["/_meta/project_id", "/id"],
        model=FeatureFlag,
        open=open,
        initial_state=ResourceState(
            inc={
                f"{project_id}": ResourceState.Incremental(cursor=cutoff)
                for project_id in project_ids
            },
            backfill={
                f"{project_id}": ResourceState.Backfill(cutoff=cutoff, next_page=None)
                for project_id in project_ids
            },
        ),
        initial_config=ResourceConfig(
            name=FeatureFlag.resource_name,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[Resource]:
    """Return all resources for the PostHog connector."""

    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    project_ids = await fetch_project_ids(http, config, log)
    log.info(
        f"Capturing data from {len(project_ids)} projects in org {config.organization_id}"
    )

    resources = [
        *snapshot_resources(log, http, config),
        await events(log, http, config),
        await feature_flags(log, http, config),
    ]

    return await filter_resources_by_scopes(log, http, config, resources)
