"""Resource definitions for PostHog connector."""

import functools
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState,
    Resource,
    SnapshotResource,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    SESSIONS_CURSOR_TICK,
    SESSIONS_LOOKBACK_LAG,
    SESSIONS_REALTIME_LAG,
    backfill_feature_flags,
    backfill_persons,
    backfill_project_events,
    backfill_sessions,
    fetch_entity,
    fetch_feature_flags,
    fetch_persons,
    fetch_project_entity,
    fetch_project_events,
    fetch_project_ids,
    fetch_sessions,
    fetch_token_scopes,
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
    Session,
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
    "Sessions": "query:read",
}

PostHogResource = Resource[
    BasePostHogEntity[str] | BasePostHogEntity[int], ResourceConfig, ResourceState
]

PostHogSnapshotResource = SnapshotResource[
    BasePostHogEntity[str] | BasePostHogEntity[int], ResourceConfig
]


SNAPSHOT_RESOURCES = [
    Annotation,
    Cohort,
    Organization,
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
    resources: list[PostHogResource],
) -> list[PostHogResource]:
    scopes = await fetch_token_scopes(http, config, log)

    if "*" in scopes:
        return resources

    def _is_resource_in_scopes(resource: PostHogResource):
        required_scope = RESOURCE_REQUIRED_SCOPES.get(resource.name)

        return required_scope is None or required_scope in scopes

    return list(filter(_is_resource_in_scopes, resources))


def snapshot_resources(
    http: HTTPMixin, config: EndpointConfig
) -> list[PostHogResource]:
    """Return Resource objects for all snapshot (full-refresh) resources."""

    snapshot_fetchers = {
        "Organizations": functools.partial(fetch_entity, Organization, http, config),
        "Projects": functools.partial(fetch_entity, Project, http, config),
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
        PostHogSnapshotResource(
            name=model.resource_name,
            open=functools.partial(open, model.resource_name),
            initial_config=ResourceConfig(
                name=model.resource_name,
                interval=timedelta(minutes=5),
            ),
        )
        for model in SNAPSHOT_RESOURCES
    ]


def _is_project_in_state(
    project_id: int, state: ResourceState, uses_delayed_streams: bool = False
) -> bool:
    if uses_delayed_streams:
        key = f"{project_id}_{REALTIME}"
    else:
        key = str(project_id)

    assert isinstance(state.inc, dict)
    return key in state.inc


def _generate_resource_state(
    project_ids: list[int],
    cutoff: datetime,
    inc_offset: timedelta = timedelta(),
) -> ResourceState:
    """Seed one backfill and one incremental subtask per project.

    A stream whose incremental task drops rows at or before its cursor must
    pass `inc_offset` of one tick, so the instant the backfill stopped short of
    is still emitted.
    """
    return ResourceState(
        inc={
            f"{project_id}": ResourceState.Incremental(cursor=cutoff - inc_offset)
            for project_id in project_ids
        },
        backfill={
            f"{project_id}": ResourceState.Backfill(cutoff=cutoff, next_page=None)
            for project_id in project_ids
        },
    )


def _generate_delayed_stream_resource_state(
    project_ids: list[int],
    cutoff: datetime,
    lookback_cutoff: datetime,
    inc_offset: timedelta = timedelta(),
) -> ResourceState:
    """Seed one backfill and a realtime and lookback incremental per project.

    Only the live tail needs the delayed second pass. A lookback backfill would
    re-walk a subset of the realtime backfill's range and emit the same rows.
    """
    return ResourceState(
        # Keying backfills by `_{REALTIME}` is non-standard and doesn't make
        # sense: there's one backfill per project, and "realtime" names an
        # incremental stage. It was an unfortunate naming decision. It's kept
        # because it has no functional impact, and renaming it isn't worth
        # migrating existing captures' state.
        backfill={
            f"{project_id}_{REALTIME}": ResourceState.Backfill(
                cutoff=cutoff, next_page=None
            )
            for project_id in project_ids
        },
        inc={
            **{
                f"{project_id}_{REALTIME}": ResourceState.Incremental(
                    cursor=cutoff - inc_offset
                )
                for project_id in project_ids
            },
            **{
                f"{project_id}_{LOOKBACK}": ResourceState.Incremental(
                    cursor=lookback_cutoff - inc_offset
                )
                for project_id in project_ids
            },
        },
    )


async def _patch_missing_project_states(
    binding: CaptureBinding[ResourceConfig],
    state: ResourceState,
    task: Task,
    project_ids: list[int],
    cutoff: datetime,
    lookback_cutoff: datetime | None = None,
    inc_offset: timedelta = timedelta(),
):
    if not (isinstance(state.inc, dict) and isinstance(state.backfill, dict)):
        return

    missing_project_ids = [
        pid
        for pid in project_ids
        if not _is_project_in_state(
            pid, state, uses_delayed_streams=lookback_cutoff is not None
        )
    ]
    if not missing_project_ids:
        return

    if lookback_cutoff is None:
        new_states = _generate_resource_state(missing_project_ids, cutoff, inc_offset)
    else:
        new_states = _generate_delayed_stream_resource_state(
            missing_project_ids, cutoff, lookback_cutoff, inc_offset
        )
    assert isinstance(new_states.inc, dict)
    assert isinstance(new_states.backfill, dict)

    old_state = deepcopy(state)

    state.inc.update(new_states.inc)
    state.backfill.update(new_states.backfill)

    task.log.info(
        f"Checkpointing state to ensure any new state is persisted for {binding.stateKey}.",
        {
            "prevState": old_state,
            "newState": state,
        },
    )
    await task.checkpoint(ConnectorState(bindingStateV1={binding.stateKey: state}))


async def events(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> PostHogResource:
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
    # See `_generate_delayed_stream_resource_state` for the `_{REALTIME}` key.
    backfill_fetchers = {
        f"{project_id}_{REALTIME}": functools.partial(
            backfill_project_events, http, config, project_id
        )
        for project_id in project_ids
    }

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # Captures created before the lookback backfill was dropped may still
        # hold an unfinished `_{LOOKBACK}` backfill. Nothing runs it anymore and
        # the realtime backfill covers its range, so delete it.
        if isinstance(state.backfill, dict):
            stale_keys = [k for k in state.backfill if k.endswith(f"_{LOOKBACK}")]
            if stale_keys:
                for k in stale_keys:
                    del state.backfill[k]

                task.log.info(
                    "Removing lookback backfill state.",
                    {"stale_keys": stale_keys},
                )
                await task.checkpoint(
                    ConnectorState(
                        bindingStateV1={
                            binding.stateKey: ResourceState(
                                backfill={k: None for k in stale_keys}
                            )
                        }
                    )
                )

        await _patch_missing_project_states(
            binding, state, task, project_ids, cutoff, lookback_cutoff
        )

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=incremental_fetchers,
            fetch_page=backfill_fetchers,
        )

    return PostHogResource(
        name=Event.resource_name,
        key=["/_meta/project_id", "/uuid"],
        model=Event,
        open=open,
        initial_state=_generate_delayed_stream_resource_state(
            project_ids, cutoff, lookback_cutoff
        ),
        initial_config=ResourceConfig(
            name=Event.resource_name,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


async def feature_flags(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> PostHogResource:
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

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        await _patch_missing_project_states(binding, state, task, project_ids, cutoff)

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=incremental_fetchers,
            fetch_page=backfill_fetchers,
        )

    return PostHogResource(
        name=FeatureFlag.resource_name,
        key=["/_meta/project_id", "/id"],
        model=FeatureFlag,
        open=open,
        initial_state=_generate_resource_state(project_ids, cutoff),
        initial_config=ResourceConfig(
            name=FeatureFlag.resource_name,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


async def persons(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> PostHogResource:
    project_ids = await fetch_project_ids(http, config, log)
    cutoff = datetime.now(tz=UTC)

    incremental_fetchers = {
        f"{project_id}": functools.partial(fetch_persons, http, config, project_id)
        for project_id in project_ids
    }

    backfill_fetchers = {
        f"{project_id}": functools.partial(backfill_persons, http, config, project_id)
        for project_id in project_ids
    }

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        await _patch_missing_project_states(binding, state, task, project_ids, cutoff)

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=incremental_fetchers,
            fetch_page=backfill_fetchers,
        )

    return PostHogResource(
        name=Person.resource_name,
        key=["/_meta/project_id", "/id"],
        model=Person,
        open=open,
        initial_state=_generate_resource_state(project_ids, cutoff),
        initial_config=ResourceConfig(
            name=Person.resource_name,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


async def sessions(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> PostHogResource:
    project_ids = await fetch_project_ids(http, config, log)
    base_url = config.advanced.base_url

    cutoff = datetime.now(tz=UTC)
    lookback_cutoff = cutoff - SESSIONS_LOOKBACK_LAG

    # The trailing boolean is `should_evict_emitted_changes`: only the lookback
    # stage may evict, since the realtime one runs ahead of entries it still
    # needs. It has to be bound positionally — the CDK appends `log` and
    # `cursor`.
    incremental_fetchers = {
        **{
            f"{project_id}_{REALTIME}": functools.partial(
                fetch_sessions,
                http,
                base_url,
                project_id,
                SESSIONS_REALTIME_LAG,
                False,
            )
            for project_id in project_ids
        },
        **{
            f"{project_id}_{LOOKBACK}": functools.partial(
                fetch_sessions,
                http,
                base_url,
                project_id,
                SESSIONS_LOOKBACK_LAG,
                True,
            )
            for project_id in project_ids
        },
    }

    backfill_fetchers = {
        # The shared state helpers key backfills by `_{REALTIME}`. Existing
        # Events captures depend on that key, so it can't change.
        f"{project_id}_{REALTIME}": functools.partial(
            backfill_sessions, http, base_url, config.start_date, project_id
        )
        for project_id in project_ids
    }

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        await _patch_missing_project_states(
            binding,
            state,
            task,
            project_ids,
            cutoff,
            lookback_cutoff,
            inc_offset=SESSIONS_CURSOR_TICK,
        )

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=incremental_fetchers,
            fetch_page=backfill_fetchers,
        )

    return PostHogResource(
        name=Session.resource_name,
        key=["/_meta/project_id", "/session_id"],
        model=Session,
        open=open,
        # `fetch_sessions` drops rows at or before its cursor, so each
        # incremental subtask is seeded one tick back and its own cutoff
        # becomes the first instant it emits. That closes the seam with the
        # realtime backfill, which stops one tick short of `cutoff`.
        initial_state=_generate_delayed_stream_resource_state(
            project_ids, cutoff, lookback_cutoff, SESSIONS_CURSOR_TICK
        ),
        initial_config=ResourceConfig(
            name=Session.resource_name,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[PostHogResource]:
    """Return all resources for the PostHog connector."""

    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    project_ids = await fetch_project_ids(http, config, log)
    log.info(
        f"Capturing data from {len(project_ids)} projects in org {config.organization_id}"
    )

    resources = [
        *snapshot_resources(http, config),
        await events(log, http, config),
        await feature_flags(log, http, config),
        await persons(log, http, config),
        await sessions(log, http, config),
    ]

    return await filter_resources_by_scopes(log, http, config, resources)
