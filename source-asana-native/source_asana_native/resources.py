"""Resource definitions for Asana source connector."""

import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any

from estuary_cdk.capture import Task, common
from estuary_cdk.http import HTTPSession

from .api import (
    _collect_projects,
    backfill_resources,
    backfill_stories,
    fetch_events,
    fetch_project_scoped,
    fetch_team_memberships,
    fetch_top_level,
    fetch_workspace_scoped,
)
from .models import (
    Attachment,
    CustomField,
    EndpointConfig,
    Goal,
    Membership,
    Portfolio,
    Project,
    ProjectScopedEntity,
    ProjectTemplate,
    ResourceConfig,
    ResourceState,
    Section,
    StatusUpdate,
    Story,
    Tag,
    Task as AsanaTask,
    Team,
    TeamMembership,
    TimePeriod,
    TopLevelEntity,
    TOMBSTONE,
    User,
    Workspace,
    WorkspaceScopedEntity,
)


# Snapshot-only resources (full refresh).
SNAPSHOT_MODELS = [
    Workspace,
    User,
    Team,
    Project,
    Tag,
    Portfolio,
    Goal,
    CustomField,
    TimePeriod,
    ProjectTemplate,
    StatusUpdate,
    Membership,
]

# Snapshot resources with multi-level iteration that need dedicated fetch functions.
SNAPSHOT_CUSTOM_FETCHERS: dict[str, Any] = {
    "TeamMemberships": fetch_team_memberships,
}

# Incremental resources tracked via the project Events API.
# Each gets a per-project sync token and polls for changes.
INCREMENTAL_MODELS = [
    AsanaTask,
    Section,
    Attachment,
    Story,
]


def _get_snapshot_fetcher(model, http, config):
    """Return a snapshot fetch partial for a model based on its scope type."""
    if issubclass(model, TopLevelEntity):
        return functools.partial(fetch_top_level, model, http, config)
    elif issubclass(model, WorkspaceScopedEntity):
        return functools.partial(fetch_workspace_scoped, model, http, config)
    elif issubclass(model, ProjectScopedEntity):
        return functools.partial(fetch_project_scoped, model, http, config)
    else:
        raise ValueError(f"Unknown entity scope for {model}")


def _get_backfill_fn(model):
    """Return the appropriate backfill function for an incremental model."""
    if model is Story:
        return backfill_stories
    return functools.partial(backfill_resources, model)


async def all_resources(
    log: Logger,
    http: HTTPSession,
    config: EndpointConfig,
) -> list[common.Resource]:
    """Build list of all available resources."""
    base_url = config.advanced.base_url.rstrip("/")

    def open_snapshot_binding(
        fetch_fn: Any,
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
            fetch_snapshot=fetch_fn,
            tombstone=TOMBSTONE,
        )

    # --- Snapshot resources ---

    resources: list[common.Resource] = []

    for model in SNAPSHOT_MODELS:
        fetcher = _get_snapshot_fetcher(model, http, config)
        resources.append(
            common.Resource(
                name=model.resource_name,
                key=["/gid"],
                model=model,
                open=functools.partial(open_snapshot_binding, fetcher),
                initial_state=ResourceState(
                    snapshot=ResourceState.Snapshot(
                        updated_at=datetime.min.replace(tzinfo=UTC),
                        last_count=0,
                        last_digest="",
                    )
                ),
                initial_config=ResourceConfig(
                    name=model.resource_name,
                    interval=timedelta(minutes=5),
                ),
                schema_inference=True,
            )
        )

    for name, fetch_fn in SNAPSHOT_CUSTOM_FETCHERS.items():
        model = TeamMembership if name == "TeamMemberships" else None
        assert model is not None
        fetcher = functools.partial(fetch_fn, http, config)
        resources.append(
            common.Resource(
                name=name,
                key=["/gid"],
                model=model,
                open=functools.partial(open_snapshot_binding, fetcher),
                initial_state=ResourceState(
                    snapshot=ResourceState.Snapshot(
                        updated_at=datetime.min.replace(tzinfo=UTC),
                        last_count=0,
                        last_digest="",
                    )
                ),
                initial_config=ResourceConfig(
                    name=name,
                    interval=timedelta(minutes=5),
                ),
                schema_inference=True,
            )
        )

    # --- Incremental resources via project Events API ---

    projects = await _collect_projects(http, config, log)

    for model in INCREMENTAL_MODELS:
        backfill_fn = _get_backfill_fn(model)

        fetch_page: dict[str, Any] = {}
        fetch_changes: dict[str, Any] = {}
        initial_inc: dict[str, ResourceState.Incremental] = {}
        initial_backfill: dict[str, ResourceState.Backfill] = {}

        for project in projects:
            fetch_page[project.gid] = functools.partial(
                backfill_fn, http, base_url, project.gid,
            )
            fetch_changes[project.gid] = functools.partial(
                fetch_events, model, http, base_url, project.gid,
            )
            initial_inc[project.gid] = ResourceState.Incremental(cursor=("",))
            initial_backfill[project.gid] = ResourceState.Backfill(
                cutoff=("",), next_page=None,
            )

        def open_incremental_binding(
            fp: dict,
            fc: dict,
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
                fetch_page=fp,
                fetch_changes=fc,
                tombstone=TOMBSTONE,
            )

        resources.append(
            common.Resource(
                name=model.resource_name,
                key=["/gid"],
                model=model,
                open=functools.partial(open_incremental_binding, fetch_page, fetch_changes),
                initial_state=ResourceState(
                    inc=initial_inc,
                    backfill=initial_backfill,
                ),
                initial_config=ResourceConfig(
                    name=model.resource_name,
                    interval=timedelta(minutes=1),
                ),
                schema_inference=True,
            )
        )

    return resources
