"""Resource definitions for Asana source connector."""

import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any

from estuary_cdk.capture import Task, common
from estuary_cdk.http import HTTPSession

from .api import (
    _collect_projects,
    fetch_project_scoped,
    fetch_project_tasks,
    fetch_stories,
    fetch_task_events,
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


# Models whose snapshots use the standard scope-based generic fetch.
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
    Section,
    StatusUpdate,
    Attachment,
    Membership,
]

# Models with multi-level iteration that need dedicated fetch functions.
SNAPSHOT_CUSTOM_FETCHERS: dict[str, Any] = {
    "TeamMemberships": fetch_team_memberships,
    "Stories": fetch_stories,
}


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

    # --- Build snapshot resources from models ---

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

    # --- Custom-fetched snapshot resources ---

    for name, fetch_fn in SNAPSHOT_CUSTOM_FETCHERS.items():
        model = TeamMembership if name == "TeamMemberships" else Story
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

    # --- Tasks: per-project incremental via events API ---

    projects = await _collect_projects(http, config, log)

    task_fetch_page: dict[str, Any] = {}
    task_fetch_changes: dict[str, Any] = {}
    task_initial_inc: dict[str, ResourceState.Incremental] = {}
    task_initial_backfill: dict[str, ResourceState.Backfill] = {}

    for project in projects:
        task_fetch_page[project.gid] = functools.partial(
            fetch_project_tasks, http, base_url, project.gid,
        )
        task_fetch_changes[project.gid] = functools.partial(
            fetch_task_events, http, base_url, project.gid,
        )
        task_initial_inc[project.gid] = ResourceState.Incremental(cursor=("",))
        task_initial_backfill[project.gid] = ResourceState.Backfill(
            cutoff=("",), next_page=None,
        )

    def open_tasks_binding(
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
            fetch_page=task_fetch_page,
            fetch_changes=task_fetch_changes,
            tombstone=TOMBSTONE,
        )

    resources.append(
        common.Resource(
            name="Tasks",
            key=["/gid"],
            model=AsanaTask,
            open=open_tasks_binding,
            initial_state=ResourceState(
                inc=task_initial_inc,
                backfill=task_initial_backfill,
            ),
            initial_config=ResourceConfig(
                name="Tasks",
                interval=timedelta(minutes=1),
            ),
            schema_inference=True,
        )
    )

    return resources
