"""Resource definitions for Asana source connector."""

import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any

from estuary_cdk.capture import Task, common
from estuary_cdk.http import HTTPSession

from .api import (
    _collect_projects,
    fetch_attachments,
    fetch_custom_fields,
    fetch_goals,
    fetch_memberships,
    fetch_portfolios,
    fetch_project_templates,
    fetch_project_tasks,
    fetch_projects,
    fetch_sections,
    fetch_status_updates,
    fetch_stories,
    fetch_tags,
    fetch_task_events,
    fetch_team_memberships,
    fetch_teams,
    fetch_time_periods,
    fetch_users,
    fetch_workspaces,
)
from .models import (
    Attachment,
    CustomField,
    EndpointConfig,
    Goal,
    Membership,
    Portfolio,
    Project,
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
    TOMBSTONE,
    User,
    Workspace,
)


# (fetch_fn, model_class, key_field, display_name)
SNAPSHOT_RESOURCES = [
    (fetch_workspaces, Workspace, "/gid", "Workspaces"),
    (fetch_users, User, "/gid", "Users"),
    (fetch_teams, Team, "/gid", "Teams"),
    (fetch_projects, Project, "/gid", "Projects"),
    (fetch_sections, Section, "/gid", "Sections"),
    (fetch_tags, Tag, "/gid", "Tags"),
    (fetch_portfolios, Portfolio, "/gid", "Portfolios"),
    (fetch_goals, Goal, "/gid", "Goals"),
    (fetch_custom_fields, CustomField, "/gid", "CustomFields"),
    (fetch_time_periods, TimePeriod, "/gid", "TimePeriods"),
    (fetch_project_templates, ProjectTemplate, "/gid", "ProjectTemplates"),
    (fetch_status_updates, StatusUpdate, "/gid", "StatusUpdates"),
    (fetch_attachments, Attachment, "/gid", "Attachments"),
    (fetch_stories, Story, "/gid", "Stories"),
    (fetch_memberships, Membership, "/gid", "Memberships"),
    (fetch_team_memberships, TeamMembership, "/gid", "TeamMemberships"),
]


async def all_resources(
    log: Logger,
    http: HTTPSession,
    config: EndpointConfig,
) -> list[common.Resource]:
    """Build list of all available resources."""
    base_url = config.advanced.base_url.rstrip("/")

    # --- Snapshot binding opener ---

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
            fetch_snapshot=functools.partial(fetch_fn, http, config),
            tombstone=TOMBSTONE,
        )

    # --- Tasks: sync-token incremental, one subtask per project ---
    # Fetch all projects to build per-project dicts for fetch_page and fetch_changes.

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

    # --- Build resource list ---

    resources: list[common.Resource] = []

    for fetch_fn, model_class, key_field, name in SNAPSHOT_RESOURCES:
        resources.append(
            common.Resource(
                name=name,
                key=[key_field],
                model=model_class,
                open=functools.partial(open_snapshot_binding, fetch_fn),
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

    # Tasks resource: per-project incremental via events API
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
