"""Asana API client functions."""

import json
from collections.abc import AsyncGenerator
from logging import Logger

from estuary_cdk.capture.common import BaseDocument
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    AsanaPageMeta,
    Attachment,
    BaseEntity,
    ChangeEvent,
    CustomField,
    EndpointConfig,
    Goal,
    Membership,
    Portfolio,
    Project,
    ProjectTemplate,
    Section,
    StatusUpdate,
    Story,
    SyncTokenResponse,
    Tag,
    Task,
    Team,
    TeamMembership,
    TimePeriod,
    User,
    Workspace,
    _Tombstone,
)


API_PAGE_LIMIT = 100


def _extract_response_body(e: HTTPError) -> str:
    """Extract the response body from an HTTPError message.

    CDK formats as: 'Encountered HTTP error status {code}.\\nURL: ...\\nResponse:\\n{body}'
    """
    marker = "Response:\n"
    idx = e.message.find(marker)
    if idx == -1:
        return ""
    return e.message[idx + len(marker):]


async def validate_credentials(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> None:
    """Validate API credentials by fetching the authenticated user."""
    url = f"{base_url}/users/me"
    await http.request(log, url)


# --- Generic paginated fetch ---
# Asana wraps collections in {"data": [...], "next_page": {...}}

async def _fetch_paginated[T: BaseEntity](
    start_url: str,
    model: type[T],
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[T]:
    """Generic paginated fetch using IncrementalJsonProcessor."""
    url: str | None = start_url

    while url is not None:
        _, body = await http.request_stream(log, url)
        processor = IncrementalJsonProcessor(
            body(), "data.item", model, remainder_cls=AsanaPageMeta
        )

        async for item in processor:
            yield item

        meta = processor.get_remainder()
        url = meta.next_page.uri if meta.next_page else None


# --- Workspace and project collection (reused by scoped fetches) ---

async def _collect_workspaces(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> list[Workspace]:
    """Fetch and buffer all workspaces."""
    result: list[Workspace] = []
    url = f"{config.advanced.base_url}/workspaces?limit={API_PAGE_LIMIT}"
    async for ws in _fetch_paginated(url, Workspace, http, log):
        result.append(ws)
    return result


async def _collect_projects(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> list[Project]:
    """Fetch and buffer all projects across workspaces."""
    result: list[Project] = []
    for ws in await _collect_workspaces(http, config, log):
        url = f"{config.advanced.base_url}/projects?workspace={ws.gid}&limit={API_PAGE_LIMIT}&archived=false"
        async for project in _fetch_paginated(url, Project, http, log):
            result.append(project)
    return result


# --- Snapshot fetch functions ---
# Each yields all instances of a resource type.

async def fetch_workspaces(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Workspace]:
    url = f"{config.advanced.base_url}/workspaces?limit={API_PAGE_LIMIT}"
    async for item in _fetch_paginated(url, Workspace, http, log):
        yield item


async def fetch_users(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[User]:
    seen: set[str] = set()
    for ws in await _collect_workspaces(http, config, log):
        url = f"{config.advanced.base_url}/users?workspace={ws.gid}&limit={API_PAGE_LIMIT}"
        async for user in _fetch_paginated(url, User, http, log):
            if user.gid not in seen:
                seen.add(user.gid)
                yield user


async def fetch_teams(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Team]:
    for ws in await _collect_workspaces(http, config, log):
        url = f"{config.advanced.base_url}/organizations/{ws.gid}/teams?limit={API_PAGE_LIMIT}"
        try:
            async for team in _fetch_paginated(url, Team, http, log):
                yield team
        except HTTPError as e:
            if e.code in (403, 404):
                log.info(f"Skipping teams for workspace {ws.gid} (not an organization)")
            else:
                raise


async def fetch_projects(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Project]:
    for ws in await _collect_workspaces(http, config, log):
        url = f"{config.advanced.base_url}/projects?workspace={ws.gid}&limit={API_PAGE_LIMIT}&archived=false"
        async for project in _fetch_paginated(url, Project, http, log):
            yield project


async def fetch_tags(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Tag]:
    for ws in await _collect_workspaces(http, config, log):
        url = f"{config.advanced.base_url}/tags?workspace={ws.gid}&limit={API_PAGE_LIMIT}"
        async for tag in _fetch_paginated(url, Tag, http, log):
            yield tag


async def fetch_portfolios(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Portfolio]:
    for ws in await _collect_workspaces(http, config, log):
        url = f"{config.advanced.base_url}/portfolios?workspace={ws.gid}&owner=me&limit={API_PAGE_LIMIT}"
        try:
            async for portfolio in _fetch_paginated(url, Portfolio, http, log):
                yield portfolio
        except HTTPError as e:
            if e.code == 402:
                log.warning(f"Portfolios not available for workspace {ws.gid} (requires paid plan)")
            else:
                raise


async def fetch_goals(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Goal]:
    for ws in await _collect_workspaces(http, config, log):
        url = f"{config.advanced.base_url}/goals?workspace={ws.gid}&limit={API_PAGE_LIMIT}"
        try:
            async for goal in _fetch_paginated(url, Goal, http, log):
                yield goal
        except HTTPError as e:
            if e.code == 402:
                log.warning(f"Goals not available for workspace {ws.gid} (requires paid plan)")
            else:
                raise


async def fetch_custom_fields(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[CustomField]:
    for ws in await _collect_workspaces(http, config, log):
        url = f"{config.advanced.base_url}/workspaces/{ws.gid}/custom_fields?limit={API_PAGE_LIMIT}"
        try:
            async for cf in _fetch_paginated(url, CustomField, http, log):
                yield cf
        except HTTPError as e:
            if e.code == 402:
                log.warning(f"Custom fields not available for workspace {ws.gid} (requires paid plan)")
            else:
                raise


async def fetch_time_periods(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[TimePeriod]:
    for ws in await _collect_workspaces(http, config, log):
        url = f"{config.advanced.base_url}/time_periods?workspace={ws.gid}&limit={API_PAGE_LIMIT}"
        async for tp in _fetch_paginated(url, TimePeriod, http, log):
            yield tp


async def fetch_project_templates(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[ProjectTemplate]:
    for ws in await _collect_workspaces(http, config, log):
        url = f"{config.advanced.base_url}/project_templates?workspace={ws.gid}&limit={API_PAGE_LIMIT}"
        try:
            async for pt in _fetch_paginated(url, ProjectTemplate, http, log):
                yield pt
        except HTTPError as e:
            if e.code in (400, 402):
                log.warning(f"Project templates not available for workspace {ws.gid} ({e.code})")
            else:
                raise


async def fetch_team_memberships(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[TeamMembership]:
    for ws in await _collect_workspaces(http, config, log):
        # Team memberships require iterating over teams
        teams_url = f"{config.advanced.base_url}/organizations/{ws.gid}/teams?limit={API_PAGE_LIMIT}"
        try:
            async for team in _fetch_paginated(teams_url, Team, http, log):
                url = f"{config.advanced.base_url}/team_memberships?team={team.gid}&limit={API_PAGE_LIMIT}"
                async for tm in _fetch_paginated(url, TeamMembership, http, log):
                    yield tm
        except HTTPError as e:
            if e.code in (403, 404):
                log.info(f"Skipping team memberships for workspace {ws.gid} (not an organization)")
            else:
                raise


# --- Project-scoped snapshot fetches ---

async def fetch_tasks(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Task]:
    for project in await _collect_projects(http, config, log):
        url = f"{config.advanced.base_url}/tasks?project={project.gid}&limit={API_PAGE_LIMIT}"
        async for task in _fetch_paginated(url, Task, http, log):
            yield task


async def fetch_sections(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Section]:
    for project in await _collect_projects(http, config, log):
        url = f"{config.advanced.base_url}/projects/{project.gid}/sections?limit={API_PAGE_LIMIT}"
        async for section in _fetch_paginated(url, Section, http, log):
            yield section


async def fetch_status_updates(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[StatusUpdate]:
    for project in await _collect_projects(http, config, log):
        url = f"{config.advanced.base_url}/status_updates?parent={project.gid}&limit={API_PAGE_LIMIT}"
        try:
            async for su in _fetch_paginated(url, StatusUpdate, http, log):
                yield su
        except HTTPError as e:
            if e.code in (403, 404):
                log.info(f"Skipping status updates for project {project.gid}")
            else:
                raise


async def fetch_attachments(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Attachment]:
    for project in await _collect_projects(http, config, log):
        url = f"{config.advanced.base_url}/attachments?parent={project.gid}&limit={API_PAGE_LIMIT}"
        try:
            async for att in _fetch_paginated(url, Attachment, http, log):
                yield att
        except HTTPError as e:
            if e.code in (403, 404):
                log.info(f"Skipping attachments for project {project.gid}")
            else:
                raise


# --- Task-scoped snapshot fetches ---

async def fetch_stories(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Story]:
    """Fetch stories (comments/activity) for all tasks across all projects."""
    for project in await _collect_projects(http, config, log):
        tasks_url = f"{config.advanced.base_url}/tasks?project={project.gid}&limit={API_PAGE_LIMIT}"
        async for task in _fetch_paginated(tasks_url, Task, http, log):
            url = f"{config.advanced.base_url}/tasks/{task.gid}/stories?limit={API_PAGE_LIMIT}"
            async for story in _fetch_paginated(url, Story, http, log):
                yield story


# --- Membership fetch ---

async def fetch_memberships(
    http: HTTPSession, config: EndpointConfig, log: Logger,
) -> AsyncGenerator[Membership]:
    for project in await _collect_projects(http, config, log):
        url = f"{config.advanced.base_url}/memberships?parent={project.gid}&limit={API_PAGE_LIMIT}"
        try:
            async for m in _fetch_paginated(url, Membership, http, log):
                yield m
        except HTTPError as e:
            if e.code in (403, 404):
                log.info(f"Skipping memberships for project {project.gid}")
            else:
                raise


# --- Sync-token incremental: Events API ---
# Asana's /events endpoint uses an opaque sync token per project.
# First call returns 412 + initial token. Subsequent calls return change events.
# Events are notifications, not full documents — changed resources must be re-fetched.
#
# CDK pattern: dict[project_gid, FetchChangesFn] with dict[project_gid, Incremental].
# Each project gets its own subtask with an independent tuple[str] cursor.


async def _fetch_single_task(
    http: HTTPSession,
    base_url: str,
    gid: str,
    log: Logger,
) -> Task:
    """Re-fetch a single task by GID."""
    url = f"{base_url}/tasks/{gid}"
    response = await http.request(log, url)
    data = json.loads(response)
    return Task.model_validate(data["data"])


async def fetch_project_tasks(
    http: HTTPSession,
    base_url: str,
    project_gid: str,
    log: Logger,
    _log_cursor: object,
    _cutoff: object,
) -> AsyncGenerator[Task]:
    """FetchPageFn: backfill all tasks for a single project."""
    url = f"{base_url}/tasks?project={project_gid}&limit={API_PAGE_LIMIT}"
    async for task in _fetch_paginated(url, Task, http, log):
        yield task


async def fetch_task_events(
    http: HTTPSession,
    base_url: str,
    project_gid: str,
    log: Logger,
    log_cursor: tuple[str | int],
) -> AsyncGenerator[Task | _Tombstone | tuple[str], None]:
    """FetchChangesFn: poll project events, re-fetch changed tasks, yield docs + new cursor.

    Cursor is a 1-tuple wrapping the sync token: ("sync_token_value",).
    Empty string means bootstrap needed.
    """
    sync_token = str(log_cursor[0])
    events_url = f"{base_url}/projects/{project_gid}/events"

    # Bootstrap: acquire initial sync token if empty
    if not sync_token:
        try:
            await http.request(log, events_url)
            # Asana should return 412 on first call
            log.warning(f"Expected 412 from {events_url}, got 200 — using empty token")
            return
        except HTTPError as e:
            if e.code == 412:
                body = json.loads(_extract_response_body(e))
                sync_token = body["sync"]
                yield (sync_token,)
                return
            raise

    # Poll for changes
    token = sync_token
    changed_gids: set[str] = set()
    deleted_gids: set[str] = set()

    has_more = True
    while has_more:
        try:
            response = await http.request(log, f"{events_url}?sync={token}")
        except HTTPError as e:
            if e.code == 412:
                # Token expired — yield new token, CDK triggers re-backfill
                body = json.loads(_extract_response_body(e))
                yield (body["sync"],)
                return
            raise

        data = SyncTokenResponse.model_validate_json(response)

        for event in data.data:
            gid = event.resource.get("gid")
            resource_type = event.resource.get("resource_type")
            if not gid or resource_type != "task":
                continue
            if event.action == "deleted":
                deleted_gids.add(gid)
            else:
                changed_gids.add(gid)

        token = data.sync
        has_more = data.has_more

    # Yield tombstones for deleted tasks
    for gid in deleted_gids:
        tombstone = _Tombstone(gid=gid, _meta=_Tombstone.Meta(op="d"))
        yield tombstone

    # Re-fetch changed tasks (exclude deleted)
    for gid in changed_gids - deleted_gids:
        try:
            doc = await _fetch_single_task(http, base_url, gid, log)
            doc.meta_ = BaseDocument.Meta(op="c")
            yield doc
        except HTTPError as e:
            if e.code in (403, 404):
                log.info(f"Skipping task {gid} (not accessible)")
            else:
                raise

    # Yield new cursor
    yield (token,)
