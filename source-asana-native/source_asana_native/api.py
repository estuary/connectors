"""Asana API client functions."""

from collections.abc import AsyncGenerator
from logging import Logger

from estuary_cdk.capture.common import BaseDocument
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    AsanaDetailResponse,
    AsanaPageMeta,
    BaseEntity,
    EndpointConfig,
    Project,
    ProjectScopedEntity,
    Story,
    SyncTokenResponse,
    Task,
    Team,
    TeamMembership,
    TopLevelEntity,
    Workspace,
    WorkspaceScopedEntity,
    _Tombstone,
)


async def validate_credentials(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> None:
    url = f"{base_url}/users/me"
    await http.request(log, url)


# --- Generic paginated fetch ---


async def _fetch_paginated[T: BaseEntity](
    url: str,
    model: type[T],
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[T]:
    current_url: str | None = url

    while current_url is not None:
        _, body = await http.request_stream(log, current_url)
        processor = IncrementalJsonProcessor(
            body(), "data.item", model, remainder_cls=AsanaPageMeta
        )

        async for item in processor:
            yield item

        meta = processor.get_remainder()
        current_url = meta.next_page.uri if meta.next_page else None


# --- Scope-aware generic fetch functions ---


async def _collect_workspaces(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> list[Workspace]:
    return [item async for item in fetch_top_level(Workspace, http, config, log)]


async def _collect_projects(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> list:
    return [item async for item in fetch_workspace_scoped(Project, http, config, log)]


async def fetch_top_level[T: TopLevelEntity](
    model: type[T],
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[T]:
    url = model.get_url(config.advanced.base_url)
    async for item in _fetch_paginated(url, model, http, log):
        yield item


async def fetch_workspace_scoped[T: WorkspaceScopedEntity](
    model: type[T],
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[T]:
    seen: set[str] = set()
    base_url = config.advanced.base_url

    for ws in await _collect_workspaces(http, config, log):
        url = model.get_url(base_url, ws.gid)
        try:
            async for item in _fetch_paginated(url, model, http, log):
                if model.deduplicate:
                    if item.gid in seen:
                        continue
                    seen.add(item.gid)
                yield item
        except HTTPError as e:
            if e.code in model.tolerated_errors:
                log.info(f"Skipping {model.resource_name} for workspace {ws.gid} ({e.code})")
            else:
                raise


async def fetch_project_scoped[T: ProjectScopedEntity](
    model: type[T],
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[T]:
    base_url = config.advanced.base_url

    for project in await _collect_projects(http, config, log):
        url = model.get_url(base_url, project.gid)
        try:
            async for item in _fetch_paginated(url, model, http, log):
                yield item
        except HTTPError as e:
            if e.code in model.tolerated_errors:
                log.info(f"Skipping {model.resource_name} for project {project.gid} ({e.code})")
            else:
                raise


# --- Multi-level fetch functions (can't be fully generic) ---


async def fetch_team_memberships(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[TeamMembership]:
    base_url = config.advanced.base_url

    for ws in await _collect_workspaces(http, config, log):
        teams_url = Team.get_url(base_url, ws.gid)
        try:
            async for team in _fetch_paginated(teams_url, Team, http, log):
                url = TeamMembership.get_url(base_url, team.gid)
                async for tm in _fetch_paginated(url, TeamMembership, http, log):
                    yield tm
        except HTTPError as e:
            if e.code in Team.tolerated_errors:
                log.info(f"Skipping team memberships for workspace {ws.gid} ({e.code})")
            else:
                raise


async def fetch_stories(
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[Story]:
    base_url = config.advanced.base_url

    for project in await _collect_projects(http, config, log):
        tasks_url = Task.get_url(base_url, project.gid)
        async for task in _fetch_paginated(tasks_url, Task, http, log):
            url = Story.get_url(base_url, task.gid)
            async for story in _fetch_paginated(url, Story, http, log):
                yield story


# --- Sync-token incremental: Events API ---


async def _fetch_single[T: BaseEntity](
    model: type[T],
    http: HTTPSession,
    base_url: str,
    gid: str,
    log: Logger,
) -> T:
    """Fetch a single resource by GID using its detail endpoint."""
    url = model.get_detail_url(base_url, gid)
    response = await http.request(log, url)
    envelope = AsanaDetailResponse.model_validate_json(response)
    return model.model_validate(envelope.data)


async def backfill_resources[T: ProjectScopedEntity](
    model: type[T],
    http: HTTPSession,
    base_url: str,
    project_gid: str,
    log: Logger,
    _log_cursor: object,
    _cutoff: object,
) -> AsyncGenerator[T]:
    """FetchPageFn: backfill all resources of a type for a single project."""
    url = model.get_url(base_url, project_gid)
    async for item in _fetch_paginated(url, model, http, log):
        yield item


async def backfill_stories(
    http: HTTPSession,
    base_url: str,
    project_gid: str,
    log: Logger,
    _log_cursor: object,
    _cutoff: object,
) -> AsyncGenerator[Story]:
    """FetchPageFn: backfill stories for a project (requires task iteration)."""
    tasks_url = Task.get_url(base_url, project_gid)
    async for task in _fetch_paginated(tasks_url, Task, http, log):
        url = Story.get_url(base_url, task.gid)
        async for story in _fetch_paginated(url, Story, http, log):
            yield story


async def fetch_events[T: BaseEntity](
    model: type[T],
    http: HTTPSession,
    base_url: str,
    project_gid: str,
    log: Logger,
    log_cursor: tuple[str | int],
) -> AsyncGenerator[T | _Tombstone | tuple[str], None]:
    """FetchChangesFn: poll project events for a resource type, re-fetch changed resources."""
    sync_token = str(log_cursor[0])

    if not sync_token:
        bootstrap_url = Project.get_events_url(base_url, project_gid)
        try:
            await http.request(log, bootstrap_url)
            log.warning(f"Expected 412 from {bootstrap_url}, got 200 — using empty token")
            return
        except HTTPError as e:
            if e.code == 412:
                sync_token = SyncTokenResponse.model_validate_json(e.body).sync
                yield (sync_token,)
                return
            raise

    token = sync_token
    changed_gids: set[str] = set()
    deleted_gids: set[str] = set()

    has_more = True
    while has_more:
        try:
            url = Project.get_events_url(base_url, project_gid, sync=token)
            response = await http.request(log, url)
        except HTTPError as e:
            if e.code == 412:
                new_token = SyncTokenResponse.model_validate_json(e.body).sync
                yield (new_token,)
                return
            raise

        data = SyncTokenResponse.model_validate_json(response)

        for event in data.data:
            gid = event.resource.get("gid")
            resource_type = event.resource.get("resource_type")
            if not gid or resource_type != model.event_type:
                continue
            if event.action == "deleted":
                deleted_gids.add(gid)
            else:
                changed_gids.add(gid)

        token = data.sync
        has_more = data.has_more

    for gid in deleted_gids:
        tombstone = _Tombstone(gid=gid, _meta=_Tombstone.Meta(op="d"))
        yield tombstone

    for gid in changed_gids - deleted_gids:
        try:
            doc = await _fetch_single(model, http, base_url, gid, log)
            doc.meta_ = BaseDocument.Meta(op="c")
            yield doc
        except HTTPError as e:
            if e.code in (403, 404):
                log.info(f"Skipping {model.event_type} {gid} (not accessible)")
            else:
                raise

    yield (token,)
