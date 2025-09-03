from datetime import datetime, timedelta
import functools
from logging import Logger
from typing import AsyncGenerator, Callable
from zoneinfo import ZoneInfo

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    BoardChildStream,
    EndpointConfig,
    Epics,
    FilterSharing,
    FullRefreshArrayedStream,
    FullRefreshNestedArrayStream,
    FullRefreshPaginatedArrayedStream,
    FullRefreshPaginatedStream,
    FullRefreshResource,
    FullRefreshStream,
    IssueChildResource,
    IssueChildStream,
    IssueCustomFieldContexts,
    IssueCustomFieldOptions,
    JiraAPI,
    JiraResource,
    Labels,
    Permissions,
    PermissionsResponse,
    MyPermissionsResponse,
    ProjectChildStream,
    Projects,
    ResourceConfig,
    ResourceState,
    ScreenTabFields,
    StandardPermissions,
    Statuses,
    SystemAvatars,
    FULL_REFRESH_STREAMS,
    ISSUE_CHILD_STREAMS,
    PERMISSION_BLOCKED_STREAMS,
)
from .api import (
    backfill_issues,
    backfill_issues_child_resources,
    dt_to_str,
    fetch_issues,
    fetch_issues_child_resources,
    fetch_timezone,
    snapshot_board_child_resources,
    snapshot_epics,
    snapshot_filter_sharing,
    snapshot_issue_custom_field_contexts,
    snapshot_issue_custom_field_options,
    snapshot_labels,
    snapshot_nested_arrayed_resources,
    snapshot_non_paginated_arrayed_resources,
    snapshot_paginated_arrayed_resources,
    snapshot_paginated_resources,
    snapshot_permissions,
    snapshot_project_child_resources,
    snapshot_screen_tab_fields,
    snapshot_statuses,
    snapshot_system_avatars,
    url_base,
    ISSUE_JQL_SEARCH_LAG,
)


TEST_CONFIG_DOMAIN = "dontchangethisdomain.notareal.jira.domain"


async def validate_projects(
    log: Logger, http: HTTPMixin, config: EndpointConfig,
):
    if not config.advanced.projects:
        return

    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    valid_project_ids_and_keys: set[str] = set()

    async for project in snapshot_paginated_resources(http, config.domain, Projects, log):
        d = project.model_dump()
        id = d.get("id", None)
        key = d.get("key", None)
        if id:
            valid_project_ids_and_keys.add(id)
        if key:
            valid_project_ids_and_keys.add(key)

    errors: list[str] = []
    projects = config.advanced.projects.split(",")
    for project in projects:
        if project not in valid_project_ids_and_keys:
            msg = f"Project id/key {project} does not exist in your Jira account. Please fix this invalid project id/key in the the Advanced/Projects input and try again."
            errors.append(msg)

    if errors:
        raise ValidationError(errors)


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    url = f"{url_base(config.domain, JiraAPI.PLATFORM)}/myself"

    try:
        await http.request(log, url)
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


async def remove_permission_blocked_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig, resources: list[common.Resource]
) -> list[common.Resource]:
    # Not all Jira instances support all possible permissions, so we
    # have to check which permissions are actually available before checking
    # which ones the user has with the /mypermissions endpoint.
    url = f"{url_base(config.domain, JiraAPI.PLATFORM)}/permissions"
    response = PermissionsResponse.model_validate_json(
        await http.request(log, url)
    )

    params: dict[str, list[str]] = {
        "permissions": [p.value for p in StandardPermissions if p.value in response.permissions]
    }

    url = f"{url_base(config.domain, JiraAPI.PLATFORM)}/mypermissions"

    response = MyPermissionsResponse.model_validate_json(
        await http.request(log, url, params=params)
    )

    for (permissions, streams) in PERMISSION_BLOCKED_STREAMS:
        is_accessible = False
        for name in permissions:
            this_permission = response.permissions.get(name)
            if this_permission and this_permission.havePermission:
                is_accessible = True

        if not is_accessible:
            for stream in streams:
                resources = [r for r in resources if r.name != stream.name]

    return resources


def _get_partial_snapshot_fn(
    stream: type[FullRefreshStream],
    http: HTTPMixin,
    config: EndpointConfig,
    timezone: ZoneInfo,
) -> Callable[[Logger], AsyncGenerator[FullRefreshResource, None]]:
    if issubclass(stream, FullRefreshArrayedStream):
        snapshot_fn = functools.partial(
            snapshot_non_paginated_arrayed_resources,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, FullRefreshNestedArrayStream):
        snapshot_fn = functools.partial(
            snapshot_nested_arrayed_resources,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, FullRefreshPaginatedArrayedStream):
        snapshot_fn = functools.partial(
            snapshot_paginated_arrayed_resources,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, FullRefreshPaginatedStream):
        snapshot_fn = functools.partial(
            snapshot_paginated_resources,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, Labels):
        snapshot_fn = functools.partial(
            snapshot_labels,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, Permissions):
        snapshot_fn = functools.partial(
            snapshot_permissions,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, SystemAvatars):
        snapshot_fn = functools.partial(
            snapshot_system_avatars,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, FilterSharing):
        snapshot_fn = functools.partial(
            snapshot_filter_sharing,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, IssueCustomFieldContexts):
        snapshot_fn = functools.partial(
            snapshot_issue_custom_field_contexts,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, IssueCustomFieldOptions):
        snapshot_fn = functools.partial(
            snapshot_issue_custom_field_options,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, ScreenTabFields):
        snapshot_fn = functools.partial(
            snapshot_screen_tab_fields,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, Statuses):
        snapshot_fn = functools.partial(
            snapshot_statuses,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, ProjectChildStream):
        snapshot_fn = functools.partial(
            snapshot_project_child_resources,
            http,
            config.domain,
            stream,
        )
    elif issubclass(stream, Epics):
        snapshot_fn = functools.partial(
            snapshot_epics,
            http,
            config.domain,
            stream,
            timezone,
        )
    elif issubclass(stream, BoardChildStream):
        snapshot_fn = functools.partial(
            snapshot_board_child_resources,
            http,
            config.domain,
            stream,
        )
    else:
        raise RuntimeError(f"Unknown full refresh stream type {stream.__name__} for stream {stream.name}")

    return snapshot_fn


def full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig, timezone: ZoneInfo
) -> list[common.Resource]:

    def open(
            stream: type[FullRefreshStream],
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=_get_partial_snapshot_fn(stream, http, config, timezone),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    resources: list[common.Resource] = []

    for stream in FULL_REFRESH_STREAMS:
        resources.append(
            common.Resource(
                name=stream.name,
                key=["/_meta/row_id"],
                model=FullRefreshResource,
                open=functools.partial(open, stream),
                initial_state=ResourceState(),
                initial_config=ResourceConfig(
                    name=stream.name, interval=timedelta(minutes=60)
                ),
                schema_inference=True,
                disable=stream.disable,
            )
        )

    return resources


def issues(
        log: Logger, http: HTTPMixin, config: EndpointConfig, timezone: ZoneInfo
) -> common.Resource:

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_issues,
                http,
                config.domain,
                timezone,
                config.advanced.projects,
            ),
            fetch_page=functools.partial(
                backfill_issues,
                http,
                config.domain,
                timezone,
                config.advanced.projects,
            )
        )

    # Shift the cutoff back ISSUE_JQL_SEARCH_LAG duration to ensure backfills
    # always cover ranges where Jira's API returns consistent results.
    cutoff = datetime.now(tz=timezone) - ISSUE_JQL_SEARCH_LAG
    start = config.start_date.astimezone(timezone)

    return common.Resource(
        name="issues",
        key=["/id"],
        model=JiraResource,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=dt_to_str(start))
        ),
        initial_config=ResourceConfig(
            name="issues", interval=timedelta(minutes=5)
        ),
        schema_inference=True,
        disable=False,
    )


def issue_child_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig, timezone: ZoneInfo
) -> list[common.Resource]:

    def open(
        stream: type[IssueChildStream],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_issues_child_resources,
                http,
                config.domain,
                timezone,
                config.advanced.projects,
                stream,
            ),
            fetch_page=functools.partial(
                backfill_issues_child_resources,
                http,
                config.domain,
                timezone,
                config.advanced.projects,
                stream,
            )
        )

    # Shift the cutoff back ISSUE_JQL_SEARCH_LAG duration to ensure backfills
    # always cover ranges where Jira's API returns consistent results.
    cutoff = datetime.now(tz=timezone) - ISSUE_JQL_SEARCH_LAG
    start = config.start_date.astimezone(timezone)

    resources: list[common.Resource] = []

    for stream in ISSUE_CHILD_STREAMS:
        resources.append(
            common.Resource(
                name=stream.name,
                key=["/id", "/issueId"],
                model=IssueChildResource,
                open=functools.partial(open, stream),
                initial_state=ResourceState(
                    inc=ResourceState.Incremental(cursor=cutoff),
                    backfill=ResourceState.Backfill(cutoff=cutoff, next_page=dt_to_str(start))
                ),
                initial_config=ResourceConfig(
                    name=stream.name, interval=timedelta(minutes=5)
                ),
                schema_inference=True,
                disable=stream.disable,
            )
        )

    return resources


async def all_resources(
    log: Logger, http: HTTPMixin,
    config: EndpointConfig,
    should_fetch_timezone: bool = True,
    should_check_permissions: bool = False,
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    timezone = await fetch_timezone(http, config.domain, log) if should_fetch_timezone else ZoneInfo("UTC")

    resources = [
        *full_refresh_resources(log, http, config, timezone),
        issues(log, http, config, timezone),
        *issue_child_resources(log, http, config, timezone)
    ]

    # To discover all resources for the discover snapshot, don't try to check permissions when using
    # our test config domain. We don't have a test Jira account, so this permission check will crash
    # without an escape hatch like this.
    if should_check_permissions and config.domain != TEST_CONFIG_DOMAIN:
        accessible_resources = await remove_permission_blocked_resources(log, http, config, resources)
    else:
        accessible_resources = resources

    return accessible_resources
