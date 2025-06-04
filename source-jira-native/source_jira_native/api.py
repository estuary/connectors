from datetime import datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator
from zoneinfo import ZoneInfo

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import TypeAdapter

from .models import (
    FullRefreshResource,
    PaginatedResponse,
    JiraResource,
    Issue,
    IssuesResponse,
    MyselfResponse,
    LabelsResponse,
    PermissionsResponse,
    ProjectAvatarsResponse,
    ScreenTabs,
    SystemAvatarsResponse,
    Filters,
    IssueFields,
    Projects,
    ProjectChildStream,
    ProjectAvatars,
    ProjectComponents,
    ProjectEmails,
    ProjectVersions,
    IssueChildResource,
    IssueChildStream,
    IssueComments,
    IssueChangelogs,
    IssueTransitions,
    IssueWorklogs,
    APIRecord,
)

# Jira has documentation stating that its API doesn't provide read-after-write consistency by default.
# https://developer.atlassian.com/cloud/jira/platform/search-and-reconcile/
# They have a specific `reconcileIssues` param that can be used to address that & ensure at max 50 requested
# issues are consistent. However, that has its own drawbacks - we'd likely need to make one request to fetch
# a list of ids, then make 1+ requests to fetch the full issue for those ids. That's a lot more HTTP requests
# & roundtrip latency. For now, we take a simpler approach to avoid eventual consistency - don't request
# issues updated in the most recent 5 minutes.
ISSUE_JQL_SEARCH_LAG = timedelta(minutes=5)
MIN_CHECKPOINT_INTERVAL = 200

MISSING_RESOURCE_TITLE = r"Oops, you&#39;ve found a dead link"
CUSTOM_FIELD_NOT_FOUND = r"The custom field was not found."

ALL_ISSUE_FIELDS = "*all"
MINIMAL_ISSUE_FIELDS = "id,updated"


def dt_to_str(dt: datetime) -> str:
    return dt.isoformat()


def str_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


def url_base(domain: str) -> str:
    return f"https://{domain}/rest/api/3"


def _format_utc_offset(tz: ZoneInfo) -> str:
    dt = datetime.now(tz)
    offset = dt.utcoffset()
    if offset is None:
        return "+00:00"

    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    hours, minutes = divmod(abs(total_minutes), 60)
    return f"{sign}{hours:02}:{minutes:02}"


async def fetch_timezone(
    http: HTTPSession,
    domain: str,
    log: Logger,
) -> ZoneInfo:
    url = f"{url_base(domain)}/myself"

    response = MyselfResponse.model_validate_json(
        await http.request(log, url)
    )

    timezone = ZoneInfo(response.timeZone)

    log.info(f"Using timezone {response.timeZone} ({_format_utc_offset(timezone)}).")

    return timezone


async def snapshot_nested_arrayed_resources(
    http: HTTPSession,
    domain: str,
    path: str,
    extra_params: dict[str, str] | None,
    response_field: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(domain)}/{path}"

    _, body = await http.request_stream(log, url, params=extra_params)
    processor = IncrementalJsonProcessor(
        body(),
        f"{response_field}.item",
        FullRefreshResource,
    )

    async for resource in processor:
        yield resource


async def snapshot_non_paginated_arrayed_resources(
    http: HTTPSession,
    domain: str,
    path: str,
    extra_params: dict[str, str] | None,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(domain)}/{path}"

    resources = TypeAdapter(list[FullRefreshResource]).validate_json(await http.request(log, url, params=extra_params))

    for resource in resources:
        yield resource


async def snapshot_paginated_arrayed_resources(
    http: HTTPSession,
    domain: str,
    path: str,
    extra_params: dict[str, str] | None,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(domain)}/{path}"

    count = 0

    params: dict[str, str | int] = {
        "startAt": count,
        # maxResults is more of a suggested maximum - Jira sometimes sends fewer 
        # than maxResults results in a response depending on the size of the results.
        "maxResults": 100,
    }
    if extra_params:
        params.update(extra_params)

    while True:
        resources = TypeAdapter(list[FullRefreshResource]).validate_json(await http.request(log, url, params=params))

        if len(resources) == 0:
            break

        for resource in resources:
            yield resource
            count += 1

        params["startAt"] = count


async def _paginate_through_resources(
    http: HTTPSession,
    domain: str,
    path: str,
    extra_params: dict[str, str] | None,
    response_model: type[PaginatedResponse],
    log: Logger,
) -> AsyncGenerator[APIRecord, None]:
    url = f"{url_base(domain)}/{path}"

    count = 0

    params: dict[str, str | int] = {
        "startAt": count,
        # maxResults is more of a suggested maximum - Jira sometimes sends fewer 
        # than maxResults results in a response depending on the size of the results.
        "maxResults": 100,
    }
    if extra_params:
        params.update(extra_params)

    while True:
        response = response_model.model_validate_json(
            await http.request(log, url, params=params)
        )

        if not response.values:
            break

        for record in response.values:
            yield record
            count += 1

        if response.isLast or count >= response.total:
            break

        params["startAt"] = count



async def snapshot_paginated_resources(
    http: HTTPSession,
    domain: str,
    path: str,
    extra_params: dict[str, str] | None,
    response_model: type[PaginatedResponse],
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    async for record in _paginate_through_resources(
        http,
        domain,
        path,
        extra_params,
        response_model,
        log
    ):
        yield FullRefreshResource.model_validate(record)


async def snapshot_labels(
    http: HTTPSession,
    domain: str,
    path: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(domain)}/{path}"

    count = 0

    params: dict[str, str | int] = {
        "startAt": count,
        "maxResults": 1000,
    }

    while True:
        response = LabelsResponse.model_validate_json(
            await http.request(log, url, params=params)
        )

        for label in response.values:
            doc = FullRefreshResource.model_validate({
                "label": label,
            })
            yield doc
            count += 1

        if response.isLast:
            break

        params["startAt"] = count


async def snapshot_system_avatars(
    http: HTTPSession,
    domain: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    SYSTEM_AVATAR_TYPES = [
        "project",
        "user",
        "issuetype",
        "priority",
    ]

    for avatar_type in SYSTEM_AVATAR_TYPES:
        url = f"{url_base(domain)}/avatar/{avatar_type}/system"

        response = SystemAvatarsResponse.model_validate_json(
            await http.request(log, url)
        )

        for avatar in response.system:
            yield avatar


async def snapshot_permissions(
    http: HTTPSession,
    domain: str,
    path: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(domain)}/{path}"

    response = PermissionsResponse.model_validate_json(
        await http.request(log, url)
    )

    for permission in response.permissions.values():
        yield FullRefreshResource.model_validate(permission)


async def snapshot_filter_sharing(
    http: HTTPSession,
    domain: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    filter_ids: list[str] = []
    async for filter in snapshot_paginated_resources(http, domain, Filters.path, Filters.extra_params, Filters.response_model, log):
        if filter_id := filter.model_dump().get("id", None):
            assert isinstance(filter_id, str)
            filter_ids.append(filter_id)

    for id in filter_ids:
        path = f"{Filters.path}/{id}/permission"
        try:
            async for doc in snapshot_non_paginated_arrayed_resources(http, domain, path, None, log):
                record = doc.model_dump()
                record["filterId"] = id
                yield FullRefreshResource.model_validate(record)
        except HTTPError as err:
            # If we request filter sharing permissions we don't have access to, Jira will return a 404 page.
            # TODO(bair): is there a way we can determine which filter sharing permissions we have access to before making the request?
            if err.code == 404 and MISSING_RESOURCE_TITLE in err.message:
                continue
            else:
                raise


async def snapshot_issue_custom_field_contexts(
    http: HTTPSession,
    domain: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    issue_field_ids: list[str] = []
    async for issue_field in snapshot_non_paginated_arrayed_resources(http, domain, IssueFields.path, IssueFields.extra_params, log):
        record = issue_field.model_dump()
        is_custom_field: bool | None = record.get("custom", None)
        issue_field_id: str | None = record.get("id", None)
        if is_custom_field and issue_field_id:
            assert isinstance(issue_field_id, str)
            issue_field_ids.append(issue_field_id)

    for id in issue_field_ids:
        path = f"{IssueFields.path}/{id}/context"
        try:
            async for doc in snapshot_paginated_resources(http, domain, path, None, PaginatedResponse, log):
                record = doc.model_dump()
                record["issueFieldId"] = id
                yield FullRefreshResource.model_validate(record)
        except HTTPError as err:
            # Requesting custom fields for "classic" style projects returns a 404.
            # https://community.developer.atlassian.com/t/get-custom-field-contexts-not-found-returned/48408
            # There's not an efficient way to figure out if a field is for a "classic" project.
            if err.code == 404 and CUSTOM_FIELD_NOT_FOUND in err.message:
                continue
            else:
                raise


async def snapshot_issue_custom_field_options(
    http: HTTPSession,
    domain: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    # In each tuple, the first element is the field id and the second element is the context id.
    field_and_context_ids: list[tuple[str, str]] = []
    async for context in snapshot_issue_custom_field_contexts(http, domain, log):
        record = context.model_dump()
        field_id = record.get("issueFieldId", None)
        context_id = record.get("id", None)
        if field_id and context_id:
            assert (isinstance(field_id, str) and isinstance(context_id, str))
            field_and_context_ids.append((field_id, context_id))

    for field_id, context_id in field_and_context_ids:
        path = f"{IssueFields.path}/{field_id}/context/{context_id}/option"
        async for doc in snapshot_paginated_resources(http, domain, path, None, PaginatedResponse, log):
            record = doc.model_dump()
            record["issueFieldId"] = field_id
            record["fieldContextId"] = context_id
            yield FullRefreshResource.model_validate(record)


async def snapshot_screen_tab_fields(
    http: HTTPSession,
    domain: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    # In each tuple, the first element is the screen id and the second element is the tab id.
    screen_and_tab_ids: list[tuple[int, int]] = []
    async for tab in snapshot_paginated_resources(http, domain, ScreenTabs.path, ScreenTabs.extra_params, PaginatedResponse, log):
        record = tab.model_dump()
        screen_id = record.get("screenId", None)
        tab_id = record.get("tabId", None)
        if screen_id is not None and tab_id is not None:
            assert (isinstance(screen_id, int) and isinstance(tab_id, int))
            screen_and_tab_ids.append((screen_id, tab_id))

    for screen_id, tab_id in screen_and_tab_ids:
        path = f"screens/{screen_id}/tabs/{tab_id}/fields"
        async for doc in snapshot_non_paginated_arrayed_resources(http, domain, path, None, log):
            record = doc.model_dump()
            record["screenId"] = screen_id
            record["tabId"] = tab_id
            yield FullRefreshResource.model_validate(record)


async def _fetch_project_avatars(
    http: HTTPSession,
    domain: str,
    project_id: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(domain)}/project/{project_id}/avatars"

    response = ProjectAvatarsResponse.model_validate_json(
        await http.request(log, url)
    )

    for avatar in response.system:
        yield avatar

    for avatar in response.custom:
        yield avatar


async def _fetch_project_email(
    http: HTTPSession,
    domain: str,
    project_id: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{url_base(domain)}/project/{project_id}/email"

    yield FullRefreshResource.model_validate_json(
        await http.request(log, url)
    )


async def _fetch_project_child_resources(
    http: HTTPSession,
    domain: str,
    project_id: str,
    stream: type[ProjectChildStream],
    log: Logger
) -> AsyncGenerator[FullRefreshResource, None]:
    if issubclass(stream, ProjectAvatars):
        gen = _fetch_project_avatars(http, domain, project_id, log)
    elif (issubclass(stream, ProjectComponents) or issubclass(stream, ProjectVersions)):
        path = f"project/{project_id}/{stream.path}"
        gen = snapshot_paginated_resources(http, domain, path, None, PaginatedResponse, log)
    elif issubclass(stream, ProjectEmails):
        gen = _fetch_project_email(http, domain, project_id, log)
    else:
        raise RuntimeError(f"Unknown project child stream {stream.name}.")

    async for doc in gen:
        if stream.add_parent_id_to_documents:
            record = doc.model_dump()
            record["projectId"] = project_id
            doc = FullRefreshResource.model_validate(record)

        yield doc


async def snapshot_project_child_resources(
    http: HTTPSession,
    domain: str,
    stream: type[ProjectChildStream],
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    # Trying to fetch child resources of an archived or deleted project fails, so we only use ids
    # of live projects when snapshotting project child resources.
    extra_params = {
        "status": "live",
    }

    # In each tuple, the first element is the project id and the second element is the project key.
    project_ids: list[str] = []
    async for project in snapshot_paginated_resources(http, domain, Projects.path, extra_params, Projects.response_model, log):
        record = project.model_dump()
        id = record.get("id", None)
        if id:
            assert (isinstance(id, str))
            project_ids.append(id)

    for id in project_ids:
        async for doc in _fetch_project_child_resources(http, domain, id, stream, log):
            yield doc


def _is_within_dst_fallback_window(
    dt: datetime,
    tz: ZoneInfo,
    window: timedelta = timedelta(hours=1),
) -> bool:
    """
    Returns True if the datetime is within `window` of a fallback (DST end)
    transition, and the fallback has not yet occurred (i.e., offset will decrease).
    """
    if dt.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware")

    dt_now = dt.astimezone(tz)
    dt_later = (dt + window).astimezone(tz)

    offset_now = dt_now.utcoffset()
    offset_later = dt_later.utcoffset()
    assert isinstance(offset_later, timedelta) and isinstance(offset_now, timedelta)

    # Return True if the offset will decrease (i.e., fallback *will* happen soon)
    return offset_later < offset_now


def _determine_bounds(
    start: datetime,
    end: datetime,
    timezone: ZoneInfo,
) -> tuple[str, str]:
    # There are a couple quirks with filtering issues by their updated field.
    # - Top level timestamps are in the system default user timezone, not UTC.
    # - Jira filters issues with minute-level granularity.
    # - Jira's timestamps' offsets change during daylight savings time transitions
    #   but Jira does not support timezone offsets in JQL queries.
    JQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M"

    # If the start time is within the hour before a DST fallback transition,
    # adjust it to *just* before that hour begins (e.g., 00:59).
    # This avoids missing results during the "fall back" DST transition hour
    # when clock times repeat. Duplicate results may be returned by the API,
    # but these are filtered out downstream.
    if _is_within_dst_fallback_window(start, timezone):
        start = (start - timedelta(hours=1)).replace(minute=59)

    lower_bound = start.strftime(JQL_DATETIME_FORMAT)
    upper_bound = end.strftime(JQL_DATETIME_FORMAT)

    return (lower_bound, upper_bound)


def _build_jql(
    start: datetime,
    end: datetime,
    timezone: ZoneInfo,
    projects: str | None,
) -> str:
    lower_bound, upper_bound = _determine_bounds(start, end, timezone)

    lower_bound_jql = f"updated >= '{lower_bound}'"
    upper_bound_jql = f"updated <= '{upper_bound}'"

    # If users provided specific projects, only request issues from those projects.
    # Otherwise, omitting the `project in (p1,p2,...)` is the same as requesting
    # issues for all projects.
    if projects:
        projects_jql = f"project in ({projects})"

        jql = " AND ".join([projects_jql, lower_bound_jql, upper_bound_jql])
    else:
        jql = " AND ".join([lower_bound_jql, upper_bound_jql])

    jql += " ORDER BY updated asc"

    return jql


async def _fetch_issues_between(
    http: HTTPSession,
    domain: str,
    log: Logger,
    timezone: ZoneInfo,
    projects: str | None,
    start: datetime,
    end: datetime | None = None,
    # Fetch only the minimal fields by default.
    fields: str = "",
    # Expand nothing by default.
    expand: str = "",
    # Do not yield issues incrementally by default. should_yield_incrementally is used
    # to avoid holding open (and potentially timing out) the issues
    # API response as we send separate API requests to fetch child resources.
    should_yield_incrementally: bool = False,
) -> AsyncGenerator[Issue, None]:
    url = f"{url_base(domain)}/search/jql"

    now = datetime.now(tz=timezone)

    if end is None:
        end = now

    # Reduce end due to eventual consistency.
    end = min(end, now - ISSUE_JQL_SEARCH_LAG)

    if end <= start:
        return

    # Ensure the MINIMAL_ISSUE_FIELDS (id and updated) required for incremental
    # replication are always included.
    if fields and fields != "*all":
        fields = f"{MINIMAL_ISSUE_FIELDS},{fields}"
    else:
        fields = MINIMAL_ISSUE_FIELDS

    params: dict[str, str | int] = {
        "maxResults": 250,
        "jql": _build_jql(start, end, timezone, projects),
        "fields": fields,
    }

    if expand:
        params["expand"] = expand

    while True:
        _, body = await http.request_stream(
            log,
            url,
            # Jira has a similar POST endpoint that should be used if
            # the JQL query param becomes too large to be encoded.
            # The only way that can happen here is if the user provides an
            # enormous number of projects. If we encounter that, we
            # can add support to use the POST endpoint instead.
            method="GET",
            params=params
        )
        processor = IncrementalJsonProcessor(
            body(),
            "issues.item",
            Issue,
            IssuesResponse,
        )

        if should_yield_incrementally:
            async for issue in processor:
                yield issue
        else:
            issues: list[Issue] = []
            async for issue in processor:
                issues.append(issue)

            for issue in issues:
                yield issue

        remainder = processor.get_remainder()

        if remainder.nextPageToken:
            params["nextPageToken"] = remainder.nextPageToken
        else:
            break


async def fetch_issues(
    http: HTTPSession,
    domain: str,
    timezone: ZoneInfo,
    projects: str | None,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Issue | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    last_seen = log_cursor

    count = 0
    async for issue in _fetch_issues_between(
        http,
        domain,
        log,
        timezone,
        projects,
        log_cursor,
        fields=ALL_ISSUE_FIELDS,
        expand="renderedFields,transitions,operations,changelog",
        should_yield_incrementally=True,
    ):
        # Checkpoint previously yielded documents if the
        # current document moves forward in time.
        if (
            last_seen > log_cursor
            and issue.fields.updated > last_seen
            and count >= MIN_CHECKPOINT_INTERVAL
        ):
            yield last_seen
            count = 0

        if issue.fields.updated > log_cursor:
            count += 1
            yield issue

        if issue.fields.updated > last_seen:
            last_seen = issue.fields.updated

    # Emit a final checkpoint if we saw any new documents.
    if last_seen > log_cursor:
        yield last_seen


async def backfill_issues(
    http: HTTPSession,
    domain: str,
    timezone: ZoneInfo,
    projects: str | None,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[JiraResource | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    start = str_to_dt(page)
    last_seen = start

    count = 0
    async for issue in _fetch_issues_between(
        http,
        domain,
        log,
        timezone,
        projects,
        start,
        # Jira filters issues with minute-level granularity, so we add one minute
        # to include all issues updated during the cutoff minute.
        cutoff + timedelta(minutes=1),
        fields=ALL_ISSUE_FIELDS,
        expand="renderedFields,transitions,operations,changelog",
        should_yield_incrementally=True,
    ):
        if issue.fields.updated > cutoff:
            return

        # Checkpoint previously yielded documents if the
        # current document moves forward in time.
        if (
            last_seen > start
            and issue.fields.updated > last_seen
            and count >= MIN_CHECKPOINT_INTERVAL
        ):
            yield dt_to_str(last_seen)
            count = 0

        if issue.fields.updated > start:
            count += 1
            yield issue

        if issue.fields.updated > last_seen:
            last_seen = issue.fields.updated


async def _fetch_comments_or_worklogs_for_issue(
    http: HTTPSession,
    domain: str,
    stream: type[IssueChildStream],
    issue_id: int,
    nested_resources: Issue.Fields.Comment | Issue.Fields.Worklog,
    log: Logger,
) -> AsyncGenerator[APIRecord, None]:
    assert stream.response_model is not None
    if nested_resources.total == 0:
        return

    # If all of the issue's child resources were returned in the issue, then yield them.
    if nested_resources.maxResults >= nested_resources.total:
        for child_resource in nested_resources.resources:
            yield child_resource
    # Otherwise if there are more child resources for this issue than were returned with it,
    # fetch and yield all of them.
    else:
        path = f"issue/{issue_id}/{stream.path}"
        async for child_resource in _paginate_through_resources(
            http,
            domain,
            path,
            None,
            stream.response_model,
            log,
        ):
            yield child_resource


async def _fetch_changelogs_for_issue(
    http: HTTPSession,
    domain: str,
    issue_id: int,
    changelog: Issue.ChangeLog,
    log: Logger,
) -> AsyncGenerator[APIRecord, None]:
    if changelog.total == 0:
        return

    # If all of the issue's changelogs were returned with the issue, then yield them.
    if changelog.maxResults >= changelog.total:
        for record in changelog.histories:
            yield record
    # Otherwise if there are more changlogs for this issue than were returned with it,
    # fetch and yield all of them.
    else:
        path = f"issue/{issue_id}/changelog"
        async for record in _paginate_through_resources(
            http,
            domain,
            path,
            None,
            PaginatedResponse,
            log,
        ):
            yield record


async def _fetch_transitions_for_issue(
    transitions: list[APIRecord],
) -> AsyncGenerator[APIRecord, None]:
    if len(transitions) == 0:
        return

    for transition in transitions:
        yield transition


async def _fetch_child_resources_for_issue(
    http: HTTPSession,
    domain: str,
    stream: type[IssueChildStream],
    issue: Issue,
    log: Logger,
) -> AsyncGenerator[IssueChildResource, None]:
    if issubclass(stream, IssueComments):
        assert issue.fields.comment is not None
        gen = _fetch_comments_or_worklogs_for_issue(http, domain, stream, issue.id, issue.fields.comment, log)
    elif issubclass(stream, IssueWorklogs):
        assert issue.fields.worklog is not None
        gen = _fetch_comments_or_worklogs_for_issue(http, domain, stream, issue.id, issue.fields.worklog, log)
    elif issubclass(stream, IssueChangelogs):
        assert issue.changelog is not None
        gen = _fetch_changelogs_for_issue(http, domain, issue.id, issue.changelog, log)
    elif issubclass(stream, IssueTransitions):
        assert issue.transitions is not None
        gen = _fetch_transitions_for_issue(issue.transitions)
    else:
        raise RuntimeError(f"Unknown issues child stream type {stream.__name__} for stream {stream.name}")

    async for record in gen:
        record["issueId"] = issue.id
        yield IssueChildResource.model_validate(record)


async def fetch_issues_child_resources(
    http: HTTPSession,
    domain: str,
    timezone: ZoneInfo,
    projects: str | None,
    stream: type[IssueChildStream],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[IssueChildResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    last_seen = log_cursor

    count = 0
    async for issue in _fetch_issues_between(
        http,
        domain,
        log,
        timezone,
        projects,
        log_cursor,
        fields=stream.fields,
        expand=stream.expand,
    ):
        # Checkpoint previously yielded documents if the
        # current document moves forward in time.
        if (
            last_seen > log_cursor
            and issue.fields.updated > last_seen
            and count >= MIN_CHECKPOINT_INTERVAL
        ):
            yield last_seen
            count = 0

        if issue.fields.updated > log_cursor:
            count += 1
            async for child_resource in _fetch_child_resources_for_issue(http, domain, stream, issue, log):
                yield child_resource

        if issue.fields.updated > last_seen:
            last_seen = issue.fields.updated

    # Emit a final checkpoint if we saw any new documents.
    if last_seen > log_cursor:
        yield last_seen


# NEED TO ADD A BACKFILL FUNCTION FOR CHILD RESOURCES!!!!!!!!!!!!!

async def backfill_issues_child_resources(
    http: HTTPSession,
    domain: str,
    timezone: ZoneInfo,
    projects: str | None,
    stream: type[IssueChildStream],
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[IssueChildResource | PageCursor, None]:
    assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    start = str_to_dt(page)
    last_seen = start

    count = 0
    async for issue in _fetch_issues_between(
        http,
        domain,
        log,
        timezone,
        projects,
        start,
        # Jira filters issues with minute-level granularity, so we add one minute
        # to include all issues updated during the cutoff minute.
        cutoff + timedelta(minutes=1),
        fields=stream.fields,
        expand=stream.expand,
    ):
        if issue.fields.updated > cutoff:
            return

        # Checkpoint previously yielded documents if the
        # current document moves forward in time.
        if (
            last_seen > start
            and issue.fields.updated > last_seen
            and count >= MIN_CHECKPOINT_INTERVAL
        ):
            yield dt_to_str(last_seen)
            count = 0

        if issue.fields.updated > start:
            count += 1
            async for child_resource in _fetch_child_resources_for_issue(http, domain, stream, issue, log):
                yield child_resource

        if issue.fields.updated > last_seen:
            last_seen = issue.fields.updated