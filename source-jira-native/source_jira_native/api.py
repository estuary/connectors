from datetime import datetime, timedelta
from logging import Logger
from typing import AsyncGenerator
from zoneinfo import ZoneInfo

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from pydantic import TypeAdapter

from .models import (
    FullRefreshResource,
    PaginatedResponse,
    JiraResource,
    Issue,
    IssuesResponse,
    MyselfResponse,
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


async def snapshot_paginated_resources(
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
        response = PaginatedResponse.model_validate_json(
            await http.request(log, url, params=params)
        )

        for resource in response.values:
            yield resource
            count += 1

        if response.isLast:
            break

        params["startAt"] = count


def _build_jql(
    start: datetime,
    end: datetime,
    projects: str | None,
) -> str:
    # There are a couple quirks with filtering issues by their updated field.
    # - Top level timestamps are in the system default user timezone, not UTC.
    # - Jira filters issues with minute-level granularity.
    JQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M"

    lower_bound = start.strftime(JQL_DATETIME_FORMAT)
    upper_bound = end.strftime(JQL_DATETIME_FORMAT)

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
    should_fetch_minimal_fields: bool = False,
) -> AsyncGenerator[Issue, None]:
    url = f"{url_base(domain)}/search/jql"

    now = datetime.now(tz=timezone)

    if end is None:
        end = now

    # Reduce end due to eventual consistency.
    end = min(end, now - ISSUE_JQL_SEARCH_LAG)

    if end <= start:
        return

    params: dict[str, str | int] = {
        "maxResults": 250,
        "jql": _build_jql(start, end, projects)
    }

    if should_fetch_minimal_fields:
        params["fields"] = "id,updated"
    else:
        params["fields"] = "*all"
        params["expand"] = "renderedFields,transitions,operations,editmeta,changelog"

    while True:
        response = IssuesResponse.model_validate_json(
            await http.request(
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
        )

        for issue in response.issues:
            yield issue

        if response.nextPageToken:
            params["nextPageToken"] = response.nextPageToken
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
