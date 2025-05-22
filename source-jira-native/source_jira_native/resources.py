from datetime import datetime, timedelta
import functools
from logging import Logger
from typing import Any
from zoneinfo import ZoneInfo

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    EndpointConfig,
    FullRefreshResource,
    ResourceConfig,
    ResourceState,
    JiraResource,
    FULL_REFRESH_ARRAYED_RESOURCES,
    FULL_REFRESH_PAGINATED_ARRAYED_RESOURCES,
    FULL_REFRESH_PAGINATED_RESOURCES,
    FullRefreshFn,
)
from .api import (
    fetch_timezone,
    fetch_issues,
    backfill_issues,
    snapshot_non_paginated_arrayed_resources,
    snapshot_paginated_arrayed_resources,
    snapshot_paginated_resources,
    url_base,
    dt_to_str,
    ISSUE_JQL_SEARCH_LAG,
)


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    url = f"{url_base(config.domain)}/myself"

    try:
        await http.request(log, url)
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            snapshot_fn: FullRefreshFn,
            path: str,
            params: dict[str, str] | None,
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
            fetch_snapshot=functools.partial(
                snapshot_fn,
                http,
                config.domain,
                path,
                params,
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    resources: list[common.Resource] = []

    for resources_list, snapshot_fn in [
        (FULL_REFRESH_ARRAYED_RESOURCES, snapshot_non_paginated_arrayed_resources),
        (FULL_REFRESH_PAGINATED_ARRAYED_RESOURCES, snapshot_paginated_arrayed_resources),
        (FULL_REFRESH_PAGINATED_RESOURCES, snapshot_paginated_resources),
    ]:
        for name, path, params in resources_list:
            resources.append(
                common.Resource(
                    name=name,
                    key=["/_meta/row_id"],
                    model=FullRefreshResource,
                    open=functools.partial(open, snapshot_fn, path, params),
                    initial_state=ResourceState(),
                    initial_config=ResourceConfig(
                        name=name, interval=timedelta(minutes=60)
                    ),
                    schema_inference=True,
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
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig, should_fetch_timezone: bool = True
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    timezone = await fetch_timezone(http, config.domain, log) if should_fetch_timezone else ZoneInfo("UTC")

    resources = [
        *full_refresh_resources(log, http, config),
        issues(log, http, config, timezone),
    ]

    return resources
