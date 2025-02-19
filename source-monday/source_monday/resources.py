from datetime import datetime, timedelta, UTC
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource
from estuary_cdk.capture.common import ResourceConfig

from source_monday.models import (
    EndpointConfig,
    OAUTH2_SPEC,
    ResourceState,
    FullRefreshResource,
    IncrementalResource,
    FullRefreshResourceFetchFn,
    IncrementalResourceFetchChangesFn,
    IncrementalResourceFetchPageFn,
)
from source_monday.api import (
    fetch_boards_changes,
    fetch_boards_page,
    fetch_items_changes,
    fetch_items_page,
    snapshot_teams,
    snapshot_users,
    snapshot_tags,
)
from source_monday.graphql import API


# Supported full refresh resources and their corresponding name and snapshot function.
FULL_REFRESH_RESOURCES = [
    ("teams", snapshot_teams),
    ("users", snapshot_users),
    ("tags", snapshot_tags),
]


# Supported incremental resources and their corresponding name, fetch_changes function, and fetch_page function.
INCREMENTAL_RESOURCES = [
    ("boards", fetch_boards_changes, fetch_boards_page),
    ("items", fetch_items_changes, fetch_items_page),
]


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """
    Checks if the provided client credentials belong to a valid OAuth app.
    """
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    try:
        await http.request(log, API, method="POST", json={"query": "query {me {id}}"})
    except HTTPError as err:
        msg = "Unknown error occurred."
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def full_refresh_resouces(log: Logger, http: HTTPMixin, config: EndpointConfig):
    def open(
        fetch_snapshot_fn: FullRefreshResourceFetchFn,
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
            fetch_snapshot=functools.partial(fetch_snapshot_fn, http),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d")),
        )

    return [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, fetch_snapshot_fn),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(name=name, interval=timedelta(seconds=30)),
            schema_inference=True,
        )
        for name, fetch_snapshot_fn in FULL_REFRESH_RESOURCES
    ]


def incremental_resources(log: Logger, http: HTTPMixin, config: EndpointConfig):
    def open(
        fetch_changes_fn: IncrementalResourceFetchChangesFn,
        fetch_page_fn: IncrementalResourceFetchPageFn,
        limit: int,
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
            fetch_changes=functools.partial(fetch_changes_fn, http, limit),
            fetch_page=functools.partial(fetch_page_fn, http, limit),
        )

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    return [
        common.Resource(
            name=name,
            key=["/id"],
            model=IncrementalResource,
            open=functools.partial(
                open,
                fetch_changes_fn,
                fetch_page_fn,
                config.advanced.limit,
            ),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=1),
            ),
            initial_config=ResourceConfig(name=name, interval=timedelta(seconds=30)),
            schema_inference=True,
        )
        for name, fetch_changes_fn, fetch_page_fn in INCREMENTAL_RESOURCES
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    return [
        *full_refresh_resouces(log, http, config),
        *incremental_resources(log, http, config),
    ]
