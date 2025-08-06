import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import ResourceConfig
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from source_monday.api import (
    fetch_boards_changes,
    fetch_boards_page,
    fetch_items_changes,
    fetch_items_page,
    snapshot_resource,
    snapshot_resource_paginated,
    INCREMENTAL_SYNC_DELAY,
)
from source_monday.graphql import (
    API,
    API_VERSION,
    TAGS,
    TEAMS,
    USERS,
)
from source_monday.models import (
    EndpointConfig,
    FullRefreshResource,
    IncrementalResource,
    IncrementalResourceFetchChangesFn,
    IncrementalResourceFetchPageFn,
    FullRefreshResourceFetchSnapshotFn,
    ResourceState,
)

FULL_REFRESH_RESOURCES: list[tuple[str, str, FullRefreshResourceFetchSnapshotFn]] = [
    ("teams", TEAMS, snapshot_resource),
    ("users", USERS, snapshot_resource_paginated),
    ("tags", TAGS, snapshot_resource),
]


INCREMENTAL_RESOURCES: list[
    tuple[str, IncrementalResourceFetchChangesFn, IncrementalResourceFetchPageFn]
] = [
    ("boards", fetch_boards_changes, fetch_boards_page),
    ("items", fetch_items_changes, fetch_items_page),
]


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    try:
        await http.request(
            log,
            API,
            method="POST",
            headers={"API-Version": API_VERSION},
            json={"query": "query {me {id}}"},
        )
    except HTTPError as err:
        msg = "Unknown error occurred."
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def full_refresh_resources(log: Logger, http: HTTPMixin, config: EndpointConfig):
    def open(
        name: str,
        query: str,
        snapshot_fn: FullRefreshResourceFetchSnapshotFn,
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
            fetch_snapshot=functools.partial(snapshot_fn, http, name, query),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d")),
        )

    return [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, name, query, snapshot_fn),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(name=name, interval=timedelta(hours=2)),
            schema_inference=True,
        )
        for name, query, snapshot_fn in FULL_REFRESH_RESOURCES
    ]


def incremental_resources(log: Logger, http: HTTPMixin, config: EndpointConfig):
    cutoff = datetime.now(tz=UTC).replace(microsecond=0) - INCREMENTAL_SYNC_DELAY

    def open(
        name: str,
        fetch_changes_fn: IncrementalResourceFetchChangesFn,
        fetch_page_fn: IncrementalResourceFetchPageFn,
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
            fetch_changes=functools.partial(fetch_changes_fn, http),
            fetch_page=functools.partial(fetch_page_fn, http),
        )

    return [
        common.Resource(
            name=name,
            key=["/id"],
            model=IncrementalResource,
            open=functools.partial(open, name, fetch_changes_fn, fetch_page_fn),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
            ),
            initial_config=ResourceConfig(name=name, interval=timedelta(hours=1)),
            schema_inference=True,
        )
        for name, fetch_changes_fn, fetch_page_fn in INCREMENTAL_RESOURCES
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    return [
        *full_refresh_resources(log, http, config),
        *incremental_resources(log, http, config),
    ]
