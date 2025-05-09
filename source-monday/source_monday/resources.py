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
    snapshot_tags,
    snapshot_teams,
    snapshot_users,
)
from source_monday.graphql import API
from source_monday.models import (
    EndpointConfig,
    FullRefreshResourceFetchFn,
    IncrementalResourceFetchChangesFn,
    IncrementalResourceFetchPageFn,
    ResourceState,
    FullRefreshResource,
    IncrementalResource,
)

FULL_REFRESH_RESOURCES: list[tuple[str, FullRefreshResourceFetchFn]] = [
    ("teams", snapshot_teams),
    ("users", snapshot_users),
    ("tags", snapshot_tags),
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
        await http.request(log, API, method="POST", json={"query": "query {me {id}}"})
    except HTTPError as err:
        msg = "Unknown error occurred."
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def full_refresh_resources(log: Logger, http: HTTPMixin, config: EndpointConfig):
    def open(
        fetch_snapshot_fn: FullRefreshResourceFetchFn,
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
            fetch_snapshot=functools.partial(fetch_snapshot_fn, http, limit),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d")),
        )

    return [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, fetch_snapshot_fn, config.advanced.limit),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(name=name, interval=timedelta(minutes=5)),
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
                open, fetch_changes_fn, fetch_page_fn, config.advanced.limit
            ),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=1),
            ),
            initial_config=ResourceConfig(name=name, interval=timedelta(minutes=5)),
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
