import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import ResourceConfig
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, HTTPSession, TokenSource

from source_monday.api import (
    fetch_boards_changes,
    fetch_boards_page,
    fetch_items_changes,
    fetch_items_page,
    snapshot_resource,
)
from source_monday.graphql import API, API_VERSION, TAGS, TEAMS, USERS
from source_monday.models import (
    EndpointConfig,
    FullRefreshResource,
    IncrementalResource,
    IncrementalResourceFetchChangesFn,
    AnyFetchPageFn,
    ResourceState,
)
from source_monday.graphql.items.item_cache import ItemCacheSession

FULL_REFRESH_RESOURCES: list[tuple[str, str]] = [
    ("teams", TEAMS),
    ("users", USERS),
    ("tags", TAGS),
]


INCREMENTAL_RESOURCES: list[
    tuple[str, IncrementalResourceFetchChangesFn, AnyFetchPageFn]
] = [
    ("boards", fetch_boards_changes, fetch_boards_page),
    ("items", fetch_items_changes, fetch_items_page),
]

_item_cache_session: ItemCacheSession | None = None



async def _get_or_create_item_cache_session(
    http: HTTPSession, log: Logger, cutoff: datetime
) -> ItemCacheSession:
    global _item_cache_session

    if _item_cache_session is None or _item_cache_session.cutoff != cutoff:
        log.debug(f"Creating new item cache session for cutoff: {cutoff}")
        _item_cache_session = ItemCacheSession(
            http=http,
            log=log,
            cutoff=cutoff,
            boards_batch_size=100,
            items_batch_size=500,
        )
        await _item_cache_session.initialize()

    return _item_cache_session


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
            fetch_snapshot=functools.partial(snapshot_resource, http, name, query),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d")),
        )

    return [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, name, query),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(name=name, interval=timedelta(hours=2)),
            schema_inference=True,
        )
        for name, query in FULL_REFRESH_RESOURCES
    ]


async def incremental_resources(log: Logger, http: HTTPMixin, config: EndpointConfig):
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)
    cache_session = await _get_or_create_item_cache_session(http, log, cutoff)

    def open(
        name: str,
        fetch_changes_fn: IncrementalResourceFetchChangesFn,
        fetch_page_fn: AnyFetchPageFn,
        cache_session: ItemCacheSession,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        if name == "items":
            fetch_page_partial = functools.partial(fetch_page_fn, http, cache_session)
        else:
            fetch_page_partial = functools.partial(fetch_page_fn, http)
        
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_changes_fn, http),
            fetch_page=fetch_page_partial,
        )

    return [
        common.Resource(
            name=name,
            key=["/id"],
            model=IncrementalResource,
            open=functools.partial(open, name, fetch_changes_fn, fetch_page_fn, cache_session),
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
        *await incremental_resources(log, http, config),
    ]
