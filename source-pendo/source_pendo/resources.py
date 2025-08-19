from datetime import timedelta, datetime, UTC
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError


from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    Event,
    EventAggregate,
    FullRefreshResource,
    IncrementalResource,
    Metadata,
    FULL_REFRESH_RESOURCE_TYPES,
    INCREMENTAL_RESOURCE_TYPES,
    EVENT_TYPES,
    AGGREGATED_EVENT_TYPES,
    METADATA_TYPES,
)
from .api import (
    snapshot_resources,
    backfill_resources,
    fetch_resources,
    backfill_events,
    fetch_events,
    backfill_aggregated_events,
    fetch_aggregated_events,
    snapshot_metadata,
    _dt_to_ms,
    API,
    API_EVENT_LAG,
)


AUTHORIZATION_HEADER = "x-pendo-integration-key"


async def validate_api_key(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials, authorization_header=AUTHORIZATION_HEADER)
    url = f"{API}/metadata/schema/AccountMetadata"

    try:
        await http.request(log, url)
    except HTTPError as err:
        if err.code == 403:
            msg = "Invalid API key. Please confirm the provided API key is correct."
            raise ValidationError([msg])
        else:
            raise err


def full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        entity: str,
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
                snapshot_resources,
                http,
                entity,
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    resources = [
        common.Resource(
            name=stream.resource_name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, stream.entity_name),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=stream.resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for stream in FULL_REFRESH_RESOURCE_TYPES
    ]

    return resources


def incremental_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    def open(
        entity: str,
        model: type[IncrementalResource],
        updated_at_field: str,
        identifying_field: str,
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
            fetch_changes=functools.partial(
                fetch_resources,
                http,
                entity,
                model,
                updated_at_field,
                identifying_field,
            ),
            fetch_page=functools.partial(
                backfill_resources,
                http,
                entity,
                model,
                updated_at_field,
                identifying_field,
            )
        )

    backfill_start_ts = _dt_to_ms(datetime.fromisoformat(config.startDate))
    cutoff = datetime.now(tz=UTC) - API_EVENT_LAG

    resources = [
        common.Resource(
            name=stream.resource_name,
            key=sorted([f"/{key}" for key in stream.primary_keys]),
            model=stream,
            open=functools.partial(open, stream.entity_name, stream, stream.cursor_field, stream.identifying_field),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(next_page=backfill_start_ts, cutoff=cutoff)
            ),
            initial_config=ResourceConfig(
                name=stream.resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for stream in INCREMENTAL_RESOURCE_TYPES
    ]

    return resources


def metadata(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        entity: str,
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
                snapshot_metadata,
                http,
                entity,
            ),
            tombstone=Metadata(_meta=Metadata.Meta(op="d"))
        )

    metadata = [
        common.Resource(
            name=stream.resource_name,
            key=["/_meta/row_id"],
            model=Metadata,
            open=functools.partial(open, stream.path),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=stream.resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for stream in METADATA_TYPES
    ]

    return metadata


def events(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    def open(
        entity: str,
        model: type[Event],
        identifying_field: str,
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
            fetch_changes=functools.partial(
                fetch_events,
                http,
                entity,
                model,
                identifying_field,
            ),
            fetch_page=functools.partial(
                backfill_events,
                http,
                entity,
                model,
                identifying_field,
            )
        )

    backfill_start_ts = _dt_to_ms(datetime.fromisoformat(config.startDate))
    cutoff = datetime.now(tz=UTC) - API_EVENT_LAG

    shared_keys = [
        "/accountId",
        "/appId",
        "/guideTimestamp",
        "/remoteIp",
        "/serverName",
        "/visitorId",
        "/userAgent",
    ]

    events = [
        common.Resource(
            name=stream.resource_name,
            key=sorted(shared_keys + [f"/{key}" for key in stream.primary_keys]),
            model=stream,
            open=functools.partial(open, stream.entity_name, stream, stream.identifying_field),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(next_page=backfill_start_ts, cutoff=cutoff)
            ),
            initial_config=ResourceConfig(
                name=stream.resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for stream in EVENT_TYPES
    ]

    return events


def aggregated_events(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    def open(
        entity: str,
        model: type[EventAggregate],
        identifying_field: str,
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
            fetch_changes=functools.partial(
                fetch_aggregated_events,
                http,
                entity,
                model,
                identifying_field,
            ),
            fetch_page=functools.partial(
                backfill_aggregated_events,
                http,
                entity,
                model,
                identifying_field,
            )
        )

    backfill_start_ts = _dt_to_ms(datetime.fromisoformat(config.startDate))
    cutoff = datetime.now(tz=UTC) - API_EVENT_LAG 

    shared_keys = [
        "/accountId",
        "/appId",
        "/firstTime",
        "/lastTime",
        "/remoteIp",
        "/server",
        "/visitorId",
        "/userAgent",
    ]

    events = [
        common.Resource(
            name=stream.resource_name,
            key=sorted(shared_keys + [f"/{key}" for key in stream.primary_keys]),
            model=stream,
            open=functools.partial(open, stream.entity_name, stream, stream.identifying_field),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(next_page=backfill_start_ts, cutoff=cutoff)
            ),
            initial_config=ResourceConfig(
                name=stream.resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for stream in AGGREGATED_EVENT_TYPES
    ]

    return events


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials, authorization_header=AUTHORIZATION_HEADER)

    return [
        *full_refresh_resources(log, http, config),
        *incremental_resources(log, http, config),
        *metadata(log, http, config), 
        *events(log, http, config),
        *aggregated_events(log, http, config),
    ]
