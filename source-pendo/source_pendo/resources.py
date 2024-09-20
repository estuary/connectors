from datetime import timedelta, datetime
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource


from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    Resource,
    Metadata,
    RESOURCE_TYPES,
    EVENT_TYPES,
    AGGREGATED_EVENT_TYPES,
    METADATA_TYPES,
)
from .api import (
    fetch_resources,
    fetch_events,
    fetch_aggregated_events,
    fetch_metadata,
)


def resources(
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
                fetch_resources,
                http,
                entity,
            ),
            tombstone=Resource(_meta=Resource.Meta(op="d"))
        )

    resources = [
        common.Resource(
            name=resource_name,
            key=["/_meta/row_id"],
            model=Resource,
            open=functools.partial(open, entity),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (entity, resource_name) in RESOURCE_TYPES
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
                fetch_metadata,
                http,
                entity,
            ),
            tombstone=Metadata(_meta=Metadata.Meta(op="d"))
        )

    metadata = [
        common.Resource(
            name=resource_name,
            key=["/_meta/row_id"],
            model=Metadata,
            open=functools.partial(open, entity),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (entity, resource_name) in METADATA_TYPES
    ]

    return metadata


def events(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    def open(
        entity: str,
        model: type[common.BaseDocument],
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
            )
        )

    events = [
        common.Resource(
            name=resource_name,
            key=["/appId", "/guideTimestamp", "/remoteIp", identifying_field],
            model=model,
            open=functools.partial(open, entity, model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.startDate) # type: ignore
            ),
            initial_config=ResourceConfig(
                name=resource_name, interval=timedelta(seconds=0)
            ),
            schema_inference=True,
        )
        for (entity, resource_name, identifying_field, model) in EVENT_TYPES
    ]

    return events


def aggregated_events(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    def open(
        entity: str,
        model: type[common.BaseDocument],
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
            )
        )

    events = [
        common.Resource(
            name=resource_name,
            key=["/appId", "/hour", "/remoteIp", identifying_field],
            model=model,
            open=functools.partial(open, entity, model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=config.startDate) # type: ignore
            ),
            initial_config=ResourceConfig(
                name=resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (entity, resource_name, identifying_field, model) in AGGREGATED_EVENT_TYPES
    ]

    return events


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials, authorization_header="x-pendo-integration-key")

    return [
        *resources(log, http, config), 
        *metadata(log, http, config), 
        *events(log, http, config),
        *aggregated_events(log, http, config),
    ]
