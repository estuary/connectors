import calendar
from datetime import timedelta
import functools
from logging import Logger
from typing import AsyncGenerator
import pendulum

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin


from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    Resource,
    Event,
    Metadata,
    RESOURCE_TYPES,
    EVENT_TYPES,
    METADATA_TYPES,
)
from .api import (
    fetch_resources,
    fetch_events,
    fetch_metadata,
)

def resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    async def snapshot(http: HTTPMixin, entity: str, log: Logger) -> AsyncGenerator[Metadata, None]:
        resources = await fetch_resources(http=http, entity=entity, log=log)
        for resource in resources:
            yield resource

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
                snapshot,
                http,
                entity,
            ),
            tombstone=Resource(_meta=Resource.Meta(op="d"))
        )

    resources = [
        common.Resource(
            name=resource_name,
            key=["/id"],
            model=Resource,
            open=functools.partial(open, entity),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=resource_name, interval=timedelta(seconds=30)
            ),
            schema_inference=True,
        )
        for (entity, resource_name) in RESOURCE_TYPES
    ]

    return resources

def metadata(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    async def snapshot(http: HTTPMixin, entity: str, log: Logger) -> AsyncGenerator[Metadata, None]:
        metadata = await fetch_metadata(http=http, entity=entity, log=log)
        for m in metadata:
            yield m

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
                snapshot,
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
                name=resource_name, interval=timedelta(seconds=30)
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
        timestamp_field: str,
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
                timestamp_field
            )
        )

    start_date = config.startDate
    # Timestamp must be in milliseconds. start_date is in seconds, so we multiply by 1000.
    start_date_timestamp = calendar.timegm(pendulum.parse(start_date).utctimetuple()) * 1000 

    events = [
        common.Resource(
            name=resource_name,
            key=["/appId", "/hour", "/remoteIp", identifying_field],
            model=Event,
            open=functools.partial(open, entity, timestamp_field),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=start_date_timestamp)
            ),
            initial_config=ResourceConfig(
                name=resource_name, interval=timedelta(seconds=30)
            ),
            schema_inference=True,
        )
        for (entity, resource_name, identifying_field, timestamp_field) in EVENT_TYPES
    ]

    return events

async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.auth_headers = {"x-pendo-integration-key": config.apiToken}

    return [
        *resources(log, http, config), 
        *metadata(log, http, config), 
        *events(log, http, config),
    ]
