from datetime import datetime, timedelta, UTC
import functools
from estuary_cdk.logger import FlowLogger
from logging import Logger

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource

from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    Resource,
)
from .api import (
    fetch_resources,
)


def incremental_resources(
        log: FlowLogger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

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
                fetch_resources,
                http,
                config,
            ),
        )

    cutoff = datetime.now(tz=UTC) - timedelta(minutes=5)

    resources = [
            common.Resource(
            name="config_update_events",
            key=["/id"],
            model=Resource,
            open=open,
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
            ),
            initial_config=ResourceConfig(
                name="config_update_events", interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
    ]

    return resources


async def all_resources(
    log: FlowLogger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    resources = [
        *incremental_resources(log, http, config),
    ]

    return resources
