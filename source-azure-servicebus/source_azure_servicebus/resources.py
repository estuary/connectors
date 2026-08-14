import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin

from .api import fetch_changes
from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    ServiceBusLog,
)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    entity = config.entity_name()

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
            fetch_changes=functools.partial(fetch_changes, config),
        )

    return [
        common.Resource(
            name=entity,
            key=["/sequence_number"],
            model=ServiceBusLog,
            open=open,
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC)),
            ),
            # 15-minute poll: after the entity is drained, wait ~15m before the
            # next sweep. When messages are flowing, batches re-invoke immediately.
            initial_config=ResourceConfig(
                name=entity, interval=timedelta(minutes=15)
            ),
            schema_inference=True,
        )
    ]
