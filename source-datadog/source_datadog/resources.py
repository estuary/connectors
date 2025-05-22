from datetime import UTC, datetime, timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin

from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    RUMEvent,
    RUM_EVENTS_STREAM,
)
from .api import fetch_rum_events

async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Define all available resources for the Datadog connector."""
    
    def open(
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
                fetch_rum_events,
                http,
                config,
            ),
        )

    # Start from 1 hour ago to ensure we don't miss any events
    started_at = datetime.now(tz=UTC) - timedelta(hours=1)

    return [
        common.Resource(
            name=RUM_EVENTS_STREAM,
            key=["/id"],
            model=RUMEvent,
            open=open,
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=started_at),
            ),
            initial_config=ResourceConfig(
                name=RUM_EVENTS_STREAM,
                interval=timedelta(seconds=30)  # Poll every 30 seconds
            ),
            schema_inference=True,
        )
    ] 