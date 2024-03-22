from datetime import UTC, datetime, timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, BasicAuth

from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    Event,
    EVENTS_ENTITY_TYPES,
)
from .api import (
    fetch_events,
)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=BasicAuth(
            username=config.agentEmail,
            password=config.apiToken,
        ),
    )

    def open(
        entity: str,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_events,
                http,
                config,
                entity,
            ),
        )

    # The Gladly Events API allows looking back within the last 24 hours only and does not support
    # backfilling. The offset of 23 hours means we won't try to immediately request data outside of
    # the allowable 24 hour window while still making a reasonable effort to grab as much historical
    # data as possible. This also means that the connector will have 1 hour to advance the cursor
    # after its initial invocation, which generally shouldn't be a problem.
    started_at = datetime.now(tz=UTC) - timedelta(hours=23)

    return [
        common.Resource(
            name=resource_name,
            key=["/id"],
            model=Event,
            open=functools.partial(open, entity),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=started_at),
            ),
            initial_config=ResourceConfig(
                name=resource_name, interval=timedelta(seconds=30)
            ),
            schema_inference=True,
        )
        for (entity, resource_name) in EVENTS_ENTITY_TYPES
    ]
