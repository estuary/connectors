import functools
from datetime import timedelta, datetime, UTC
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    Resource,
    ResourceConfig,
    ResourceState,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin, TokenSource

from .api import fetch_bookings_page, fetch_bookings_changes
from .models import Booking, EndpointConfig, OAUTH2_SPEC


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    def open_bookings(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(
                fetch_bookings_page,
                http,
            ),
            fetch_changes=functools.partial(
                fetch_bookings_changes,
                http,
            ),
        )

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    return [
        Resource(
            name="bookings",
            key=["/uuid"],
            model=Booking,
            open=open_bookings,
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff),
            ),
            initial_config=ResourceConfig(
                name="bookings",
                interval=timedelta(minutes=5),
            ),
            schema_inference=True,
        ),
    ]
