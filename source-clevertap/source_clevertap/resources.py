import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin

from .api import fetch_changes, noop_fetch
from .models import (
    EVENTS,
    PROFILE,
    EndpointConfig,
    EventsData,
    ProfileData,
    ResourceConfig,
    ResourceState,
)


def _start_cursor(config: EndpointConfig) -> datetime:
    if config.advanced.start_date:
        return datetime.strptime(config.advanced.start_date, "%Y-%m-%d").replace(tzinfo=UTC)
    return datetime.now(tz=UTC) - timedelta(days=2)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    start = _start_cursor(config)

    def open_events(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # Locate the profile_data binding so we can route ProfileData docs to it.
        profile_index = next(
            (i for i, (_, r) in enumerate(all_bindings) if r.name == PROFILE), None
        )
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_changes, http, config, profile_index),
        )

    def open_profile(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding, binding_index, state, task, fetch_changes=noop_fetch
        )

    return [
        common.Resource(
            name=EVENTS,
            key=["/event_id"],
            model=EventsData,
            open=open_events,
            initial_state=ResourceState(inc=ResourceState.Incremental(cursor=start)),
            initial_config=ResourceConfig(name=EVENTS, interval=timedelta(days=1)),
            schema_inference=True,
        ),
        common.Resource(
            name=PROFILE,
            key=["/clevertapId"],
            model=ProfileData,
            open=open_profile,
            initial_state=ResourceState(inc=ResourceState.Incremental(cursor=start)),
            initial_config=ResourceConfig(name=PROFILE, interval=timedelta(days=1)),
            schema_inference=True,
        ),
    ]
