import functools
from collections.abc import AsyncGenerator, Callable
from logging import Logger

from estuary_cdk.capture import common
from estuary_cdk.capture.common import (
    ResourceConfig,
    ResourceState,
    SnapshotResource,
    Task,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, HTTPSession, TokenSource

from .api import (
    AUTHORIZATION_HEADER,
    fetch_self,
    snapshot_calendars,
    snapshot_events,
    snapshot_guests,
    snapshot_list_resource,
)
from .models import (
    Calendar,
    EndpointConfig,
    Event,
    Guest,
    LumaResource,
    MembershipTier,
)

type SnapshotFn = Callable[[HTTPSession, Logger], AsyncGenerator[LumaResource, None]]
type LumaCaptureResource = common.Resource[LumaResource, ResourceConfig, ResourceState]


def _token_source(config: EndpointConfig) -> TokenSource:
    return TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured API key authenticates against Luma."""
    http.token_source = _token_source(config)

    try:
        _ = await fetch_self(http, log)
    except HTTPError as err:
        msg = "Unknown error occurred."
        if err.code == 401:
            msg = f"Invalid API key. Please confirm the provided API key is correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating API key.\n\n{err.message}"

        raise ValidationError([msg])


def _snapshot_resource(
    model: type[LumaResource], snapshot_fn: SnapshotFn, http: HTTPMixin
) -> LumaCaptureResource:
    def open(
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
            fetch_snapshot=functools.partial(snapshot_fn, http),
        )

    return SnapshotResource(
        name=model.NAME,
        open=open,
        initial_config=ResourceConfig(name=model.NAME, interval=model.INTERVAL),
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[LumaCaptureResource]:
    """Enumerate every stream the connector exposes. Discovery is static: no
    provider call is made here."""
    if http.token_source is None:
        http.token_source = _token_source(config)

    return [
        _snapshot_resource(Calendar, snapshot_calendars, http),
        _snapshot_resource(
            MembershipTier,
            functools.partial(snapshot_list_resource, MembershipTier),
            http,
        ),
        # `events` and `guests` share the list engine but need their own
        # snapshot functions: events sweeps two `status` values and guests
        # fans out per event.
        _snapshot_resource(Event, snapshot_events, http),
        _snapshot_resource(Guest, snapshot_guests, http),
    ]
