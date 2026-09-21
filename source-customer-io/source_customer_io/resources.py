import functools
from datetime import datetime, timedelta, UTC
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.capture.common import (
    ResourceConfig,
    SnapshotResource,
    open_binding,
)
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    CONFIG_OBJECTS,
    PAGINATED_OBJECTS,
    ConfigObject,
    ConnectorState,
    Delivery,
    EndpointConfig,
    PaginatedObject,
    ResourceConfigWithSchedule,
    ResourceState,
)
from .api import (
    backfill_deliveries,
    backfill_floor,
    base_url,
    fetch_deliveries,
    snapshot_config_objects,
    snapshot_paginated_objects,
)

# Tails new deliveries; RESCAN trails it by the configured window to re-read the
# same deliveries once their engagement metrics have settled.
REALTIME = "realtime"
RESCAN = "rescan"

DEFAULT_SNAPSHOT_INTERVAL = timedelta(minutes=5)

# A snapshot buffers its whole result set to sort it, and opt-outs are the only
# resource here that scales with the profile base rather than with how much a
# workspace has been configured. It is also opt-out state, which does not need
# minute-level freshness.
SNAPSHOT_INTERVALS = {"optouts": timedelta(hours=1)}


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """
    Validates that the configured App API Key authenticates against Customer.io.

    Probes `/v1/workspaces`: it takes no query parameters, every account has at
    least one workspace, and the response is small. Because a wrong `region` also
    returns 401, the failure message has to name both possible causes.
    """
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    url = f"{base_url(config.region)}/v1/workspaces"

    try:
        await http.request(log, url)
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid credentials or wrong region. Confirm the App API Key is correct and that the configured region ({config.region}) matches your Customer.io account's data region.\n\n{err.message}"
        elif err.code == 403:
            msg = f"The provided App API Key is not authorized to read workspaces. Confirm the key has read access.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def deliveries(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> common.Resource:
    base = base_url(config.region)
    rescan_window = config.advanced.metrics_rescan_window
    # Resolved once: it logs when it clamps, and it is the backfill's start point.
    floor = backfill_floor(config.start_date, log)

    def open(
        binding: CaptureBinding[ResourceConfigWithSchedule],
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
            fetch_changes={
                REALTIME: functools.partial(
                    fetch_deliveries,
                    http,
                    base,
                    timedelta(0),
                    config.advanced.window_size,
                ),
                RESCAN: functools.partial(
                    fetch_deliveries,
                    http,
                    base,
                    rescan_window,
                    config.advanced.window_size,
                ),
            },
            fetch_page=functools.partial(
                backfill_deliveries,
                http,
                base,
                config.advanced.window_size,
            ),
        )

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)
    # The last tick the backfill covers. Each incremental subtask seeds here so
    # its first emitted tick is exactly `cutoff`, leaving no gap at the seam.
    seam = cutoff - timedelta(seconds=1)

    return common.Resource(
        name=Delivery.NAME,
        key=["/id"],
        model=Delivery,
        open=open,
        initial_state=ResourceState(
            inc={
                REALTIME: ResourceState.Incremental(cursor=seam),
                RESCAN: ResourceState.Incremental(cursor=seam - rescan_window),
            },
            backfill=ResourceState.Backfill(
                next_page=floor.isoformat(),
                cutoff=cutoff,
            ),
        ),
        initial_config=ResourceConfigWithSchedule(
            name=Delivery.NAME,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


def config_object(
    http: HTTPMixin, config: EndpointConfig, model: type[ConfigObject]
) -> common.Resource:
    base = base_url(config.region)

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
            fetch_snapshot=functools.partial(
                snapshot_config_objects, http, base, model
            ),
            tombstone=model(_meta=model.Meta(op="d")),
        )

    return SnapshotResource(
        name=model.NAME,
        open=open,
        initial_config=ResourceConfig(
            name=model.NAME,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


def paginated_object(
    http: HTTPMixin, config: EndpointConfig, model: type[PaginatedObject]
) -> common.Resource:
    base = base_url(config.region)

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
            fetch_snapshot=functools.partial(
                snapshot_paginated_objects, http, base, model
            ),
            tombstone=model(_meta=model.Meta(op="d")),
        )

    return SnapshotResource(
        name=model.NAME,
        open=open,
        initial_config=ResourceConfig(
            name=model.NAME,
            interval=SNAPSHOT_INTERVALS.get(model.NAME, DEFAULT_SNAPSHOT_INTERVAL),
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every resource the connector exposes."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    return [
        deliveries(log, http, config),
        *(config_object(http, config, model) for model in CONFIG_OBJECTS),
        *(paginated_object(http, config, model) for model in PAGINATED_OBJECTS),
    ]
