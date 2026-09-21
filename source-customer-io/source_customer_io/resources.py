import functools
from datetime import datetime, timedelta, UTC
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.capture.common import Resource, open_binding
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    ConnectorState,
    Delivery,
    EndpointConfig,
    ResourceConfigWithSchedule,
    ResourceState,
)
from .api import backfill_deliveries, backfill_floor, base_url, fetch_deliveries

# Tails new deliveries; RESCAN trails it by the configured window to re-read the
# same deliveries once their engagement metrics have settled.
REALTIME = "realtime"
RESCAN = "rescan"


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


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    return [deliveries(log, http, config)]
