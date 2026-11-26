import functools
from datetime import timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import ResourceState
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPSession

from .api import FetchState, fetch_points, format_timestamp, list_buckets, list_measurements
from .models import MIN_TIMESTAMP, EndpointConfig, Point, ResourceConfig, parse_timestamp, resource_name


def open_binding(
    http: HTTPSession,
    config: EndpointConfig,
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    state: ResourceState,
    task: Task,
    all_bindings: list[tuple[CaptureBinding[ResourceConfig], common.Resource]],
) -> None:
    bucket, measurement = binding.resourceConfig.components()
    # Like source-front's cursor streams, history and new points share a cursor.
    # InfluxDB has event times, so a wall-clock backfill cutoff would skip writes.
    common.open_binding(
        binding,
        binding_index,
        state,
        task,
        fetch_changes=functools.partial(
            fetch_points,
            http,
            config.url,
            config.org,
            bucket,
            measurement,
            config.advanced.lag_seconds,
            fetch_state=FetchState(),
            interval=binding.resourceConfig.interval,
            lookback_hours=config.advanced.lookback_hours,
            start_date=config.start_date,
        ),
    )


def make_resource(
    http: HTTPSession, config: EndpointConfig, resource_config: ResourceConfig,
) -> common.Resource:
    start_cursor = (format_timestamp(max(parse_timestamp(config.initial_start_date), MIN_TIMESTAMP) - 1),)
    return common.Resource(
        name=resource_config.name,
        key=["/series", "/time"],
        model=Point,
        open=functools.partial(open_binding, http, config),
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=start_cursor),
            backfill=None,
        ),
        initial_config=resource_config,
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPSession, config: EndpointConfig
) -> list[common.Resource]:
    resources = []

    async for bucket in list_buckets(log, http, config.url, config.org):
        if bucket.name.startswith("_"):
            continue

        measurements = await list_measurements(
            log, http, config.url, config.org, bucket.name
        )
        for measurement in measurements:
            name = resource_name(bucket.name, measurement)
            resources.append(make_resource(http, config, ResourceConfig(
                name=name,
                bucket=bucket.name,
                measurement=measurement,
                interval=timedelta(minutes=5),
            )))

    return sorted(resources, key=lambda resource: resource.name)
