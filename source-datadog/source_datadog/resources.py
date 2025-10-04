import functools
from datetime import timedelta, datetime, UTC
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import ResourceConfig, ResourceState
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPSession

from source_datadog.api import fetch_events_page, fetch_events_changes

from .models import (
    EndpointConfig,
    IncrementalResource,
    RealUserMonitoringResource,
    LogResource
)

INCREMENTAL_RESOURCES: list[
    tuple[
        str,
        type[IncrementalResource],
    ]
] = [
    ("/rum/events/search", RealUserMonitoringResource),
    ("/logs/events/search", LogResource),
]


async def validate_credentials(log: Logger, http: HTTPSession, config: EndpointConfig):
    url = f"{config.base_url}/logs/events/search"
    headers = config.common_headers
    body = {
        "filter": {
            "from": "now-1s",
            "query": "*",
        },
        "page": {
            "limit": 1,
        },
    }

    try:
        await http.request(log, url, method="POST", headers=headers, json=body)
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                [
                    "Invalid credentials. Please check your capture's API Token and Application Key configuration values."
                ]
            )
        elif err.code == 404:
            raise ValidationError(
                [
                    f"Invalid URL endpoint {url}. Please check your capture's Subdomain and Site configuration values."
                ]
            )
        else:
            raise ValidationError([f"Failed to connect to Datadog API: {err}"])


def incremental_resources(
    http: HTTPSession,
    config: EndpointConfig,
) -> list[common.Resource]:
    def open(
        endpoint: str,
        resource: type[IncrementalResource],
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
                fetch_events_changes,
                http,
                config.base_url,
                config.common_headers,
                endpoint,
                resource,
                config.advanced.window_size,
                config.query,
            ),
            fetch_page=functools.partial(
                fetch_events_page,
                http,
                config.base_url,
                config.common_headers,
                endpoint,
                resource,
                config.start_date,
                config.query,
            ),
        )

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    return [
        common.Resource(
            name=resource.RESOURCE_NAME,
            key=resource.PRIMARY_KEYS,
            model=resource,
            open=functools.partial(
                open,
                endpoint,
                resource,
            ),
            initial_state=common.ResourceState(
                inc=common.ResourceState.Incremental(cursor=cutoff),
                backfill=common.ResourceState.Backfill(cutoff=cutoff, next_page=None),
            ),
            initial_config=common.ResourceConfig(
                name=resource.RESOURCE_NAME, interval=timedelta(minutes=1)
            ),
            schema_inference=True,
        )
        for endpoint, resource in INCREMENTAL_RESOURCES
    ]


async def all_resources(
    log: Logger, http: HTTPSession, config: EndpointConfig
) -> list[common.Resource]:
    return [
        *incremental_resources(http, config),
    ]
