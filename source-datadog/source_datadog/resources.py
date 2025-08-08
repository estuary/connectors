import functools
from datetime import timedelta, datetime, UTC
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import ResourceConfig, ResourceState
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPSession

from source_datadog.api import fetch_rum_changes, fetch_rum_page

from .models import (
    EndpointConfig,
    IncrementalResource,
    IncrementalResourceFetchChangesFn,
    IncrementalResourceFetchPageFn,
    RealUserMonitoringResource,
)

API_KEY_HEADER = "DD-API-KEY"
APP_KEY_HEADER = "DD-APPLICATION-KEY"
INCREMENTAL_RESOURCES: list[
    tuple[
        type[IncrementalResource],
        IncrementalResourceFetchPageFn,
        IncrementalResourceFetchChangesFn,
    ]
] = [
    (RealUserMonitoringResource, fetch_rum_page, fetch_rum_changes),
]


async def validate_credentials(log: Logger, http: HTTPSession, config: EndpointConfig):
    url = f"{config.base_url}/rum/events/search"
    headers = config.common_headers
    body = {
        "filter": {
            "from": "now-1s",
            "query": "*",
        },
        "page": {
            "limit": 1,
        },
        "sort": "timestamp",
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
        fetch_changes_fn: IncrementalResourceFetchChangesFn,
        fetch_page_fn: IncrementalResourceFetchPageFn,
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
                fetch_changes_fn,
                http,
                config.base_url,
                config.common_headers,
                config.advanced.window_size,
            ),
            fetch_page=functools.partial(
                fetch_page_fn,
                http,
                config.base_url,
                config.common_headers,
                config.start_date,
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
                fetch_changes_fn,
                fetch_page_fn,
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
        for resource, fetch_page_fn, fetch_changes_fn in INCREMENTAL_RESOURCES
    ]


async def all_resources(
    log: Logger, http: HTTPSession, config: EndpointConfig
) -> list[common.Resource]:
    return [
        *incremental_resources(http, config),
    ]
