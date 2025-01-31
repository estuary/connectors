from datetime import datetime, timedelta, UTC
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    EndpointConfig,
    FullRefreshResource,
    FullRefreshFn,
    ResourceConfig,
    ResourceState,
)
from .api import (
    snapshot_surveys,
    snapshot_survey_responses,
)


AUTHORIZATION_HEADER = "x-api-key"

FULL_REFRESH_RESOURCES: list[tuple[str, FullRefreshFn]] = [
    ("surveys", snapshot_surveys),
    ("survey_responses", snapshot_survey_responses),
]


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials, authorization_header=AUTHORIZATION_HEADER)

    try:
        async for _ in snapshot_surveys(http, log):
            break
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            snapshot_fn: FullRefreshFn,
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
            fetch_snapshot=functools.partial(
            snapshot_fn,
            http,
        ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )

    resources = [
        common.Resource(
            name=name,
            key=["/_meta/row_id"],
            model=FullRefreshResource,
            open=functools.partial(open, snapshot_fn),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for (name, snapshot_fn) in FULL_REFRESH_RESOURCES
    ]

    return resources


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials, authorization_header=AUTHORIZATION_HEADER)

    return [
        *full_refresh_resources(log, http, config),
    ]
