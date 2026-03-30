from datetime import timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import BaseDocument, Resource, open_binding
from estuary_cdk.http import HTTPMixin, HTTPError

from .models import (
    INCREMENTAL_RESOURCES,
    EndpointConfig,
    GreenhouseResource,
    GreenhouseTokenSource,
    Interviewers,
    OAUTH2_SPEC,
    ResourceConfig,
    ResourceState,
)
from .api import (
    EVENTUAL_CONSISTENCY_LAG,
    GREENHOUSE_BASE_URL,
    fetch_greenhouse_resources,
    backfill_greenhouse_resources,
)

from .shared import now


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = GreenhouseTokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    url = f"{GREENHOUSE_BASE_URL}/{Interviewers.path}"

    try:
        await http.request(log, url, params={"per_page": 1})
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def incremental_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    def open(
            stream: type[GreenhouseResource],
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_greenhouse_resources,
                http,
                stream,
            ),
            fetch_page=functools.partial(
                backfill_greenhouse_resources,
                http,
                stream,
                config.start_date,
            ),
        )

    cutoff = now() - EVENTUAL_CONSISTENCY_LAG

    resources = [
        Resource(
            name=stream.name,
            key=["/id"],
            model=GreenhouseResource,
            open=functools.partial(open, stream),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
            ),
            initial_config=ResourceConfig(
                name=stream.name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for stream in INCREMENTAL_RESOURCES
    ]

    return resources


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = GreenhouseTokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    return [
        *incremental_resources(log, http, config),
    ]
