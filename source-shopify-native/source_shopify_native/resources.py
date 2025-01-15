from datetime import datetime, timedelta, UTC
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    TimestampedResource,
    OAUTH2_SPEC,
)
from .api import (
    fetch_products,
)

from .graphql.bulk_job_manager import BulkJobManager


AUTHORIZATION_HEADER = "X-Shopify-Access-Token"


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    """
    Validates that the provided access token is a valid Front access token.
    """
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials, authorization_header=AUTHORIZATION_HEADER)
    bulk_job_manager = BulkJobManager(http, log, config.store)

    try:
        await bulk_job_manager._get_currently_running_job()
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating access token.\n\n{err.message}"

        raise ValidationError([msg])


def products(
        log: Logger, http: HTTPMixin, config: EndpointConfig, bulk_job_manager: BulkJobManager,
) -> common.Resource:

    def open(
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
            fetch_changes=functools.partial(
                fetch_products,
                http,
                config.advanced.window_size,
                bulk_job_manager,
            )
        )

    return common.Resource(
        name='products',
        key=['/id'],
        model=TimestampedResource,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.start_date),
        ),
        initial_config=ResourceConfig(
            name='products', interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials, authorization_header=AUTHORIZATION_HEADER)
    bulk_job_manager = BulkJobManager(http, log, config.store)

    # Cancel any ongoing bulk query jobs before the connector starts submitting its own bulk query jobs.
    await bulk_job_manager.cancel_current()

    return [
        products(log, http, config, bulk_job_manager)
    ]
