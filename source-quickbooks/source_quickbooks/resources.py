import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import TypeVar

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    BaseDocument,
    Resource,
    ResourceConfig,
    ResourceState,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    PROD_API_BASE_URL,
    SANDBOX_API_BASE_URL,
    backfill_entity,
    fetch_entity,
    query_entity,
)
from .models import (
    ALL_RESOURCES,
    EPOCH,
    OAUTH2_SPEC,
    Account,
    EndpointConfig,
    QuickBooksEntity,
)


async def validate_credentials(http: HTTPMixin, config: EndpointConfig, log: Logger):
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    base_url = SANDBOX_API_BASE_URL if config.is_sandbox else PROD_API_BASE_URL

    try:
        _ = await anext(
            query_entity(
                Account,
                EPOCH,
                datetime.now(tz=UTC),
                http,
                base_url,
                config.realm_id,
                log,
            )
        )
    except HTTPError as err:
        msg = f"Encountered error validating credentials.\n\n{err.message}"
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"

        raise ValidationError([msg])
    except StopAsyncIteration:
        pass


T = TypeVar("T", bound=QuickBooksEntity)


async def all_resources(
    _log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[Resource[BaseDocument, ResourceConfig, ResourceState]]:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    base_url = SANDBOX_API_BASE_URL if config.is_sandbox else PROD_API_BASE_URL

    # API works with second precision
    start_date = config.start_date.replace(microsecond=0)
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    def open_all_bindings(
        entity: type[QuickBooksEntity],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_entity, entity, http, base_url, config.realm_id
            ),
            fetch_page=functools.partial(
                backfill_entity,
                entity,
                http,
                base_url,
                config.realm_id,
            ),
        )

    return [
        Resource(
            name=resource.resource_name,
            key=["/Id"],
            model=resource,
            open=functools.partial(open_all_bindings, resource),
            initial_state=ResourceState(
                backfill=ResourceState.Backfill(
                    cutoff=cutoff, next_page=start_date.isoformat(timespec="seconds")
                ),
                inc=ResourceState.Incremental(cursor=cutoff),
            ),
            initial_config=ResourceConfig(
                name=resource.resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for resource in ALL_RESOURCES
    ]
