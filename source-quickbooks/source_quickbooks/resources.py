import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import TypeVar

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    Resource,
    ResourceConfig,
    ResourceState,
    open_binding,
)
from estuary_cdk.capture.document import BaseDocument
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

AUTHENTICATION_ERROR_MSG = (
    "Authentication with QuickBooks failed. This connector requires "
    "your own QuickBooks app: please create one, or check the status "
    "of your existing app, at https://developer.intuit.com. Then "
    "confirm this capture is configured with the app's client ID and "
    "client secret, and with an unexpired refresh token issued by it. "
    "See https://go.estuary.dev/source-quickbooks for setup instructions."
    "\n\n"
)


async def validate_credentials(http: HTTPMixin, config: EndpointConfig, log: Logger):
    credentials = config.credentials
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=credentials)

    # An absent or expired access token must be temporarily exchanged here for
    # the validation probe to have a usable token. Durable rotation will happen
    # during connector startup. While most sources only offer single-use
    # refresh tokens, QuickBooks' are reusable for prolonged periods of time.
    if credentials.access_token_expires_at < datetime.now(tz=UTC) + timedelta(
        minutes=5
    ):
        try:
            token_response = await http.token_source.initialize_oauth2_tokens(log, http)
        except HTTPError as err:
            raise ValidationError([AUTHENTICATION_ERROR_MSG + err.message])

        credentials.access_token = token_response.access_token
        credentials.access_token_expires_at = datetime.now(tz=UTC) + timedelta(
            seconds=token_response.expires_in
        )
        if token_response.refresh_token:
            credentials.refresh_token = token_response.refresh_token

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
            msg = AUTHENTICATION_ERROR_MSG + err.message

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
                start_date,
            ),
        )

    return [
        Resource(
            name=resource.resource_name,
            key=["/Id"],
            model=resource,
            open=functools.partial(open_all_bindings, resource),
            initial_state=ResourceState(
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
                inc=ResourceState.Incremental(cursor=cutoff),
            ),
            initial_config=ResourceConfig(
                name=resource.resource_name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for resource in ALL_RESOURCES
    ]
