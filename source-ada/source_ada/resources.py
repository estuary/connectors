from datetime import timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import BaseDocument, Resource, open_binding
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError

from .models import (
    Articles,
    EndpointConfig,
    PullResourceConfig,
    ResourceState,
    Tags,
    WebhookResourceConfig,
)
from .api import (
    snapshot_resources,
)


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    return
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    try:
        async for _ in snapshot_resources(http, config.bot_handle, Tags, log):
            break
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def pull_resource(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> Resource:

    def open(
            binding: CaptureBinding[PullResourceConfig],
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
            fetch_snapshot=functools.partial(
                snapshot_resources,
                http,
                config.bot_handle,
                Articles,
            ),
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d"))
        )

    return Resource(
        name=Articles.name,
        key=["/_meta/row_id"],
        model=BaseDocument,
        open=open,
        initial_state=ResourceState(),
        initial_config=PullResourceConfig(
            type="pull", name=Articles.name, interval=timedelta(minutes=15)
        ),
        schema_inference=True,
    )


def webhook_resource(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> Resource:
    """Mock webhook resource to test oneOf discriminator UI rendering."""

    def open(
            binding: CaptureBinding[WebhookResourceConfig],
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
            fetch_snapshot=functools.partial(
                snapshot_resources,
                http,
                config.bot_handle,
                Tags,
            ),
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d"))
        )

    return Resource(
        name="webhook_events",
        key=["/_meta/row_id"],
        model=BaseDocument,
        open=open,
        initial_state=ResourceState(),
        initial_config=WebhookResourceConfig(
            type="webhook", name="webhook_events",
            webhook_path="/events/*",
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    return [
        pull_resource(log, http, config),
        webhook_resource(log, http, config),
    ]
