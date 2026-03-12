import asyncio
import sys
from collections.abc import AsyncGenerator, Mapping
from datetime import datetime
from enum import Enum
from logging import Logger
from typing import Callable, TypeVar

from aiohttp import web
from pydantic import BaseModel

from estuary_cdk.capture.common import (
    AssociatedDocument,
    BaseDocument,
    CaptureBinding,
    Resource,
    ResourceConfig,
    ResourceState,
    Task,
    WebhookDocument,
    open_binding,
)
from estuary_cdk.pydantic_polyfill import JsonValue

_BaseDocument = TypeVar("_BaseDocument", bound=BaseDocument)
_ResourceConfig = TypeVar("_ResourceConfig", bound=ResourceConfig)
_ResourceState = TypeVar("_ResourceState", bound=ResourceState)

WebhookCursor = str | int | datetime | dict[str, JsonValue] | None
"""
TODO: explain that I don't really have any use for this yet, I just want to be permissive for future uses
"""

# TODO: Should these fns receive an async generator of messages, as produced by the webhook listener?
# How would they inform the server that the data's been checkpointed so it can ACK the HTTP request?
# I need to provide a "heavy lifting" ReceiveWebhookFn type function that performs all the heavy lifting.
# Any fns that extend it should call it so we perform all the usual data validation and transformation,
# and only then would it do its own thing. The same way in existing connectors we define an `open` fn
# to decorate `open_binding`'s behaviour.
# I mean, kind of. Some checks we want to perform before collection routing, like signature verifications.
# Some messages will come encrypted and we can't even read the discriminator until we verify.
ReceiveWebhookFn = Callable[
    [
        Logger,
        WebhookCursor,
    ],
    AsyncGenerator[_BaseDocument | dict | AssociatedDocument | WebhookCursor, None],
]
"""
TODO: Add high-level description. 

This fn should execute after all usual verifications (auth, signatures) happen
and should be responsible for publishing documents

We need to mention this fn needs to run as fast as possible, we don't want HTTP reqs to time out
"""


def _open_webhook_binding(
    binding: CaptureBinding[ResourceConfig],
    binding_index: int,
    resource_state: ResourceState,
    task: Task,
    all_bindings,
):
    open_binding(binding, binding_index, resource_state, task)


GENERIC_WEBHOOK_RESOURCE = Resource(
    name="webhook-data",
    key=["/_meta/webhookId"],
    model=WebhookDocument,
    open=_open_webhook_binding,
    initial_state=ResourceState(),
    initial_config=ResourceConfig(name="webhook-data"),
    schema_inference=True,
)


class DiscriminatorLocation(Enum):
    HEADERS = "HEADERS"
    BODY = "BODY"


class CollectionDiscriminatorSpec(BaseModel):
    # TODO: Support URL path routing
    location: DiscriminatorLocation
    key: str
    # TODO: Do we actually want an individual model per message type? Probably yes.
    value_mappings: Mapping[str, type[BaseDocument]]


# TODO: At some point we'll need to inject _meta data like s-h-i's `prepare_doc` fn does


# TODO: I don't like having a per-binding spec. Maybe the spec can be a binding factory?
class WebhookCaptureSpec(BaseModel):
    # TODO: Maybe we want a routing fn?
    collection_discriminator: CollectionDiscriminatorSpec | None = None
    # processing_fn: WebhookCaptureFn | None

    def create_bindings(
        self,
    ) -> list[Resource[_BaseDocument, _ResourceConfig, _ResourceState]]:
        if self.collection_discriminator is None:
            return [GENERIC_WEBHOOK_RESOURCE]

        return [
            Resource(
                name=name,
                key=["/_meta/webhookId"],  # TODO: Users will want to re-key this
                model=model,
                open=_open_webhook_binding,
                initial_state=ResourceState(),
                # TODO: Maybe we need a webhook-specific ResourceConfig? What'd I put in there? Same stuff as s-h-i?
                initial_config=ResourceConfig(name=name),
                schema_inference=True,
            )
            for name, model in self.collection_discriminator.value_mappings.items()
        ]


async def _run_webhook_server(
    bindings: list[Resource[_BaseDocument, _ResourceConfig, _ResourceState]],
):
    # TODO: We need to classify webhook processing features as pre- and post-
    # collection routing. So, we need an universal handler for all requests,
    # then it gets routed to the collection handler.

    # TODO: Make URL paths configurable, with the default being the resource name
    async def webhook_handler(req: web.Request) -> web.Response:
        return web.Response(text="{published: 1}")

    listener = web.Application()
    listener.router.add_post("/{path:.*}", webhook_handler)

    runner = web.AppRunner(listener)
    await runner.setup()
    site = web.TCPSite(runner, port=8080)
    await site.start()


def start_webhook_server(
    bindings: list[Resource[_BaseDocument, _ResourceConfig, _ResourceState]],
):
    """Start the webhook listener for the given bindings.

    Called from common.open() after resolving webhook bindings.
    """
    print("Running server", file=sys.stderr)
    # TODO: We want to reject bad messages as fast as possible.
    # 1. Verify IP ranges, signatures, auth, everything
    # 2. Only then, route to the appropriate collection handling fn
    _ = asyncio.create_task(_run_webhook_server(bindings))
