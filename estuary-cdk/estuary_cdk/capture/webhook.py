import asyncio
import traceback
from collections.abc import Mapping
from datetime import datetime
from enum import Enum
from typing import TypeVar

from aiohttp import web
from pydantic import BaseModel

from estuary_cdk.capture.common import (
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
    task: Task,
    bindings: list[Resource[_BaseDocument, _ResourceConfig, _ResourceState]],
):
    # TODO: We need to classify webhook processing features as pre- and post-
    # collection routing. So, we need an universal handler for all requests,
    # then it gets routed to the collection handler.

    _rejecting = False

    # TODO: Make URL paths configurable, with the default being the resource name
    async def webhook_handler(req: web.Request) -> web.Response:
        if _rejecting:
            return web.Response(status=503, text="Server shutting down")
        return web.Response(text="{published: 1}")

    listener = web.Application()
    listener.router.add_post("/{path:.*}", webhook_handler)

    runner = web.AppRunner(listener)
    await runner.setup()
    site = web.TCPSite(runner, port=8080)
    await site.start()

    try:
        await task.stopping.webhook_event.wait()
        task.log.debug("webhook server is yielding to stop")
        _rejecting = True
    finally:
        await runner.cleanup()


def start_webhook_server(
    bindings: list[Resource[_BaseDocument, _ResourceConfig, _ResourceState]],
    task: Task,
):
    """Start the webhook listener for the given bindings.

    Called from common.open() after resolving webhook bindings.
    Runs outside the main TaskGroup so the webhook server survives
    until all non-webhook tasks complete their graceful shutdown.
    """
    task.log.info("Starting webhook server")

    # TODO: We want to reject bad messages as fast as possible.
    # 1. Verify IP ranges, signatures, auth, everything
    # 2. Only then, route to the appropriate collection handling fn
    async def run():
        async with asyncio.TaskGroup() as webhook_tg:
            try:
                webhook_task = Task(
                    task.log.getChild("webhook-server"),
                    task.connector_status,
                    "capture.webhook-server",
                    task._output,
                    task.stopping,
                    webhook_tg,
                )
                await _run_webhook_server(webhook_task, bindings)
            except Exception as exc:
                task.log.error("".join(traceback.format_exception(exc)))
                if task.stopping.first_error is None:
                    task.stopping.first_error = exc
                    task.stopping.first_error_task = "webhook-server"
                task.stopping.pull_api_event.set()

    task.stopping.webhook_task = asyncio.create_task(run())
