from __future__ import annotations

import asyncio
import traceback
from collections.abc import Mapping, Sequence
from typing import ClassVar, override

from aiohttp import web
from pydantic import BaseModel
from pydantic.fields import Field

from estuary_cdk.capture.common import (
    BaseResourceConfig,
    ConnectorState,
    Resource,
    ResourceState,
    Task,
    WebhookDocument,
    open_binding,  # pyright: ignore[reportUnknownVariableType]
)
from estuary_cdk.flow import CaptureBinding

from .match import (
    CATCH_ALL_DISCRIMINATOR,
    CollectionDiscriminatorSpec,
    CollectionMatchingSpec,
    UrlDiscriminator,
    UrlMatch,
)


def _open_webhook_binding(
    binding: CaptureBinding[WebhookResourceConfig],
    binding_index: int,
    resource_state: ResourceState,
    task: Task,
    all_bindings: Sequence[
        tuple[
            CaptureBinding[BaseResourceConfig],
            Resource[WebhookDocument, BaseResourceConfig, ResourceState],
        ]
    ],
):
    open_binding(
        binding,  # pyright: ignore[reportArgumentType]
        binding_index,
        resource_state,
        task,
    )

    # The webhook server is started here rather than in common.open() to keep
    # the webhook dependency one-way (webhook → common, never common → webhook).
    # Moving this to common.py would require a circular or deferred import.
    if task.stopping.webhook_task is None:
        webhook_bindings = {
            idx: rsc
            for idx, (_, rsc) in enumerate(all_bindings)
            if rsc.is_webhook_resource
        }
        start_webhook_server(
            webhook_bindings, task  # pyright: ignore[reportArgumentType]
        )


class WebhookResourceConfig(BaseResourceConfig):
    # TODO: What does this classvar do exactly?
    PATH_POINTERS: ClassVar[list[str]] = ["/webhookId"]

    name: str = Field(description="Name of this resource")
    match_rule: CollectionMatchingSpec = Field(
        default_factory=lambda: UrlMatch(value="*"),
        description="Matching spec for routing incoming webhooks to this collection",
    )

    @override
    def path(self) -> list[str]:
        return [self.name]


GENERIC_WEBHOOK_RESOURCE = Resource(
    name="webhook-data",
    key=["/_meta/webhookId"],
    model=WebhookDocument,
    open=_open_webhook_binding,  # pyright: ignore[reportArgumentType]
    initial_state=ResourceState(),
    initial_config=WebhookResourceConfig(
        name="webhook-data", match_rule=CATCH_ALL_DISCRIMINATOR
    ),
    schema_inference=True,
)


class WebhookCaptureSpec(BaseModel):
    name: str = Field(
        description="Unique name representing the set of captured messages"
    )
    discriminator: CollectionDiscriminatorSpec = Field(default_factory=UrlDiscriminator)

    def create_resources(
        self,
    ) -> list[Resource[WebhookDocument, WebhookResourceConfig, ResourceState]]:
        # TODO: Tighten this up so it only applies to bare bones instantiations
        if not self.discriminator.known_values:
            return [GENERIC_WEBHOOK_RESOURCE]

        return [
            Resource(
                name=f"{self.name}_{value}",
                key=["/_meta/webhookId"],
                model=WebhookDocument,
                open=_open_webhook_binding,  # pyright: ignore[reportArgumentType]
                initial_state=ResourceState(),
                initial_config=WebhookResourceConfig(
                    name=value,
                    match_rule=self.discriminator.for_value(value),
                ),
                schema_inference=True,
            )
            for value in self.discriminator.known_values
        ]


async def _run_webhook_server(
    binding_index_mapping: Mapping[
        int, Resource[WebhookDocument, WebhookResourceConfig, ResourceState]
    ],
    task: Task,
):
    # TODO: We need to classify webhook processing features as pre- and post-
    # collection routing. So, we need an universal handler for all requests,
    # then it gets routed to the collection handler.

    # Sort resources by their match rule, going from most to least specific.
    # The ordering is:
    #     1. Header matches (exact key-value)
    #     2. Body matches (exact dot-path value)
    #     3. URL paths, from most to least specific
    #     4. URL wildcard '*'
    binding_index_mapping = dict(
        sorted(
            binding_index_mapping.items(),
            key=lambda idx_and_rsc: idx_and_rsc[
                1
            ].initial_config.match_rule.sort_key,
            reverse=True,
        )
    )
    _rejecting = False

    async def webhook_handler(req: web.Request) -> web.Response:
        if _rejecting:
            return web.Response(status=503, text="Server shutting down")

        matching_binding_index = await anext(
            (
                idx
                for idx, rsc in binding_index_mapping.items()
                if await rsc.initial_config.match_rule.matches(req)
            ),
            None,
        )
        if matching_binding_index is None:
            # TODO: Log error message and exit
            return web.Response(status=500)

        task.captured(
            matching_binding_index,
            WebhookDocument.model_validate(await req.json()),
        )
        await task.checkpoint(
            state=ConnectorState()  # pyright: ignore[reportUnknownArgumentType]
        )

        return web.Response(text="{published: 1}")

    listener = web.Application()
    _ = listener.router.add_post("/{path:.*}", webhook_handler)

    runner = web.AppRunner(listener)
    await runner.setup()
    site = web.TCPSite(runner, port=8080)
    await site.start()

    try:
        _ = await task.stopping.webhook_event.wait()
        task.log.debug("webhook server is yielding to stop")
        _rejecting = True
    finally:
        await runner.cleanup()


def start_webhook_server(
    binding_index_mapping: Mapping[
        int, Resource[WebhookDocument, WebhookResourceConfig, ResourceState]
    ],
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
                    task._output,  # pyright: ignore[reportPrivateUsage]
                    task.stopping,
                    webhook_tg,
                )
                await _run_webhook_server(binding_index_mapping, webhook_task)
            except Exception as exc:
                task.log.error("".join(traceback.format_exception(exc)))
                if task.stopping.first_error is None:
                    task.stopping.first_error = exc
                    task.stopping.first_error_task = "webhook-server"
                task.stopping.pull_api_event.set()

    task.stopping.webhook_task = asyncio.create_task(run())
