from __future__ import annotations

import asyncio
import traceback
from collections.abc import Mapping, Sequence
from typing import ClassVar, cast, override

from aiohttp import web, web_exceptions
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
from estuary_cdk.pydantic_polyfill import JsonValue

from .match import (
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
    PATH_POINTERS: ClassVar[list[str]] = ["/name"]

    name: str = Field(description="Name of this resource")
    match_rule: CollectionMatchingSpec = Field(
        default_factory=lambda: UrlMatch(value="*"),
        description="Matching spec for routing incoming webhooks to this collection",
    )

    @override
    def path(self) -> list[str]:
        return [self.name]


CATCH_ALL_DISCRIMINATOR = UrlDiscriminator()
_GENERIC_WEBHOOK_RESOURCE = Resource(
    name="webhook-data",
    key=["/_meta/webhookId"],
    model=WebhookDocument,
    open=_open_webhook_binding,  # pyright: ignore[reportArgumentType]
    initial_state=ResourceState(),
    initial_config=WebhookResourceConfig(
        name="webhook-data",
        match_rule=CATCH_ALL_DISCRIMINATOR.for_value("*"),
    ),
    schema_inference=True,
)


class WebhookCaptureSpec(BaseModel):
    name: str = Field(
        default="webhook-data",
        description="Unique name representing the set of captured messages",
    )
    discriminator: CollectionDiscriminatorSpec = Field(
        default_factory=UrlDiscriminator, description="TODO: Write this"
    )

    def create_resources(
        self,
    ) -> list[Resource[WebhookDocument, WebhookResourceConfig, ResourceState]]:
        if self.discriminator == CATCH_ALL_DISCRIMINATOR:
            return [_GENERIC_WEBHOOK_RESOURCE]

        return [
            Resource(
                name=f"{self.name}_{rule.display_name}",
                key=["/_meta/webhookId"],
                model=WebhookDocument,
                open=_open_webhook_binding,  # pyright: ignore[reportArgumentType]
                initial_state=ResourceState(),
                initial_config=WebhookResourceConfig(
                    name=f"{self.name}_{rule.display_name}",
                    match_rule=rule,
                ),
                schema_inference=True,
            )
            for rule in self.discriminator.create_match_rules()
        ]


_rejecting_key = web.AppKey("rejecting", bool)


def build_webhook_app(
    binding_index_mapping: Mapping[
        int, Resource[WebhookDocument, WebhookResourceConfig, ResourceState]
    ],
    task: Task,
) -> web.Application:
    # Sort resources by their match rule, going from most to least specific.
    # The ordering is:
    #     1. Header matches (exact key-value)
    #     2. Body matches (exact dot-path value)
    #     3. URL paths, from most to least specific
    #     4. URL wildcard '*'
    sorted_mapping = dict(
        sorted(
            binding_index_mapping.items(),
            key=lambda idx_and_rsc: idx_and_rsc[1].initial_config.match_rule.sort_key,
            reverse=True,
        )
    )

    async def _handle_webhook_doc(
        raw_doc: JsonValue,
        req: web.Request,
    ) -> None:
        assert isinstance(raw_doc, dict), "Documents are expected to be JSON objects"
        try:
            matching_binding_index, _ = await anext(
                (
                    (idx, rsc)
                    for idx, rsc in sorted_mapping.items()
                    if await rsc.initial_config.match_rule.matches(req)
                ),
            )
        except StopAsyncIteration:
            task.log.error(
                "No handler found for incoming webhook request",
                {
                    "path": req.path,
                    "header_names": sorted(req.headers.keys()),
                    "body_keys": sorted(raw_doc.keys()),
                },
            )
            raise web_exceptions.HTTPNotFound(text="No matching binding for request")

        meta: dict[str, JsonValue] = {
            "op": "u",
            "headers": dict(req.headers),
            "reqPath": req.path,
        }
        if req.match_info:
            meta["pathParams"] = dict(req.match_info)
        if req.query:
            meta["queryParams"] = dict(req.query)

        raw_doc["_meta"] = meta

        # NOTE: This is where we would execute arbitrary document post-processing fns

        task.captured(
            matching_binding_index,
            WebhookDocument.model_validate(raw_doc),
        )

    async def webhook_handler(req: web.Request) -> web.Response:
        if req.app.get(_rejecting_key, False):
            return web.Response(status=503, text="Server shutting down")

        parsed_body = cast(JsonValue, await req.json())
        all_docs = parsed_body if isinstance(parsed_body, list) else [parsed_body]

        for raw_doc in all_docs:
            await _handle_webhook_doc(raw_doc, req)

        await task.checkpoint(
            state=ConnectorState()  # pyright: ignore[reportUnknownArgumentType]
        )

        return web.Response(text=f"{{published: {len(all_docs)}}}")

    app = web.Application()
    _ = app.router.add_post("/{path:.*}", webhook_handler)
    return app


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

                app = build_webhook_app(binding_index_mapping, webhook_task)
                runner = web.AppRunner(app)
                await runner.setup()

                site = web.TCPSite(runner, port=8080)
                await site.start()

                try:
                    _ = await webhook_task.stopping.webhook_event.wait()
                    webhook_task.log.debug("webhook server is yielding to stop")
                    app[_rejecting_key] = True
                finally:
                    await runner.cleanup()

            except Exception as exc:
                task.log.error("".join(traceback.format_exception(exc)))
                if task.stopping.first_error is None:
                    task.stopping.first_error = exc
                    task.stopping.first_error_task = "webhook-server"
                task.stopping.pull_api_event.set()

    task.stopping.webhook_task = asyncio.create_task(run())
