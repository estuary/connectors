from __future__ import annotations

import asyncio
import traceback
from collections.abc import Mapping
from typing import cast

from aiohttp import web, web_exceptions
from pydantic import JsonValue

from estuary_cdk.capture.common import (
    AssociatedDocument,
    ConnectorState,
    Resource,
    ResourceState,
    Task,
    WebhookResourceConfig,
)
from estuary_cdk.capture.webhook.resources import WebhookDocument, WebhookResource

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

    async def _enrich_doc(
        raw_doc: JsonValue,
        req: web.Request,
    ) -> AssociatedDocument[WebhookDocument]:
        if not isinstance(raw_doc, dict):
            task.log.error(
                "Documents are expected to be JSON objects", {"doc_type": type(raw_doc)}
            )
            raise web_exceptions.HTTPBadRequest(
                text="Documents are expected to be JSON objects"
            )

        try:
            # Passing in `raw_doc` to `matches` makes its signature a little awkward,
            # considering only body match rules make any use of it.
            # But the performance benefit of not parsing the raw JSON twice makes it worth it.
            binding_idx, resource_model = await anext(
                (
                    (idx, rsc.model)
                    for idx, rsc in sorted_mapping.items()
                    if await rsc.initial_config.match_rule.matches(
                        req, task.log, raw_doc
                    )
                ),
            )
            assert not isinstance(
                resource_model, Resource.FixedSchema
            ), "FixedSchemas are not supported"
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
        return AssociatedDocument(
            doc=resource_model.model_validate(raw_doc),
            binding=binding_idx,
        )

    async def webhook_handler(req: web.Request) -> web.Response:
        if req.app.get(_rejecting_key, False):
            return web.Response(status=503, text="Server shutting down")

        try:
            parsed_body = cast(JsonValue, await req.json())
        except Exception:
            raise web_exceptions.HTTPBadRequest(text="Request body must be valid JSON")

        all_docs = parsed_body if isinstance(parsed_body, list) else [parsed_body]

        # WARNING: If at least one document doesn't match any of our match
        # rules, we reject the entire request and miss all documents in it.

        enriched_docs = [await _enrich_doc(raw_doc, req) for raw_doc in all_docs]

        for doc in enriched_docs:
            await task.captured(doc.binding, doc.doc)
        await task.checkpoint(state=ConnectorState())

        return web.json_response({"published": len(enriched_docs)})

    app = web.Application()
    _ = app.router.add_post("/{path:.*}", webhook_handler)
    return app


def start_webhook_server(
    binding_index_mapping: Mapping[
        int, WebhookResource[WebhookDocument, WebhookResourceConfig]
    ],
    task: Task,
):
    """Start the webhook listener for the given bindings.

    Called from open_webhook_binding.

    Deliberately runs outside the main TaskGroup to maximize uptime.
    Since non-webhook tasks can take minutes/hours to gracefully shut down,
    we want to wait until they are done to stop listening for webhooks.
    """
    task.log.info(
        "Starting webhook server",
        {
            "bindings": [
                {"index": idx, "name": rsc.initial_config.name}
                for idx, rsc in binding_index_mapping.items()
            ]
        },
    )

    async def run():
        async with asyncio.TaskGroup() as webhook_tg:
            webhook_task = Task(
                task.log.getChild("webhook-server"),
                task.connector_status,
                "capture.webhook-server",
                task._output,  # pyright: ignore[reportPrivateUsage]
                task.stopping,
                webhook_tg,
                task.transactor,
                requires_ack=True,
            )

            app = build_webhook_app(binding_index_mapping, webhook_task)
            runner = web.AppRunner(app)

            try:
                await runner.setup()
                site = web.TCPSite(runner, port=8080)
                await site.start()
            except OSError as exc:
                task.log.error(
                    "Webhook server failed to bind to port 8080",
                    {"error": str(exc)},
                )

                if task.stopping.first_error is None:
                    task.stopping.first_error = exc
                    task.stopping.first_error_task = "webhook-server"
                task.stopping.event.set()

                return

            try:
                _ = await webhook_task.stopping.webhook_event.wait()
                webhook_task.log.debug("webhook server is yielding to stop")
                app[_rejecting_key] = True
            except Exception as exc:
                task.log.error(
                    "Webhook server runtime error",
                    {"error": "".join(traceback.format_exception(exc))},
                )

                if task.stopping.first_error is None:
                    task.stopping.first_error = exc
                    task.stopping.first_error_task = "webhook-server"
                task.stopping.event.set()
            finally:
                await runner.cleanup()

    task.stopping.webhook_task = asyncio.create_task(run())
