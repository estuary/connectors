import asyncio
import io
import json
import logging
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from typing import Any, NamedTuple
from unittest.mock import MagicMock, patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from pydantic import model_validator

from estuary_cdk.capture.common import (
    Resource,
    ResourceState,
)
from estuary_cdk.capture.task import Task
from estuary_cdk.capture.webhook.match import (
    BodyMatch,
    CollectionMatchingSpec,
    HeaderMatch,
    UrlMatch,
)
from estuary_cdk.capture.webhook.resources import (
    WebhookDocument,
    WebhookResourceConfig,
    open_webhook_binding,
)
from estuary_cdk.capture.webhook.server import (
    _rejecting_key,  # pyright: ignore[reportPrivateUsage]
    build_webhook_app,
    start_webhook_server,
)


def _make_resource(
    name: str,
    match_rule: CollectionMatchingSpec | None = None,
) -> Resource[WebhookDocument, WebhookResourceConfig, ResourceState]:
    if match_rule is None:
        match_rule = UrlMatch(value="*")
    return Resource(
        name=name,
        key=["/_meta/webhookId"],
        model=WebhookDocument,
        open=open_webhook_binding,  # pyright: ignore[reportArgumentType]
        initial_state=ResourceState(),
        initial_config=WebhookResourceConfig(
            name=name,
            match_rule=match_rule,
        ),
        schema_inference=True,
    )


class ServerContext(NamedTuple):
    task: Task
    client: TestClient[web.Request, web.Application]
    app: web.Application


@asynccontextmanager
async def run_server(
    binding_index_mapping: Mapping[
        int, Resource[WebhookDocument, WebhookResourceConfig, ResourceState]
    ],
) -> AsyncGenerator[ServerContext, None]:
    output = io.BytesIO()
    stopping = Task.Stopping()

    task = Task(
        log=logging.getLogger("test-webhook"),
        connector_status=MagicMock(),
        name="test-webhook",
        output=output,
        stopping=stopping,
        tg=MagicMock(),
    )

    app = build_webhook_app(binding_index_mapping, task)
    async with TestClient(TestServer(app)) as client:
        yield ServerContext(task, client, app)


def _read_output(task: Task) -> list[dict[str, Any]]:
    _ = task._output.seek(0)  # pyright: ignore[reportPrivateUsage]
    lines = (
        task._output.read()  # pyright: ignore[reportPrivateUsage]
        .decode()
        .strip()
        .splitlines()
    )
    return [json.loads(line) for line in lines if line]


class TestWebhookHandler:
    @pytest.mark.asyncio
    async def test_successful_post(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json={"foo": "bar"})
            assert resp.status == 200
            assert await resp.json() == {"published": 1}

    @pytest.mark.asyncio
    async def test_meta_enrichment(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json={"data": 1})
            assert resp.status == 200

            output = _read_output(ctx.task)
            assert len(output) >= 1

            captured = output[0]["captured"]
            assert isinstance(captured, dict)
            assert captured["binding"] == 0

            doc = captured["doc"]
            assert isinstance(doc, dict)

            meta = doc["_meta"]
            assert isinstance(meta, dict)
            assert meta["reqPath"] == "/hook"
            assert meta["op"] == "u"
            assert isinstance(meta["headers"], dict)
            assert "Content-Type" in meta["headers"]

    @pytest.mark.asyncio
    async def test_multiple_posts_capture_separately(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp1 = await ctx.client.post("/hook", json={"n": 1})
            resp2 = await ctx.client.post("/hook", json={"n": 2})
            assert resp1.status == 200
            assert resp2.status == 200

            output = _read_output(ctx.task)
            captured_lines = [line for line in output if "captured" in line]
            assert len(captured_lines) >= 2

    @pytest.mark.asyncio
    async def test_404_no_matching_binding(self):
        resource = _make_resource("specific", match_rule=UrlMatch(value="/specific"))
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/other", json={"x": 1})
            assert resp.status == 404
            assert "No matching binding" in await resp.text()

    @pytest.mark.asyncio
    async def test_503_when_rejecting(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            ctx.app[_rejecting_key] = True
            resp = await ctx.client.post("/hook", json={"x": 1})
            assert resp.status == 503
            assert "shutting down" in await resp.text()

    @pytest.mark.asyncio
    async def test_non_json_body(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post(
                "/hook",
                data=b"not json",
                headers={"Content-Type": "application/json"},
            )
            assert resp.status == 400

    @pytest.mark.asyncio
    async def test_array_body_publishes_all(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json=[{"a": 1}, {"b": 2}, {"c": 3}])
            assert resp.status == 200
            assert await resp.json() == {"published": 3}

            output = _read_output(ctx.task)
            captured_lines = [line for line in output if "captured" in line]
            assert len(captured_lines) == 3

    @pytest.mark.asyncio
    async def test_array_body_single_element(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json=[{"x": 1}])
            assert resp.status == 200
            assert await resp.json() == {"published": 1}

    @pytest.mark.asyncio
    async def test_non_dict_in_array_rejected(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json=[{"ok": 1}, "bad"])
            assert resp.status == 400

    @pytest.mark.asyncio
    async def test_subclassed_document_model_respected(self):
        class CustomDoc(WebhookDocument, extra="allow"):
            @model_validator(mode="before")
            @classmethod
            def uppercase_name(cls, data: Any) -> Any:
                if isinstance(data, dict) and isinstance(data.get("name"), str):
                    data["name"] = data["name"].upper()
                return data

        resource = Resource(
            name="custom",
            key=["/_meta/webhookId"],
            model=CustomDoc,
            open=open_webhook_binding,
            initial_state=ResourceState(),
            initial_config=WebhookResourceConfig(
                name="custom", match_rule=UrlMatch(value="*")
            ),
            schema_inference=True,
        )

        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json={"name": "hello"})
            assert resp.status == 200

            output = _read_output(ctx.task)
            doc = output[0]["captured"]["doc"]
            assert doc["name"] == "HELLO"

    @pytest.mark.asyncio
    async def test_non_dict_scalar_body_rejected(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post(
                "/hook",
                data=b"42",
                headers={"Content-Type": "application/json"},
            )
            assert resp.status == 400


class TestRequestRouting:
    @pytest.mark.asyncio
    async def test_header_match_priority_over_url(self):
        header_resource = _make_resource(
            "header", match_rule=HeaderMatch(key="X-Event", value="push")
        )
        url_resource = _make_resource("catch-all", match_rule=UrlMatch(value="*"))
        async with run_server({0: header_resource, 1: url_resource}) as ctx:
            resp = await ctx.client.post(
                "/hook",
                json={"x": 1},
                headers={"X-Event": "push"},
            )
            assert resp.status == 200

            output = _read_output(ctx.task)

            first_line = output[0]
            assert isinstance(first_line, dict)

            captured = first_line["captured"]
            assert isinstance(captured, dict)
            assert captured["binding"] == 0

    @pytest.mark.asyncio
    async def test_body_match_priority_over_url(self):
        body_resource = _make_resource(
            "body", match_rule=BodyMatch(key="event", value="push")
        )
        url_resource = _make_resource("catch-all", match_rule=UrlMatch(value="*"))
        async with run_server({0: body_resource, 1: url_resource}) as ctx:
            resp = await ctx.client.post("/hook", json={"event": "push"})
            assert resp.status == 200

            output = _read_output(ctx.task)

            first_line = output[0]
            assert isinstance(first_line, dict)

            captured = first_line["captured"]
            assert isinstance(captured, dict)
            assert captured["binding"] == 0

    @pytest.mark.asyncio
    async def test_more_specific_url_first(self):
        specific = _make_resource("specific", match_rule=UrlMatch(value="/foo/bar"))
        parametric = _make_resource("parametric", match_rule=UrlMatch(value="/foo/{x}"))
        wildcard = _make_resource("wildcard", match_rule=UrlMatch(value="*"))

        async with run_server({0: specific, 1: parametric, 2: wildcard}) as ctx:
            resp = await ctx.client.post("/foo/bar", json={"x": 1})
            assert resp.status == 200

            output = _read_output(ctx.task)

            first_line = output[0]
            assert isinstance(first_line, dict)

            captured = first_line["captured"]
            assert isinstance(captured, dict)
            assert captured["binding"] == 0

    @pytest.mark.asyncio
    async def test_wildcard_catches_unmatched(self):
        specific = _make_resource("specific", match_rule=UrlMatch(value="/specific"))
        wildcard = _make_resource("wildcard", match_rule=UrlMatch(value="*"))
        async with run_server({0: specific, 1: wildcard}) as ctx:
            resp = await ctx.client.post("/something-else", json={"x": 1})
            assert resp.status == 200

            output = _read_output(ctx.task)

            first_line = output[0]
            assert isinstance(first_line, dict)

            captured = first_line["captured"]
            assert isinstance(captured, dict)
            assert captured["binding"] == 1


def _make_task() -> Task:
    return Task(
        log=logging.getLogger("test-webhook"),
        connector_status=MagicMock(),
        name="test-webhook",
        output=io.BytesIO(),
        stopping=Task.Stopping(),
        tg=MagicMock(),
    )


class TestWebhookServerLifecycle:
    @pytest.mark.asyncio
    async def test_start_sets_webhook_task(self):
        task = _make_task()

        resource = _make_resource("catch-all")
        start_webhook_server({0: resource}, task)

        webhook_task = task.stopping.webhook_task
        assert webhook_task is not None

        task.stopping.webhook_event.set()
        await webhook_task

    @pytest.mark.asyncio
    async def test_webhook_event_triggers_shutdown(self):
        task = _make_task()
        resource = _make_resource("catch-all")
        start_webhook_server({0: resource}, task)

        # Give server time to bind
        await asyncio.sleep(0.1)

        task.stopping.webhook_event.set()
        assert task.stopping.webhook_task is not None
        await task.stopping.webhook_task

        # Task completed without error
        assert task.stopping.first_error is None

    @pytest.mark.asyncio
    async def test_startup_failure_propagates_error(self):
        task = _make_task()
        resource = _make_resource("catch-all")

        with patch(
            "aiohttp.web.TCPSite.start", side_effect=OSError("Address already in use")
        ):
            start_webhook_server({0: resource}, task)
            assert task.stopping.webhook_task is not None
            await task.stopping.webhook_task

        assert isinstance(task.stopping.first_error, OSError)
        assert task.stopping.first_error_task == "webhook-server"
        assert task.stopping.event.is_set()
