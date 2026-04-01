import io
import json
import logging
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from typing import NamedTuple
from unittest.mock import MagicMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from estuary_cdk.capture.common import (
    Resource,
    ResourceState,
    WebhookDocument,
)
from estuary_cdk.capture.task import Task
from estuary_cdk.capture.webhook.match import (
    BodyMatch,
    CollectionMatchingSpec,
    HeaderDiscriminator,
    HeaderMatch,
    UrlMatch,
)
from estuary_cdk.capture.webhook.server import (
    WebhookCaptureSpec,
    WebhookResourceConfig,
    _open_webhook_binding,  # pyright: ignore[reportPrivateUsage]
    _rejecting_key,  # pyright: ignore[reportPrivateUsage]
    build_webhook_app,
)
from estuary_cdk.pydantic_polyfill import JsonValue


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
        open=_open_webhook_binding,  # pyright: ignore[reportArgumentType]
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


def _read_output(task: Task) -> list[dict[str, JsonValue]]:
    _ = task._output.seek(0)  # pyright: ignore[reportPrivateUsage]
    lines = (
        task._output.read()  # pyright: ignore[reportPrivateUsage]
        .decode()
        .strip()
        .splitlines()
    )
    return [json.loads(line) for line in lines if line]


class TestWebhookResourceConfig:
    def test_default_match_rule(self):
        cfg = WebhookResourceConfig(name="test")
        assert isinstance(cfg.match_rule, UrlMatch)
        assert cfg.match_rule.value == "*"

    def test_path(self):
        assert WebhookResourceConfig(name="my-hook").path() == ["my-hook"]

    def test_custom_match_rule(self):
        rule = UrlMatch(value="/custom")
        cfg = WebhookResourceConfig(name="test", match_rule=rule)
        assert cfg.match_rule.value == "/custom"


class TestWebhookCaptureSpec:
    def test_no_known_values_returns_generic(self):
        spec = WebhookCaptureSpec(name="test")
        resources = spec.create_resources()
        assert len(resources) == 1
        assert resources[0].name == "webhook-data"

    def test_with_known_values(self):
        spec = WebhookCaptureSpec(
            name="events",
            discriminator=HeaderDiscriminator(
                key="X-Event", known_values={"push", "pull"}
            ),
        )
        resources = spec.create_resources()
        assert len(resources) == 2
        names = {r.name for r in resources}
        assert names == {"events_push", "events_pull"}
        for r in resources:
            assert r.key == ["/_meta/webhookId"]
            assert r.schema_inference is True
            assert isinstance(r.initial_config.match_rule, HeaderMatch)
            assert r.initial_config.match_rule.key == "X-Event"


class TestWebhookHandler:
    @pytest.mark.asyncio
    async def test_successful_post(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json={"foo": "bar"})
            assert resp.status == 200
            assert await resp.text() == "{published: 1}"

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
            assert resp.status == 500

    @pytest.mark.asyncio
    async def test_array_body_publishes_all(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json=[{"a": 1}, {"b": 2}, {"c": 3}])
            assert resp.status == 200
            assert await resp.text() == "{published: 3}"

            output = _read_output(ctx.task)
            captured_lines = [line for line in output if "captured" in line]
            assert len(captured_lines) == 3

    @pytest.mark.asyncio
    async def test_array_body_single_element(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json=[{"x": 1}])
            assert resp.status == 200
            assert await resp.text() == "{published: 1}"

    @pytest.mark.asyncio
    async def test_non_dict_in_array_rejected(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json=[{"ok": 1}, "bad"])
            assert resp.status == 500

    @pytest.mark.asyncio
    async def test_non_dict_scalar_body_rejected(self):
        resource = _make_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post(
                "/hook",
                data=b"42",
                headers={"Content-Type": "application/json"},
            )
            assert resp.status == 500


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
