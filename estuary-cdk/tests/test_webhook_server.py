import asyncio
import io
import json
import logging
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from typing import Any, BinaryIO, NamedTuple
from unittest.mock import MagicMock, patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from pydantic import model_validator

from estuary_cdk.capture.common import (
    Resource,
    ResourceState,
    WebhookResourceConfig,
)
from estuary_cdk.capture.task import MultipleWritersTask, Task
from estuary_cdk.capture.transactor import Transactor
from estuary_cdk.capture.webhook.match import (
    BodyMatch,
    HeaderMatch,
    UrlMatch,
)
from estuary_cdk.capture.webhook.resources import (
    WebhookDocument,
    open_webhook_binding,
)
from estuary_cdk.capture.webhook.server import (
    _rejecting_key,  # pyright: ignore[reportPrivateUsage]
    build_webhook_app,
    start_webhook_server,
)
from tests.utils import make_webhook_resource


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
    transactor = Transactor(output, requires_explicit_acks=True)

    task = Task(
        log=logging.getLogger("test-webhook"),
        connector_status=MagicMock(),
        name="test-webhook",
        output=output,
        stopping=stopping,
        tg=MagicMock(),
        transactor=transactor,
        requires_ack=True,
    )

    async def auto_ack() -> None:
        """Drain pending checkpoints continuously so handlers awaiting ACK
        unblock without a real Flow runtime."""
        while True:
            if transactor._pending:  # pyright: ignore[reportPrivateUsage]
                transactor.deliver_ack(
                    len(transactor._pending)  # pyright: ignore[reportPrivateUsage]
                )
            await asyncio.sleep(0)

    ack_task = asyncio.create_task(auto_ack())
    try:
        app = build_webhook_app(binding_index_mapping, task)
        async with TestClient(TestServer(app)) as client:
            yield ServerContext(task, client, app)
    finally:
        _ = ack_task.cancel()
        try:
            await ack_task
        except asyncio.CancelledError:
            pass


def read_emitted_records(output: BinaryIO) -> list[dict[str, Any]]:
    """Rewind `output` and parse the JSON-lines a Task has emitted to it."""
    _ = output.seek(0)
    return [json.loads(line) for line in output.read().decode().splitlines() if line]


class TestWebhookHandler:
    @pytest.mark.asyncio
    async def test_successful_post(self):
        resource = make_webhook_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json={"foo": "bar"})
            assert resp.status == 200
            assert await resp.json() == {"published": 1}

    @pytest.mark.asyncio
    async def test_meta_enrichment(self):
        resource = make_webhook_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json={"data": 1})
            assert resp.status == 200

            output = read_emitted_records(
                ctx.task._output  # pyright: ignore[reportPrivateUsage]
            )
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
        resource = make_webhook_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp1 = await ctx.client.post("/hook", json={"n": 1})
            resp2 = await ctx.client.post("/hook", json={"n": 2})
            assert resp1.status == 200
            assert resp2.status == 200

            output = read_emitted_records(
                ctx.task._output  # pyright: ignore[reportPrivateUsage]
            )
            captured_lines = [line for line in output if "captured" in line]
            assert len(captured_lines) >= 2

    @pytest.mark.asyncio
    async def test_404_no_matching_binding(self):
        resource = make_webhook_resource(
            "specific", match_rule=UrlMatch(value="/specific")
        )
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/other", json={"x": 1})
            assert resp.status == 404
            assert "No matching binding" in await resp.text()

    @pytest.mark.asyncio
    async def test_503_when_rejecting(self):
        resource = make_webhook_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            ctx.app[_rejecting_key] = True
            resp = await ctx.client.post("/hook", json={"x": 1})
            assert resp.status == 503
            assert "shutting down" in await resp.text()

    @pytest.mark.asyncio
    async def test_non_json_body(self):
        resource = make_webhook_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post(
                "/hook",
                data=b"not json",
                headers={"Content-Type": "application/json"},
            )
            assert resp.status == 400

    @pytest.mark.asyncio
    async def test_array_body_publishes_all(self):
        resource = make_webhook_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json=[{"a": 1}, {"b": 2}, {"c": 3}])
            assert resp.status == 200
            assert await resp.json() == {"published": 3}

            output = read_emitted_records(
                ctx.task._output  # pyright: ignore[reportPrivateUsage]
            )
            captured_lines = [line for line in output if "captured" in line]
            assert len(captured_lines) == 3

    @pytest.mark.asyncio
    async def test_array_body_single_element(self):
        resource = make_webhook_resource("catch-all")
        async with run_server({0: resource}) as ctx:
            resp = await ctx.client.post("/hook", json=[{"x": 1}])
            assert resp.status == 200
            assert await resp.json() == {"published": 1}

    @pytest.mark.asyncio
    async def test_non_dict_in_array_rejected(self):
        resource = make_webhook_resource("catch-all")
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

            output = read_emitted_records(
                ctx.task._output  # pyright: ignore[reportPrivateUsage]
            )
            doc = output[0]["captured"]["doc"]
            assert doc["name"] == "HELLO"

    @pytest.mark.asyncio
    async def test_non_dict_scalar_body_rejected(self):
        resource = make_webhook_resource("catch-all")
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
        header_resource = make_webhook_resource(
            "header", match_rule=HeaderMatch(key="X-Event", value="push")
        )
        url_resource = make_webhook_resource(
            "catch-all", match_rule=UrlMatch(value="*")
        )
        async with run_server({0: header_resource, 1: url_resource}) as ctx:
            resp = await ctx.client.post(
                "/hook",
                json={"x": 1},
                headers={"X-Event": "push"},
            )
            assert resp.status == 200

            output = read_emitted_records(
                ctx.task._output  # pyright: ignore[reportPrivateUsage]
            )

            first_line = output[0]
            assert isinstance(first_line, dict)

            captured = first_line["captured"]
            assert isinstance(captured, dict)
            assert captured["binding"] == 0

    @pytest.mark.asyncio
    async def test_body_match_priority_over_url(self):
        body_resource = make_webhook_resource(
            "body", match_rule=BodyMatch(key="event", value="push")
        )
        url_resource = make_webhook_resource(
            "catch-all", match_rule=UrlMatch(value="*")
        )
        async with run_server({0: body_resource, 1: url_resource}) as ctx:
            resp = await ctx.client.post("/hook", json={"event": "push"})
            assert resp.status == 200

            output = read_emitted_records(
                ctx.task._output  # pyright: ignore[reportPrivateUsage]
            )

            first_line = output[0]
            assert isinstance(first_line, dict)

            captured = first_line["captured"]
            assert isinstance(captured, dict)
            assert captured["binding"] == 0

    @pytest.mark.asyncio
    async def test_more_specific_url_first(self):
        specific = make_webhook_resource(
            "specific", match_rule=UrlMatch(value="/foo/bar")
        )
        parametric = make_webhook_resource(
            "parametric", match_rule=UrlMatch(value="/foo/{x}")
        )
        wildcard = make_webhook_resource("wildcard", match_rule=UrlMatch(value="*"))

        async with run_server({0: specific, 1: parametric, 2: wildcard}) as ctx:
            resp = await ctx.client.post("/foo/bar", json={"x": 1})
            assert resp.status == 200

            output = read_emitted_records(
                ctx.task._output  # pyright: ignore[reportPrivateUsage]
            )

            first_line = output[0]
            assert isinstance(first_line, dict)

            captured = first_line["captured"]
            assert isinstance(captured, dict)
            assert captured["binding"] == 0

    @pytest.mark.asyncio
    async def test_wildcard_catches_unmatched(self):
        specific = make_webhook_resource(
            "specific", match_rule=UrlMatch(value="/specific")
        )
        wildcard = make_webhook_resource("wildcard", match_rule=UrlMatch(value="*"))
        async with run_server({0: specific, 1: wildcard}) as ctx:
            resp = await ctx.client.post("/something-else", json={"x": 1})
            assert resp.status == 200

            output = read_emitted_records(
                ctx.task._output  # pyright: ignore[reportPrivateUsage]
            )

            first_line = output[0]
            assert isinstance(first_line, dict)

            captured = first_line["captured"]
            assert isinstance(captured, dict)
            assert captured["binding"] == 1


def _make_task() -> Task:
    output = io.BytesIO()
    return Task(
        log=logging.getLogger("test-webhook"),
        connector_status=MagicMock(),
        name="test-webhook",
        output=output,
        stopping=Task.Stopping(),
        tg=MagicMock(),
        transactor=Transactor(output, requires_explicit_acks=True),
        requires_ack=True,
    )


class TestWebhookAckBlocking:
    @pytest.mark.asyncio
    async def test_handler_blocks_until_ack(self):
        output = io.BytesIO()
        stopping = Task.Stopping()
        transactor = Transactor(output, requires_explicit_acks=True)

        task = Task(
            log=logging.getLogger("test-webhook-ack"),
            connector_status=MagicMock(),
            name="test-webhook-ack",
            output=output,
            stopping=stopping,
            tg=MagicMock(),
            transactor=transactor,
            requires_ack=True,
        )

        app = build_webhook_app({0: make_webhook_resource("catch-all")}, task)
        async with TestClient(TestServer(app)) as client:
            request_task = asyncio.create_task(client.post("/hook", json={"x": 1}))

            # Give the handler time to enter checkpoint() and start awaiting the ACK.
            await asyncio.sleep(0.1)
            assert (
                len(transactor._pending) == 1  # pyright: ignore[reportPrivateUsage]
            ), "Handler did not register a pending checkpoint slot"
            assert (
                not request_task.done()
            ), "Webhook handler returned before ACK was delivered"

            transactor.deliver_ack(1)

            resp = await asyncio.wait_for(request_task, timeout=1.0)
            assert resp.status == 200
            assert await resp.json() == {"published": 1}


class TestSharedWebhookTask:
    """
    Verify that a single webhook request whose documents fan out across
    multiple bindings emits exactly one checkpoint — the shared
    MultipleWritersTask coalesces them under one transaction.
    """

    @pytest.mark.asyncio
    async def test_array_body_splits_across_bindings(self):
        rsc_a = make_webhook_resource("a", match_rule=BodyMatch(key="kind", value="a"))
        rsc_b = make_webhook_resource("b", match_rule=BodyMatch(key="kind", value="b"))

        async with run_server({0: rsc_a, 1: rsc_b}) as ctx:
            resp = await ctx.client.post("/hook", json=[{"kind": "a"}, {"kind": "b"}])
            assert resp.status == 200
            assert await resp.json() == {"published": 2}

            output = read_emitted_records(
                ctx.task._output  # pyright: ignore[reportPrivateUsage]
            )
            captured_bindings = [
                line["captured"]["binding"] for line in output if "captured" in line
            ]
            assert captured_bindings == [
                0,
                1,
            ], "Bindings must be captured in input order, not coalesced"

            checkpoint_lines = [line for line in output if "checkpoint" in line]
            assert len(checkpoint_lines) == 1

    @pytest.mark.asyncio
    async def test_concurrent_requests_pipeline_without_interleaving(self):
        """Two requests in flight at once must both reach the transactor
        before either ACK is delivered (the lock releases after stdout
        flush, not after ACK), and their output must not interleave."""

        output = io.BytesIO()
        stopping = Task.Stopping()
        transactor = Transactor(output, requires_explicit_acks=True)

        task = Task(
            log=logging.getLogger("test-webhook-pipeline"),
            connector_status=MagicMock(),
            name="test-webhook-pipeline",
            output=output,
            stopping=stopping,
            tg=MagicMock(),
            transactor=transactor,
            requires_ack=True,
        )

        app = build_webhook_app({0: make_webhook_resource("catch-all")}, task)
        async with TestClient(TestServer(app)) as client:
            req_a = asyncio.create_task(client.post("/hook/A", json={}))
            req_b = asyncio.create_task(client.post("/hook/B", json={}))

            for _ in range(100):
                await asyncio.sleep(0.01)
                if len(transactor._pending) == 2:  # pyright: ignore[reportPrivateUsage]
                    break
            assert (
                len(transactor._pending) == 2  # pyright: ignore[reportPrivateUsage]
            ), "Both requests should be awaiting ACK concurrently"
            assert not req_a.done() and not req_b.done()

            transactor.deliver_ack(2)
            resp_a = await asyncio.wait_for(req_a, timeout=1.0)
            resp_b = await asyncio.wait_for(req_b, timeout=1.0)
            assert resp_a.status == 200
            assert resp_b.status == 200

            records = read_emitted_records(output)
            kinds = [
                "captured" if "captured" in line else "checkpoint" for line in records
            ]
            assert kinds == [
                "captured",
                "checkpoint",
                "captured",
                "checkpoint",
            ], f"Output interleaved across requests: {kinds}"

            req_paths = [
                line["captured"]["doc"]["_meta"]["reqPath"]
                for line in records
                if "captured" in line
            ]
            assert sorted(req_paths) == ["/hook/A", "/hook/B"]

    @pytest.mark.asyncio
    async def test_emit_failure_discards_partial_buffer(self):
        """If checkpoint flush raises, the next request must not inherit the
        failed batch's documents in its own checkpoint."""

        async with run_server({0: make_webhook_resource("catch-all")}) as ctx:
            real_emit_checkpoint = (
                MultipleWritersTask._emit_checkpoint  # pyright: ignore[reportPrivateUsage]
            )
            calls = {"n": 0}

            async def fail_first(self: MultipleWritersTask, *args: Any, **kwargs: Any):
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RuntimeError("simulated flush failure")
                return await real_emit_checkpoint(self, *args, **kwargs)

            with patch.object(MultipleWritersTask, "_emit_checkpoint", fail_first):
                resp_fail = await ctx.client.post("/hook/doomed", json={})
                assert resp_fail.status == 500

                resp_ok = await ctx.client.post("/hook/survivor", json={})
                assert resp_ok.status == 200

            records = read_emitted_records(
                ctx.task._output  # pyright: ignore[reportPrivateUsage]
            )
            captured_paths = [
                line["captured"]["doc"]["_meta"]["reqPath"]
                for line in records
                if "captured" in line
            ]
            assert captured_paths == [
                "/hook/survivor"
            ], "Failed batch's documents leaked into the next checkpoint"

            checkpoint_lines = [line for line in records if "checkpoint" in line]
            assert len(checkpoint_lines) == 1


class TestWebhookServerLifecycle:
    @pytest.mark.asyncio
    async def test_start_sets_webhook_task(self):
        task = _make_task()

        resource = make_webhook_resource("catch-all")
        start_webhook_server({0: resource}, task)

        webhook_task = task.stopping.webhook_task
        assert webhook_task is not None

        task.stopping.webhook_event.set()
        await webhook_task

    @pytest.mark.asyncio
    async def test_webhook_event_triggers_shutdown(self):
        task = _make_task()
        resource = make_webhook_resource("catch-all")
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
        resource = make_webhook_resource("catch-all")

        with patch(
            "aiohttp.web.TCPSite.start", side_effect=OSError("Address already in use")
        ):
            start_webhook_server({0: resource}, task)
            assert task.stopping.webhook_task is not None
            await task.stopping.webhook_task

        assert isinstance(task.stopping.first_error, OSError)
        assert task.stopping.first_error_task == "webhook-server"
        assert task.stopping.event.is_set()
