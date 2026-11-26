import asyncio
import base64
import io
import json
import sys
from collections import deque
from http import HTTPStatus
from logging import getLogger
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import pytest
from azure.core.credentials import AccessToken
from azure.core.pipeline.transport import AsyncHttpResponse, AsyncHttpTransport, HttpRequest
from azure.cosmos.aio import CosmosClient
from estuary_cdk.capture import Task, request
from estuary_cdk.capture import _emit
from estuary_cdk.capture.transactor import Transactor
from estuary_cdk.flow import RangeSpec

from source_cosmosdb_nosql import Connector, resources
from source_cosmosdb_nosql.api import fetch_container
from source_cosmosdb_nosql.models import ConnectorState, EndpointConfig, ResourceConfig


ENDPOINT = "http://cosmos.test:8081/"
KEY = base64.b64encode(b"made-up-unit-test-key").decode()
CONFIG = {
    "endpoint": ENDPOINT,
    "credentials": {"credentials_title": "Account Key", "key": KEY},
    "databases": ["shop"],
}

type Reply = tuple[int, dict[str, Any], dict[str, str]]


class FakeResponse(AsyncHttpResponse):
    def __init__(self, request: HttpRequest, reply: Reply):
        super().__init__(request, internal_response=None)
        self.status_code, payload, headers = reply
        self.headers = {"content-type": "application/json", **headers}
        self.content_type = "application/json"
        self.reason = HTTPStatus(self.status_code).phrase
        self._body = b"" if self.status_code == 304 else json.dumps(payload).encode()

    def body(self) -> bytes:
        return self._body


class FakeTransport(AsyncHttpTransport):
    """Exercise the real SDK's authentication and paging without opening sockets."""

    def __init__(self, replies: list[Reply], endpoint: str = ENDPOINT, handle_health_checks: bool = False):
        self.endpoint = endpoint
        self.handle_health_checks = handle_health_checks
        self.replies = deque(replies)
        self.requests: list[tuple[str, str, str | None]] = []
        self.request_headers: list[dict[str, str]] = []
        self.request_bodies: list[Any] = []
        self.client_options: dict[str, Any] = {}
        self.closed = False

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> "FakeTransport":
        await self.open()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def send(self, request: HttpRequest, **kwargs: Any) -> AsyncHttpResponse:
        if self.handle_health_checks and request.url == self.endpoint and (not self.replies or self.replies[0] != (200, {}, {})):
            self.replies.appendleft((200, {}, {}))
        self.request_headers.append({name.lower(): value for name, value in request.headers.items()})
        self.request_bodies.append(request.body)
        self.requests.append((
            request.method, request.url, request.headers.get("x-ms-continuation")
        ))
        assert request.method == "GET" or (
            request.method == "POST" and request.url.endswith("/docs/")
            and request.headers.get("x-ms-documentdb-isquery") == "true"
        )
        assert request.url.startswith(self.endpoint)
        assert request.headers.get("authorization")
        assert self.replies, f"Unexpected request: {request.method} {request.url}"
        return FakeResponse(request, self.replies.popleft())


class RoutedTransport(FakeTransport):
    """Reply by URL. Account reads, including the SDK's background health check, succeed."""

    def __init__(self, routes: dict[str, deque[Reply]]):
        super().__init__([])
        self.routes = routes

    async def send(self, request: HttpRequest, **kwargs: Any) -> AsyncHttpResponse:
        self.replies = deque([(200, {}, {})]) if request.url == self.endpoint else self.routes[request.url]
        return await super().send(request, **kwargs)


def feed_headers(transport: FakeTransport) -> list[dict[str, str]]:
    return [
        headers for (method, url, _), headers in zip(transport.requests, transport.request_headers)
        if method == "GET" and url.endswith("/docs/")
    ]


def sample_replies(container: dict[str, Any], *samples: list[Any]) -> list[Reply]:
    return [
        (200, {"_rid": "dGVzdHJpZDA=", **container}, {}),
        *[(200, {"Documents": values}, {}) for values in samples],
    ]


def install_cosmos(
    monkeypatch: pytest.MonkeyPatch, replies: list[Reply] | FakeTransport, *,
    endpoint: str = ENDPOINT, credential: Any = KEY,
    handle_health_checks: bool = False,
) -> FakeTransport:
    transport = (
        replies if isinstance(replies, FakeTransport)
        else FakeTransport(replies, endpoint, handle_health_checks)
    )

    def client(url: str, **kwargs: Any) -> CosmosClient:
        assert url == endpoint
        assert kwargs["credential"] == credential
        transport.client_options = kwargs
        return CosmosClient(url, transport=transport, retry_total=0, **kwargs)

    monkeypatch.setattr(resources, "CosmosClient", client)
    return transport


class FakeCredential:
    def __init__(self):
        self.scopes = []
        self.closed = False
        self.error = None

    async def get_token(self, *scopes, **kwargs):
        self.scopes.append(scopes)
        if self.error:
            raise self.error
        return AccessToken("fake-entra-token", 4102444800)

    async def close(self):
        self.closed = True


def install_identity(monkeypatch):
    # The identity service is fake even when azure-identity is installed.
    identity = ModuleType("azure.identity")
    identity.aio = ModuleType("azure.identity.aio")
    credential = FakeCredential()
    constructor = MagicMock(return_value=credential)
    vars(identity.aio)["ClientSecretCredential"] = constructor
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    monkeypatch.setitem(sys.modules, "azure.identity.aio", identity.aio)
    return credential, constructor


async def capture_once(monkeypatch, capture_spec, state):
    # Each pytest case has a new loop; the connector process has only one.
    monkeypatch.setattr(_emit, "_emit_lock", asyncio.Lock())
    stopping = Task.Stopping()
    polled = set()

    async def fetch_once(*args, **kwargs):
        async for item in fetch_container(*args, **kwargs):
            yield item
        polled.add((args[1], args[2]))
        if len(polled) == len(capture_spec.bindings):
            stopping.event.set()
        await stopping.event.wait()

    monkeypatch.setattr(resources, "fetch_container", fetch_once)
    opened, capture = await Connector().open(
        getLogger(__name__),
        request.Open[EndpointConfig, ResourceConfig, ConnectorState](
            capture=capture_spec, version="test", range=RangeSpec(), state=state,
        ),
    )
    assert opened.explicitAcknowledgements is False
    output = io.BytesIO()
    async with asyncio.timeout(5), asyncio.TaskGroup() as tg:
        task = Task(
            log=getLogger(__name__), connector_status=MagicMock(), name="test-capture", output=output,
            stopping=stopping, tg=tg, transactor=Transactor(output), catalog_task_name="test/capture",
        )
        await capture(task)
    assert stopping.first_error is None, stopping.first_error
    return [json.loads(line) for line in output.getvalue().splitlines()]
