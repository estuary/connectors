from logging import Logger
from typing import Any, AsyncGenerator

import aiohttp
import pytest
from pydantic import BaseModel

from estuary_cdk import Mixin
from estuary_cdk.capture import BaseCaptureConnector, Request
from estuary_cdk.capture.common import ConnectorState, ResourceConfig
from estuary_cdk.http import HTTPMixin
from estuary_cdk.logger import init_logger


class EndpointConfig(BaseModel):
    pass


Base = BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState]


class CustomHTTPMixin(HTTPMixin):
    pass


def connector(*bases: type) -> Any:
    class Connector(*bases):
        def request_class(self):
            return Request[EndpointConfig, ResourceConfig, ConnectorState]

        async def spec(self, log: Logger, _: Any) -> Any: ...
        async def discover(self, log: Logger, _: Any) -> Any: ...
        async def validate(self, log: Logger, _: Any) -> Any: ...
        async def open(self, log: Logger, _: Any) -> Any: ...

    return Connector()


async def no_requests(_cls: type) -> AsyncGenerator[Any, None]:
    return
    yield


@pytest.fixture
def sessions(monkeypatch: pytest.MonkeyPatch) -> list[aiohttp.ClientSession]:
    created: list[aiohttp.ClientSession] = []
    init = aiohttp.ClientSession.__init__

    def tracking_init(self: aiohttp.ClientSession, *args: Any, **kwargs: Any):
        created.append(self)
        init(self, *args, **kwargs)

    monkeypatch.setattr(aiohttp.ClientSession, "__init__", tracking_init)
    return created


# Regression test: listing an HTTPMixin subclass next to BaseCaptureConnector
# used to open a second session that was never closed (source-stripe-native).
@pytest.mark.parametrize(
    "bases",
    [(Base,), (CustomHTTPMixin, Base), (Base, CustomHTTPMixin)],
    ids=["base-only", "custom-first", "custom-last"],
)
@pytest.mark.asyncio
async def test_http_mixin_entered_once(
    sessions: list[aiohttp.ClientSession], bases: tuple[type, ...]
):
    c = connector(*bases)
    await c.serve(log=init_logger(), requests=no_requests)

    assert len(sessions) == 1
    assert sessions[0] is c.inner
    assert sessions[0].closed


@pytest.mark.asyncio
async def test_each_mixin_entered_once_and_exited_in_reverse(
    sessions: list[aiohttp.ClientSession],
):
    calls: list[str] = []

    class A(Mixin):
        async def _mixin_enter(self, log: Logger, /):
            calls.append("enter a")

        async def _mixin_exit(self, log: Logger, /):
            calls.append("exit a")

    class SubA(A):
        pass

    class B(Mixin):
        async def _mixin_enter(self, log: Logger, /):
            calls.append("enter b")

        async def _mixin_exit(self, log: Logger, /):
            calls.append("exit b")

    c = connector(SubA, B, Base)
    await c.serve(log=init_logger(), requests=no_requests)

    assert calls == ["enter a", "enter b", "exit b", "exit a"]
    assert len(sessions) == 1 and sessions[0].closed


# Overrides must work like any Python method. Otherwise a parent runs twice
# when its override calls super(), or runs anyway when the override replaces it.
@pytest.mark.asyncio
async def test_overridden_hook_runs_only_through_the_override(
    sessions: list[aiohttp.ClientSession],
):
    calls: list[str] = []

    class A(Mixin):
        async def _mixin_enter(self, log: Logger, /):
            calls.append("enter a")

    class Cooperative(A):
        async def _mixin_enter(self, log: Logger, /):
            calls.append("enter cooperative")
            await super()._mixin_enter(log)

    class Replacing(A):
        async def _mixin_enter(self, log: Logger, /):
            calls.append("enter replacing")

    await connector(Cooperative, Base).serve(log=init_logger(), requests=no_requests)
    assert calls == ["enter cooperative", "enter a"]

    calls.clear()
    await connector(Replacing, Base).serve(log=init_logger(), requests=no_requests)
    assert calls == ["enter replacing"]


# The usual way to extend HTTPMixin's setup. Also calling HTTPMixin's own hook
# would bring the leaked session back.
@pytest.mark.asyncio
async def test_cooperative_http_mixin_override_opens_one_session(
    sessions: list[aiohttp.ClientSession],
):
    class Cooperative(HTTPMixin):
        async def _mixin_enter(self, _: Logger):
            return await super()._mixin_enter(_)

    c = connector(Cooperative, Base)
    await c.serve(log=init_logger(), requests=no_requests)

    assert len(sessions) == 1
    assert sessions[0] is c.inner
    assert sessions[0].closed
