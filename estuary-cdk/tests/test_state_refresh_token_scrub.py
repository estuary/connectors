import io
import json
from typing import Any, Awaitable, Callable

import pytest
from pydantic import BaseModel

from estuary_cdk import Stopped
from estuary_cdk.capture import Request, Task, request, response
from estuary_cdk.capture.base_capture_connector import BaseCaptureConnector
from estuary_cdk.capture.common import ConnectorState, ResourceConfig, ResourceState
from estuary_cdk.flow import ConnectorSpec
from estuary_cdk.logger import FlowLogger


class EndpointConfig(BaseModel):
    pass


_ConnectorState = ConnectorState[ResourceState]
_Request = Request[EndpointConfig, ResourceConfig, _ConnectorState]


class _Connector(BaseCaptureConnector[EndpointConfig, ResourceConfig, _ConnectorState]):
    @classmethod
    def request_class(cls):
        return _Request

    async def spec(self, log: FlowLogger, _: request.Spec) -> ConnectorSpec:
        raise NotImplementedError()

    async def discover(self, log, discover):
        raise NotImplementedError()

    async def validate(self, log, validate):
        raise NotImplementedError()

    async def open(
        self,
        log: FlowLogger,
        open: request.Open[EndpointConfig, ResourceConfig, _ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        async def capture(task: Task) -> None:
            pass

        return (response.Opened(explicitAcknowledgements=False), capture)


async def _run_open(state: dict[str, Any]) -> list[dict[str, Any]]:
    output = io.BytesIO()
    connector = _Connector()
    connector.output = output

    req = _Request.model_validate(
        {
            "open": {
                "capture": {
                    "name": "acmeCo/test",
                    "connectorType": "IMAGE",
                    "config": {},
                    "intervalSeconds": 0,
                },
                "version": "test",
                "range": {},
                "state": state,
            }
        }
    )

    with pytest.raises(Stopped):
        await connector.handle(FlowLogger("test"), req)

    return [json.loads(line) for line in output.getvalue().splitlines()]


@pytest.mark.asyncio
async def test_scrubs_refresh_token_from_state():
    lines = await _run_open({"refresh_token": "stale-token"})

    checkpoints = [line["checkpoint"] for line in lines if "checkpoint" in line]
    assert checkpoints == [
        {"state": {"updated": {"refresh_token": None}, "mergePatch": True}}
    ]


@pytest.mark.asyncio
async def test_no_checkpoint_without_refresh_token():
    lines = await _run_open({"bindingStateV1": {}})

    assert not any("checkpoint" in line for line in lines)
