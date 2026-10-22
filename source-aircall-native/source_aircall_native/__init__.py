import asyncio
import time
from collections import deque
from logging import Logger
from typing import Any, Awaitable, Callable

import aiohttp
from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.capture.common import ResourceConfigWithSchedule
from estuary_cdk.flow import ConnectorSpec
from typing_extensions import override

from .models import ConnectorState, EndpointConfig
from .resources import all_resources, validate_credentials

# Aircall allows about 60 requests per minute per company, and going over it
# blocks the company's API access for 60 seconds. Pacing requests up front
# avoids those lockouts, which back-off after a 429 keeps running into. The
# limit is shared with the company's other integrations, so the connector
# uses only half of it.
REQUESTS_PER_MINUTE = 30


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfigWithSchedule, ConnectorState],
):
    _request_times: deque[float] = deque()
    _request_lock = asyncio.Lock()

    def request_class(self):
        return Request[EndpointConfig, ResourceConfigWithSchedule, ConnectorState]

    @override
    async def _establish_connection_and_get_response(
        self,
        log: Logger,
        url: str,
        method: str,
        params: dict[str, Any] | None,
        json: dict[str, Any] | None,
        form: dict[str, Any] | None,
        with_token: bool,
        headers: dict[str, Any],
        timeout: aiohttp.ClientTimeout | None,
    ) -> aiohttp.ClientResponse:
        """Sends at most `REQUESTS_PER_MINUTE` requests in any 60 seconds."""
        async with self._request_lock:
            now = time.monotonic()
            while self._request_times and now - self._request_times[0] >= 60:
                self._request_times.popleft()

            if len(self._request_times) >= REQUESTS_PER_MINUTE:
                await asyncio.sleep(60 - (now - self._request_times[0]))
                self._request_times.popleft()

            self._request_times.append(time.monotonic())

        return await super()._establish_connection_and_get_response(
            log, url, method, params, json, form, with_token, headers, timeout
        )

    async def spec(self, _: request.Spec, logger: Logger) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-aircall-native",
            resourceConfigSchema=ResourceConfigWithSchedule.model_json_schema(),
            resourcePathPointers=ResourceConfigWithSchedule.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfigWithSchedule]:
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfigWithSchedule],
    ) -> response.Validated:
        await validate_credentials(log, self, validate.config)
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfigWithSchedule, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
