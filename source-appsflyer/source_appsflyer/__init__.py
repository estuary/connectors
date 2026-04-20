from collections.abc import Awaitable, Callable
from logging import Logger
from typing import override

from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.flow import ConnectorSpec

from .models import (
    ConnectorState,
    EndpointConfig,
)
from .resources import (
    AnyResourceConfig,
    AnyResourceConfigAdapter,
    PullApiResourceConfig,
    all_resources,
)


class Connector(
    BaseCaptureConnector[EndpointConfig, AnyResourceConfig, ConnectorState]
):
    def request_class(self):
        return Request[EndpointConfig, AnyResourceConfig, ConnectorState]

    async def spec(self, _: Logger, __: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-appsflyer",
            resourceConfigSchema=AnyResourceConfigAdapter.json_schema(),
            resourcePathPointers=PullApiResourceConfig.PATH_POINTERS,
        )

    @override
    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[AnyResourceConfig]:
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    @override
    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, AnyResourceConfig],
    ) -> response.Validated:
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    @override
    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, AnyResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
