from logging import Logger
from typing import Awaitable, Callable

from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.flow import ConnectorSpec

from .models import ConnectorState, EndpointConfig, OrdersResourceConfig
from .resources import all_resources, validate_bindings, validate_credentials


class Connector(
    BaseCaptureConnector[EndpointConfig, OrdersResourceConfig, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, OrdersResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=None,
            documentationUrl="https://go.estuary.dev/source-amazon-selling-partner",
            resourceConfigSchema=OrdersResourceConfig.model_json_schema(),
            resourcePathPointers=OrdersResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[OrdersResourceConfig]:
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, OrdersResourceConfig],
    ) -> response.Validated:
        await validate_credentials(log, self, validate.config)
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        validate_bindings(validate.config, resolved)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, OrdersResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
