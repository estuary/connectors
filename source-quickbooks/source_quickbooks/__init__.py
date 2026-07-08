from collections.abc import Awaitable
from logging import Logger
from typing import Callable

from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.flow import (
    ConnectorSpec,
)

from .models import (
    ConnectorState,
    EndpointConfig,
)
from .resources import all_resources, validate_credentials


class Connector(
    BaseCaptureConnector[EndpointConfig, common.ResourceConfig, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, common.ResourceConfig, ConnectorState]

    async def spec(self, _: request.Spec, logger: Logger) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-quickbooks",
            resourceConfigSchema=common.ResourceConfig.model_json_schema(),
            resourcePathPointers=common.ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[common.ResourceConfig]:
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, common.ResourceConfig],
    ) -> response.Validated:
        await validate_credentials(self, validate.config, log)
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, common.ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
