from logging import Logger
from typing import Callable, Awaitable

from estuary_cdk.flow import (
    ConnectorSpec,
)
from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.http import HTTPMixin

from .resources import all_resources
from .models import ConnectorState, EndpointConfig, ResourceConfig


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
    HTTPMixin,
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://docs.estuary.dev",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self,
        log: Logger,
        discover: request.Discover[EndpointConfig],
    ) -> response.Discovered[ResourceConfig]:
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(
            validate.bindings, resources, resource_term="Item"
        )
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(
            open.capture.bindings, resources, resource_term="Item"
        )
        return common.open(open, resolved)

    async def exit(self, log: Logger):
        # Call parent class exit method to ensure proper cleanup
        await super().exit(log)
        # Ensure HTTP session is closed
        if hasattr(self, "inner") and self.inner:
            await self.inner.close()
            self.inner = None
