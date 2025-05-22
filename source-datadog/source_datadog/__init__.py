from logging import Logger
from typing import Callable, Awaitable

from estuary_cdk.flow import ConnectorSpec
from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)

from .resources import all_resources
from .models import (
    ConnectorState,
    EndpointConfig,
    ResourceConfig,
)

class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    """Datadog connector implementation."""

    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, _: request.Spec, logger: Logger) -> ConnectorSpec:
        """Return the connector specification."""
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-datadog",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        """Discover available resources."""
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        """Validate the connector configuration and bindings."""
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        """Open the connector and start capturing data."""
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved) 