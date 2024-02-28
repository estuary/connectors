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
from estuary_cdk.http import HTTPMixin

from .resources import all_resources
from .models import (
    ConnectorState,
    EndpointConfig,
    OAUTH2_SPEC,
    ResourceConfig,
)


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
    HTTPMixin,
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, _: request.Spec, logger: Logger) -> ConnectorSpec:
        return ConnectorSpec(
            documentationUrl="https://docs.estuary.dev",
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=OAUTH2_SPEC,
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, discover: request.Discover[EndpointConfig], logger: Logger
    ) -> response.Discovered[ResourceConfig]:
        resources = await all_resources(self, discover.config, logger)
        return common.discovered(resources)

    async def validate(
        self,
        validate: request.Validate[EndpointConfig, ResourceConfig],
        logger: Logger,
    ) -> response.Validated:
        resources = await all_resources(self, validate.config, logger)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
        logger: Logger,
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(self, open.capture.config, logger)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
