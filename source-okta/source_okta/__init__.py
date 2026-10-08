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
from estuary_cdk.flow import (
    ConnectorSpec,
)

from .models import (
    ConnectorState,
    EndpointConfig,
    ResourceConfig,
)
from .resources import all_resources, validate_credentials, validate_intervals


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    @override
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    @override
    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-okta",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    @override
    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        await validate_credentials(log, self, discover.config)
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    @override
    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        validate_intervals(binding.resourceConfig for binding in validate.bindings)
        await validate_credentials(log, self, validate.config)
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    @override
    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
