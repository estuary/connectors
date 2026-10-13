from collections.abc import Awaitable, Callable
from logging import Logger

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
from typing_extensions import override

from .models import (
    ConnectorState,
    CostResourceConfig,
    EndpointConfig,
)
from .resources import all_resources, validate_credentials


class Connector(
    BaseCaptureConnector[EndpointConfig, CostResourceConfig, ConnectorState],
):
    @classmethod
    @override
    # CDK uses deprecated abstractclassmethod; same diagnostic in source-front.
    def request_class(  # pyright: ignore[reportIncompatibleVariableOverride]
        cls,
    ) -> type[Request[EndpointConfig, CostResourceConfig, ConnectorState]]:
        return Request[EndpointConfig, CostResourceConfig, ConnectorState]

    @override
    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-aws-cost-explorer",
            resourceConfigSchema=CostResourceConfig.model_json_schema(),
            resourcePathPointers=CostResourceConfig.PATH_POINTERS,
        )

    @override
    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[CostResourceConfig]:
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    @override
    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, CostResourceConfig],
    ) -> response.Validated:
        await validate_credentials(log, self, validate.config)
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    @override
    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, CostResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
