from logging import Logger
from typing import Awaitable, Callable, Type

from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.capture.common import ResourceConfigWithSchedule
from estuary_cdk.flow import (
    ConnectorSpec,
)

from source_monday.models import ConnectorState, EndpointConfig
from source_monday.resources import all_resources, validate_credentials


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfigWithSchedule, ConnectorState],
):
    def request_class(self):  # type: ignore[override]
        return Request[EndpointConfig, ResourceConfigWithSchedule, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-monday",
            resourceConfigSchema=ResourceConfigWithSchedule.model_json_schema(),
            resourcePathPointers=ResourceConfigWithSchedule.PATH_POINTERS,
        )

    async def discover(
        self,
        log: Logger,
        discover: request.Discover[EndpointConfig],
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
