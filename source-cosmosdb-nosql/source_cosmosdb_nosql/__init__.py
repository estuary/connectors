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

from .api import sdk_reads_point_in_time_as_seconds
from .models import ConnectorState, EndpointConfig, ResourceConfig
from .resources import all_resources


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    retain_projections = False

    def __init__(self) -> None:
        super().__init__()
        sdk_reads_point_in_time_as_seconds()

    def request_class(self) -> type[Request[EndpointConfig, ResourceConfig, ConnectorState]]:
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-cosmosdb-nosql",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        resources = await all_resources(log, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        resources = await all_resources(log, validate.config, sample_keys=False)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, open.capture.config, sample_keys=False)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
