from collections.abc import Awaitable
from logging import Logger
from typing import Callable, override

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
    OAUTH2_SPEC,
    ConnectorState,
    EndpointConfig,
    ZohoResourceConfigWithSchedule,
)
from .resources import all_resources, validate_credentials


class Connector(
    BaseCaptureConnector[EndpointConfig, ZohoResourceConfigWithSchedule, ConnectorState],
):
    @override
    def request_class(self):
        return Request[EndpointConfig, ZohoResourceConfigWithSchedule, ConnectorState]

    @override
    async def spec(self, _: request.Spec, logger: Logger) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=OAUTH2_SPEC,
            documentationUrl="https://go.estuary.dev/source-zoho",
            resourceConfigSchema=ZohoResourceConfigWithSchedule.model_json_schema(),
            resourcePathPointers=ZohoResourceConfigWithSchedule.PATH_POINTERS,
        )

    @override
    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ZohoResourceConfigWithSchedule]:
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    @override
    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ZohoResourceConfigWithSchedule],
    ) -> response.Validated:
        await validate_credentials(self, validate.config, log)
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    @override
    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ZohoResourceConfigWithSchedule, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
