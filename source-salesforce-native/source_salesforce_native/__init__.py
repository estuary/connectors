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

from .resources import all_resources, enabled_resources
from .models import (
    ConnectorState,
    EndpointConfig,
    OAUTH2_SPEC,
    SalesforceResourceConfigWithSchedule,
)


class Connector(
    BaseCaptureConnector[EndpointConfig, SalesforceResourceConfigWithSchedule, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, SalesforceResourceConfigWithSchedule, ConnectorState]

    async def spec(self, _: request.Spec, logger: Logger) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=OAUTH2_SPEC,
            documentationUrl="https://go.estuary.dev/source-salesforce-native",
            resourceConfigSchema=SalesforceResourceConfigWithSchedule.model_json_schema(),
            resourcePathPointers=SalesforceResourceConfigWithSchedule.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[SalesforceResourceConfigWithSchedule]:
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, SalesforceResourceConfigWithSchedule],
    ) -> response.Validated:
        resources = await enabled_resources(log, self, validate.config, validate.bindings)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, SalesforceResourceConfigWithSchedule, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await enabled_resources(log, self, open.capture.config, open.capture.bindings)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
