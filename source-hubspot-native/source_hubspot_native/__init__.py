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
    OAUTH2_SPEC,
    HubspotResourceConfigWithSchedule,
)


class Connector(
    BaseCaptureConnector[EndpointConfig, HubspotResourceConfigWithSchedule, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, HubspotResourceConfigWithSchedule, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            documentationUrl="https://go.estuary.dev/hubspot-real-time",
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=OAUTH2_SPEC,
            resourceConfigSchema=HubspotResourceConfigWithSchedule.model_json_schema(),
            resourcePathPointers=HubspotResourceConfigWithSchedule.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[HubspotResourceConfigWithSchedule]:
        resources = await all_resources(log, self, discover.config, should_check_permissions=True)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, HubspotResourceConfigWithSchedule],
    ) -> response.Validated:
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, HubspotResourceConfigWithSchedule, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
