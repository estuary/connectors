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

from .resources import all_resources, validate_credentials, validate_projects
from .models import (
    ConnectorState,
    EndpointConfig,
    ResourceConfig,
)


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=None,
            documentationUrl="https://go.estuary.dev/source-jira-native",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        resources = await all_resources(log, self, discover.config, should_fetch_timezone=False, should_check_permissions=True)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        await validate_credentials(log, self, validate.config)
        await validate_projects(log, self, validate.config)
        resources = await all_resources(log, self, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
