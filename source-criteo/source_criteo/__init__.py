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
from estuary_cdk.flow import ConnectorSpec, ValidationError

from .models import (
    ConnectorState,
    EndpointConfig,
    ResourceConfig,
)
from .resources import (
    all_resources,
    validate_advertiser_ids,
    validate_credentials,
    validate_reports,
)


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-criteo",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        # Credentials gate everything below — the other checks all call the API,
        # so there is nothing to report until these work.
        await validate_credentials(log, self, validate.config)

        # The remaining checks are independent of each other, so they are
        # collected and reported together rather than making the user fix one
        # problem per publish attempt.
        errors = [
            *await validate_advertiser_ids(log, self, validate.config),
            *await validate_reports(log, self, validate.config),
        ]
        if errors:
            raise ValidationError(errors)

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
