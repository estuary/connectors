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
from estuary_cdk.flow import ValidationError

from estuary_cdk.capture.common import ResourceConfigWithSchedule

from .resources import all_resources
from .models import (
    ConnectorState,
    EndpointConfig,
)


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfigWithSchedule, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfigWithSchedule, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            documentationUrl="https://go.estuary.dev/source-stripe-native",
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=None,
            resourceConfigSchema=ResourceConfigWithSchedule.model_json_schema(),
            resourcePathPointers=ResourceConfigWithSchedule.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfigWithSchedule]:
        resources = await all_resources(log, self, discover.config, should_fetch_connected_accounts=False)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfigWithSchedule],
    ) -> response.Validated:
        if (
            not validate.lastCapture
            or (
                validate.lastCapture.config.config.get("capture_connected_accounts", False)
                == validate.config.capture_connected_accounts
            )
            or not validate.lastCapture.bindings
        ):
            resources = await all_resources(log, self, validate.config, should_fetch_connected_accounts=False)
            resolved = common.resolve_bindings(validate.bindings, resources)
            return common.validated(resolved)

        prev_bindings_by_name = {
            binding.resourceConfig.name: binding.backfill
            for binding in validate.lastCapture.bindings
        }

        for current_binding in validate.bindings:
            resource_name = current_binding.resourceConfig.name
            if (
                resource_name in prev_bindings_by_name
                and current_binding.backfill <= prev_bindings_by_name[resource_name]
            ):
                raise ValidationError(
                    [
                        "Cannot change the `capture_connected_accounts` property without backfilling all collections again."
                    ]
                )

        resources = await all_resources(log, self, validate.config, should_fetch_connected_accounts=False)
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
