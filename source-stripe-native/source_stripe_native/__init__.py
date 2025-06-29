import asyncio
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
from estuary_cdk.capture.common import ResourceConfig
from estuary_cdk.flow import ValidationError

from .resources import all_resources
from .models import (
    ConnectorState,
    EndpointConfig,
)

# When the capture_connected_accounts config setting is enabled,
# and there are thousands of connected accounts, we can't capture
# from all connected accounts in a single connector invocation
# without running out of memory. So we stop the connector after 30 minutes
# and restart it to capture from a different subset of connected accounts
# on the next invocation.
async def periodic_stop_to_rotate_connected_accounts(task: Task) -> None:
    await asyncio.sleep(30 * 60)  # 30 minutes
    task.stopping.event.set()


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            documentationUrl="https://go.estuary.dev/source-stripe-native",
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=None,
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        resources = await all_resources(log, self, discover.config, should_fetch_connected_accounts=False)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
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
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resources = await all_resources(log, self, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        opened, run_fn = common.open(open, resolved)

        # Wrap the run function to add periodic stop when connected accounts are enabled.
        async def wrapped_run(task: Task) -> None:
            if open.capture.config.capture_connected_accounts:
                asyncio.create_task(periodic_stop_to_rotate_connected_accounts(task))
            await run_fn(task)

        return (opened, wrapped_run)
