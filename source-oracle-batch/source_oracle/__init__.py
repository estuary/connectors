import asyncio
from datetime import timedelta
from logging import Logger
from textwrap import dedent
from typing import Any, Awaitable, Callable

from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.flow import ConnectorSpec

from source_netsuite.api import check_nulls
from source_netsuite.api.pool import make_pool

from .models import ConnectorState, EndpointConfig, ResourceConfig
from .resources import all_resources


class Connector(BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState]):
    @classmethod
    def request_class(cls) -> type[Request[EndpointConfig, ResourceConfig, ConnectorState]]:
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            documentationUrl="https://docs.estuary.dev",
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=None,
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self,
        log: Logger,
        discover: request.Discover[EndpointConfig],
    ) -> response.Discovered[ResourceConfig]:
        resources = await all_resources(log, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        resources = await all_resources(log, validate.config)
        resolved = common.resolve_bindings(validate.bindings, resources)

        pool = make_pool(validate.config, validate.config.advanced.connection_limit)

        errors: list[str] = []

        async def check_binding_for_nulls(binding: ResourceConfig) -> None:
            if binding.page_cursor:
                async with pool.get_connection() as conn:
                    if await check_nulls(log, conn, binding.name, binding.page_cursor, timedelta(minutes=5)):
                        errors.append(
                            dedent(
                                f"""
                            {binding.name} has some rows with a null backfill page cursor ({binding.page_cursor}).
                            
                            Please choose another backfill cursor, disable this binding if it's not needed,
                            or contact support@estuary.dev for help.""".strip()
                            )
                        )

        tasks = []
        # Verify that all bindings have page_cursors,
        # and that all page_cursors and log_cursors actually exist.
        # We have to gate this currently as it'll time out for bigger netsuite accounts
        if validate.config.advanced.enable_auto_cursor:
            for binding, _ in resolved:
                task = asyncio.create_task(check_binding_for_nulls(binding.resourceConfig))
                tasks.append(task)

        await asyncio.gather(*tasks)

        if len(errors) > 0:
            formatted = "\n".join(errors)
            raise ValueError(
                f"""There were {len(errors)} errors: 
                    {formatted}
                """.strip()
            )

        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        # debugpy.listen(5678)
        resources = await all_resources(log, open.capture.config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
