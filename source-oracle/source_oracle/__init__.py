from logging import Logger
from typing import Callable, Awaitable
import oracledb

from estuary_cdk.flow import ConnectorSpec
from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.http import HTTPMixin

from .api import (
    create_pool,
    fetch_tables,
    fetch_columns,
    fetch_page,
    fetch_changes,
)

from .resources import (
    all_resources,
    validate_flashback
)
from .models import (
    ConnectorState,
    EndpointConfig,
    ResourceConfig,
)


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
    HTTPMixin,
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            documentationUrl="https://go.estuary.dev/source-oracle-batch",
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=None,
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        pool = create_pool(discover.config)
        resources = await all_resources(log, self, discover.config, pool)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        pool = create_pool(validate.config)
        await validate_flashback(log, validate.config, pool)
        resources = await all_resources(log, self, validate.config, pool)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        pool = create_pool(open.capture.config)
        await validate_flashback(log, open.capture.config, pool)
        resources = await all_resources(log, self, open.capture.config, pool)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)