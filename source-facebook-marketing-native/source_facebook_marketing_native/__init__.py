from logging import Logger
from typing import Awaitable, Callable, Generic
from pydantic import Field

from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request as BaseRequest,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.capture.common import ResourceConfig
from estuary_cdk.flow import (
    ConnectorSpec,
    EndpointConfig as EndpointConfigType,
    ConnectorState as ConnectorStateType,
    ResourceConfig as ResourceConfigType,
)

from .models import EndpointConfig, OAUTH2_SPEC, ConnectorState
from .resources import all_resources


class Validate(request.Validate):
    lastCapture: dict | None = None  # type: ignore[assignment]


class Request(
    BaseRequest, Generic[EndpointConfigType, ResourceConfigType, ConnectorStateType]
):
    validate_: Validate | None = Field(  # type: ignore[override]
        default=None, alias="validate"
    )


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=OAUTH2_SPEC,
            documentationUrl="https://go.estuary.dev/source-facebook-marketing-native",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        resources = await all_resources(
            log,
            self,
            discover.config,
            should_validate_access=True,
        )
        return common.discovered(resources)

    async def validate(  # type: ignore[override]
        self,
        log: Logger,
        validate: Validate,
    ) -> response.Validated:
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
