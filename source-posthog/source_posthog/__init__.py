"""
Estuary Flow source connector for PostHog analytics.

This connector captures data from PostHog including:
- Organizations
- Projects
- Events
- Persons
- Cohorts
- Feature Flags
- Annotations
"""

from collections.abc import Awaitable, Callable
from logging import Logger

from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.flow import AccessToken, ConnectorSpec, ValidationError
from estuary_cdk.http import TokenSource

from .api import validate_credentials
from .models import ConnectorState, EndpointConfig, ResourceConfig
from .resources import all_resources


class Connector(BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState]):
    """PostHog source connector."""

    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    def _configure_auth(self, config: EndpointConfig) -> None:
        """Configure token source for API authentication."""
        self.token_source = TokenSource(
            oauth_spec=None,
            credentials=AccessToken(access_token=config.personal_api_key),
        )

    async def spec(self, _: Logger, __: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-posthog",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        config = discover.config
        self._configure_auth(config)

        resources = await all_resources(log, self, config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        config = validate.config
        self._configure_auth(config)

        # Validate organization access
        validation = await validate_credentials(self, config, log)

        if not validation.valid:
            raise ValidationError([validation.error])

        # Proceed with normal resource validation
        resources = await all_resources(log, self, config)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        config = open.capture.config
        self._configure_auth(config)

        resources = await all_resources(log, self, config)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
