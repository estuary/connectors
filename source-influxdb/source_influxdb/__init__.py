import ssl
from logging import Logger
from typing import Awaitable, Callable

from aiohttp import ClientSession, TCPConnector
from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.flow import ConnectorSpec, ValidationError
from estuary_cdk.http import HTTPError, TokenSource
from pydantic import ConfigDict

from .api import list_buckets
from .models import ConnectorState, EndpointConfig, ResourceConfig
from .resources import all_resources, make_resource


class CaptureRequest(Request[EndpointConfig, ResourceConfig, ConnectorState]):
    # The outer request controls how nested validation errors are logged.
    model_config = ConfigDict(hide_input_in_errors=True)


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    retain_projections = False
    _tls_settings: tuple[str, bool] = ("", False)

    async def configure_http(self, config: EndpointConfig) -> None:
        self.token_source = TokenSource(
            oauth_spec=None,
            credentials=config.credentials,
            authorization_token_type="Token",
        )
        settings = (config.advanced.ssl_ca_certificate, config.advanced.ssl_skip_verify)
        if settings == self._tls_settings:
            return

        context: ssl.SSLContext | bool = True
        if config.advanced.ssl_skip_verify:
            context = False
        elif config.advanced.ssl_ca_certificate:
            context = ssl.create_default_context()
            try:
                context.load_verify_locations(cadata=config.advanced.ssl_ca_certificate)
            except (ssl.SSLError, ValueError) as err:
                raise ValidationError(["Invalid SSL CA Certificate: provide PEM-encoded CA certificates."]) from err

        # Retain the CDK's retries and streaming while configuring aiohttp's TLS.
        await self.inner.close()
        self.inner = ClientSession(connector=TCPConnector(ssl=context))
        self._tls_settings = settings

    def request_class(self):
        return CaptureRequest

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            documentationUrl="https://go.estuary.dev/source-influxdb",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        await self.configure_http(discover.config)
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        config = validate.config
        await self.configure_http(config)
        try:
            buckets = {
                bucket.name async for bucket in list_buckets(
                    log, self, config.url, config.org
                )
            }
        except HTTPError as err:
            if err.code not in (401, 403, 404):
                raise
            raise ValidationError([
                f"Unable to list InfluxDB buckets. Check the URL, credentials, and organization.\n\n{err.message}"
            ]) from err

        resources = []
        for binding in validate.bindings:
            bucket, _ = binding.resourceConfig.components()
            if bucket not in buckets:
                raise ValidationError([f"Bucket '{bucket}' was not found or is not readable by the credentials."])
            resources.append(make_resource(self, config, binding.resourceConfig))
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        await self.configure_http(open.capture.config)
        resources = [
            make_resource(self, open.capture.config, binding.resourceConfig)
            for binding in open.capture.bindings
        ]
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
