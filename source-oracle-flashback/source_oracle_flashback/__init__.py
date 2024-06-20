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

from .ssh_tunnel import ssh_tunnel

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
            documentationUrl="https://go.estuary.dev/source-oracle-flashback",
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=None,
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        if discover.config.network_tunnel:
            ssh_forwarding = discover.config.network_tunnel.ssh_forwarding
            params = oracledb.ConnectParams()
            params.parse_connect_string(discover.config.address)
            host = params.host
            port = params.port

            discover.config.address = discover.config.address.replace(host, 'localhost')

            self.tunnel = ssh_tunnel(
                log=log,
                endpoint=ssh_forwarding.ssh_endpoint,
                key=ssh_forwarding.private_key,
                remote_bind_address=(host, port),
                local_bind_port=port,
            )
        self.pool = create_pool(log, discover.config)
        resources = await all_resources(log, self, discover.config, self.pool)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        if validate.config.network_tunnel:
            ssh_forwarding = validate.config.network_tunnel.ssh_forwarding
            params = oracledb.ConnectParams()
            params.parse_connect_string(validate.config.address)
            host = params.host
            port = params.port

            validate.config.address = validate.config.address.replace(host, 'localhost')

            self.tunnel = ssh_tunnel(
                log=log,
                endpoint=ssh_forwarding.ssh_endpoint,
                key=ssh_forwarding.private_key,
                remote_bind_address=(host, port),
                local_bind_port=port,
            )
        self.pool = create_pool(log, validate.config)
        await validate_flashback(log, validate.config, self.pool)
        resources = await all_resources(log, self, validate.config, self.pool)
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        if open.capture.config.network_tunnel:
            ssh_forwarding = open.capture.config.network_tunnel.ssh_forwarding
            params = oracledb.ConnectParams()
            params.parse_connect_string(open.capture.config.address)
            host = params.host
            port = params.port

            open.capture.config.address = open.capture.config.address.replace(host, 'localhost')
            self.tunnel = ssh_tunnel(
                log=log,
                endpoint=ssh_forwarding.ssh_endpoint,
                key=ssh_forwarding.private_key,
                remote_bind_address=(host, port),
                local_bind_port=port,
            )
        self.pool = create_pool(log, open.capture.config)
        await validate_flashback(log, open.capture.config, self.pool)
        resources = await all_resources(log, self, open.capture.config, self.pool)
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)

    async def exit(self, _: Logger):
        if pool := getattr(self, "pool", None):
            await pool.close()
        if tunnel := getattr(self, "tunnel", None):
            tunnel.terminate()
