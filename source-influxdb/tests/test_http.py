import asyncio
import base64
import logging
import ssl
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from estuary_cdk.capture import request
from estuary_cdk.flow import ValidationError

from source_influxdb import Connector
from source_influxdb.api import first_point_time, read_points
from source_influxdb.models import ConnectorState, EndpointConfig, ResourceConfig

LOG = logging.getLogger("test")
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize("authentication, expected", [
    ({"token": "made-up-token"}, "Token made-up-token"),
    ({"credentials": {
        "credentials_title": "Username & Password", "username": "telegraf°", "password": "p:word",
    }}, "Basic " + base64.b64encode("telegraf°:p:word".encode()).decode()),
    ({"credentials": {
        "credentials_title": "Database Token", "access_token": "made-up-database-token",
    }}, "Bearer made-up-database-token"),
])
def test_authentication_through_cdk_transport(config, authentication, expected):
    config = EndpointConfig.model_validate({
        **config.model_dump(exclude={"token", "credentials"}), **authentication,
    })

    async def run():
        connector = Connector()
        await connector._mixin_enter(LOG)
        try:
            async def respond(**kwargs):
                assert kwargs["method"] == "POST"
                assert kwargs["url"] == config.url + "/api/v2/query"
                assert kwargs["params"] == {"org": config.org}
                assert kwargs["headers"] == {"Authorization": expected, "Accept": "application/csv"}
                assert kwargs["json"]["type"] == "flux"
                query = kwargs["json"]["query"]

                async def body():
                    if query == "buckets()":
                        yield b",result,table,name\n,,0,sensors\n"
                    elif 'distinct(column: "_measurement")' in query:
                        yield b",result,table,_value\n,,0,app.requests\n"

                return Mock(
                    status=200, headers={"Content-Type": "application/csv"},
                    content=Mock(iter_any=body), release=AsyncMock(),
                )

            connector.inner.request = AsyncMock(side_effect=respond)
            connector.rate_limiter.delay = 0
            discovered = await connector.discover(LOG, request.Discover[EndpointConfig](
                connectorType="IMAGE", config=config,
            ))
            assert len(discovered.bindings) == 1
            assert discovered.bindings[0].resourcePath == ["sensors/app%2Erequests"]

            connector.token_source = None
            validated = await connector.validate(LOG, request.Validate[EndpointConfig, ResourceConfig].model_validate({
                "name": "test/capture", "connectorType": "IMAGE", "config": config,
                "bindings": [{
                    "resourceConfig": discovered.bindings[0].resourceConfig,
                    "collection": {"name": "test/requests", "key": ["/series", "/time"], "writeSchema": {}},
                }],
            }))
            assert validated.bindings[0].resourcePath == ["sensors/app%2Erequests"]

            connector.token_source = None
            await connector.open(LOG, request.Open[EndpointConfig, ResourceConfig, ConnectorState].model_validate({
                "capture": {
                    "name": "test/capture", "connectorType": "IMAGE", "config": config,
                    "intervalSeconds": 300, "bindings": [],
                },
                "state": {}, "version": "test",
                "range": {"keyBegin": 0, "keyEnd": 4294967295, "rClockBegin": 0, "rClockEnd": 4294967295},
            }))
            assert await first_point_time(
                LOG, connector, config.url, config.org, config.common_headers, "sensors", "app.requests", 0, 1,
            ) is None
            assert await read_points(
                LOG, connector, config.url, config.org, config.common_headers, "sensors", "app.requests", 0, 1,
            ) == []
            assert connector.inner.request.await_count == 5
        finally:
            await connector._mixin_exit(LOG)

    asyncio.run(run())


def handshake(client_context, hostname):
    # Complete a real TLS handshake entirely in memory, without opening sockets.
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(FIXTURES / "test-ca.pem", FIXTURES / "test-key.pem")
    client_in, client_out = ssl.MemoryBIO(), ssl.MemoryBIO()
    server_in, server_out = ssl.MemoryBIO(), ssl.MemoryBIO()
    client = client_context.wrap_bio(client_in, client_out, server_hostname=hostname)
    server = server_context.wrap_bio(server_in, server_out, server_side=True)
    complete = set()
    for _ in range(20):
        for endpoint in (client, server):
            try:
                endpoint.do_handshake()
                complete.add(endpoint)
            except ssl.SSLWantReadError:
                pass
        server_in.write(client_out.read())
        client_in.write(server_out.read())
        if len(complete) == 2:
            return
    pytest.fail("TLS handshake did not complete")


@pytest.mark.parametrize("settings, hostname, succeeds", [
    ({}, "influxdb.test", False),
    ({"ssl_ca_certificate": (FIXTURES / "test-ca.pem").read_text()}, "influxdb.test", True),
    ({"ssl_ca_certificate": (FIXTURES / "test-ca.pem").read_text()}, "wrong.test", False),
    ({"ssl_skip_verify": True}, "influxdb.test", True),
    ({"ssl_skip_verify": True}, "wrong.test", True),
])
def test_tls_transport_settings(config, settings, hostname, succeeds):
    config = EndpointConfig.model_validate({**config.model_dump(), "advanced": settings})

    async def run():
        connector = Connector()
        await connector._mixin_enter(LOG)
        original = connector.inner
        try:
            await connector.configure_http(config)
            configured = connector.inner
            context = configured.connector._ssl
            if context is True:
                assert configured is original
                context = ssl.create_default_context()
            elif context is False:
                assert settings["ssl_skip_verify"] is True
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            else:
                assert context.check_hostname is True
                assert context.verify_mode == ssl.CERT_REQUIRED
                ca = ssl.PEM_cert_to_DER_cert(settings["ssl_ca_certificate"])
                assert ca in context.get_ca_certs(binary_form=True)
                assert len(context.get_ca_certs()) >= len(ssl.create_default_context().get_ca_certs())

            if succeeds:
                handshake(context, hostname)
            else:
                with pytest.raises(ssl.SSLCertVerificationError):
                    handshake(context, hostname)
            await connector.configure_http(config)
            assert connector.inner is configured
            if settings:
                assert original.closed
                await connector.configure_http(EndpointConfig.model_validate({**config.model_dump(), "advanced": {}}))
                assert configured.closed
                assert connector.inner.connector._ssl is True
        finally:
            await connector._mixin_exit(LOG)
        assert connector.inner.closed

    asyncio.run(run())


def test_invalid_ca_is_rejected_before_a_request(config):
    config.advanced.ssl_ca_certificate = "not a PEM certificate"

    async def run():
        connector = Connector()
        await connector._mixin_enter(LOG)
        try:
            connector.inner.request = AsyncMock()
            with pytest.raises(ValidationError, match="Invalid SSL CA Certificate"):
                await connector.discover(LOG, request.Discover[EndpointConfig](connectorType="IMAGE", config=config))
            connector.inner.request.assert_not_awaited()
            assert not connector.inner.closed
        finally:
            await connector._mixin_exit(LOG)

    asyncio.run(run())
