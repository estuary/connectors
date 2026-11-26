import asyncio
import logging
import ssl
from unittest.mock import AsyncMock, Mock

import pytest
from estuary_cdk.capture import request
from estuary_cdk.flow import ValidationError

from source_influxdb import Connector
from source_influxdb.api import first_point_time, read_points
from source_influxdb.models import ConnectorState, EndpointConfig, ResourceConfig

LOG = logging.getLogger("test")


def test_authentication_through_cdk_transport(config):
    async def run():
        connector = Connector()
        await connector._mixin_enter(LOG)
        try:
            async def respond(**kwargs):
                assert kwargs["method"] == "POST"
                assert kwargs["url"] == config.url + "/api/v2/query"
                assert kwargs["params"] == {"org": config.org}
                assert kwargs["headers"] == {
                    "Authorization": "Token made-up-unit-test-token", "Accept": "application/csv",
                }
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
            assert discovered.bindings[0].resourcePath == ["sensors/app.requests"]

            connector.token_source = None
            validated = await connector.validate(LOG, request.Validate[EndpointConfig, ResourceConfig].model_validate({
                "name": "test/capture", "connectorType": "IMAGE", "config": config,
                "bindings": [{
                    "resourceConfig": discovered.bindings[0].resourceConfig,
                    "collection": {"name": "test/requests", "key": ["/series", "/time"], "writeSchema": {}},
                }],
            }))
            assert validated.bindings[0].resourcePath == ["sensors/app.requests"]

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
                LOG, connector, config.url, config.org, "sensors", "app.requests", 0, 1,
            ) is None
            assert await read_points(
                LOG, connector, config.url, config.org, "sensors", "app.requests", 0, 1,
            ) == []
            assert connector.inner.request.await_count == 5
        finally:
            await connector._mixin_exit(LOG)

    asyncio.run(run())


def handshake(client_context, hostname, certificate, key):
    # Complete a real TLS handshake entirely in memory, without opening sockets.
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(certificate, key)
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


@pytest.mark.parametrize("setting, hostname, succeeds", [
    (None, "influxdb.test", False),
    ("ssl_ca_certificate", "influxdb.test", True),
    ("ssl_ca_certificate", "wrong.test", False),
    ("ssl_skip_verify", "influxdb.test", True),
    ("ssl_skip_verify", "wrong.test", True),
])
def test_tls_transport_settings(config, tls_certificate, setting, hostname, succeeds):
    certificate, key = tls_certificate
    settings = {setting: certificate.read_text() if setting == "ssl_ca_certificate" else True} if setting else {}
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
                handshake(context, hostname, certificate, key)
            else:
                with pytest.raises(ssl.SSLCertVerificationError):
                    handshake(context, hostname, certificate, key)
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
