"""Tests for the source-claude-admin-api connector."""

import logging

import pytest

from source_claude_admin_api import Connector
from source_claude_admin_api.models import EndpointConfig, ResourceConfig

EXPECTED_RESOURCES = ["Organization", "Users", "ClaudeCodeUsageReport"]


@pytest.fixture
def connector() -> Connector:
    return Connector()


@pytest.fixture
async def initialized_connector():
    connector = Connector()
    log = logging.getLogger()
    await connector._mixin_enter(log)
    yield connector
    await connector._mixin_exit(log)


class TestSpec:
    async def test_spec_returns_config_schema(self, connector: Connector):
        spec = await connector.spec(logging.getLogger(), None)
        assert spec.configSchema is not None
        assert "admin_api_key" in spec.configSchema["properties"]

    async def test_admin_api_key_is_secret(self, connector: Connector):
        spec = await connector.spec(logging.getLogger(), None)
        assert spec.configSchema["properties"]["admin_api_key"].get("secret") is True

    async def test_spec_has_resource_path_pointers(self, connector: Connector):
        spec = await connector.spec(logging.getLogger(), None)
        assert spec.resourcePathPointers == ResourceConfig.PATH_POINTERS
        assert "/name" in spec.resourcePathPointers


class TestDiscover:
    async def test_discover_returns_expected_resources(
        self, initialized_connector: Connector, endpoint_config: EndpointConfig
    ):
        from estuary_cdk.capture import request

        discover_request = request.Discover(
            config=endpoint_config,
            connectorType="IMAGE",
        )
        result = await initialized_connector.discover(
            logging.getLogger(), discover_request
        )

        names = [b.resourceConfig.name for b in result.bindings]
        for expected in EXPECTED_RESOURCES:
            assert expected in names, f"Missing resource: {expected}"
