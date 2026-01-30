"""Tests for the PostHog connector."""

import logging

import pytest

from source_posthog import Connector
from source_posthog.models import EndpointConfig, ResourceConfig


@pytest.fixture
def connector() -> Connector:
    """Create a Connector instance."""
    return Connector()


@pytest.fixture
async def initialized_connector():
    """Create an initialized Connector instance with HTTP session."""
    connector = Connector()
    log = logging.getLogger()
    await connector._mixin_enter(log)
    yield connector
    await connector._mixin_exit(log)


class TestSpec:
    """Tests for connector spec."""

    @pytest.mark.asyncio
    async def test_spec_returns_config_schema(self, connector: Connector):
        """Spec should return a valid config schema."""
        spec = await connector.spec(logging.getLogger(), None)

        assert spec.configSchema is not None
        assert "properties" in spec.configSchema
        assert "personal_api_key" in spec.configSchema["properties"]
        assert "organization_id" in spec.configSchema["properties"]

    @pytest.mark.asyncio
    async def test_spec_returns_resource_config_schema(self, connector: Connector):
        """Spec should return a valid resource config schema."""
        spec = await connector.spec(logging.getLogger(), None)

        assert spec.resourceConfigSchema is not None
        assert "properties" in spec.resourceConfigSchema
        assert "name" in spec.resourceConfigSchema["properties"]

    @pytest.mark.asyncio
    async def test_spec_marks_api_key_as_secret(self, connector: Connector):
        """Personal API key should be marked as secret."""
        spec = await connector.spec(logging.getLogger(), None)

        api_key_schema = spec.configSchema["properties"]["personal_api_key"]
        assert api_key_schema.get("secret") is True

    @pytest.mark.asyncio
    async def test_spec_has_resource_path_pointers(self, connector: Connector):
        """Spec should have resource path pointers."""
        spec = await connector.spec(logging.getLogger(), None)

        assert spec.resourcePathPointers == ResourceConfig.PATH_POINTERS
        assert "/name" in spec.resourcePathPointers


class TestDiscover:
    """Tests for connector discover."""

    @pytest.mark.asyncio
    async def test_discover_returns_resources(
        self, initialized_connector: Connector, endpoint_config: EndpointConfig
    ):
        """Discover should return available resources."""
        from estuary_cdk.capture import request

        discover_request = request.Discover(
            config=endpoint_config,
            connectorType="IMAGE",
        )
        result = await initialized_connector.discover(logging.getLogger(), discover_request)

        assert result.bindings is not None
        assert len(result.bindings) > 0

    @pytest.mark.asyncio
    async def test_discover_includes_expected_resources(
        self, initialized_connector: Connector, endpoint_config: EndpointConfig
    ):
        """Discover should include all expected resource types."""
        from estuary_cdk.capture import request

        discover_request = request.Discover(
            config=endpoint_config,
            connectorType="IMAGE",
        )
        result = await initialized_connector.discover(logging.getLogger(), discover_request)

        resource_names = [b.resourceConfig.name for b in result.bindings]

        expected_resources = [
            "Organizations",
            "Projects",
            "Events",
            "Persons",
            "Cohorts",
            "FeatureFlags",
            "Annotations",
        ]

        for expected in expected_resources:
            assert expected in resource_names, f"Missing resource: {expected}"
