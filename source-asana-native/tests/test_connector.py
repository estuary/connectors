"""Tests for the Asana connector."""

import logging

import pytest
from estuary_cdk.capture import request

from source_asana_native import Connector
from source_asana_native.models import EndpointConfig, ResourceConfig


@pytest.fixture
def connector() -> Connector:
    return Connector()


class TestSpec:
    @pytest.mark.asyncio
    async def test_spec_returns_config_schema(self, connector: Connector):
        spec = await connector.spec(logging.getLogger(), None)
        assert spec.configSchema is not None
        assert "properties" in spec.configSchema
        assert "api_key" in spec.configSchema["properties"]

    @pytest.mark.asyncio
    async def test_spec_returns_resource_config_schema(self, connector: Connector):
        spec = await connector.spec(logging.getLogger(), None)
        assert spec.resourceConfigSchema is not None
        assert "properties" in spec.resourceConfigSchema
        assert "name" in spec.resourceConfigSchema["properties"]

    @pytest.mark.asyncio
    async def test_spec_marks_api_key_as_secret(self, connector: Connector):
        spec = await connector.spec(logging.getLogger(), None)
        api_key_schema = spec.configSchema["properties"]["api_key"]
        assert api_key_schema.get("secret") is True

    @pytest.mark.asyncio
    async def test_spec_has_resource_path_pointers(self, connector: Connector):
        spec = await connector.spec(logging.getLogger(), None)
        assert spec.resourcePathPointers == ResourceConfig.PATH_POINTERS
        assert "/name" in spec.resourcePathPointers


class TestDiscover:
    """Tests for discover. No manual token_source setup — discover() must handle its own auth."""

    @pytest.mark.asyncio
    async def test_discover_returns_resources(self, endpoint_config: EndpointConfig):
        connector = Connector()
        log = logging.getLogger()
        await connector._mixin_enter(log)

        discover_request = request.Discover(
            config=endpoint_config,
            connectorType="IMAGE",
        )
        result = await connector.discover(log, discover_request)

        assert result.bindings is not None
        assert len(result.bindings) > 0

        await connector._mixin_exit(log)

    @pytest.mark.asyncio
    async def test_discover_includes_expected_resources(self, endpoint_config: EndpointConfig):
        connector = Connector()
        log = logging.getLogger()
        await connector._mixin_enter(log)

        discover_request = request.Discover(
            config=endpoint_config,
            connectorType="IMAGE",
        )
        result = await connector.discover(log, discover_request)

        resource_names = [b.resourceConfig.name for b in result.bindings]

        expected_resources = [
            # Snapshot: top-level
            "Workspaces",
            # Snapshot: workspace-scoped
            "Users",
            "Teams",
            "Projects",
            "Tags",
            "Portfolios",
            "Goals",
            "CustomFields",
            "TimePeriods",
            "ProjectTemplates",
            "TeamMemberships",
            # Snapshot: project-scoped
            "StatusUpdates",
            "Memberships",
            # Incremental via Events API
            "Tasks",
            "Sections",
            "Attachments",
            "Stories",
        ]

        for expected in expected_resources:
            assert expected in resource_names, f"Missing resource: {expected}"

        await connector._mixin_exit(log)
