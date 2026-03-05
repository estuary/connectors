"""API connectivity tests for Asana connector.

These tests require valid API credentials in config.yaml.
"""

import logging

import pytest

from source_asana_native import Connector
from source_asana_native.api import (
    fetch_project_scoped,
    fetch_top_level,
    fetch_workspace_scoped,
    validate_credentials,
)
from source_asana_native.models import EndpointConfig, Project, Section, User, Workspace


@pytest.fixture
async def http_client():
    connector = Connector()
    log = logging.getLogger()
    await connector._mixin_enter(log)
    yield connector
    await connector._mixin_exit(log)


def _setup_auth(http_client: Connector, endpoint_config: EndpointConfig):
    from estuary_cdk.flow import AccessToken
    from estuary_cdk.http import TokenSource

    http_client.token_source = TokenSource(
        oauth_spec=None,
        credentials=AccessToken(access_token=endpoint_config.api_key),
    )


class TestAPIConnectivity:

    @pytest.mark.asyncio
    async def test_api_authentication(
        self, http_client: Connector, endpoint_config: EndpointConfig
    ):
        _setup_auth(http_client, endpoint_config)
        base_url = endpoint_config.advanced.base_url.rstrip("/")
        await validate_credentials(http_client, base_url, logging.getLogger())

    @pytest.mark.asyncio
    async def test_invalid_api_key_fails(self, http_client: Connector):
        from estuary_cdk.flow import AccessToken
        from estuary_cdk.http import TokenSource

        http_client.token_source = TokenSource(
            oauth_spec=None,
            credentials=AccessToken(access_token="invalid_key"),
        )

        with pytest.raises(Exception):
            await validate_credentials(
                http_client,
                "https://app.asana.com/api/1.0",
                logging.getLogger(),
            )


class TestResourceEndpoints:

    @pytest.mark.asyncio
    async def test_fetch_workspaces(
        self, http_client: Connector, endpoint_config: EndpointConfig
    ):
        _setup_auth(http_client, endpoint_config)
        log = logging.getLogger()

        results = []
        async for ws in fetch_top_level(Workspace, http_client, endpoint_config, log):
            results.append(ws)

        assert len(results) > 0
        assert all(hasattr(ws, "gid") for ws in results)

    @pytest.mark.asyncio
    async def test_fetch_users(
        self, http_client: Connector, endpoint_config: EndpointConfig
    ):
        _setup_auth(http_client, endpoint_config)
        log = logging.getLogger()

        results = []
        async for user in fetch_workspace_scoped(User, http_client, endpoint_config, log):
            results.append(user)

        assert len(results) > 0

    @pytest.mark.asyncio
    async def test_fetch_projects(
        self, http_client: Connector, endpoint_config: EndpointConfig
    ):
        _setup_auth(http_client, endpoint_config)
        log = logging.getLogger()

        results = []
        async for project in fetch_workspace_scoped(Project, http_client, endpoint_config, log):
            results.append(project)

        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_fetch_sections(
        self, http_client: Connector, endpoint_config: EndpointConfig
    ):
        _setup_auth(http_client, endpoint_config)
        log = logging.getLogger()

        results = []
        async for section in fetch_project_scoped(Section, http_client, endpoint_config, log):
            results.append(section)

        assert isinstance(results, list)
        assert all(hasattr(s, "gid") for s in results)
