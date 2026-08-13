"""Tests for deduplication logic in fetch_workspace_scoped."""

import logging
from unittest.mock import patch

import pytest

from source_asana_native.api import fetch_workspace_scoped
from source_asana_native.models import EndpointConfig, User, Workspace


@pytest.fixture
def config():
    return EndpointConfig(api_key="fake", advanced=EndpointConfig.Advanced())


def _make_workspace(gid: str) -> Workspace:
    return Workspace.model_validate({"gid": gid})


def _make_user(gid: str, name: str = "") -> User:
    return User.model_validate({"gid": gid, "name": name})


class TestDeduplication:

    @pytest.mark.asyncio
    async def test_users_deduplicated_across_workspaces(self, config):
        """Users appearing in multiple workspaces should only be yielded once."""
        ws1 = _make_workspace("ws1")
        ws2 = _make_workspace("ws2")

        # Same user (gid=u1) appears in both workspaces
        ws1_users = [_make_user("u1"), _make_user("u2")]
        ws2_users = [_make_user("u1"), _make_user("u3")]

        async def mock_paginated(url, model, http, log):
            if "ws1" in url:
                for u in ws1_users:
                    yield u
            elif "ws2" in url:
                for u in ws2_users:
                    yield u

        async def mock_collect_workspaces(http, config, log):
            return [ws1, ws2]

        with (
            patch("source_asana_native.api._fetch_paginated", side_effect=mock_paginated),
            patch("source_asana_native.api._collect_workspaces", side_effect=mock_collect_workspaces),
        ):
            results = [u async for u in fetch_workspace_scoped(User, None, config, logging.getLogger())]

        gids = [u.gid for u in results]
        assert gids == ["u1", "u2", "u3"], f"Expected deduplication, got {gids}"

    @pytest.mark.asyncio
    async def test_non_dedup_model_yields_all(self, config):
        """Models with deduplicate=False should yield duplicates across workspaces."""
        from source_asana_native.models import Tag

        ws1 = _make_workspace("ws1")
        ws2 = _make_workspace("ws2")

        def _make_tag(gid):
            return Tag.model_validate({"gid": gid})

        # Same tag gid in both workspaces
        ws1_tags = [_make_tag("t1")]
        ws2_tags = [_make_tag("t1")]

        async def mock_paginated(url, model, http, log):
            if "ws1" in url:
                for t in ws1_tags:
                    yield t
            elif "ws2" in url:
                for t in ws2_tags:
                    yield t

        async def mock_collect_workspaces(http, config, log):
            return [ws1, ws2]

        with (
            patch("source_asana_native.api._fetch_paginated", side_effect=mock_paginated),
            patch("source_asana_native.api._collect_workspaces", side_effect=mock_collect_workspaces),
        ):
            results = [t async for t in fetch_workspace_scoped(Tag, None, config, logging.getLogger())]

        gids = [t.gid for t in results]
        assert gids == ["t1", "t1"], f"Expected both, got {gids}"
