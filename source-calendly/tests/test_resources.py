"""Offline tests for which streams all_resources discovers."""

from logging import getLogger
from unittest.mock import MagicMock

import pytest
from estuary_cdk.flow import AccessToken

from source_calendly import resources
from source_calendly.models import EndpointConfig, UserInfo

LOG = getLogger(__name__)


# The snapshot tests only cover whichever plan the test account has.
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("has_teams_access", "expected"),
    [
        (
            True,
            [
                "event_types",
                "scheduled_events",
                "event_invitees",
                "organization_memberships",
                "routing_forms",
                "groups",
                "routing_form_submissions",
            ],
        ),
        (
            False,
            [
                "event_types",
                "scheduled_events",
                "event_invitees",
                "organization_memberships",
            ],
        ),
    ],
    ids=["with_teams_access", "without_teams_access"],
)
async def test_discovered_streams_depend_on_teams_access(
    monkeypatch: pytest.MonkeyPatch,
    has_teams_access: bool,
    expected: list[str],
):
    async def fetch_user_info(http: object, log: object) -> UserInfo:
        return UserInfo(
            current_organization="https://api.calendly.com/organizations/ORG"
        )

    async def is_teams_account(org_uri: str, http: object, log: object) -> bool:
        return has_teams_access

    monkeypatch.setattr(resources, "fetch_user_info", fetch_user_info)
    monkeypatch.setattr(resources, "is_teams_account", is_teams_account)

    config = EndpointConfig(credentials=AccessToken(access_token="unused"))
    discovered = await resources.all_resources(LOG, MagicMock(), config)

    assert [resource.name for resource in discovered] == expected
