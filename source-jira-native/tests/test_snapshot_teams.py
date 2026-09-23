import asyncio
import json
from logging import Logger
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from source_jira_native.api import snapshot_teams
from source_jira_native.models import EndpointConfig, Teams
from source_jira_native.resources import full_refresh_resources

DOMAIN = "example.atlassian.net"
ORG_ID = "org-abc"
CLOUD_ID = "cloud-xyz"


class StubSession:
    """Replays canned Teams API responses and records what was requested."""

    def __init__(self, pages: list[dict[str, Any]]):
        self.pages = pages
        self.requests: list[tuple[str, dict[str, Any]]] = []

    async def request(self, log, url, params=None, **kwargs) -> bytes:
        # params is mutated between pages, so record a copy.
        self.requests.append((url, dict(params or {})))

        if url.endswith("/_edge/tenant_info"):
            return json.dumps({"cloudId": CLOUD_ID}).encode()

        return json.dumps(self.pages.pop(0)).encode()


def run_snapshot(http: StubSession) -> list[dict[str, Any]]:
    async def collect():
        return [
            doc.model_dump(by_alias=True)
            async for doc in snapshot_teams(http, DOMAIN, ORG_ID, Teams, Logger("test"))
        ]

    return asyncio.run(collect())


def teams_requests(http: StubSession) -> list[dict[str, Any]]:
    return [params for url, params in http.requests if "/teams" in urlparse(url).path]


def test_single_page_sends_site_id_and_stops():
    http = StubSession([{"cursor": None, "entities": [{"teamId": "t1"}]}])

    docs = run_snapshot(http)

    assert [d["teamId"] for d in docs] == ["t1"]
    requests = teams_requests(http)
    assert len(requests) == 1
    # The API rejects requests without siteId.
    assert requests[0]["siteId"] == CLOUD_ID


def test_cloud_id_is_looked_up_from_the_accounts_domain():
    http = StubSession([{"cursor": None, "entities": []}])

    run_snapshot(http)

    assert http.requests[0][0] == f"https://{DOMAIN}/_edge/tenant_info"


def test_teams_are_requested_from_the_teams_api_host():
    http = StubSession([{"cursor": None, "entities": [{"teamId": "t1"}]}])

    run_snapshot(http)

    url = [url for url, _ in http.requests if "/teams" in urlparse(url).path][0]
    assert url == f"https://api.atlassian.com/public/teams/v1/org/{ORG_ID}/teams"


def test_pages_are_walked_until_the_cursor_is_null():
    http = StubSession([
        {"cursor": "c1", "entities": [{"teamId": "t1"}]},
        {"cursor": "c2", "entities": [{"teamId": "t2"}]},
        {"cursor": None, "entities": [{"teamId": "t3"}]},
    ])

    docs = run_snapshot(http)

    assert [d["teamId"] for d in docs] == ["t1", "t2", "t3"]
    requests = teams_requests(http)
    assert len(requests) == 3
    # The first page is unpaginated; later pages thread the previous cursor.
    assert "cursor" not in requests[0]
    assert [r["cursor"] for r in requests[1:]] == ["c1", "c2"]


def test_empty_page_with_a_non_null_cursor_terminates():
    # Following the cursor until it's null, as the docs instruct, would never end here.
    http = StubSession([{"cursor": "c1", "entities": []}])

    docs = run_snapshot(http)

    assert docs == []
    assert len(teams_requests(http)) == 1


def _config(**overrides: Any) -> EndpointConfig:
    return EndpointConfig.model_validate({
        "domain": DOMAIN,
        "credentials": {
            "credentials_title": "Email & API Token",
            "username": "user@example.com",
            "password": "token",
        },
        **overrides,
    })


def _resource_names(config: EndpointConfig) -> list[str]:
    return [r.name for r in full_refresh_resources(None, None, config, ZoneInfo("UTC"))]


def test_teams_is_not_offered_without_an_organization_id():
    # Without an org id the stream can't be fetched, so it isn't discovered.
    assert "teams" not in _resource_names(_config())


def test_teams_is_offered_once_an_organization_id_is_set():
    names = _resource_names(_config(organization_id=ORG_ID))

    assert "teams" in names
    # Gating teams must not disturb any other stream.
    assert set(_resource_names(_config())) | {"teams"} == set(names)
