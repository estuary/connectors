"""Offline tests for validation, account discovery and the entity snapshots.

A stub session answers each endpoint with canned JSON shaped after
Microsoft's REST responses, so edge cases the sandbox can't produce are
covered too.
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from logging import getLogger
from typing import Any, Callable, cast

import pytest
from estuary_cdk.http import HTTPError, HTTPSession
from source_bing_ads_native.api import (
    ENVIRONMENT_VAR,
    AccountAccessError,
    AccountDirectory,
    customer_management_url,
    snapshot_accounts,
    snapshot_ad_groups,
    snapshot_ads,
    snapshot_asset_groups,
    snapshot_campaigns,
    token_exchange_scope,
)
from source_bing_ads_native.errors import should_retry
from source_bing_ads_native.models import EndpointConfig
from source_bing_ads_native.resources import validate_credentials
from source_bing_ads_native.http import with_developer_token

LOG = getLogger(__name__)

Handler = Callable[[dict[str, Any], dict[str, str]], Any]


def _fault(code: int, error_code: str = "") -> bytes:
    return json.dumps(
        {
            "TrackingId": "tracking-1",
            "Type": "AdApiFaultDetail",
            "Errors": [{"Code": code, "ErrorCode": error_code, "Message": "nope"}],
        }
    ).encode()


def _http_error(code: int) -> HTTPError:
    return HTTPError(
        f"Encountered HTTP error status 400.\nURL: x\nResponse:\n{_fault(code).decode()}",
        400,
    )


class StubSession:
    """Routes each POST by its path suffix to a handler returning a JSON body.

    `delays` holds out responses for a path suffix and body value, so tests
    can force concurrent requests to finish out of order."""

    def __init__(self, handlers: dict[str, Handler], delays: dict[str, float] | None = None):
        self.handlers = handlers
        self.delays = delays or {}
        self.calls: list[tuple[str, dict[str, Any], dict[str, str]]] = []

    async def request(
        self,
        log: Any,
        url: str,
        method: str = "GET",
        json: Any = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> bytes:
        suffix = next(s for s in self.handlers if url.endswith(s))
        self.calls.append((suffix, json, headers or {}))
        delay = self.delays.get(f"{suffix}:{next(iter((json or {}).values()), '')}")
        if delay:
            await asyncio.sleep(delay)
        result = self.handlers[suffix](json, headers or {})
        if isinstance(result, HTTPError):
            raise result
        return _encode(result)


def _session(handlers: dict[str, Handler], delays: dict[str, float] | None = None) -> Any:
    """A StubSession typed loosely enough to stand in for an HTTPSession."""
    return cast(HTTPSession, StubSession(handlers, delays))


def _encode(value: Any) -> bytes:
    return json.dumps(value).encode()


def _account(id: str, parent: str = "900") -> dict[str, Any]:
    return {"Id": id, "Name": f"acct-{id}", "ParentCustomerId": parent, "TimeZone": "Arizona"}


def _user(roles: list[dict[str, Any]] | None = None) -> Handler:
    return lambda body, headers: {
        "User": {"Id": "42"},
        "CustomerRoles": roles if roles is not None else [{"CustomerId": "900", "AccountIds": None}],
    }


def _accounts(*accounts: dict[str, Any]) -> Handler:
    return lambda body, headers: {"Accounts": list(accounts)}


def _campaigns(
    by_account: dict[str, list[str]], types: dict[str, str] | None = None
) -> Handler:
    return lambda body, headers: {
        "Campaigns": [
            {"Id": i, "Name": f"c{i}", "CampaignType": (types or {}).get(i, "Search")}
            for i in by_account[body["AccountId"]]
        ]
    }


@pytest.mark.asyncio
async def test_accounts_are_deduplicated_and_ordered_by_id():
    http = _session(
        {
            "/User/Query": _user(),
            "/Accounts/Search": _accounts(_account("30"), _account("4"), _account("30")),
            "/Campaigns/QueryByAccountId": _campaigns({"4": [], "30": []}),
        }
    )
    directory = AccountDirectory(http, [])

    ids = [a.Id async for a in snapshot_accounts(directory, LOG)]

    assert ids == ["4", "30"]


@pytest.mark.asyncio
async def test_accounts_are_discovered_once_per_run():
    http = _session(
        {
            "/User/Query": _user(),
            "/Accounts/Search": _accounts(_account("1")),
            "/Campaigns/QueryByAccountId": _campaigns({"1": []}),
        }
    )
    directory = AccountDirectory(http, [])

    await asyncio.gather(directory.accounts(LOG), directory.accounts(LOG))

    assert [c[0] for c in http.calls].count("/User/Query") == 1


@pytest.mark.asyncio
async def test_unknown_account_ids_are_reported_with_the_accessible_ones():
    http = _session(
        {
            "/User/Query": _user(),
            "/Accounts/Search": _accounts(_account("1"), _account("2")),
        }
    )

    with pytest.raises(AccountAccessError) as err:
        await AccountDirectory(http, ["2", "777"]).accounts(LOG)

    assert "777" in err.value.errors[0]
    assert "Accessible account IDs: 1, 2" in err.value.errors[0]


@pytest.mark.asyncio
async def test_no_reachable_accounts_fails():
    http = _session({"/User/Query": _user(), "/Accounts/Search": _accounts()})

    with pytest.raises(AccountAccessError, match="can't access any"):
        await AccountDirectory(http, []).accounts(LOG)


@pytest.mark.asyncio
async def test_linked_account_falls_back_to_its_owner_customer_id():
    # Account 2 is reached through a client link held by customer 500, but
    # (in this scenario) only its owner's CustomerId, 900, can read it.
    def campaigns(body: dict[str, Any], headers: dict[str, str]) -> Any:
        if body["AccountId"] == "2" and headers["CustomerId"] == "500":
            return _http_error(106)
        return {"Campaigns": []}

    http = _session(
        {
            "/User/Query": _user(
                [
                    {"CustomerId": "900", "AccountIds": ["1"]},
                    {"CustomerId": "500", "AccountIds": [], "LinkedAccountIds": ["2"]},
                ]
            ),
            "/Accounts/Search": _accounts(_account("1"), _account("2")),
            "/Campaigns/QueryByAccountId": campaigns,
        }
    )

    contexts = await AccountDirectory(http, []).accounts(LOG)

    assert [(c.id, c.customer_id) for c in contexts] == [("1", "900"), ("2", "900")]


@pytest.mark.asyncio
async def test_every_unreadable_account_is_reported_together():
    http = _session(
        {
            "/User/Query": _user(),
            "/Accounts/Search": _accounts(_account("1"), _account("2"), _account("3")),
            "/Campaigns/QueryByAccountId": lambda body, headers: (
                {"Campaigns": []} if body["AccountId"] == "2" else _http_error(106)
            ),
        }
    )

    with pytest.raises(AccountAccessError) as err:
        await AccountDirectory(http, []).accounts(LOG)

    assert len(err.value.errors) == 2
    assert "Account 1" in err.value.errors[0] and "TrackingId tracking-1" in err.value.errors[0]
    assert "Account 3" in err.value.errors[1]


@pytest.mark.asyncio
async def test_other_errors_fail_discovery():
    http = _session(
        {
            "/User/Query": _user(),
            "/Accounts/Search": _accounts(_account("1")),
            "/Campaigns/QueryByAccountId": lambda body, headers: _http_error(117),
        }
    )

    with pytest.raises(AccountAccessError, match="117"):
        await AccountDirectory(http, []).accounts(LOG)


@pytest.mark.asyncio
async def test_campaigns_emit_in_account_then_campaign_order_despite_concurrency():
    http = _session(
        {
            "/User/Query": _user(),
            "/Accounts/Search": _accounts(_account("1"), _account("2")),
            "/Campaigns/QueryByAccountId": _campaigns({"1": ["30", "4"], "2": ["7"]}),
        },
        # Account 1's full campaign query finishes last.
        delays={"/Campaigns/QueryByAccountId:1": 0.05},
    )
    directory = AccountDirectory(http, [])

    campaigns = [(c.AccountId, c.Id) async for c in snapshot_campaigns(http, directory, LOG)]

    assert campaigns == [("1", "4"), ("1", "30"), ("2", "7")]
    full_queries = [b for s, b, _ in http.calls if s.endswith("QueryByAccountId") and b["CampaignType"] != "Search"]
    assert all("PerformanceMax" in b["CampaignType"] for b in full_queries)


@pytest.mark.asyncio
async def test_ad_groups_and_ads_carry_parent_ids_in_order():
    http = _session(
        {
            "/User/Query": _user(),
            "/Accounts/Search": _accounts(_account("1")),
            "/Campaigns/QueryByAccountId": _campaigns({"1": ["10", "9"]}),
            "/AdGroups/QueryByCampaignId": lambda body, headers: {
                "AdGroups": [{"Id": f"{body['CampaignId']}{n}"} for n in ("2", "1")]
            },
            "/Ads/QueryByAdGroupId": lambda body, headers: {
                "Ads": [
                    {
                        "Id": f"{body['AdGroupId']}0",
                        "Type": "ResponsiveSearch",
                        "Headlines": [
                            {"Asset": {"Id": "5", "Text": "hi"}, "AssetPerformanceLabel": "Good"}
                        ],
                    }
                ]
            },
        },
        delays={"/AdGroups/QueryByCampaignId:9": 0.05},
    )
    directory = AccountDirectory(http, [])

    ad_groups = [
        (g.CampaignId, g.Id) async for g in snapshot_ad_groups(http, directory, LOG)
    ]
    ads = [a async for a in snapshot_ads(http, directory, LOG)]

    assert ad_groups == [("9", "91"), ("9", "92"), ("10", "101"), ("10", "102")]
    assert [(a.CampaignId, a.AdGroupId, a.Id) for a in ads] == [
        ("9", "91", "910"),
        ("9", "92", "920"),
        ("10", "101", "1010"),
        ("10", "102", "1020"),
    ]
    assert all(a.AccountId == "1" for a in ads)
    headline = ads[0].model_dump()["Headlines"][0]
    assert headline == {"Asset": {"Id": "5", "Text": "hi"}, "AssetPerformanceLabel": "Good"}
    ads_calls = [(b, h) for s, b, h in http.calls if s == "/Ads/QueryByAdGroupId"]
    assert all(h == {"CustomerId": "900", "CustomerAccountId": "1"} for _, h in ads_calls)
    assert all("ResponsiveSearch" in b["AdTypes"] for b, _ in ads_calls)


@pytest.mark.asyncio
async def test_asset_groups_come_only_from_performance_max_campaigns():
    http = _session(
        {
            "/User/Query": _user(),
            "/Accounts/Search": _accounts(_account("1")),
            "/Campaigns/QueryByAccountId": _campaigns(
                {"1": ["10", "9", "8"]}, types={"10": "PerformanceMax", "8": "PerformanceMax"}
            ),
            "/AssetGroups/QueryByCampaignId": lambda body, headers: {
                "AssetGroups": [
                    {
                        "Id": f"{body['CampaignId']}{n}",
                        "Headlines": [
                            {"Asset": {"Id": "5", "Text": "hi"}, "AssetPerformanceLabel": "Good"}
                        ],
                    }
                    for n in ("2", "1")
                ],
                "PartialErrors": [],
            },
        },
        delays={"/AssetGroups/QueryByCampaignId:8": 0.05},
    )
    directory = AccountDirectory(http, [])

    groups = [g async for g in snapshot_asset_groups(http, directory, LOG)]

    assert [(g.AccountId, g.CampaignId, g.Id) for g in groups] == [
        ("1", "8", "81"),
        ("1", "8", "82"),
        ("1", "10", "101"),
        ("1", "10", "102"),
    ]
    # Campaign 9 is a Search campaign, which the endpoint doesn't accept.
    asked = [b["CampaignId"] for s, b, _ in http.calls if s == "/AssetGroups/QueryByCampaignId"]
    assert sorted(asked) == ["10", "8"]
    headline = groups[0].model_dump()["Headlines"][0]
    assert headline == {"Asset": {"Id": "5", "Text": "hi"}, "AssetPerformanceLabel": "Good"}


@pytest.mark.parametrize(
    "body, attempt, expected",
    [
        (_fault(0, "InternalError"), 1, True),
        (_fault(0, "InternalError"), 5, False),
        (_fault(117, "CallRateExceeded"), 1, False),
        (_fault(207, "ConcurrentRequestOverLimit"), 1, False),
        (b"<html>Bad Gateway</html>", 1, True),
        (b"<html>Bad Gateway</html>", 5, False),
    ],
)
def test_should_retry_only_internal_errors(body: bytes, attempt: int, expected: bool):
    assert should_retry(LOG, 500, None, body, attempt) is expected  # type: ignore[arg-type]


def test_authenticated_request_needs_developer_token():
    with pytest.raises(RuntimeError):
        with_developer_token(None, None, with_token=True)
    assert with_developer_token(None, None, with_token=False) is None

    assert with_developer_token("token", {"CustomerId": "1"}, with_token=True) == {
        "CustomerId": "1",
        "DeveloperToken": "token",
    }


@pytest.mark.parametrize(
    "environment, host, scope",
    [
        (None, "clientcenter.api.bingads.microsoft.com", "https://ads.microsoft.com/"),
        ("sandbox", "clientcenter.api.sandbox.bingads.microsoft.com", "https://si.ads.microsoft.com/"),
    ],
)
def test_environment_picks_hosts_and_token_scope(
    monkeypatch: pytest.MonkeyPatch, environment: str | None, host: str, scope: str
):
    # The sandbox rejects production-scoped tokens, so the refresh scope has to
    # follow the hosts.
    if environment is None:
        monkeypatch.delenv(ENVIRONMENT_VAR, raising=False)
    else:
        monkeypatch.setenv(ENVIRONMENT_VAR, environment)
    assert customer_management_url().startswith(f"https://{host}/")
    assert token_exchange_scope().startswith(scope)


def test_unknown_environment_fails(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(ENVIRONMENT_VAR, "staging")
    with pytest.raises(RuntimeError, match=ENVIRONMENT_VAR):
        token_exchange_scope()


@pytest.mark.asyncio
@pytest.mark.parametrize("expires_in, exchanges", [(timedelta(hours=-1), 1), (timedelta(hours=1), 0)])
async def test_validate_exchanges_only_an_expired_access_token(expires_in: timedelta, exchanges: int):
    http = _session(
        {
            "/oauth2/v2.0/token": lambda body, headers: {
                "access_token": "fresh",
                "expires_in": 3599,
                "refresh_token": "rotated",
            },
            "/User/Query": _user(),
            "/Accounts/Search": _accounts(_account("1")),
            "/Campaigns/QueryByAccountId": _campaigns({"1": []}),
        }
    )
    config = EndpointConfig.model_validate(
        {
            "credentials": {
                "credentials_title": "OAuth Credentials",
                "client_id": "id",
                "client_secret": "secret",
                "refresh_token": "stored",
                "developer_token": "token",
                "access_token": "stored",
                "access_token_expires_at": (datetime.now(tz=UTC) + expires_in).isoformat(),
            }
        }
    )

    await validate_credentials(LOG, http, config)

    assert sum(1 for s, _, _ in http.calls if s == "/oauth2/v2.0/token") == exchanges
    assert config.credentials.access_token == ("fresh" if exchanges else "stored")
    # Saving a rotated refresh token is left to capture startup.
    assert config.credentials.refresh_token == "stored"
