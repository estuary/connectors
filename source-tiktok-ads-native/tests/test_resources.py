"""Unit tests for the resources: config checks and validation, the starting
state, and its per-advertiser subtasks."""

import asyncio
import json
import logging
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from estuary_cdk.flow import ValidationError as ConfigError
from estuary_cdk.http import HTTPError
from pydantic import ValidationError
from source_tiktok_ads_native.api import (
    ENTITY_HISTORY_START,
    AdvertiserAccounts,
    InaccessibleAdvertiserError,
    NoAdvertisersError,
    resolve_advertiser_ids,
)
from source_tiktok_ads_native.api.entities import EVENTUAL_CONSISTENCY_DELAY
from source_tiktok_ads_native.models import EndpointConfig, ResourceState
from estuary_cdk.capture import common
from source_tiktok_ads_native.resources import (
    DEFAULT_REPORT_HISTORY,
    _add_new_advertisers,
    _per_advertiser_state,
    all_resources,
    validate_credentials,
)

from .stubs import ReportStub, stub_client

LOG = logging.getLogger(__name__)
CUTOFF = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)


class TaskStub:
    """Stand-in for Task, recording checkpoints."""

    def __init__(self):
        self.log = LOG
        self.checkpoints: list[Any] = []

    async def checkpoint(self, state: Any, merge_patch: bool = True) -> None:
        self.checkpoints.append(state)


async def _initial_states(**config: Any) -> dict[str, ResourceState]:
    endpoint = EndpointConfig.model_validate(
        {
            "credentials": {"credentials_title": "Private App Credentials", "access_token": "t"},
            "advertiser_ids": ["1001", "1002"],
            **config,
        }
    )
    # Configured advertiser IDs need no API call to resolve.
    resources = await all_resources(LOG, SimpleNamespace(), endpoint)  # type: ignore[arg-type]
    return {r.name: r.initial_state for r in resources if r.initial_state is not None}


@pytest.mark.asyncio
async def test_each_advertiser_starts_with_an_entity_state_meeting_at_the_cutoff():
    campaigns = (await _initial_states())["Campaigns"]

    assert isinstance(campaigns.inc, dict) and isinstance(campaigns.backfill, dict)
    assert set(campaigns.inc) == set(campaigns.backfill) == {"1001", "1002"}
    for advertiser_id in ("1001", "1002"):
        cutoff = campaigns.backfill[advertiser_id].cutoff
        assert isinstance(cutoff, datetime) and cutoff.microsecond == 0
        # The seam lags the present like every poll's horizon.
        assert cutoff <= datetime.now(tz=UTC) - EVENTUAL_CONSISTENCY_DELAY
        # The backfill ends at `cutoff - 1s`; the first incremental window
        # `(cursor, horizon]` must begin exactly at `cutoff`.
        assert campaigns.inc[advertiser_id].cursor == cutoff - timedelta(seconds=1)


@pytest.mark.asyncio
async def test_entity_keys_lead_with_the_advertiser():
    endpoint = EndpointConfig.model_validate(
        {
            "credentials": {"credentials_title": "Private App Credentials", "access_token": "t"},
            "advertiser_ids": ["1001"],
        }
    )
    resources = await all_resources(LOG, SimpleNamespace(), endpoint)  # type: ignore[arg-type]

    keys = {r.name: r.key for r in resources}
    assert keys["Campaigns"] == ["/advertiser_id", "/campaign_id"]
    assert keys["AdGroups"] == ["/advertiser_id", "/adgroup_id"]
    assert keys["Ads"] == ["/advertiser_id", "/ad_id"]


@pytest.mark.asyncio
async def test_each_advertiser_starts_with_a_report_state_meeting_at_the_cutoff():
    custom = [{"name": "life", "granularity": "lifetime", "metrics": "spend"}]
    states = await _initial_states(custom_reports=custom)

    daily = states["ad_report_daily"]
    assert isinstance(daily.inc, dict) and isinstance(daily.backfill, dict)
    for advertiser_id in ("1001", "1002"):
        # The backfill reads up to the day before the cutoff's date, and the first
        # sweep re-reads from the cursor's date: the same instant.
        assert daily.inc[advertiser_id].cursor == daily.backfill[advertiser_id].cutoff

    lifetime = states["custom_report_life"]
    assert isinstance(lifetime.inc, dict) and set(lifetime.inc) == {"1001", "1002"}
    assert lifetime.backfill is None


@pytest.mark.asyncio
async def test_an_advertiser_added_later_gets_its_own_backfill():
    initial = _per_advertiser_state(["a1", "a2"], CUTOFF - timedelta(seconds=1), CUTOFF)
    caught_up = ResourceState.Incremental(cursor=CUTOFF + timedelta(days=30))
    # a1 has captured for a month and finished its backfill, which removed its
    # backfill state.
    state = ResourceState(inc={"a1": caught_up}, backfill=None)
    task = TaskStub()

    await _add_new_advertisers(SimpleNamespace(stateKey="Campaigns"), state, initial, task)  # type: ignore[arg-type]

    assert isinstance(state.inc, dict) and isinstance(initial.inc, dict)
    assert isinstance(initial.backfill, dict)
    assert state.inc == {"a1": caught_up, "a2": initial.inc["a2"]}
    assert state.backfill == {"a2": initial.backfill["a2"]}
    assert len(task.checkpoints) == 1


@pytest.mark.asyncio
async def test_known_advertisers_leave_the_state_alone():
    initial = _per_advertiser_state(["a1"], CUTOFF - timedelta(seconds=1), CUTOFF)
    state = ResourceState(inc={"a1": ResourceState.Incremental(cursor=CUTOFF)}, backfill=None)
    task = TaskStub()

    await _add_new_advertisers(SimpleNamespace(stateKey="Campaigns"), state, initial, task)  # type: ignore[arg-type]

    assert state.backfill is None
    assert task.checkpoints == []


class InfoStub:
    """`advertiser/info`, answering each requested advertiser's time zone."""

    def __init__(self, zones: dict[str, str]):
        self.zones = zones

    async def request(self, log: Any, url: str, method: str = "GET", params: Any = None, **kwargs: Any) -> bytes:
        ids = json.loads(params["advertiser_ids"])
        unknown = [i for i in ids if i not in self.zones]
        if unknown:
            message = f"The advertiser {unknown[0]} doesn't exist or has been deleted."
            return json.dumps({"code": 40001, "message": message, "data": {}}).encode()
        accounts = [
            {"advertiser_id": i, "create_time": 1791482410, "display_timezone": self.zones[i]}
            for i in ids
        ]
        return json.dumps({"code": 0, "message": "OK", "data": {"list": accounts}}).encode()


@pytest.mark.asyncio
async def test_each_subtask_reads_its_own_advertiser_in_its_own_zone(monkeypatch):
    opened: dict[str, dict[str, Any]] = {}

    def record(binding: Any, index: int, state: Any, task: Any, **fetch_fns: Any) -> None:
        opened[binding.stateKey] = fetch_fns

    monkeypatch.setattr(common, "open_binding", record)
    zones = {"1001": "UTC", "1002": "Asia/Tokyo"}
    endpoint = EndpointConfig.model_validate(
        {
            "credentials": {"credentials_title": "Private App Credentials", "access_token": "t"},
            "advertiser_ids": list(zones),
        }
    )
    bindings = [r for r in await all_resources(LOG, InfoStub(zones), endpoint) if r.name != "Advertisers"]  # type: ignore[arg-type]

    for resource in bindings:
        # The binding has captured 1001 only; 1002 was added to the config later.
        state = deepcopy(resource.initial_state)
        assert isinstance(state.inc, dict)
        del state.inc["1002"]
        if isinstance(state.backfill, dict):
            del state.backfill["1002"]

        await resource.open(SimpleNamespace(stateKey=resource.name), 0, state, TaskStub(), [])  # type: ignore[arg-type]

        assert set(state.inc) == {"1001", "1002"}, resource.name
        for kind in ("fetch_changes", "fetch_page"):
            for advertiser_id, fn in (opened[resource.name].get(kind) or {}).items():
                # Every fetch function is bound as (client, advertiser_id, ...).
                assert fn.args[1] == advertiser_id, (resource.name, kind)
                # Entities backfill their full history, whatever start_date says.
                if kind == "fetch_page" and resource.name in ("Campaigns", "AdGroups", "Ads"):
                    assert fn.args[2] == ENTITY_HISTORY_START, resource.name
                if resource.name == "ad_report_daily":
                    assert fn.args[2] == ZoneInfo(zones[advertiser_id]), kind
    assert {"Campaigns", "AdGroups", "Ads", "ad_report_daily"} <= set(opened)


@pytest.mark.parametrize("start_date", [None, "2026-11-01T00:00:00Z"], ids=["blank", "set"])
@pytest.mark.asyncio
async def test_only_a_start_date_the_user_set_holds_back_the_report_incremental(monkeypatch, start_date: str | None):
    opened: dict[str, Any] = {}
    monkeypatch.setattr(common, "open_binding", lambda binding, *args, **fetch_fns: opened.update(fetch_fns))
    endpoint = EndpointConfig.model_validate(
        {
            "credentials": {"credentials_title": "Private App Credentials", "access_token": "t"},
            "advertiser_ids": ["1001"],
            "start_date": start_date,
        }
    )
    resources = await all_resources(LOG, InfoStub({"1001": "UTC"}), endpoint)  # type: ignore[arg-type]
    report = next(r for r in resources if r.name == "ad_report_daily")
    state = deepcopy(report.initial_state)
    await report.open(SimpleNamespace(stateKey=report.name), 0, state, TaskStub(), [])  # type: ignore[arg-type]

    # Both are bound as (client, advertiser_id, zone, report, model, start, ...).
    incremental_start = opened["fetch_changes"]["1001"].args[5]
    backfill_start = opened["fetch_page"]["1001"].args[5]
    if start_date is None:
        assert isinstance(state.backfill, dict)
        backfill = state.backfill["1001"]
        assert backfill is not None and isinstance(backfill.cutoff, datetime)
        assert incremental_start is None
        assert backfill_start == backfill.cutoff - DEFAULT_REPORT_HISTORY
    else:
        assert incremental_start == backfill_start == endpoint.start_date


@pytest.mark.asyncio
async def test_entity_bindings_fail_at_start_up_for_an_advertiser_tiktok_cannot_read(monkeypatch):
    monkeypatch.setattr(common, "open_binding", lambda *args, **kwargs: None)
    endpoint = EndpointConfig.model_validate(
        {
            "credentials": {"credentials_title": "Private App Credentials", "access_token": "t"},
            "advertiser_ids": ["1001", "1002"],
        }
    )
    # 1002 has since been deleted.
    resources = await all_resources(LOG, InfoStub({"1001": "UTC"}), endpoint)  # type: ignore[arg-type]
    campaigns = next(r for r in resources if r.name == "Campaigns")

    with pytest.raises(InaccessibleAdvertiserError, match="Remove it from Advertiser IDs"):
        await campaigns.open(SimpleNamespace(stateKey="Campaigns"), 0, deepcopy(campaigns.initial_state), TaskStub(), [])  # type: ignore[arg-type]


# --- Configuration and validation ---------------------------------------------


def _zones(http: ReportStub) -> AdvertiserAccounts:
    return AdvertiserAccounts(stub_client(http), list(http.zones))


def _token_config(**overrides: Any) -> EndpointConfig:
    return EndpointConfig.model_validate(
        {
            "credentials": {"credentials_title": "Private App Credentials", "access_token": "t"},
            **overrides,
        }
    )


@pytest.mark.asyncio
async def test_an_access_token_needs_advertiser_ids():
    http = ReportStub({"a1": "UTC"})

    with pytest.raises(ConfigError, match="Advertiser IDs are required"):
        await validate_credentials(LOG, http, _token_config())  # type: ignore[arg-type]

    assert http.report_requests == []


@pytest.mark.asyncio
async def test_an_access_token_with_advertiser_ids_validates():
    # The stub answers `advertiser/info` and reports only; a call to
    # `oauth2/advertiser/get` would fail.
    http = ReportStub({"1001": "UTC"})

    await validate_credentials(LOG, http, _token_config(advertiser_ids=["1001"]))  # type: ignore[arg-type]

    # One single-row check of the default report.
    assert len(http.report_requests) == 1


@pytest.mark.asyncio
async def test_configured_advertiser_ids_need_no_app_credentials():
    http = ReportStub({"a1": "UTC"})

    assert await resolve_advertiser_ids(LOG, stub_client(http), ["a1"], None, None) == ["a1"]
    assert http.oauth_params is None


@pytest.mark.asyncio
async def test_validation_reads_configured_advertisers_in_batches_of_100():
    ids = [str(1000 + i) for i in range(150)]
    http = ReportStub({i: "UTC" for i in ids})

    await validate_credentials(LOG, http, _token_config(advertiser_ids=ids))  # type: ignore[arg-type]

    assert http.info_batches == [100, 50]


@pytest.mark.asyncio
async def test_concurrent_zone_lookups_read_advertiser_info_once():
    http = ReportStub({"a1": "UTC", "a2": "Asia/Tokyo"})
    zones = _zones(http)

    found = await asyncio.gather(zones.zone(LOG, "a1"), zones.zone(LOG, "a2"))

    assert found == [ZoneInfo("UTC"), ZoneInfo("Asia/Tokyo")]
    assert http.info_batches == [2]


@pytest.mark.asyncio
async def test_an_advertiser_missing_from_advertiser_info_fails_clearly():
    http = ReportStub({"a1": "UTC"})

    with pytest.raises(RuntimeError, match="no account details for advertiser a2"):
        await _zones(http).zone(LOG, "a2")


def _oauth_config(**overrides: Any) -> EndpointConfig:
    return EndpointConfig.model_validate(
        {
            "credentials": {
                "credentials_title": "OAuth Credentials",
                "client_id": "app",
                "client_secret": "secret",
                "access_token": "t",
            },
            **overrides,
        }
    )


@pytest.mark.asyncio
async def test_validation_reports_every_report_error_at_once():
    http = ReportStub({"1001": "UTC"}, error_code=40002)
    config = _token_config(advertiser_ids=["1001"], custom_reports=[{"name": "extra", "metrics": "spend"}])

    with pytest.raises(ConfigError) as err:
        await validate_credentials(LOG, http, config)  # type: ignore[arg-type]

    assert len(err.value.errors) == 2
    assert "ad_report_daily" in err.value.errors[0]
    assert "custom_report_extra" in err.value.errors[1]


@pytest.mark.asyncio
async def test_oauth_without_advertiser_ids_lists_the_authorized_ones():
    http = ReportStub({"a1": "UTC", "a2": "America/Los_Angeles"})

    await validate_credentials(LOG, http, _oauth_config())  # type: ignore[arg-type]

    # Listing advertisers authenticates with the app ID and secret.
    assert http.oauth_params == {"app_id": "app", "secret": "secret"}
    assert [p["advertiser_id"] for p in http.report_requests] == ["a1"]


@pytest.mark.asyncio
async def test_a_sandbox_account_needs_advertiser_ids():
    http = ReportStub({"a1": "UTC"})

    with pytest.raises(ConfigError, match="Sandbox"):
        await validate_credentials(LOG, http, _oauth_config(advanced={"is_sandbox": True}))  # type: ignore[arg-type]

    # The sandbox has no `oauth2/advertiser/get`.
    assert http.oauth_params is None


@pytest.mark.parametrize(
    "config, message",
    [
        (_token_config(), "Advertiser IDs are required"),
        (_oauth_config(advanced={"is_sandbox": True}), "Sandbox"),
        (_token_config(credentials={"credentials_title": "Private App Credentials", "access_token": " "}), "didn.t return an access token"),
    ],
    ids=["access-token-without-ids", "sandbox-without-ids", "empty-token"],
)
@pytest.mark.asyncio
async def test_discover_explains_config_mistakes_before_any_request(config: EndpointConfig, message: str):
    # The web app runs discover before validate.
    http = ReportStub({"a1": "UTC"})

    with pytest.raises(ConfigError, match=message):
        await all_resources(LOG, http, config)  # type: ignore[arg-type]

    assert http.oauth_params is None and http.info_batches == []


@pytest.mark.asyncio
async def test_an_advertiser_tiktok_cannot_read_fails_validation_with_advice():
    http = ReportStub({"1001": "UTC"})

    with pytest.raises(ConfigError) as err:
        await validate_credentials(LOG, http, _token_config(advertiser_ids=["1001", "1009"]))  # type: ignore[arg-type]

    # TikTok's own message names the advertiser; ours says what to do.
    (message,) = err.value.errors
    assert "1009 doesn't exist or has been deleted" in message
    assert "Remove it from Advertiser IDs" in message


@pytest.mark.asyncio
async def test_an_oauth_advertiser_tiktok_cannot_read_fails_validation():
    # Listed as authorized, but `advertiser/info` can't read it, which would
    # otherwise only fail once the capture starts.
    http = ReportStub({"1001": "UTC"}, unreadable={"1009"})

    with pytest.raises(ConfigError, match="enter the IDs of the advertisers to capture without it"):
        await validate_credentials(LOG, http, _oauth_config())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_an_advertiser_tiktok_cannot_read_fails_the_zone_lookup_with_advice():
    http = ReportStub({"1001": "UTC"}, unreadable={"1009"})
    zones = AdvertiserAccounts(stub_client(http), ["1001", "1009"])

    with pytest.raises(InaccessibleAdvertiserError, match="enter the IDs of the advertisers"):
        await zones.zone(LOG, "1001")


def test_advertiser_ids_carry_a_digits_pattern_for_the_web_app():
    schema = EndpointConfig.model_json_schema()

    assert schema["properties"]["advertiser_ids"]["items"]["pattern"] == "^[0-9]+$"


@pytest.mark.parametrize("advertiser_id", ["", " 123 ", "12a", "７"])
def test_an_advertiser_id_that_is_not_digits_is_rejected(advertiser_id: str):
    with pytest.raises(ValidationError, match="pattern"):
        _token_config(advertiser_ids=["123", advertiser_id])


def test_repeated_advertiser_ids_are_dropped():
    config = _token_config(advertiser_ids=["123", "456", "123"])

    assert config.advertiser_ids == ["123", "456"]


@pytest.mark.asyncio
async def test_an_http_error_listing_advertisers_never_carries_the_app_secret():
    app_secret = "estuary-app-s3cret"
    http = ReportStub({"1001": "UTC"}, oauth_http_status=400)

    with pytest.raises(HTTPError) as err:
        await resolve_advertiser_ids(LOG, stub_client(http), [], "app", app_secret)
    assert err.value.code == 400
    assert app_secret not in str(err.value)
    # The original error, which quotes the URL, isn't chained into tracebacks.
    assert err.value.__cause__ is None and err.value.__suppress_context__

    config = _oauth_config()
    config.credentials.client_secret = app_secret
    with pytest.raises(ConfigError) as validation:
        await validate_credentials(LOG, http, config)  # type: ignore[arg-type]
    assert app_secret not in str(validation.value)


@pytest.mark.asyncio
async def test_an_oauth_account_with_no_advertisers_raises_rather_than_capturing_nothing():
    # A running capture re-lists its advertisers at start-up; none would empty
    # the Advertisers collection.
    http = ReportStub({})

    with pytest.raises(NoAdvertisersError):
        await resolve_advertiser_ids(LOG, stub_client(http), [], "app", "secret")
    with pytest.raises(ConfigError, match="has no advertisers"):
        await validate_credentials(LOG, http, _oauth_config())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_a_permission_error_naming_no_advertiser_gets_no_advertiser_advice():
    # 40001 is TikTok's code for any missing permission.
    http = ReportStub({"1001": "UTC"}, info_error=(40001, "No permission to perform the related operation."))

    with pytest.raises(ConfigError) as err:
        await validate_credentials(LOG, http, _token_config(advertiser_ids=["1001"]))  # type: ignore[arg-type]

    (message,) = err.value.errors
    assert "No permission" in message and "Remove it from Advertiser IDs" not in message


@pytest.mark.asyncio
async def test_an_empty_access_token_fails_validation_before_any_request():
    # What a failed OAuth sign-in leaves in the config.
    http = ReportStub({"1001": "UTC"})
    config = _oauth_config()
    config.credentials.access_token = ""

    with pytest.raises(ConfigError, match="didn't return an access token"):
        await validate_credentials(LOG, http, config)  # type: ignore[arg-type]

    assert http.oauth_params is None and http.info_batches == []


def test_the_access_token_option_is_titled_for_the_web_app():
    schema = EndpointConfig.model_json_schema()
    titles = {d.get("title") for d in schema["$defs"].values()}

    assert "Access Token" in titles and "AccessToken" not in titles
