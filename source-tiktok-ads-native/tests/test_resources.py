"""Unit tests for the resources' starting state and its per-advertiser subtasks."""

import json
import logging
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from source_tiktok_ads_native.api import (
    ENTITY_HISTORY_START,
    EVENTUAL_CONSISTENCY_DELAY,
    InaccessibleAdvertiserError,
)
from source_tiktok_ads_native.models import EndpointConfig, ResourceState
from estuary_cdk.capture import common
from source_tiktok_ads_native.resources import (
    _add_new_advertisers,
    _per_advertiser_state,
    all_resources,
)

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
