"""Unit tests for the parts of the entity fetch layer the snapshot tests cannot
reach.

`tests/test_snapshots.py` captures a live sandbox account, so it only exercises
whatever that account holds and walks a single backfill window. These cover:

  - backfill windows: each queries its exclusive creation start bound one second
    early, adjacent windows share a boundary, and the last ends at the cutoff;
  - the incremental window `(cursor, horizon]`, applied client-side;
  - deleted objects, read with the deleted-only filter, emitted as deletions,
    including cascades and deletions made while the backfill walks;
  - paging, the two buying-type listings and their page sizes;
  - TikTok's in-body error codes and throttling, which arrive with HTTP 200.
"""

import asyncio
import json
import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncGenerator
from zoneinfo import ZoneInfoNotFoundError

import pytest
from pydantic import ValidationError
from source_tiktok_ads_native import api
from source_tiktok_ads_native.api import (
    BACKFILL_WINDOW,
    TikTokClient,
    TikTokAPIError,
    backfill_ad_groups,
    backfill_campaigns,
    fetch_ad_groups,
    fetch_ads,
    fetch_campaigns,
    snapshot_advertisers,
)
from source_tiktok_ads_native.models import Ad, AdGroup, Advertiser, Campaign

LOG = logging.getLogger(__name__)
# Captured before any test swaps out `asyncio.sleep`.
_real_sleep = asyncio.sleep
ADVERTISER_ID = "7694356963175268360"
CURSOR = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)


def _client(http: Any) -> TikTokClient:
    """An unpaced client over a stub session."""
    return TikTokClient(http, api.API, math.inf)


TOP_VIEW = "RESERVATION_TOP_VIEW"


class StubSession:
    """Stand-in for HTTPSession that answers entity listings by TikTok's rules.

    `listings` maps (endpoint path, `secondary_status`) to the objects in that
    listing; an unlisted pair is empty. An object is listed only for its own
    buying type, from its stub-only `buying_type` key (`AUCTION` when absent).
    The stub applies the creation-time filters (both bounds exclusive) and
    `modified_after`, pages by `page` and `page_size`, and rejects what TikTok
    rejects: `RESERVATION_TOP_VIEW` combined with another buying type, and a
    top-view ad group page over 100. `later` replaces a pair's objects from its
    second request on, for a change made between two reads. `error_codes`
    answer requests in order before any normal response; a 0 answers normally.

    Signatures mirror HTTPSession positionally, and serving encoded bytes keeps
    the real IncrementalJsonProcessor and Pydantic parsing inside the test."""

    def __init__(
        self,
        listings: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
        error_codes: list[int] | None = None,
        later: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
    ):
        self.listings = listings or {}
        self.later = later or {}
        self.reads: dict[tuple[str, str], int] = {}
        self.error_codes = list(error_codes or [])
        self.requests: list[tuple[str, dict[str, Any]]] = []

    async def request_stream(
        self,
        log: logging.Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[dict[str, str], Any]:
        params = params or {}
        self.requests.append((url, params))
        code = self.error_codes.pop(0) if self.error_codes else 0
        if code:
            return {}, _body({"code": code, "message": "bad", "data": {}})

        path = url.removeprefix(api.API + "/")
        filters = json.loads(params["filtering"])
        buying_types = set(filters["buying_types"])
        page, page_size = int(params["page"]), int(params["page_size"])
        if TOP_VIEW in buying_types and len(buying_types) > 1:
            return {}, _body({"code": 40002, "message": "top view combined", "data": {}})
        if TOP_VIEW in buying_types and path == AdGroup.PATH and page_size > 100:
            return {}, _body({"code": 40002, "message": "page_size over 100", "data": {}})

        listed = [
            o
            for o in self._objects(path, filters.get("secondary_status", ""))
            if o.get("buying_type", "AUCTION") in buying_types and _passes(o, filters)
        ]
        pages = [listed[i : i + page_size] for i in range(0, len(listed), page_size)]
        return {}, _body(
            {
                "code": 0,
                "message": "OK",
                "data": {
                    "list": pages[page - 1] if page <= len(pages) else [],
                    "page_info": {"page": page, "total_page": len(pages)},
                },
            }
        )

    def _objects(self, path: str, status: str) -> list[dict[str, Any]]:
        key = (path, status)
        self.reads[key] = self.reads.get(key, 0) + 1
        if self.reads[key] > 1 and key in self.later:
            return self.later[key]
        return self.listings.get(key, [])

    async def request(
        self,
        log: logging.Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> bytes:
        """`advertiser/info`: echoes each requested advertiser, in reverse order
        so that callers must sort for themselves."""
        params = params or {}
        self.requests.append((url, params))
        code = self.error_codes.pop(0) if self.error_codes else 0
        if code:
            return json.dumps({"code": code, "message": "bad", "data": {}}).encode()
        ids = json.loads(params["advertiser_ids"])
        advertisers = [
            {"advertiser_id": i, "create_time": 1791482410, "name": f"adv {i}", "timezone": "Etc/GMT+8", "display_timezone": "America/Los_Angeles"}
            for i in reversed(ids)
        ]
        return json.dumps(
            {"code": 0, "message": "OK", "data": {"list": advertisers}}
        ).encode()


def _passes(obj: dict[str, Any], filters: dict[str, Any]) -> bool:
    """TikTok's time filters. Its timestamps sort as strings."""
    after = filters.get("creation_filter_start_time")
    before = filters.get("creation_filter_end_time")
    modified_after = filters.get("modified_after")
    return (
        (after is None or obj["create_time"] > after)
        and (before is None or obj["create_time"] < before)
        and (modified_after is None or obj["modify_time"] > modified_after)
    )


def _body(payload: dict[str, Any]) -> Any:
    body = json.dumps(payload).encode()

    async def gen() -> AsyncGenerator[bytes, None]:
        yield body

    return gen


def campaigns(
    live: list[dict[str, Any]] | None = None, deleted: list[dict[str, Any]] | None = None
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Campaign listings as TikTok returns them: the all-status filter lists live
    and deleted campaigns, the deleted filter only deleted ones."""
    live, deleted = live or [], deleted or []
    return {
        (Campaign.PATH, Campaign.ALL_STATUS): live + deleted,
        (Campaign.PATH, Campaign.DELETED_STATUS): deleted,
    }


def _campaign(campaign_id: str, modified: datetime) -> dict[str, Any]:
    stamp = modified.strftime("%Y-%m-%d %H:%M:%S")
    return {"campaign_id": campaign_id, "create_time": stamp, "modify_time": stamp}


async def _drain(gen: AsyncGenerator[Any, None]) -> list[Any]:
    return [item async for item in gen]


def _statuses(http: StubSession) -> set[str]:
    return {json.loads(p["filtering"]).get("secondary_status") for _, p in http.requests}


def _creation_bounds(http: StubSession) -> set[tuple[str, str]]:
    bounds = set()
    for _, params in http.requests:
        filters = json.loads(params["filtering"])
        bounds.add(
            (filters["creation_filter_start_time"], filters["creation_filter_end_time"])
        )
    return bounds


@pytest.mark.parametrize(
    "start_date",
    [
        datetime(2026, 1, 1, tzinfo=UTC),
        # The default start date, 30 days ago, carries microseconds.
        datetime(2026, 1, 1, 0, 0, 0, 500000, tzinfo=UTC),
    ],
)
@pytest.mark.asyncio
async def test_backfill_windows_run_newest_first_and_share_boundaries(start_date: datetime):
    cutoff = datetime(2026, 7, 20, tzinfo=UTC)

    def stamp(value: datetime) -> str:
        return value.strftime("%Y-%m-%d %H:%M:%S")

    http = StubSession()
    first = await _drain(
        backfill_campaigns(_client(http), ADVERTISER_ID, start_date, LOG, None, cutoff)
    )
    newest_start = cutoff - BACKFILL_WINDOW
    # The newest window ends at the cutoff; its start bound is exclusive, so it
    # is queried one second before the window starts.
    assert _creation_bounds(http) == {(stamp(newest_start - timedelta(seconds=1)), stamp(cutoff))}
    assert first == [newest_start.isoformat()]

    http = StubSession()
    second = await _drain(
        backfill_campaigns(_client(http), ADVERTISER_ID, start_date, LOG, first[0], cutoff)
    )
    # The next, older window ends exactly where the newer one started, and the
    # last starts at the start date without yielding a PageCursor.
    assert _creation_bounds(http) == {(stamp(start_date - timedelta(seconds=1)), stamp(newest_start))}
    assert second == []


@pytest.mark.asyncio
async def test_backfill_emits_live_objects_only():
    created = datetime(2026, 3, 1, tzinfo=UTC)
    http = StubSession(
        campaigns(live=[_campaign("live", created)], deleted=[_campaign("deleted", created)])
    )

    items = await _drain(
        backfill_campaigns(  # type: ignore[arg-type]
            _client(http),
            ADVERTISER_ID,
            datetime(2026, 1, 1, tzinfo=UTC),
            LOG,
            None,
            datetime(2026, 4, 1, tzinfo=UTC),
        )
    )

    assert [i.campaign_id for i in items if isinstance(i, Campaign)] == ["live"]
    # The listing covers every status, so a deletion mid-walk cannot shift a page.
    assert _statuses(http) == {Campaign.ALL_STATUS, Campaign.DELETED_STATUS}


@pytest.mark.asyncio
async def test_backfill_leaves_objects_changed_since_the_cutoff_to_the_incremental():
    cutoff = datetime(2026, 4, 1, tzinfo=UTC)
    created = "2026-03-01 00:00:00"
    # Deleted after the backfill's deleted listing, so the walk still sees it as
    # live; the incremental emits its deletion, which the backfill must not undo.
    changed = {"campaign_id": "changed", "create_time": created, "modify_time": "2026-04-01 00:00:00"}
    unchanged = {"campaign_id": "unchanged", "create_time": created, "modify_time": "2026-03-31 23:59:59"}
    http = StubSession(campaigns(live=[changed, unchanged]))

    items = await _drain(
        backfill_campaigns(  # type: ignore[arg-type]
            _client(http), ADVERTISER_ID, datetime(2026, 1, 1, tzinfo=UTC), LOG, None, cutoff
        )
    )

    assert {i.campaign_id for i in items if isinstance(i, Campaign)} == {"unchanged"}


@pytest.mark.asyncio
async def test_a_cascade_during_the_backfill_walk_is_emitted_after_the_live_copy():
    child = _ad_group("g1", "c1", datetime(2026, 3, 1, tzinfo=UTC))
    # Its campaign is deleted after the backfill's first cascade listing, while
    # it walks: the walk still sees the ad group as live.
    http = StubSession(
        {(AdGroup.PATH, AdGroup.ALL_STATUS): [child]},
        later={(AdGroup.PATH, CAMPAIGN_DELETE): [child]},
    )

    items = await _drain(
        backfill_ad_groups(  # type: ignore[arg-type]
            _client(http), ADVERTISER_ID, datetime(2026, 1, 1, tzinfo=UTC), LOG, None,
            datetime(2026, 4, 1, tzinfo=UTC),
        )
    )

    emitted = [(i.adgroup_id, i.meta_.op) for i in items if isinstance(i, AdGroup)]
    assert emitted == [("g1", "u"), ("g1", "d")]


@pytest.mark.asyncio
async def test_incremental_window_is_cursor_exclusive_horizon_inclusive(monkeypatch):
    horizon = CURSOR + timedelta(minutes=10)
    monkeypatch.setattr(api, "settled_second", lambda: horizon)
    http = StubSession(
        campaigns(
            live=[
                _campaign("at_cursor", CURSOR),
                _campaign("after_cursor", CURSOR + timedelta(seconds=1)),
                _campaign("at_horizon", horizon),
                _campaign("after_horizon", horizon + timedelta(seconds=1)),
            ]
        )
    )

    items = await _drain(fetch_campaigns(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]

    docs = [i for i in items if isinstance(i, Campaign)]
    assert sorted(d.campaign_id for d in docs) == ["after_cursor", "at_horizon"]
    # The cursor advances to the horizon, after the documents.
    assert items[-1] == horizon


@pytest.mark.asyncio
async def test_incremental_emits_deleted_objects_as_deletions(monkeypatch):
    horizon = CURSOR + timedelta(minutes=10)
    monkeypatch.setattr(api, "settled_second", lambda: horizon)
    http = StubSession(
        campaigns(
            live=[_campaign("paused", CURSOR + timedelta(minutes=1))],
            deleted=[_campaign("deleted", CURSOR + timedelta(minutes=2))],
        )
    )

    items = await _drain(fetch_campaigns(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]

    ops = {(d.campaign_id, d.meta_.op) for d in items if isinstance(d, Campaign)}
    assert ("deleted", "d") in ops
    assert ("paused", "d") not in ops
    # Deleted ones are found with the deleted-only filter but read from the
    # all-status listing, which a deletion mid-walk cannot shift.
    assert _statuses(http) == {Campaign.ALL_STATUS, Campaign.DELETED_STATUS}


@pytest.mark.asyncio
async def test_incremental_returns_early_without_a_full_new_second(monkeypatch):
    monkeypatch.setattr(api, "settled_second", lambda: CURSOR)
    http = StubSession(campaigns(live=[_campaign("any", CURSOR)]))

    items = await _drain(fetch_campaigns(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]

    assert items == []
    assert http.requests == []


@pytest.mark.asyncio
async def test_in_body_error_code_raises(monkeypatch):
    monkeypatch.setattr(api, "settled_second", lambda: CURSOR + timedelta(minutes=1))
    http = StubSession(error_codes=[40002])

    with pytest.raises(TikTokAPIError):
        await _drain(fetch_campaigns(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_throttled_page_is_retried(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    horizon = CURSOR + timedelta(minutes=10)
    monkeypatch.setattr(api, "settled_second", lambda: horizon)
    # TikTok throttles with HTTP 200 and an in-body code.
    http = StubSession(
        campaigns(live=[_campaign("after_throttle", CURSOR + timedelta(minutes=1))]),
        error_codes=[40100, 40133],
    )

    items = await _drain(fetch_campaigns(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]

    assert {i.campaign_id for i in items if isinstance(i, Campaign)} == {"after_throttle"}
    assert sleeps == [5, 10]


@pytest.mark.asyncio
async def test_persistent_throttling_eventually_raises(monkeypatch):
    async def fake_sleep(seconds: float) -> None:
        pass

    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(api, "settled_second", lambda: CURSOR + timedelta(minutes=1))
    http = StubSession(error_codes=[40100] * api.RETRY_ATTEMPTS)

    with pytest.raises(TikTokAPIError):
        await _drain(fetch_campaigns(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]


def _ad_group(adgroup_id: str, campaign_id: str, modified: datetime) -> dict[str, Any]:
    stamp = modified.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "adgroup_id": adgroup_id,
        "campaign_id": campaign_id,
        "create_time": stamp,
        "modify_time": stamp,
    }


CAMPAIGN_DELETE = AdGroup.CASCADES[0].status


def _ad_group_listings(
    live: list[dict[str, Any]],
    deleted: list[dict[str, Any]],
    cascaded: list[dict[str, Any]],
    deleted_campaigns: list[dict[str, Any]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    return {
        (AdGroup.PATH, AdGroup.ALL_STATUS): live + deleted + cascaded,
        (AdGroup.PATH, AdGroup.DELETED_STATUS): deleted,
        (AdGroup.PATH, CAMPAIGN_DELETE): cascaded,
        (Campaign.PATH, Campaign.DELETED_STATUS): deleted_campaigns,
    }


@pytest.mark.asyncio
async def test_campaign_deletion_cascades_to_its_ad_groups_once(monkeypatch):
    horizon = CURSOR + timedelta(minutes=10)
    monkeypatch.setattr(api, "settled_second", lambda: horizon)
    long_ago = CURSOR - timedelta(days=30)
    http = StubSession(
        _ad_group_listings(
            live=[_ad_group("live", "c_live", long_ago)],
            # Deleted directly: TikTok stamps the ad group's own modify_time.
            deleted=[_ad_group("direct", "c_live", CURSOR + timedelta(minutes=1))],
            # Cascaded: TikTok leaves the ad groups' modify_time untouched.
            cascaded=[
                _ad_group("cascade_now", "c_deleted_now", long_ago),
                _ad_group("cascade_before", "c_deleted_before", long_ago),
            ],
            deleted_campaigns=[
                _campaign("c_deleted_now", CURSOR + timedelta(minutes=2)),
                _campaign("c_deleted_before", CURSOR - timedelta(days=1)),
            ],
        )
    )

    items = await _drain(fetch_ad_groups(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]

    emitted = {(d.adgroup_id, d.meta_.op) for d in items if isinstance(d, AdGroup)}
    # The direct delete and the cascade from a campaign deleted in this window
    # are emitted as deletions; the unchanged live ad group and the cascade from
    # a campaign deleted before the window are not emitted at all.
    assert emitted == {("direct", "d"), ("cascade_now", "d")}
    assert items[-1] == horizon


@pytest.mark.asyncio
async def test_backfill_skips_direct_and_cascaded_deletions():
    created = datetime(2026, 3, 1, tzinfo=UTC)
    http = StubSession(
        _ad_group_listings(
            live=[_ad_group("live", "c_live", created)],
            deleted=[_ad_group("direct", "c_live", created)],
            cascaded=[_ad_group("cascaded", "c_deleted", created)],
            deleted_campaigns=[],
        )
    )

    items = await _drain(
        backfill_ad_groups(  # type: ignore[arg-type]
            _client(http),
            ADVERTISER_ID,
            datetime(2026, 1, 1, tzinfo=UTC),
            LOG,
            None,
            datetime(2026, 4, 1, tzinfo=UTC),
        )
    )

    assert {i.adgroup_id for i in items if isinstance(i, AdGroup)} == {"live"}


@pytest.mark.asyncio
async def test_advertisers_are_batched_by_100_in_id_order():
    ids = [f"{n:03d}" for n in range(150)]
    http = StubSession()
    advertisers = await _drain(snapshot_advertisers(_client(http), list(reversed(ids)), LOG))

    # TikTok rejects more than 100 ids per request.
    batches = [json.loads(p["advertiser_ids"]) for _, p in http.requests]
    assert [len(b) for b in batches] == [100, 50]
    assert batches[0] == ids[:100]
    # Snapshot rows are numbered in emission order, so the order must be stable.
    assert [a.advertiser_id for a in advertisers] == ids
    assert all(isinstance(a, Advertiser) for a in advertisers)
    assert advertisers[0].create_time == datetime(2026, 10, 8, 18, 0, 10, tzinfo=UTC)


def _ad(ad_id: str, adgroup_id: str, campaign_id: str, modified: datetime) -> dict[str, Any]:
    stamp = modified.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "ad_id": ad_id,
        "adgroup_id": adgroup_id,
        "campaign_id": campaign_id,
        "create_time": stamp,
        "modify_time": stamp,
    }


@pytest.mark.asyncio
async def test_ads_cascade_from_deleted_ad_groups_and_campaigns(monkeypatch):
    horizon = CURSOR + timedelta(minutes=10)
    monkeypatch.setattr(api, "settled_second", lambda: horizon)
    long_ago = CURSOR - timedelta(days=30)
    adgroup_delete, campaign_delete = (c.status for c in Ad.CASCADES)
    by_ad_group_now = _ad("by_ad_group_now", "ag_deleted_now", "c_live", long_ago)
    by_campaign_now = _ad("by_campaign_now", "ag_live", "c_deleted_now", long_ago)
    by_campaign_before = _ad("by_campaign_before", "ag_live", "c_deleted_before", long_ago)
    http = StubSession(
        {
            (Ad.PATH, Ad.ALL_STATUS): [by_ad_group_now, by_campaign_now, by_campaign_before],
            (Ad.PATH, Ad.DELETED_STATUS): [],
            (Ad.PATH, adgroup_delete): [by_ad_group_now],
            (Ad.PATH, campaign_delete): [by_campaign_now, by_campaign_before],
            (AdGroup.PATH, AdGroup.DELETED_STATUS): [
                _ad_group("ag_deleted_now", "c_live", CURSOR + timedelta(minutes=1))
            ],
            (Campaign.PATH, Campaign.DELETED_STATUS): [
                _campaign("c_deleted_now", CURSOR + timedelta(minutes=2)),
                _campaign("c_deleted_before", CURSOR - timedelta(days=1)),
            ],
        }
    )

    items = await _drain(fetch_ads(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]

    emitted = {(d.ad_id, d.meta_.op) for d in items if isinstance(d, Ad)}
    # Each cascade fires when its own parent was deleted in this window.
    assert emitted == {("by_ad_group_now", "d"), ("by_campaign_now", "d")}


@pytest.mark.asyncio
async def test_only_ads_send_modified_after_a_day_before_the_cursor(monkeypatch):
    monkeypatch.setattr(api, "settled_second", lambda: CURSOR + timedelta(minutes=1))

    for fetch, model in (
        (fetch_campaigns, Campaign),
        (fetch_ad_groups, AdGroup),
        (fetch_ads, Ad),
    ):
        http = StubSession()
        await _drain(fetch(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]

        main_listing = [
            json.loads(p["filtering"])
            for url, p in http.requests
            if url.endswith(model.PATH)
            and json.loads(p["filtering"]).get("secondary_status") == model.ALL_STATUS
        ]
        sent = {f.get("modified_after") for f in main_listing}
        day_early = (CURSOR - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        expected = day_early if model is Ad else None
        assert sent == {expected}, model.__name__


@pytest.mark.asyncio
async def test_ad_cascades_are_emitted_when_the_filtered_listing_omits_them(monkeypatch):
    horizon = CURSOR + timedelta(minutes=10)
    monkeypatch.setattr(api, "settled_second", lambda: horizon)
    long_ago = CURSOR - timedelta(days=30)
    _, campaign_delete = (c.status for c in Ad.CASCADES)
    cascaded = _ad("cascaded", "ag_live", "c_deleted_now", long_ago)
    http = StubSession(
        {
            # The modified-after listing cannot return the cascaded ad: a cascade
            # leaves its modify_time unchanged.
            (Ad.PATH, Ad.ALL_STATUS): [],
            (Ad.PATH, campaign_delete): [cascaded],
            (Campaign.PATH, Campaign.DELETED_STATUS): [
                _campaign("c_deleted_now", CURSOR + timedelta(minutes=2))
            ],
        }
    )

    items = await _drain(fetch_ads(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]

    assert {(d.ad_id, d.meta_.op) for d in items if isinstance(d, Ad)} == {("cascaded", "d")}


def test_an_advertiser_without_a_named_zone_fails_loudly():
    account = {"advertiser_id": "a1", "create_time": 1791482410, "timezone": "Etc/GMT+8"}

    with pytest.raises(ValidationError):
        Advertiser.model_validate(account)
    with pytest.raises(ZoneInfoNotFoundError):
        Advertiser.model_validate({**account, "display_timezone": "Mars/Olympus_Mons"}).zone()


def _campaigns_created(count: int, created: datetime, **extra: Any) -> list[dict[str, Any]]:
    return [{**_campaign(f"c{n:04}", created), **extra} for n in range(count)]


def _all_status_pages(http: StubSession, model: type) -> list[int]:
    return [
        int(p["page"])
        for url, p in http.requests
        if url.endswith(model.PATH)
        and json.loads(p["filtering"]).get("secondary_status") == model.ALL_STATUS
        and TOP_VIEW not in json.loads(p["filtering"])["buying_types"]
    ]


@pytest.mark.asyncio
async def test_listings_are_read_page_by_page():
    live = _campaigns_created(2500, datetime(2026, 3, 1, tzinfo=UTC))
    http = StubSession(campaigns(live=live))

    items = await _drain(
        backfill_campaigns(  # type: ignore[arg-type]
            _client(http), ADVERTISER_ID, datetime(2026, 1, 1, tzinfo=UTC), LOG, None,
            datetime(2026, 4, 1, tzinfo=UTC),
        )
    )

    assert len([i for i in items if isinstance(i, Campaign)]) == 2500
    assert _all_status_pages(http, Campaign) == [1, 2, 3]


@pytest.mark.asyncio
async def test_top_view_objects_come_from_their_own_listing():
    created = datetime(2026, 3, 1, tzinfo=UTC)
    auction = _ad_group("auction", "c1", created)
    top_view = {**_ad_group("top_view", "c2", created), "buying_type": TOP_VIEW}
    http = StubSession({(AdGroup.PATH, AdGroup.ALL_STATUS): [auction, top_view]})

    items = await _drain(
        backfill_ad_groups(  # type: ignore[arg-type]
            _client(http), ADVERTISER_ID, datetime(2026, 1, 1, tzinfo=UTC), LOG, None,
            datetime(2026, 4, 1, tzinfo=UTC),
        )
    )

    assert sorted(i.adgroup_id for i in items if isinstance(i, AdGroup)) == ["auction", "top_view"]
    # TikTok rejects top-view combined with other buying types, and a top-view
    # ad group page over 100.
    groups = {
        (tuple(sorted(json.loads(p["filtering"])["buying_types"])), int(p["page_size"]))
        for _, p in http.requests
    }
    assert groups == {(("AUCTION", "RESERVATION_RF"), 1000), ((TOP_VIEW,), 100)}


@pytest.mark.asyncio
async def test_throttle_attempts_restart_on_each_page(monkeypatch):
    async def fake_sleep(seconds: float) -> None:
        pass

    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    live = _campaigns_created(1500, datetime(2026, 3, 1, tzinfo=UTC))
    throttled = [40100, 40100, 40100]
    # The deleted listing, then each of the two pages throttled three times.
    # Counted across pages, the six throttles would exhaust the attempts.
    http = StubSession(campaigns(live=live), error_codes=[0, *throttled, 0, *throttled])

    items = await _drain(
        backfill_campaigns(  # type: ignore[arg-type]
            _client(http), ADVERTISER_ID, datetime(2026, 1, 1, tzinfo=UTC), LOG, None,
            datetime(2026, 4, 1, tzinfo=UTC),
        )
    )

    assert len([i for i in items if isinstance(i, Campaign)]) == 1500


@pytest.mark.asyncio
async def test_throttled_account_details_are_retried(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    http = StubSession(error_codes=[40100])
    advertisers = await _drain(snapshot_advertisers(_client(http), [ADVERTISER_ID], LOG))

    assert [a.advertiser_id for a in advertisers] == [ADVERTISER_ID]
    assert sleeps == [5]


@pytest.mark.asyncio
async def test_a_start_date_after_the_cutoff_backfills_nothing():
    http = StubSession(campaigns(live=[_campaign("any", datetime(2026, 3, 1, tzinfo=UTC))]))

    items = await _drain(
        backfill_campaigns(  # type: ignore[arg-type]
            _client(http), ADVERTISER_ID, datetime(2026, 5, 1, tzinfo=UTC), LOG, None,
            datetime(2026, 4, 1, tzinfo=UTC),
        )
    )

    assert items == []
    assert http.requests == []


@pytest.mark.asyncio
async def test_polls_stop_short_of_changes_listings_may_not_show_yet():
    now = datetime.now(tz=UTC)
    http = StubSession(
        campaigns(
            live=[
                _campaign("settled", now - timedelta(minutes=10)),
                _campaign("too_recent", now - timedelta(minutes=1)),
            ]
        )
    )

    items = await _drain(fetch_campaigns(_client(http), ADVERTISER_ID, LOG, now - timedelta(hours=1)))  # type: ignore[arg-type]

    assert [i.campaign_id for i in items if isinstance(i, Campaign)] == ["settled"]
    assert items[-1] <= now - api.EVENTUAL_CONSISTENCY_DELAY


class _RecordingHttp:
    """Records the fake clock's time at each request start."""

    def __init__(self, clock: list[float]):
        self.clock = clock
        self.started: list[float] = []

    async def request(self, log: Any, url: str, method: str = "GET", params: Any = None, **kwargs: Any) -> bytes:
        self.started.append(self.clock[0])
        return b"{}"


def _fake_clock(monkeypatch: Any) -> list[float]:
    clock = [100.0]

    async def fake_sleep(seconds: float) -> None:
        clock[0] += seconds
        # Yield like a real sleep, so concurrent callers can interleave.
        await _real_sleep(0)

    monkeypatch.setattr(api.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    return clock


@pytest.mark.asyncio
async def test_requests_from_every_caller_share_one_pace(monkeypatch):
    http = _RecordingHttp(_fake_clock(monkeypatch))
    client = TikTokClient(http, api.API, 2.0)  # type: ignore[arg-type]

    await asyncio.gather(*(client.request(LOG, "advertiser/info/", {}) for _ in range(3)))

    assert http.started == [100.0, 100.5, 101.0]


@pytest.mark.parametrize("is_sandbox, spacing", [(True, 1.0), (False, 0.1)])
@pytest.mark.asyncio
async def test_the_sandbox_is_paced_to_its_one_request_per_second(monkeypatch, is_sandbox, spacing):
    http = _RecordingHttp(_fake_clock(monkeypatch))
    client = api.tiktok_client(http, is_sandbox)  # type: ignore[arg-type]

    await client.request(LOG, "advertiser/info/", {})
    await client.request(LOG, "advertiser/info/", {})

    assert http.started[1] - http.started[0] == pytest.approx(spacing)


@pytest.mark.asyncio
async def test_throttle_backoff_grows_to_five_minutes(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    retries = api.RETRY_ATTEMPTS - 1
    http = StubSession(error_codes=[40100] * retries)

    advertisers = await _drain(snapshot_advertisers(_client(http), [ADVERTISER_ID], LOG))

    assert [a.advertiser_id for a in advertisers] == [ADVERTISER_ID]
    assert sleeps == [5, 10, 20, 40, 80, 160, 300]


@pytest.mark.asyncio
async def test_a_throttle_slows_every_caller_and_acceptance_recovers(monkeypatch):
    http = _RecordingHttp(_fake_clock(monkeypatch))
    client = TikTokClient(http, api.API, 10.0)  # type: ignore[arg-type]

    client.throttled()
    await client.request(LOG, "ad/get/", {})
    await client.request(LOG, "campaign/get/", {})
    for _ in range(200):
        client.accepted()
    await client.request(LOG, "ad/get/", {})
    await client.request(LOG, "ad/get/", {})

    # The throttle doubled the spacing for every endpoint's callers. A start's
    # spacing is set when it starts, so the recovered pace shows from the next
    # one on: enough accepted requests restore it, and never go past it.
    assert http.started[1] - http.started[0] == pytest.approx(0.2)
    assert http.started[3] - http.started[2] == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_requests_report_throttling_and_acceptance_to_the_pace(monkeypatch):
    async def fake_sleep(seconds: float) -> None:
        pass

    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    calls: list[str] = []
    client = _client(StubSession(campaigns(live=[_campaign("any", CURSOR)]), error_codes=[40100, 0, 0, 40100]))
    monkeypatch.setattr(client, "throttled", lambda: calls.append("throttled"))
    monkeypatch.setattr(client, "accepted", lambda: calls.append("accepted"))

    await _drain(snapshot_advertisers(client, [ADVERTISER_ID], LOG))
    await _drain(fetch_campaigns(client, ADVERTISER_ID, LOG, CURSOR - timedelta(days=1)))

    # `advertiser/info` is throttled, then accepted; the campaign listings'
    # first page is accepted, the next throttled and then accepted.
    assert calls[:4] == ["throttled", "accepted", "accepted", "throttled"]
    assert calls[4:] == ["accepted"] * (len(calls) - 4)


@pytest.mark.asyncio
async def test_a_poll_with_no_parent_deleted_lists_no_children(monkeypatch):
    horizon = CURSOR + timedelta(minutes=10)
    monkeypatch.setattr(api, "settled_second", lambda: horizon)
    http = StubSession(
        _ad_group_listings(
            live=[_ad_group("live", "c_live", CURSOR + timedelta(minutes=1))],
            deleted=[],
            cascaded=[],
            deleted_campaigns=[_campaign("c_deleted_before", CURSOR - timedelta(days=1))],
        )
    )

    await _drain(fetch_ad_groups(_client(http), ADVERTISER_ID, LOG, CURSOR))

    assert CAMPAIGN_DELETE not in _statuses(http)


@pytest.mark.asyncio
async def test_a_changed_child_of_a_deleted_parent_is_emitted_as_a_deletion(monkeypatch):
    horizon = CURSOR + timedelta(minutes=10)
    monkeypatch.setattr(api, "settled_second", lambda: horizon)
    # Its campaign was deleted before this window, so no cascade listing runs,
    # but the ad group itself changed in it.
    child = _ad_group("child", "c_deleted_before", CURSOR + timedelta(minutes=1))
    http = StubSession(
        _ad_group_listings(
            live=[child],
            deleted=[],
            cascaded=[child],
            deleted_campaigns=[_campaign("c_deleted_before", CURSOR - timedelta(days=1))],
        )
    )

    items = await _drain(fetch_ad_groups(_client(http), ADVERTISER_ID, LOG, CURSOR))

    assert {(d.adgroup_id, d.meta_.op) for d in items if isinstance(d, AdGroup)} == {("child", "d")}


@pytest.mark.asyncio
async def test_ads_list_only_recently_deleted_ads(monkeypatch):
    monkeypatch.setattr(api, "settled_second", lambda: CURSOR + timedelta(minutes=1))
    http = StubSession()

    await _drain(fetch_ads(_client(http), ADVERTISER_ID, LOG, CURSOR))

    day_early = (CURSOR - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    deleted_listings = [
        json.loads(p["filtering"])
        for url, p in http.requests
        if url.endswith(Ad.PATH)
        and json.loads(p["filtering"]).get("secondary_status") == Ad.DELETED_STATUS
    ]
    assert deleted_listings and all(f.get("modified_after") == day_early for f in deleted_listings)


@pytest.mark.asyncio
async def test_tiktok_server_errors_are_retried_without_slowing_the_pace(monkeypatch):
    async def fake_sleep(seconds: float) -> None:
        pass

    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    calls: list[str] = []
    # "Error processing request on TikTok side", then maintenance.
    client = _client(StubSession(error_codes=[50002, 60001]))
    monkeypatch.setattr(client, "throttled", lambda: calls.append("throttled"))

    advertisers = await _drain(snapshot_advertisers(client, [ADVERTISER_ID], LOG))

    assert [a.advertiser_id for a in advertisers] == [ADVERTISER_ID]
    assert calls == []

