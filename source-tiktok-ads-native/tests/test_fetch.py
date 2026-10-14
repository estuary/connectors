"""Unit tests for the parts of the entity fetch layer the snapshot tests cannot
reach.

`tests/test_snapshots.py` captures a live sandbox account, so it only exercises
whatever that account holds and walks a single backfill window. These cover:

  - backfill windows: each queries its exclusive creation start bound one second
    early, adjacent windows share a boundary, and the last ends at the cutoff;
  - the incremental window `(cursor, horizon]`, applied client-side;
  - deleted objects, emitted as listed, since deletions aren't captured yet;
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
from source_tiktok_ads_native.api import client as client_module
from source_tiktok_ads_native.api import entities as entities_module
from source_tiktok_ads_native.api import (
    TikTokClient,
    TikTokAPIError,
    backfill_ad_groups,
    backfill_ads,
    backfill_campaigns,
    fetch_ad_groups,
    fetch_ads,
    fetch_campaigns,
    snapshot_advertisers,
)
from source_tiktok_ads_native.api.entities import BACKFILL_WINDOW
from source_tiktok_ads_native.models import Ad, AdGroup, Advertiser, Campaign

LOG = logging.getLogger(__name__)
# Captured before any test swaps out `asyncio.sleep`.
_real_sleep = asyncio.sleep
ADVERTISER_ID = "7694356963175268360"
CURSOR = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)


def _client(http: Any) -> TikTokClient:
    """An unpaced client over a stub session."""
    return TikTokClient(http, client_module.API, math.inf)


TOP_VIEW = "RESERVATION_TOP_VIEW"


class StubSession:
    """Stand-in for HTTPSession that answers entity listings by TikTok's rules.

    `listings` maps (endpoint path, `secondary_status`) to the objects in that
    listing; an unlisted pair is empty. An object is listed only for its own
    buying type, from its stub-only `buying_type` key (`AUCTION` when absent).
    The stub applies the creation-time filters (both bounds exclusive) and
    `modified_after`, pages by `page` and `page_size`, and rejects what TikTok
    rejects: `RESERVATION_TOP_VIEW` combined with another buying type, and a
    top-view ad group page over 100. `error_codes` answer requests in order
    before any normal response; a 0 answers normally.

    Signatures mirror HTTPSession positionally, and serving encoded bytes keeps
    the real IncrementalJsonProcessor and Pydantic parsing inside the test."""

    def __init__(
        self,
        listings: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
        error_codes: list[int] | None = None,
    ):
        self.listings = listings or {}
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

        path = url.removeprefix(client_module.API + "/")
        filters = json.loads(params["filtering"])
        buying_types = set(filters["buying_types"])
        page, page_size = int(params["page"]), int(params["page_size"])
        if TOP_VIEW in buying_types and len(buying_types) > 1:
            return {}, _body({"code": 40002, "message": "top view combined", "data": {}})
        if TOP_VIEW in buying_types and path == AdGroup.PATH and page_size > 100:
            return {}, _body({"code": 40002, "message": "page_size over 100", "data": {}})

        listed = [
            o
            for o in self.listings.get((path, filters.get("secondary_status", "")), [])
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
    and deleted campaigns alike, and a deleted one looks paused."""
    return {(Campaign.PATH, Campaign.ALL_STATUS): (live or []) + (deleted or [])}


def _campaign(campaign_id: str, modified: datetime) -> dict[str, Any]:
    stamp = modified.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "advertiser_id": ADVERTISER_ID,
        "campaign_id": campaign_id,
        "create_time": stamp,
        "modify_time": stamp,
    }


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
async def test_backfill_emits_deleted_objects_as_listed():
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

    # Deletions aren't captured yet, so a deleted campaign comes through as listed.
    ops = {(i.campaign_id, i.meta_.op) for i in items if isinstance(i, Campaign)}
    assert ops == {("live", "u"), ("deleted", "u")}
    # The listing covers every status, so a deletion mid-walk cannot shift a page.
    assert _statuses(http) == {Campaign.ALL_STATUS}


@pytest.mark.asyncio
async def test_backfill_leaves_objects_changed_since_the_cutoff_to_the_incremental():
    cutoff = datetime(2026, 4, 1, tzinfo=UTC)
    created = "2026-03-01 00:00:00"
    # Changed at the cutoff: the incremental emits its latest state, which the
    # backfill's older copy must not overwrite.
    changed ={"advertiser_id": ADVERTISER_ID, "campaign_id": "changed", "create_time": created, "modify_time": "2026-04-01 00:00:00"}
    unchanged = {"advertiser_id": ADVERTISER_ID, "campaign_id": "unchanged", "create_time": created, "modify_time": "2026-03-31 23:59:59"}
    http = StubSession(campaigns(live=[changed, unchanged]))

    items = await _drain(
        backfill_campaigns(  # type: ignore[arg-type]
            _client(http), ADVERTISER_ID, datetime(2026, 1, 1, tzinfo=UTC), LOG, None, cutoff
        )
    )

    assert {i.campaign_id for i in items if isinstance(i, Campaign)} == {"unchanged"}


@pytest.mark.asyncio
async def test_incremental_window_is_cursor_exclusive_horizon_inclusive(monkeypatch):
    horizon = CURSOR + timedelta(minutes=10)
    monkeypatch.setattr(entities_module, "settled_second", lambda: horizon)
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
async def test_incremental_emits_deleted_objects_as_listed(monkeypatch):
    horizon = CURSOR + timedelta(minutes=10)
    monkeypatch.setattr(entities_module, "settled_second", lambda: horizon)
    http = StubSession(
        campaigns(
            live=[_campaign("paused", CURSOR + timedelta(minutes=1))],
            deleted=[_campaign("deleted", CURSOR + timedelta(minutes=2))],
        )
    )

    items = await _drain(fetch_campaigns(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]

    # Deletions aren't captured yet: a deleted campaign comes through looking
    # paused, from the all-status listing, which a deletion mid-walk cannot shift.
    ops = {(d.campaign_id, d.meta_.op) for d in items if isinstance(d, Campaign)}
    assert ops == {("paused", "u"), ("deleted", "u")}
    assert _statuses(http) == {Campaign.ALL_STATUS}


@pytest.mark.asyncio
async def test_incremental_returns_early_without_a_full_new_second(monkeypatch):
    monkeypatch.setattr(entities_module, "settled_second", lambda: CURSOR)
    http = StubSession(campaigns(live=[_campaign("any", CURSOR)]))

    items = await _drain(fetch_campaigns(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]

    assert items == []
    assert http.requests == []


@pytest.mark.asyncio
async def test_in_body_error_code_raises(monkeypatch):
    monkeypatch.setattr(entities_module, "settled_second", lambda: CURSOR + timedelta(minutes=1))
    http = StubSession(error_codes=[40002])

    with pytest.raises(TikTokAPIError):
        await _drain(fetch_campaigns(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]


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
    # TikTok's Unix seconds, unchanged.
    assert advertisers[0].model_dump(mode="json")["create_time"] == 1791482410


@pytest.mark.asyncio
async def test_only_ads_send_modified_after_a_day_before_the_cursor(monkeypatch):
    monkeypatch.setattr(entities_module, "settled_second", lambda: CURSOR + timedelta(minutes=1))

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


def _ad_group(adgroup_id: str, modified: datetime) -> dict[str, Any]:
    stamp = modified.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "advertiser_id": ADVERTISER_ID,
        "adgroup_id": adgroup_id,
        "create_time": stamp,
        "modify_time": stamp,
    }


@pytest.mark.asyncio
async def test_top_view_objects_come_from_their_own_listing():
    created = datetime(2026, 3, 1, tzinfo=UTC)
    auction = _ad_group("auction", created)
    top_view = {**_ad_group("top_view", created), "buying_type": TOP_VIEW}
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
    assert items[-1] <= now - entities_module.EVENTUAL_CONSISTENCY_DELAY


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

    monkeypatch.setattr(client_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(client_module.asyncio, "sleep", fake_sleep)
    return clock


@pytest.mark.asyncio
async def test_requests_from_every_caller_share_one_pace(monkeypatch):
    http = _RecordingHttp(_fake_clock(monkeypatch))
    client = TikTokClient(http, client_module.API, 2.0)  # type: ignore[arg-type]

    await asyncio.gather(*(client.request(LOG, "advertiser/info/", {}) for _ in range(3)))

    assert http.started == [100.0, 100.5, 101.0]


@pytest.mark.parametrize("is_sandbox, spacing", [(True, 1.0), (False, 0.1)])
@pytest.mark.asyncio
async def test_the_sandbox_is_paced_to_its_one_request_per_second(monkeypatch, is_sandbox, spacing):
    http = _RecordingHttp(_fake_clock(monkeypatch))
    client = client_module.tiktok_client(http, is_sandbox)  # type: ignore[arg-type]

    await client.request(LOG, "advertiser/info/", {})
    await client.request(LOG, "advertiser/info/", {})

    assert http.started[1] - http.started[0] == pytest.approx(spacing)


@pytest.mark.asyncio
async def test_every_entity_reads_only_its_all_status_listing(monkeypatch):
    monkeypatch.setattr(entities_module, "settled_second", lambda: CURSOR + timedelta(minutes=1))
    backfill_cutoff = datetime(2026, 4, 1, tzinfo=UTC)

    for fetch, backfill, model in (
        (fetch_campaigns, backfill_campaigns, Campaign),
        (fetch_ad_groups, backfill_ad_groups, AdGroup),
        (fetch_ads, backfill_ads, Ad),
    ):
        http = StubSession()
        await _drain(fetch(_client(http), ADVERTISER_ID, LOG, CURSOR))  # type: ignore[arg-type]
        await _drain(
            backfill(  # type: ignore[arg-type]
                _client(http), ADVERTISER_ID, datetime(2026, 1, 1, tzinfo=UTC), LOG, None, backfill_cutoff
            )
        )

        # Deletions aren't captured yet, so no deleted or cascade listing is read.
        assert _statuses(http) == {model.ALL_STATUS}, model.__name__
