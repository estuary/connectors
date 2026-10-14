"""Unit tests for reports: configuration rules, request windows, the backfill /
incremental seam across time zones, document shaping, and error handling.

The sandbox delivers no ads, so its reports never return rows; everything that
depends on report rows is exercised here against canned responses.
"""

import asyncio
import json
import logging
import math
from datetime import UTC, date, datetime, timedelta
from typing import Any, AsyncGenerator
from zoneinfo import ZoneInfo

import pytest
from estuary_cdk.flow import ValidationError as ConfigError
from multidict import CIMultiDict, CIMultiDictProxy
from pydantic import ValidationError
from source_tiktok_ads_native import api, reports
from source_tiktok_ads_native.api import (
    AdvertiserAccounts,
    InaccessibleAdvertiserError,
    NoAdvertisersError,
    TikTokAPIError,
    TikTokClient,
    resolve_advertiser_ids,
)
from source_tiktok_ads_native.resources import validate_credentials
from source_tiktok_ads_native.models import (
    DEFAULT_REPORTS,
    Campaign,
    DataLevel,
    EndpointConfig,
    Granularity,
    ReportConfig,
    report_document_model,
)
from source_tiktok_ads_native.reports import (
    QUERY_TOO_LARGE,
    STATUS_FILTER_FIELD,
    TRUNCATION_HEADER,
    _windows,
    backfill_report,
    check_report,
    fetch_report,
)

LOG = logging.getLogger(__name__)
LA = "America/Los_Angeles"
TOKYO = "Asia/Tokyo"


class ReportStub:
    """Stand-in for HTTPSession. `request` answers `oauth2/advertiser/get` with
    every advertiser in `zones` and `unreadable`, and `advertiser/info` with each
    requested advertiser's time zone; like TikTok, it fails the whole request
    with `40001` for an ID it can't read. `request_stream` answers report
    requests and the campaign listing.

    A report request covering more than `truncate_above` campaigns (an
    unfiltered one covers all of them) is flagged as truncated, from page
    `truncated_from_page` on; a filtered one answers one row per campaign. Each
    of `pages` pages repeats the rows. A dated request spanning more than
    `too_large_above_days` days answers `40067`.

    Signatures mirror HTTPSession positionally, and serving bytes keeps the real
    IncrementalJsonProcessor and Pydantic parsing inside the test."""

    def __init__(
        self,
        zones: dict[str, str],
        rows: list[dict[str, Any]] | None = None,
        campaigns: list[str] | None = None,
        truncate_above: int | None = None,
        too_large_above_days: int | None = None,
        error_code: int = 0,
        pages: int = 1,
        truncated_from_page: int = 1,
        unreadable: set[str] | None = None,
        info_error: tuple[int, str] | None = None,
    ):
        self.zones = zones
        self.rows = rows or []
        self.campaigns = campaigns or []
        self.truncate_above = truncate_above
        self.campaign_listings = 0
        self.info_batches: list[int] = []
        self.too_large_above_days = too_large_above_days
        self.error_code = error_code
        self.pages = pages
        self.truncated_from_page = truncated_from_page
        self.report_requests: list[dict[str, Any]] = []
        self.oauth_params: dict[str, Any] | None = None
        self.unreadable = unreadable or set()
        self.info_error = info_error

    async def request(
        self,
        log: logging.Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> bytes:
        params = params or {}
        # Yield like a real request, so concurrent callers interleave.
        await asyncio.sleep(0)
        if url.endswith("oauth2/advertiser/get/"):
            self.oauth_params = params
            listed = [{"advertiser_id": i} for i in [*self.zones, *self.unreadable]]
            return json.dumps({"code": 0, "message": "OK", "data": {"list": listed}}).encode()

        ids = json.loads(params["advertiser_ids"])
        self.info_batches.append(len(ids))
        if self.info_error:
            code, message = self.info_error
            return json.dumps({"code": code, "message": message, "data": {}}).encode()
        unknown = [i for i in ids if i not in self.zones or i in self.unreadable]
        if unknown:
            message = f"The advertiser {unknown[0]} doesn't exist or has been deleted."
            return json.dumps({"code": 40001, "message": message, "data": {}}).encode()
        accounts = [
            {
                "advertiser_id": i,
                "create_time": 1791482410,
                "timezone": "Etc/GMT+8",
                "display_timezone": self.zones[i],
            }
            for i in ids
        ]
        return json.dumps({"code": 0, "message": "OK", "data": {"list": accounts}}).encode()

    async def request_stream(
        self,
        log: logging.Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[Any, Any]:
        params = params or {}
        if url.endswith(Campaign.PATH):
            return self._campaign_listing(params)
        self.report_requests.append(params)
        campaign_ids = _campaign_filter(params)

        code = self.error_code
        if self.too_large_above_days is not None and "start_date" in params:
            days = (
                date.fromisoformat(params["end_date"])
                - date.fromisoformat(params["start_date"])
            ).days + 1
            if days > self.too_large_above_days:
                code = QUERY_TOO_LARGE

        if code:
            payload: dict[str, Any] = {"code": code, "message": "bad", "data": {}}
        else:
            rows = self.rows
            if campaign_ids is not None:
                rows = [
                    {
                        "dimensions": {"ad_id": f"ad-{c}", "stat_time_day": "2026-10-12 00:00:00"},
                        "metrics": {"spend": "1"},
                    }
                    for c in campaign_ids
                ]
            payload = {
                "code": 0,
                "message": "OK",
                "data": {
                    "list": rows,
                    "page_info": {"page": params["page"], "total_page": self.pages},
                },
            }
        headers = CIMultiDict()
        covered = len(self.campaigns) if campaign_ids is None else len(campaign_ids)
        if (
            self.truncate_above is not None
            and covered > self.truncate_above
            and params["page"] >= self.truncated_from_page
        ):
            headers["x-tt-ads-throttle"] = "Number of ad ids is equal or more than 20k"
        return CIMultiDictProxy(headers), _body(payload)

    def _campaign_listing(self, params: dict[str, Any]) -> tuple[Any, Any]:
        self.campaign_listings += 1
        buying_types = json.loads(params["filtering"])["buying_types"]
        stamp = "2026-10-01 00:00:00"
        listed = [
            {"campaign_id": c, "create_time": stamp, "modify_time": stamp}
            for c in self.campaigns
            if "AUCTION" in buying_types
        ]
        payload = {
            "code": 0,
            "message": "OK",
            "data": {"list": listed, "page_info": {"page": 1, "total_page": 1}},
        }
        return CIMultiDictProxy(CIMultiDict()), _body(payload)


def _body(payload: dict[str, Any]) -> Any:
    body = json.dumps(payload).encode()

    async def gen() -> AsyncGenerator[bytes, None]:
        yield body

    return gen


def _campaign_filter(params: dict[str, Any]) -> list[str] | None:
    """The campaign IDs a report request is filtered to, or None."""
    for f in json.loads(params.get("filtering", "[]")):
        if f["field_name"] == "campaign_ids":
            return json.loads(f["filter_value"])
    return None


def _client(http: Any) -> TikTokClient:
    """An unpaced client over a stub session."""
    return TikTokClient(http, api.API, math.inf)


def _zones(http: ReportStub) -> AdvertiserAccounts:
    return AdvertiserAccounts(_client(http), list(http.zones))


def _zone(http: ReportStub, advertiser_id: str = "a1") -> ZoneInfo:
    return ZoneInfo(http.zones[advertiser_id])


def _report(**overrides: Any) -> ReportConfig:
    base: dict[str, Any] = {"name": "r", "metrics": "spend,impressions"}
    return ReportConfig.model_validate({**base, **overrides})


async def _drain(gen: AsyncGenerator[Any, None]) -> list[Any]:
    return [item async for item in gen]


def _windows_requested(http: ReportStub) -> list[tuple[str, str]]:
    return [(p["start_date"], p["end_date"]) for p in http.report_requests]


def _batches_requested(http: ReportStub) -> list[int | None]:
    """How many campaigns each report request was filtered to; None if unfiltered."""
    return [
        None if ids is None else len(ids)
        for ids in map(_campaign_filter, http.report_requests)
    ]


async def _read_one_day(http: ReportStub, report: ReportConfig) -> list[Any]:
    """The documents of a single-day incremental read of advertiser a1."""
    items = await _drain(
        fetch_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 10, 12, tzinfo=UTC), 0, LOG, datetime(2026, 10, 12, tzinfo=UTC),
        )
    )
    return items[:-1]


# --- Configuration -------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"report_type": "AUDIENCE"}, "needs a breakdown"),
        (
            {"report_type": "AUDIENCE", "breakdowns": ["age", "country_code"]},
            "age and gender together",
        ),
        (
            {"report_type": "AUDIENCE", "breakdowns": ["age"], "granularity": "hourly"},
            "daily granularity only",
        ),
        ({"breakdowns": ["age"]}, "only break down by country_code"),
        ({"breakdowns": ["country_code"], "granularity": "hourly"}, "daily granularity only"),
        ({"breakdowns": ["country_code", "country_code"]}, "more than once"),
        ({"name": "Has Spaces"}, "Invalid report name"),
        ({"metrics": " , "}, "at least one metric"),
        ({"metrics": "spend,Not-A-Metric"}, "Invalid metric names"),
        ({"metrics": ",".join(f"m{i}" for i in range(101))}, "at most 100"),
    ],
)
def test_report_rules_reject_invalid_reports(overrides: dict[str, Any], message: str):
    with pytest.raises(ValidationError, match=message):
        _report(**overrides)


def test_report_dimensions_and_key_derive_from_level_and_granularity():
    report = _report(
        report_type="AUDIENCE",
        data_level="AUCTION_CAMPAIGN",
        breakdowns=["age", "gender"],
        metrics="spend, impressions, spend",
    )

    assert report.dimensions() == ["campaign_id", "stat_time_day", "age", "gender"]
    assert report.key() == ["/advertiser_id", "/campaign_id", "/stat_time_day", "/age", "/gender"]
    assert report.metric_list == ["spend", "impressions"]


def test_advertiser_level_key_does_not_repeat_the_advertiser():
    report = _report(data_level="AUCTION_ADVERTISER", granularity="lifetime")

    assert report.dimensions() == ["advertiser_id"]
    assert report.key() == ["/advertiser_id"]


def test_the_default_report_is_ad_level_daily():
    (default,) = DEFAULT_REPORTS

    assert default.name == "ad_report_daily"
    assert default.key() == ["/advertiser_id", "/ad_id", "/stat_time_day"]
    assert len(default.metric_list) == 19


def _config(custom_reports: list[dict[str, Any]]) -> EndpointConfig:
    return EndpointConfig.model_validate(
        {
            "credentials": {
                "credentials_title": "OAuth Credentials",
                "client_id": "app",
                "client_secret": "secret",
                "access_token": "token",
            },
            "custom_reports": custom_reports,
        }
    )


def test_custom_reports_are_namespaced_after_the_defaults():
    config = _config([{"name": "subscriptions", "metrics": "on_web_subscribe"}])

    assert [r.name for r in config.reports()] == ["ad_report_daily", "custom_report_subscriptions"]


def test_custom_report_names_must_be_unique():
    with pytest.raises(ValidationError, match="must be unique"):
        _config([{"name": "dup", "metrics": "spend"}, {"name": "dup", "metrics": "clicks"}])


# --- Windows and the backfill / incremental seam --------------------------------


def test_windows_cover_the_range_contiguously_in_inclusive_spans():
    first, last = date(2026, 1, 1), date(2026, 3, 6)

    windows = list(_windows(first, last, 30))

    assert windows == [
        (date(2026, 1, 1), date(2026, 1, 30)),
        (date(2026, 1, 31), date(2026, 3, 1)),
        (date(2026, 3, 2), date(2026, 3, 6)),
    ]


@pytest.mark.asyncio
async def test_incremental_reads_from_the_cursor_date_in_the_advertisers_zone(monkeypatch):
    # 03:00 UTC on Oct 13 is still Oct 12 in Los Angeles: that local day must be
    # re-read even with no lookback, or its last hours would never be captured.
    cursor = datetime(2026, 10, 13, 3, 0, tzinfo=UTC)
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 13, 9, 0, tzinfo=UTC))
    http = ReportStub({"a1": LA})
    report = _report()

    items = await _drain(
        fetch_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 1, 1, tzinfo=UTC), 0, LOG, cursor,
        )
    )

    assert _windows_requested(http) == [("2026-10-12", "2026-10-13")]
    assert items[-1] == datetime(2026, 10, 13, 9, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_incremental_re_reads_the_lookback_in_window_sized_requests(monkeypatch):
    # The first sweep of Oct 13 in Tokyo: the cursor is Oct 12, 21:00 local.
    cursor = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 13, 13, 0, tzinfo=UTC))
    http = ReportStub({"a1": TOKYO})
    daily, hourly = _report(), _report(granularity="hourly")

    for report in (daily, hourly):
        await _drain(
            fetch_report(  # type: ignore[arg-type]
                _client(http), "a1", _zone(http), report, report_document_model(report),
                datetime(2026, 1, 1, tzinfo=UTC), 7, LOG, cursor,
            )
        )

    # Seven days before the cursor's date, Oct 12, is Oct 5.
    requested = _windows_requested(http)
    assert requested[0] == ("2026-10-05", "2026-10-13")
    # Hourly reports take one day per request.
    assert requested[1:] == [(f"2026-10-{d:02d}", f"2026-10-{d:02d}") for d in range(5, 14)]


@pytest.mark.asyncio
async def test_a_later_sweep_the_same_day_skips_the_lookback(monkeypatch):
    # Both instants are Oct 13 in Los Angeles: the day's first sweep already
    # re-read the lookback.
    cursor = datetime(2026, 10, 13, 15, 0, tzinfo=UTC)
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 14, 3, 0, tzinfo=UTC))
    http = ReportStub({"a1": LA})
    report = _report()

    await _drain(
        fetch_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 1, 1, tzinfo=UTC), 28, LOG, cursor,
        )
    )

    assert _windows_requested(http) == [("2026-10-13", "2026-10-13")]


@pytest.mark.asyncio
async def test_incremental_never_reads_before_the_start_date(monkeypatch):
    cursor = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 13, 13, 0, tzinfo=UTC))
    http = ReportStub({"a1": "UTC"})
    report = _report()

    await _drain(
        fetch_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 10, 11, tzinfo=UTC), 28, LOG, cursor,
        )
    )

    assert _windows_requested(http) == [("2026-10-11", "2026-10-13")]


@pytest.mark.asyncio
async def test_backfill_and_first_incremental_poll_leave_no_day_uncovered(monkeypatch):
    # 20:00 UTC on Oct 12 is already Oct 13 in Tokyo but still Oct 12 in Los
    # Angeles, so each advertiser's backfill must end on its own local date.
    cutoff = datetime(2026, 10, 12, 20, 0, tzinfo=UTC)
    start = datetime(2026, 9, 1, tzinfo=UTC)
    report = _report()
    model = report_document_model(report)
    monkeypatch.setattr(reports, "_now", lambda: cutoff + timedelta(minutes=1))

    for zone in (ZoneInfo(LA), ZoneInfo(TOKYO)):
        http = ReportStub({"a1": str(zone)})
        cursor = None
        while True:
            items = await _drain(
                backfill_report(  # type: ignore[arg-type]
                    _client(http), "a1", zone, report, model, start, LOG, cursor, cutoff
                )
            )
            if not items:
                break
            cursor = items[-1]
        await _drain(
            fetch_report(  # type: ignore[arg-type]
                _client(http), "a1", zone, report, model, start, 0, LOG, cutoff
            )
        )

        covered: set[date] = set()
        for p in http.report_requests:
            day = date.fromisoformat(p["start_date"])
            while day <= date.fromisoformat(p["end_date"]):
                covered.add(day)
                day += timedelta(days=1)
        first_day = start.astimezone(zone).date()
        last_day = (cutoff + timedelta(minutes=1)).astimezone(zone).date()
        expected = {first_day + timedelta(days=n) for n in range((last_day - first_day).days + 1)}
        assert covered == expected, zone


@pytest.mark.asyncio
async def test_backfill_ends_without_a_cursor_after_the_last_window():
    cutoff = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)
    http = ReportStub({"a1": "UTC"})
    report = _report()

    items = await _drain(
        backfill_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 10, 1, tzinfo=UTC), LOG, None, cutoff,
        )
    )

    assert items == []
    assert _windows_requested(http) == [("2026-10-01", "2026-10-12")]


@pytest.mark.asyncio
async def test_lifetime_reports_are_read_whole_without_dates(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 13, tzinfo=UTC))
    http = ReportStub({"a1": LA})
    report = _report(granularity="lifetime")

    await _drain(
        fetch_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 1, 1, tzinfo=UTC), 7, LOG, datetime(2026, 10, 12, tzinfo=UTC),
        )
    )

    assert [p["advertiser_id"] for p in http.report_requests] == ["a1"]
    assert all(p["query_lifetime"] == "true" for p in http.report_requests)
    assert all("start_date" not in p for p in http.report_requests)


# --- Requests and documents -----------------------------------------------------


@pytest.mark.parametrize("level", list(STATUS_FILTER_FIELD))
@pytest.mark.asyncio
async def test_reports_include_deleted_entities(level: DataLevel):
    http = ReportStub({"a1": "UTC"})

    await check_report(_client(http), _report(data_level=str(level)), "a1", LOG)  # type: ignore[arg-type]

    (status_filter,) = json.loads(http.report_requests[0]["filtering"])
    assert status_filter["field_name"] == STATUS_FILTER_FIELD[level]
    assert json.loads(status_filter["filter_value"]) == ["STATUS_ALL"]


@pytest.mark.asyncio
async def test_advertiser_level_reports_send_no_status_filter():
    # TikTok's status filters apply at their own level only; none is documented
    # for advertiser-level reports, and TikTok rejects an unsupported filter.
    http = ReportStub({"a1": "UTC"})

    await check_report(_client(http), _report(data_level="AUCTION_ADVERTISER"), "a1", LOG)  # type: ignore[arg-type]

    assert "filtering" not in http.report_requests[0]


@pytest.mark.asyncio
async def test_documents_are_flattened_typed_and_stamped(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 13, tzinfo=UTC))
    rows = [
        {
            "dimensions": {"ad_id": "1800000000000000001", "stat_time_hour": "2026-10-12 10:00:00"},
            "metrics": {
                "spend": "12.50",
                "impressions": "1000",
                "conversion": "-",
                "adgroup_id": "1878524819318882",
                "ad_name": "Spring sale",
            },
        }
    ]
    http = ReportStub({"a1": LA}, rows=rows)
    report = _report(granularity="hourly")

    items = await _drain(
        fetch_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 10, 12, tzinfo=UTC), 0, LOG, datetime(2026, 10, 12, 20, tzinfo=UTC),
        )
    )

    doc = items[0].model_dump(mode="json", by_alias=True, exclude={"meta_"})
    assert doc["advertiser_id"] == "a1"
    assert doc["ad_id"] == "1800000000000000001"
    # 10:00 in Los Angeles on Oct 12 is during daylight saving time, UTC-7.
    assert doc["stat_time_hour"] == "2026-10-12T10:00:00-07:00"
    assert doc["spend"] == 12.5 and doc["impressions"] == 1000
    assert doc["conversion"] is None
    # IDs and text stay strings even when they look numeric.
    assert doc["adgroup_id"] == "1878524819318882"
    assert doc["ad_name"] == "Spring sale"


@pytest.mark.asyncio
async def test_attribute_metrics_stay_text_even_when_they_look_numeric(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    metrics = {
        "ad_name": "007", "campaign_name": "2025", "ad_text": "100", "budget": "50.00",
        "spend": "12.5", "campaign_id": "-", "conversion": "-",
    }
    rows = [{"dimensions": {"ad_id": "1", "stat_time_day": "2026-10-12 00:00:00"}, "metrics": metrics}]
    http = ReportStub({"a1": "UTC"}, rows=rows)

    (doc,) = await _read_one_day(http, _report())

    dumped = doc.model_dump(mode="json")
    assert (dumped["ad_name"], dumped["campaign_name"], dumped["ad_text"]) == ("007", "2025", "100")
    assert dumped["budget"] == "50.00"
    assert dumped["spend"] == 12.5
    # IDs keep TikTok's value like attribute metrics; only metrics turn `-` null.
    assert dumped["campaign_id"] == "-" and dumped["conversion"] is None


@pytest.mark.asyncio
async def test_daily_rows_carry_their_date_without_a_time(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 13, tzinfo=UTC))
    rows = [{"dimensions": {"ad_id": "1", "stat_time_day": "2026-10-12 00:00:00"}, "metrics": {"spend": "1"}}]
    http = ReportStub({"a1": TOKYO}, rows=rows)
    report = _report()

    items = await _drain(
        fetch_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 10, 12, tzinfo=UTC), 0, LOG, datetime(2026, 10, 12, tzinfo=UTC),
        )
    )

    assert items[0].model_dump(mode="json")["stat_time_day"] == "2026-10-12"


# --- Errors ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_truncated_report_is_read_in_batches_of_100_campaigns(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    campaigns = [f"c{i}" for i in range(250)]
    http = ReportStub({"a1": "UTC"}, campaigns=campaigns, truncate_above=100)

    docs = await _read_one_day(http, _report())

    assert _batches_requested(http) == [None, 100, 100, 50]
    assert sorted(d.ad_id for d in docs) == sorted(f"ad-{c}" for c in campaigns)
    # Batching narrows the request; it must not drop the include-deleted filter.
    status_field = STATUS_FILTER_FIELD[DataLevel.AD]
    for p in http.report_requests:
        assert status_field in {f["field_name"] for f in json.loads(p["filtering"])}
    assert TRUNCATION_HEADER.lower() == "x-tt-ads-throttle"


@pytest.mark.asyncio
async def test_a_truncated_batch_is_halved_until_it_fits(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    campaigns = [f"c{i}" for i in range(100)]
    http = ReportStub({"a1": "UTC"}, campaigns=campaigns, truncate_above=30)

    docs = await _read_one_day(http, _report())

    assert _batches_requested(http) == [None, 100, 50, 25, 25, 50, 25, 25]
    # Truncated requests emit nothing, so each campaign's row arrives once.
    assert sorted(d.ad_id for d in docs) == sorted(f"ad-{c}" for c in campaigns)


@pytest.mark.asyncio
async def test_a_campaign_too_large_to_split_is_kept_with_a_warning(monkeypatch, caplog):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    http = ReportStub({"a1": "UTC"}, campaigns=["c1", "c2", "c3"], truncate_above=0)

    with caplog.at_level(logging.WARNING):
        docs = await _read_one_day(http, _report())

    # Every request is truncated, so each campaign ends up alone; its rows are
    # kept as TikTok returns them rather than stop the whole capture.
    assert sorted(d.ad_id for d in docs) == ["ad-c1", "ad-c2", "ad-c3"]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 3
    assert all("can't be split" in r.getMessage() for r in warnings)


@pytest.mark.asyncio
async def test_a_truncated_advertiser_level_report_is_kept_with_a_warning(monkeypatch, caplog):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    row = {"dimensions": {"advertiser_id": "a1", "stat_time_day": "2026-10-12 00:00:00"}, "metrics": {"spend": "1"}}
    http = ReportStub({"a1": "UTC"}, rows=[row], campaigns=["c1", "c2"], truncate_above=0)

    with caplog.at_level(logging.WARNING):
        docs = await _read_one_day(http, _report(data_level="AUCTION_ADVERTISER"))

    # Advertiser-level rows can't be split by campaign, so none is attempted.
    assert [d.advertiser_id for d in docs] == ["a1"]
    assert http.campaign_listings == 0
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1


@pytest.mark.asyncio
async def test_a_query_too_large_window_is_split_until_it_fits():
    http = ReportStub({"a1": "UTC"}, too_large_above_days=8)
    report = _report()

    await _drain(
        backfill_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 9, 1, tzinfo=UTC), LOG, None, datetime(2026, 10, 1, tzinfo=UTC),
        )
    )

    accepted = [
        (s, e) for s, e in _windows_requested(http)
        if (date.fromisoformat(e) - date.fromisoformat(s)).days + 1 <= 8
    ]
    covered = sorted(
        date.fromisoformat(s) + timedelta(days=n)
        for s, e in accepted
        for n in range((date.fromisoformat(e) - date.fromisoformat(s)).days + 1)
    )
    assert covered == [date(2026, 9, 1) + timedelta(days=n) for n in range(30)]


@pytest.mark.asyncio
async def test_check_report_requests_one_row_of_yesterday_and_surfaces_rejections(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 13, 9, tzinfo=UTC))
    ok = ReportStub({"a1": "UTC"})

    await check_report(_client(ok), _report(), "a1", LOG)  # type: ignore[arg-type]

    (params,) = ok.report_requests
    assert (params["start_date"], params["end_date"], params["page_size"]) == ("2026-10-12", "2026-10-12", 1)

    rejected = ReportStub({"a1": "UTC"}, error_code=40002)
    with pytest.raises(TikTokAPIError):
        await check_report(_client(rejected), _report(), "a1", LOG)  # type: ignore[arg-type]


def test_lifetime_granularity_has_no_time_dimension():
    assert _report(granularity="lifetime").time_dimension is None
    assert Granularity.LIFETIME.value == "lifetime"


# --- Access token authentication ------------------------------------------------


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

    assert await resolve_advertiser_ids(_client(http), ["a1"], None, None, LOG) == ["a1"]
    assert http.oauth_params is None


@pytest.mark.asyncio
async def test_validation_reads_configured_advertisers_in_batches_of_100():
    ids = [str(1000 + i) for i in range(150)]
    http = ReportStub({i: "UTC" for i in ids})

    await validate_credentials(LOG, http, _token_config(advertiser_ids=ids))  # type: ignore[arg-type]

    assert http.info_batches == [100, 50]


@pytest.mark.asyncio
async def test_concurrent_zone_lookups_read_advertiser_info_once():
    http = ReportStub({"a1": "UTC", "a2": TOKYO})
    zones = _zones(http)

    found = await asyncio.gather(zones.zone("a1", LOG), zones.zone("a2", LOG))

    assert found == [ZoneInfo("UTC"), ZoneInfo(TOKYO)]
    assert http.info_batches == [2]


@pytest.mark.asyncio
async def test_an_advertiser_missing_from_advertiser_info_fails_clearly():
    http = ReportStub({"a1": "UTC"})

    with pytest.raises(RuntimeError, match="no account details for advertiser a2"):
        await _zones(http).zone("a2", LOG)


def _days(params: dict[str, Any]) -> set[date]:
    first, last = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
    return {first + timedelta(days=n) for n in range((last - first).days + 1)}


@pytest.mark.asyncio
async def test_a_single_day_still_too_large_fails_instead_of_splitting(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    http = ReportStub({"a1": "UTC"}, too_large_above_days=0)

    with pytest.raises(TikTokAPIError) as err:
        await _read_one_day(http, _report())

    assert err.value.code == QUERY_TOO_LARGE


@pytest.mark.asyncio
async def test_truncation_first_reported_on_a_later_page_is_still_batched(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    early = {"dimensions": {"ad_id": "early", "stat_time_day": "2026-10-12 00:00:00"}, "metrics": {}}
    http = ReportStub(
        {"a1": "UTC"}, rows=[early], campaigns=["c1", "c2"], truncate_above=1,
        pages=2, truncated_from_page=2,
    )

    docs = await _read_one_day(http, _report())

    # The first page's row was emitted before the header appeared; the
    # campaign rows only come from the batches.
    assert {d.ad_id for d in docs} == {"early", "ad-c1", "ad-c2"}


@pytest.mark.asyncio
async def test_a_truncated_lifetime_report_is_batched_without_dates(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    http = ReportStub({"a1": "UTC"}, campaigns=["c1", "c2", "c3"], truncate_above=2)

    docs = await _read_one_day(http, _report(granularity="lifetime"))

    assert sorted(d.ad_id for d in docs) == ["ad-c1", "ad-c2", "ad-c3"]
    assert all(p["query_lifetime"] == "true" for p in http.report_requests)
    assert all("start_date" not in p for p in http.report_requests)


@pytest.mark.asyncio
async def test_query_too_large_inside_a_campaign_batch_splits_the_dates(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    campaigns = ["c1", "c2", "c3"]
    http = ReportStub({"a1": "UTC"}, campaigns=campaigns, truncate_above=1, too_large_above_days=4)
    report = _report()

    await _drain(
        fetch_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 1, 1, tzinfo=UTC), 7, LOG, datetime(2026, 10, 11, tzinfo=UTC),
        )
    )

    # Seven days before the cursor's date through today: Oct 4 to Oct 12. Every
    # campaign's rows must come from requests TikTok answered: one campaign,
    # four days.
    expected = {date(2026, 10, 4) + timedelta(days=n) for n in range(9)}
    for campaign in campaigns:
        covered: set[date] = set()
        for p in http.report_requests:
            if _campaign_filter(p) == [campaign] and len(_days(p)) <= 4:
                covered |= _days(p)
        assert covered == expected, campaign


@pytest.mark.asyncio
async def test_a_report_start_date_after_the_cutoff_backfills_nothing():
    http = ReportStub({"a1": "UTC"})
    report = _report()

    items = await _drain(
        backfill_report(  # type: ignore[arg-type]
            _client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 11, 1, tzinfo=UTC), LOG, None, datetime(2026, 10, 1, tzinfo=UTC),
        )
    )

    assert items == []
    assert http.report_requests == []


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
    http = ReportStub({"a1": "UTC", "a2": LA})

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
    zones = AdvertiserAccounts(_client(http), ["1001", "1009"])

    with pytest.raises(InaccessibleAdvertiserError, match="enter the IDs of the advertisers"):
        await zones.zone("1001", LOG)


@pytest.mark.asyncio
async def test_a_blank_advertiser_id_counts_as_none():
    # The web app can submit an empty entry when no ID was typed.
    http = ReportStub({"a1": "UTC"})

    with pytest.raises(ConfigError, match="Advertiser IDs are required"):
        await validate_credentials(LOG, http, _token_config(advertiser_ids=["", "  "]))  # type: ignore[arg-type]

    assert http.info_batches == [] and http.report_requests == []


def test_advertiser_ids_are_trimmed_and_deduplicated():
    config = _token_config(advertiser_ids=[" 123 ", "", "456", "123"])

    assert config.advertiser_ids == ["123", "456"]


def test_a_non_numeric_advertiser_id_is_named_by_position_not_value():
    pasted_secret = "d0not1echo2this3value"

    with pytest.raises(ValidationError) as err:
        _token_config(advertiser_ids=["123", pasted_secret])

    assert "entry 2" in str(err.value)
    assert pasted_secret not in str(err.value)


@pytest.mark.asyncio
async def test_an_oauth_account_with_no_advertisers_raises_rather_than_capturing_nothing():
    # A running capture re-lists its advertisers at start-up; none would empty
    # the Advertisers collection.
    http = ReportStub({})

    with pytest.raises(NoAdvertisersError):
        await resolve_advertiser_ids(_client(http), [], "app", "secret", LOG)
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

