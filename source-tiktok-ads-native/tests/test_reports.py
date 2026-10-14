"""Unit tests for reports: configuration rules, request windows, the backfill /
incremental seam across time zones, document shaping, and error handling.

The sandbox delivers no ads, so its reports never return rows; everything that
depends on report rows is exercised here against canned responses.
"""

import json
import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any, AsyncGenerator
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError
from source_tiktok_ads_native.api import reports
from source_tiktok_ads_native.api import TikTokAPIError
from source_tiktok_ads_native.models import (
    DEFAULT_REPORTS,
    DataLevel,
    EndpointConfig,
    Granularity,
    ReportConfig,
    report_document_model,
)
from source_tiktok_ads_native.api.reports import (
    QUERY_TOO_LARGE,
    STATUS_FILTER_FIELD,
    TRUNCATION_HEADER,
    _windows,
    backfill_report,
    check_report,
    fetch_report,
)

from .stubs import ReportStub, campaign_filter, stub_client

LOG = logging.getLogger(__name__)
LA = "America/Los_Angeles"
TOKYO = "Asia/Tokyo"


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
        for ids in map(campaign_filter, http.report_requests)
    ]


async def _read_one_day(http: ReportStub, report: ReportConfig) -> list[Any]:
    """The documents of a single-day incremental read of advertiser a1."""
    items = await _drain(
        fetch_report(  # type: ignore[arg-type]
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
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
            "age and gender breakdowns together",
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


def test_the_key_does_not_depend_on_the_breakdown_order():
    in_order = _report(report_type="AUDIENCE", breakdowns=["age", "gender"])
    reversed_order = _report(report_type="AUDIENCE", breakdowns=["gender", "age"])

    assert reversed_order.key() == in_order.key() == [
        "/advertiser_id", "/ad_id", "/stat_time_day", "/age", "/gender"
    ]


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


def test_lifetime_granularity_has_no_time_dimension():
    assert _report(granularity="lifetime").time_dimension is None
    assert Granularity.LIFETIME.value == "lifetime"


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
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
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
                stub_client(http), "a1", _zone(http), report, report_document_model(report),
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
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
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
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 10, 11, tzinfo=UTC), 28, LOG, cursor,
        )
    )

    assert _windows_requested(http) == [("2026-10-11", "2026-10-13")]


@pytest.mark.asyncio
async def test_a_long_pause_with_a_blank_start_date_skips_no_day(monkeypatch):
    # Last swept Aug 1, resumed Oct 13, more than 30 days later.
    now = datetime(2026, 10, 13, 13, 0, tzinfo=UTC)
    monkeypatch.setattr(reports, "_now", lambda: now)
    http = ReportStub({"a1": "UTC"})
    report = _report()

    await _drain(
        fetch_report(  # type: ignore[arg-type]
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
            None, 28, LOG, datetime(2026, 8, 1, 12, 0, tzinfo=UTC),
        )
    )

    # Nothing clamps the lookback before the cursor's date.
    assert _windows_requested(http)[0][0] == "2026-07-04"
    assert _windows_requested(http)[-1][1] == "2026-10-13"


@pytest.mark.asyncio
async def test_a_future_start_date_holds_back_the_incremental(monkeypatch):
    # Created Oct 12 with a start date of Oct 20.
    start = datetime(2026, 10, 20, tzinfo=UTC)
    http = ReportStub({"a1": "UTC"})
    report = _report()

    for cursor, now in (
        (datetime(2026, 10, 12, 12, 0, tzinfo=UTC), datetime(2026, 10, 13, 13, 0, tzinfo=UTC)),
        (datetime(2026, 10, 20, 12, 0, tzinfo=UTC), datetime(2026, 10, 21, 13, 0, tzinfo=UTC)),
    ):
        monkeypatch.setattr(reports, "_now", lambda: now)
        items = await _drain(
            fetch_report(  # type: ignore[arg-type]
                stub_client(http), "a1", _zone(http), report, report_document_model(report),
                start, 28, LOG, cursor,
            )
        )
        # Each sweep still advances the cursor.
        assert items == [now]

    # Nothing is read before Oct 20, even within the lookback.
    assert _windows_requested(http) == [("2026-10-20", "2026-10-21")]


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
                    stub_client(http), "a1", zone, report, model, start, LOG, cursor, cutoff
                )
            )
            if not items:
                break
            cursor = items[-1]
        await _drain(
            fetch_report(  # type: ignore[arg-type]
                stub_client(http), "a1", zone, report, model, start, 0, LOG, cutoff
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
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
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
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 1, 1, tzinfo=UTC), 7, LOG, datetime(2026, 10, 12, tzinfo=UTC),
        )
    )

    assert [p["advertiser_id"] for p in http.report_requests] == ["a1"]
    assert all(p["query_lifetime"] == "true" for p in http.report_requests)
    assert all("start_date" not in p for p in http.report_requests)


@pytest.mark.asyncio
async def test_a_report_start_date_after_the_cutoff_backfills_nothing():
    http = ReportStub({"a1": "UTC"})
    report = _report()

    items = await _drain(
        backfill_report(  # type: ignore[arg-type]
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 11, 1, tzinfo=UTC), LOG, None, datetime(2026, 10, 1, tzinfo=UTC),
        )
    )

    assert items == []
    assert http.report_requests == []


# --- Requests and documents -----------------------------------------------------


@pytest.mark.parametrize("level", list(STATUS_FILTER_FIELD))
@pytest.mark.asyncio
async def test_reports_include_deleted_entities(level: DataLevel):
    http = ReportStub({"a1": "UTC"})

    await check_report(LOG, stub_client(http), _report(data_level=str(level)), "a1")  # type: ignore[arg-type]

    (status_filter,) = json.loads(http.report_requests[0]["filtering"])
    assert status_filter["field_name"] == STATUS_FILTER_FIELD[level]
    assert json.loads(status_filter["filter_value"]) == ["STATUS_ALL"]


@pytest.mark.asyncio
async def test_advertiser_level_reports_send_no_status_filter():
    # TikTok's status filters apply at their own level only; none is documented
    # for advertiser-level reports, and TikTok rejects an unsupported filter.
    http = ReportStub({"a1": "UTC"})

    await check_report(LOG, stub_client(http), _report(data_level="AUCTION_ADVERTISER"), "a1")  # type: ignore[arg-type]

    assert "filtering" not in http.report_requests[0]


@pytest.mark.asyncio
async def test_documents_are_flattened_and_stamped(monkeypatch):
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
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 10, 12, tzinfo=UTC), 0, LOG, datetime(2026, 10, 12, 20, tzinfo=UTC),
        )
    )

    doc = items[0].model_dump(mode="json", by_alias=True, exclude={"meta_"})
    assert doc["advertiser_id"] == "a1"
    assert doc["ad_id"] == "1800000000000000001"
    # TikTok's report time, unchanged: the advertiser's wall-clock hour, no offset.
    assert doc["stat_time_hour"] == "2026-10-12 10:00:00"
    # Metrics stay TikTok's strings, as the legacy connector kept them, and `-`
    # (no value) becomes null.
    assert doc["spend"] == "12.50" and doc["impressions"] == "1000"
    assert doc["conversion"] is None
    assert doc["adgroup_id"] == "1878524819318882"
    assert doc["ad_name"] == "Spring sale"


@pytest.mark.asyncio
async def test_daily_rows_carry_tiktok_s_date_unchanged(monkeypatch):
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 13, tzinfo=UTC))
    rows = [{"dimensions": {"ad_id": "1", "stat_time_day": "2026-10-12 00:00:00"}, "metrics": {"spend": "1"}}]
    http = ReportStub({"a1": TOKYO}, rows=rows)
    report = _report()

    items = await _drain(
        fetch_report(  # type: ignore[arg-type]
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
            datetime(2026, 10, 12, tzinfo=UTC), 0, LOG, datetime(2026, 10, 12, tzinfo=UTC),
        )
    )

    assert items[0].model_dump(mode="json")["stat_time_day"] == "2026-10-12 00:00:00"


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
    (warning,) = [r for r in caplog.records if r.levelno == logging.WARNING]
    # TikTok's header text says which limit it hit.
    assert isinstance(warning.args, dict)
    assert warning.args["throttle_warning"] == "Number of ad ids is equal or more than 20k"


@pytest.mark.asyncio
async def test_campaigns_created_after_a_window_are_left_out_of_its_batches(monkeypatch):
    # The window is Oct 12. Any time zone's Oct 12 has ended by Oct 14 00:00 UTC,
    # so only campaigns created from then on are left out.
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    created = {
        "old": "2026-10-01 00:00:00",
        "last": "2026-10-13 23:59:59",
        "later": "2026-10-14 00:00:00",
    }
    http = ReportStub({"a1": "UTC"}, campaigns=list(created), created=created, truncate_above=2)

    docs = await _read_one_day(http, _report())

    assert _batches_requested(http) == [None, 2]
    assert sorted(d.ad_id for d in docs) == ["ad-last", "ad-old"]


@pytest.mark.asyncio
async def test_a_truncated_window_before_every_campaign_reads_quietly(monkeypatch, caplog):
    # TikTok can flag a window from before any listed campaign was created; it
    # has no rows, so there is nothing to batch and nothing to warn about.
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    http = ReportStub(
        {"a1": "UTC"}, campaigns=["c1"], created={"c1": "2026-11-01 00:00:00"}, truncate_above=0
    )

    with caplog.at_level(logging.WARNING):
        docs = await _read_one_day(http, _report())

    assert docs == []
    assert _batches_requested(http) == [None, None]
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


@pytest.mark.asyncio
async def test_rows_for_campaigns_the_listing_misses_are_kept_with_a_warning(monkeypatch, caplog):
    # Batches only cover listed campaigns, so rows TikTok returns for others
    # would be dropped by batching.
    monkeypatch.setattr(reports, "_now", lambda: datetime(2026, 10, 12, 12, tzinfo=UTC))
    row = {"dimensions": {"ad_id": "unlisted", "stat_time_day": "2026-10-12 00:00:00"}, "metrics": {"spend": "1"}}
    http = ReportStub(
        {"a1": "UTC"}, rows=[row], campaigns=["c1"], created={"c1": "2026-11-01 00:00:00"}, truncate_above=0
    )

    with caplog.at_level(logging.WARNING):
        docs = await _read_one_day(http, _report())

    assert [d.ad_id for d in docs] == ["unlisted"]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "doesn't list" in warnings[0].getMessage()


@pytest.mark.asyncio
async def test_a_query_too_large_window_is_split_until_it_fits():
    http = ReportStub({"a1": "UTC"}, too_large_above_days=8)
    report = _report()

    await _drain(
        backfill_report(  # type: ignore[arg-type]
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
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

    await check_report(LOG, stub_client(ok), _report(), "a1")  # type: ignore[arg-type]

    (params,) = ok.report_requests
    assert (params["start_date"], params["end_date"], params["page_size"]) == ("2026-10-12", "2026-10-12", 1)

    rejected = ReportStub({"a1": "UTC"}, error_code=40002)
    with pytest.raises(TikTokAPIError):
        await check_report(LOG, stub_client(rejected), _report(), "a1")  # type: ignore[arg-type]


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
            stub_client(http), "a1", _zone(http), report, report_document_model(report),
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
            if campaign_filter(p) == [campaign] and len(_days(p)) <= 4:
                covered |= _days(p)
        assert covered == expected, campaign
