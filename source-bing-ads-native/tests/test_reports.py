"""Offline tests for the report pipeline and its windows.

No live credentials are available, so a stub session answers Submit and Poll
and serves zipped CSVs shaped after Microsoft's documented report format.
"""

import io
import json
from json import dumps
import zipfile
from datetime import UTC, date, datetime, timedelta
from logging import getLogger
from typing import Any, AsyncGenerator, cast

import pytest
from estuary_cdk.capture.common import ResourceState
from estuary_cdk.http import HTTPError, HTTPSession
from source_bing_ads_native import reports
from source_bing_ads_native.api import AccountContext
from source_bing_ads_native.models import Account
from source_bing_ads_native.builtin_reports import REPORTS
from source_bing_ads_native.reports import (
    backfill_report,
    earliest_day,
    fetch_report,
    fetch_report_rows,
    months_before,
)

LOG = getLogger(__name__)

CAMPAIGN_REPORT = next(r for r in REPORTS if r.name == "campaign_performance_report_daily")
BUDGET_REPORT = next(r for r in REPORTS if r.name == "budget_summary_report")
CONTEXT = AccountContext(
    account=Account.model_validate(
        {"Id": "1", "Name": "acct", "ParentCustomerId": "900"}
    ),
    customer_id="900",
)


def _zipped_csv(rows: list[dict[str, str]]) -> bytes:
    text = io.StringIO()
    if rows:
        header = list(rows[0])
        text.write(",".join(f'"{h}"' for h in header) + "\r\n")
        for row in rows:
            text.write(",".join(f'"{row[h]}"' for h in header) + "\r\n")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        # Microsoft's CSVs start with a byte-order mark.
        archive.writestr("report.csv", "﻿" + text.getvalue())
    return buffer.getvalue()


def _campaign_row(day: str, campaign_id: str = "10", **overrides: str) -> dict[str, str]:
    row = {c: "" for c in CAMPAIGN_REPORT.columns}
    row.update(
        AccountId="1",
        TimePeriod=day,
        CampaignId=campaign_id,
        CampaignName="Spring",
        DeviceType="Computer",
        Network="Microsoft sites and select traffic",
        Clicks="3",
        Ctr="1.50%",
        Spend="--",
    )
    row.update(overrides)
    return row


class ReportStub:
    """Answers each Submit with an id, each Poll with the queued statuses for
    it, and each download with one row per day of the submitted range."""

    def __init__(self, polls: list[str] | None = None, empty: bool = False):
        self.polls = polls or ["Success"]
        self.empty = empty
        self.submitted: list[dict[str, Any]] = []
        self.downloads: list[tuple[str, bool]] = []
        self._poll_count: dict[str, int] = {}

    async def request(
        self,
        log: Any,
        url: str,
        method: str = "GET",
        json: Any = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> bytes:
        if url.endswith("/GenerateReport/Submit"):
            self.submitted.append(json)
            return _encode({"ReportRequestId": f"job-{len(self.submitted)}"})
        assert url.endswith("/GenerateReport/Poll")
        request_id = json["ReportRequestId"]
        count = self._poll_count.get(request_id, 0)
        self._poll_count[request_id] = count + 1
        status = self.polls[min(count, len(self.polls) - 1)]
        url_value = None if self.empty or status != "Success" else f"https://blob/{request_id}.zip"
        return _encode({"ReportRequestStatus": {"Status": status, "ReportDownloadUrl": url_value}})

    async def request_stream(self, log: Any, url: str, with_token: bool = True, **kwargs: Any):
        self.downloads.append((url, with_token))
        request = self.submitted[int(url.rsplit("-", 1)[1].removesuffix(".zip")) - 1]
        time = request["ReportRequest"]["Time"]
        first = date(**{k.lower(): v for k, v in time["CustomDateRangeStart"].items()})
        last = date(**{k.lower(): v for k, v in time["CustomDateRangeEnd"].items()})
        rows = []
        day = first
        while day <= last:
            rows.append(_campaign_row(day.isoformat()))
            day += timedelta(days=1)
        payload = _zipped_csv(rows)

        async def body() -> AsyncGenerator[bytes, None]:
            # Split the archive to exercise streamed decompression.
            for i in range(0, len(payload), 7):
                yield payload[i : i + 7]

        return {}, body


def _encode(value: Any) -> bytes:
    return json.dumps(value).encode()


def _session(stub: ReportStub) -> HTTPSession:
    return cast(HTTPSession, stub)


def _range(request: dict[str, Any]) -> tuple[date, date]:
    time = request["ReportRequest"]["Time"]
    to_date = lambda d: date(d["Year"], d["Month"], d["Day"])  # noqa: E731
    return to_date(time["CustomDateRangeStart"]), to_date(time["CustomDateRangeEnd"])


@pytest.fixture(autouse=True)
def no_poll_wait(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(reports, "POLL_INTERVAL", timedelta(0))


def test_report_keys_are_the_grain_columns():
    keys = {r.name: r.key() for r in REPORTS}

    assert keys["keyword_performance_report_daily"] == [
        "/AccountId", "/TimePeriod", "/AdDistribution", "/AdGroupId", "/AdId",
        "/BidMatchType", "/CampaignId", "/DeliveredMatchType", "/DeviceOS",
        "/DeviceType", "/KeywordId", "/Language", "/Network", "/TopVsOther",
    ]
    assert keys["budget_summary_report"] == ["/AccountId", "/Date", "/CampaignId"]
    # Descriptive attributes that change over time never key a report.
    for r in REPORTS:
        for column in ("CurrencyCode", "CampaignName", "CampaignStatus", "QualityScore", "Keyword"):
            assert f"/{column}" not in r.key()


@pytest.mark.asyncio
async def test_report_job_submits_polls_and_parses_the_download():
    stub = ReportStub(polls=["Pending", "Pending", "Success"])

    rows = [
        row
        async for row in fetch_report_rows(
            _session(stub), LOG, CONTEXT, CAMPAIGN_REPORT, CAMPAIGN_REPORT.row_model(),
            date(2026, 10, 1), date(2026, 10, 2),
        )
    ]

    request = stub.submitted[0]["ReportRequest"]
    assert request["Type"] == "CampaignPerformanceReportRequest"
    assert request["Aggregation"] == "Daily"
    assert request["ReturnOnlyCompleteData"] is False
    assert request["Scope"] == {"AccountIds": ["1"]}
    # London days, as in Airbyte's connector.
    assert request["Time"]["ReportTimeZone"] == "GreenwichMeanTimeDublinEdinburghLisbonLondon"
    assert stub.downloads == [("https://blob/job-1.zip", False)]

    assert [r.TimePeriod for r in rows] == ["2026-10-01", "2026-10-02"]  # type: ignore[attr-defined]
    row = rows[0].model_dump()
    # Values are passed through as Microsoft writes them; only empty is null.
    assert row["AccountId"] == "1" and row["CampaignId"] == "10"
    assert row["Clicks"] == "3" and row["Ctr"] == "1.50%" and row["Spend"] == "--"
    assert row["CampaignLabels"] is None and row["DeviceType"] == "Computer"


@pytest.mark.asyncio
async def test_success_without_a_url_is_an_empty_report():
    stub = ReportStub(empty=True)

    rows = [
        row
        async for row in fetch_report_rows(
            _session(stub), LOG, CONTEXT, CAMPAIGN_REPORT, CAMPAIGN_REPORT.row_model(),
            date(2026, 10, 1), date(2026, 10, 1),
        )
    ]

    assert rows == [] and stub.downloads == []


@pytest.mark.asyncio
async def test_failed_report_raises():
    stub = ReportStub(polls=["Error"])

    with pytest.raises(RuntimeError, match="account 1 .* status Error"):
        async for _ in fetch_report_rows(
            _session(stub), LOG, CONTEXT, CAMPAIGN_REPORT, CAMPAIGN_REPORT.row_model(),
            date(2026, 10, 1), date(2026, 10, 1),
        ):
            pass


@pytest.mark.asyncio
async def test_report_still_pending_past_the_deadline_raises(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(reports, "MAX_POLL_WAIT", timedelta(0))
    stub = ReportStub(polls=["Pending"])

    with pytest.raises(RuntimeError, match="still pending"):
        async for _ in fetch_report_rows(
            _session(stub), LOG, CONTEXT, CAMPAIGN_REPORT, CAMPAIGN_REPORT.row_model(),
            date(2026, 10, 1), date(2026, 10, 1),
        ):
            pass


@pytest.mark.asyncio
async def test_report_faults_name_the_account_and_tracking_id():
    class Rejecting(ReportStub):
        async def request(
            self,
            log: Any,
            url: str,
            method: str = "GET",
            json: Any = None,
            headers: dict[str, str] | None = None,
            **kwargs: Any,
        ) -> bytes:
            body = {"TrackingId": "t-9", "Errors": [{"Code": 106, "ErrorCode": "UserIsNotAuthorized", "Message": "no"}]}
            raise HTTPError(f"Encountered HTTP error status 400.\nResponse:\n{dumps(body)}", 400)

    with pytest.raises(HTTPError, match=r"account 1 .*106 UserIsNotAuthorized.*TrackingId t-9"):
        async for _ in fetch_report_rows(
            _session(Rejecting()), LOG, CONTEXT, CAMPAIGN_REPORT, CAMPAIGN_REPORT.row_model(),
            date(2026, 10, 1), date(2026, 10, 1),
        ):
            pass


@pytest.mark.asyncio
async def test_budget_summary_has_no_aggregation_and_keeps_its_dates():
    class BudgetStub(ReportStub):
        async def request_stream(self, log: Any, url: str, with_token: bool = True, **kwargs: Any):
            payload = _zipped_csv(
                [
                    {
                        "AccountName": "a", "AccountNumber": "X1", "AccountId": "1",
                        "CampaignName": "c", "CampaignId": "10", "Date": "10/1/2026",
                        "CurrencyCode": "USD", "MonthlyBudget": "300.00",
                        "DailySpend": "9.50", "MonthToDateSpend": "9.50",
                    }
                ]
            )

            async def body() -> AsyncGenerator[bytes, None]:
                yield payload

            return {}, body

    stub = BudgetStub()
    rows = [
        r
        async for r in fetch_report_rows(
            _session(stub), LOG, CONTEXT, BUDGET_REPORT, BUDGET_REPORT.row_model(),
            date(2026, 10, 1), date(2026, 10, 1),
        )
    ]

    assert "Aggregation" not in stub.submitted[0]["ReportRequest"]
    row = rows[0].model_dump()
    assert row["Date"] == "10/1/2026"
    assert row["AccountId"] == "1" and row["CampaignId"] == "10" and row["MonthlyBudget"] == "300.00"


def test_report_schemas_declare_only_key_columns():
    keyword = next(r for r in REPORTS if r.name == "keyword_performance_report_daily")
    properties = keyword.row_model().model_json_schema()["properties"]

    assert set(properties) - {"_meta"} == {k.lstrip("/") for k in keyword.key()}
    assert properties["AccountId"]["type"] == "string"


def test_start_date_is_clamped_to_retention():
    today = date(2026, 10, 27)
    old = datetime(2020, 1, 1, tzinfo=UTC)

    assert earliest_day(CAMPAIGN_REPORT, old, today) == date(2023, 10, 28)
    assert earliest_day(BUDGET_REPORT, old, today) == date(2024, 10, 28)
    assert earliest_day(CAMPAIGN_REPORT, datetime(2026, 1, 5, tzinfo=UTC), today) == date(2026, 1, 5)
    assert months_before(date(2026, 3, 31), 1) == date(2026, 2, 28)


async def _drain(gen: AsyncGenerator[Any, None]) -> list[Any]:
    return [item async for item in gen]


def _at(now: datetime):
    return lambda: now


async def _incremental(
    stub: ReportStub, cursor: datetime, lookback_window_days: int = 30,
    start: datetime = datetime(2026, 1, 1, tzinfo=UTC),
) -> list[Any]:
    return await _drain(
        fetch_report(
            _session(stub), CONTEXT, CAMPAIGN_REPORT, CAMPAIGN_REPORT.row_model(),
            start, lookback_window_days, LOG, cursor,
        )
    )


@pytest.mark.asyncio
async def test_daily_run_with_default_lookback_is_one_job(monkeypatch: pytest.MonkeyPatch):
    now = datetime(2026, 10, 10, 8, tzinfo=UTC)
    monkeypatch.setattr(reports, "utc_now", _at(now))
    stub = ReportStub()

    items = await _incremental(stub, datetime(2026, 10, 9, 15, tzinfo=UTC))

    # The first run of Oct 10 re-fetches 30 days before the cursor's day
    # through today, with the real window size, in a single job.
    assert [_range(r) for r in stub.submitted] == [(date(2026, 9, 9), date(2026, 10, 10))]
    assert [i for i in items if isinstance(i, datetime)] == [now]


@pytest.mark.asyncio
async def test_later_runs_the_same_day_refresh_only_today(monkeypatch: pytest.MonkeyPatch):
    now = datetime(2026, 10, 10, 8, tzinfo=UTC)
    monkeypatch.setattr(reports, "utc_now", _at(now))
    stub = ReportStub()

    items = await _incremental(stub, datetime(2026, 10, 10, 3, tzinfo=UTC))

    assert [_range(r) for r in stub.submitted] == [(date(2026, 10, 10), date(2026, 10, 10))]
    assert items[-1] == now and len(items) == 2


@pytest.mark.asyncio
async def test_zero_lookback_still_refetches_the_cursor_day(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(reports, "utc_now", _at(datetime(2026, 10, 10, 8, tzinfo=UTC)))
    stub = ReportStub()

    await _incremental(stub, datetime(2026, 10, 8, 15, tzinfo=UTC), lookback_window_days=0)

    # Oct 8 was only partly over when last fetched.
    assert [_range(r) for r in stub.submitted] == [(date(2026, 10, 8), date(2026, 10, 10))]


@pytest.mark.asyncio
async def test_incremental_is_clamped_to_the_start_date(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(reports, "utc_now", _at(datetime(2026, 10, 10, 8, tzinfo=UTC)))
    stub = ReportStub()

    await _incremental(
        stub, datetime(2026, 10, 9, 15, tzinfo=UTC), start=datetime(2026, 10, 5, tzinfo=UTC)
    )

    assert [_range(r) for r in stub.submitted] == [(date(2026, 10, 5), date(2026, 10, 10))]


@pytest.mark.asyncio
async def test_catching_up_after_an_outage_checkpoints_each_window(monkeypatch: pytest.MonkeyPatch):
    now = datetime(2026, 10, 10, 8, tzinfo=UTC)
    monkeypatch.setattr(reports, "utc_now", _at(now))
    monkeypatch.setattr(reports, "INCREMENTAL_WINDOW_DAYS", 3)
    stub = ReportStub()

    items = await _incremental(stub, datetime(2026, 10, 3, 10, tzinfo=UTC), lookback_window_days=2)

    assert [_range(r) for r in stub.submitted] == [
        (date(2026, 10, 1), date(2026, 10, 3)),
        (date(2026, 10, 4), date(2026, 10, 6)),
        (date(2026, 10, 7), date(2026, 10, 9)),
        (date(2026, 10, 10), date(2026, 10, 10)),
    ]
    # Each window before today checkpoints the start of the next day to
    # fetch; the sweep ends at the current time.
    assert [i for i in items if isinstance(i, datetime)] == [
        datetime(2026, 10, 4, tzinfo=UTC),
        datetime(2026, 10, 7, tzinfo=UTC),
        datetime(2026, 10, 10, tzinfo=UTC),
        now,
    ]
    assert len([i for i in items if not isinstance(i, datetime)]) == 10


@pytest.mark.asyncio
async def test_lookback_windows_before_the_cursor_are_not_checkpointed(monkeypatch: pytest.MonkeyPatch):
    now = datetime(2026, 10, 10, 8, tzinfo=UTC)
    monkeypatch.setattr(reports, "utc_now", _at(now))
    monkeypatch.setattr(reports, "INCREMENTAL_WINDOW_DAYS", 3)
    stub = ReportStub()

    items = await _incremental(stub, datetime(2026, 10, 9, 10, tzinfo=UTC), lookback_window_days=4)

    # Oct 5-7's checkpoint would be Oct 8, before the cursor.
    assert [_range(r) for r in stub.submitted] == [
        (date(2026, 10, 5), date(2026, 10, 7)),
        (date(2026, 10, 8), date(2026, 10, 10)),
    ]
    assert [i for i in items if isinstance(i, datetime)] == [now]


@pytest.mark.asyncio
async def test_backfill_runs_from_start_to_cutoff_and_resumes_by_date(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(reports, "BACKFILL_WINDOW_DAYS", 4)
    stub = ReportStub()
    start = datetime(2026, 10, 1, tzinfo=UTC)
    cutoff = datetime(2026, 10, 10, tzinfo=UTC)

    items = await _drain(
        backfill_report(
            _session(stub), CONTEXT, CAMPAIGN_REPORT, CAMPAIGN_REPORT.row_model(),
            start, LOG, None, cutoff,
        )
    )

    assert [_range(r) for r in stub.submitted] == [
        (date(2026, 10, 1), date(2026, 10, 4)),
        (date(2026, 10, 5), date(2026, 10, 8)),
        (date(2026, 10, 9), date(2026, 10, 10)),
    ]
    # The final window ends the backfill without a trailing cursor.
    assert [i for i in items if isinstance(i, str)] == ["2026-10-05", "2026-10-09"]

    resumed = ReportStub()
    await _drain(
        backfill_report(
            _session(resumed), CONTEXT, CAMPAIGN_REPORT, CAMPAIGN_REPORT.row_model(),
            start, LOG, "2026-10-09", cutoff,
        )
    )
    assert [_range(r) for r in resumed.submitted] == [(date(2026, 10, 9), date(2026, 10, 10))]


@pytest.mark.asyncio
async def test_new_accounts_get_state_and_existing_state_is_kept(monkeypatch: pytest.MonkeyPatch):
    from source_bing_ads_native import resources

    now = datetime(2026, 10, 11, 8, tzinfo=UTC)
    monkeypatch.setattr(resources, "utc_now", lambda: now)
    existing = ResourceState.Incremental(cursor=datetime(2026, 9, 1, tzinfo=UTC))
    state = ResourceState(inc={"1": existing}, backfill={"1": None})
    checkpoints: list[Any] = []

    class Task:
        log = LOG

        async def checkpoint(self, value: Any) -> None:
            checkpoints.append(value)

    class Binding:
        stateKey = "campaign_performance_report_daily"

    await resources._reconcile_account_state(["1", "2"], Binding(), state, Task())  # type: ignore[arg-type]

    assert isinstance(state.inc, dict) and isinstance(state.backfill, dict)
    assert state.inc["1"] is existing and state.backfill["1"] is None
    # Backfill through yesterday; incremental starts with today.
    assert state.inc["2"] == ResourceState.Incremental(cursor=now)
    assert state.backfill["2"] == ResourceState.Backfill(
        cutoff=datetime(2026, 10, 10, tzinfo=UTC), next_page=None
    )
    assert len(checkpoints) == 1

    await resources._reconcile_account_state(["1", "2"], Binding(), state, Task())  # type: ignore[arg-type]
    assert len(checkpoints) == 1
