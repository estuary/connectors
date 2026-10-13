import asyncio
from datetime import UTC, date, datetime, timedelta
from logging import getLogger

import pytest
from estuary_cdk.capture import common
from estuary_cdk.flow import ValidationError

from source_aws_cost_explorer import api, resources
from source_aws_cost_explorer.models import (
    AccountCost,
    CostAndUsageResponse,
    CostResourceConfig,
    DailyCost,
    EndpointConfig,
    ServiceCost,
)

from .fakes import CREDENTIALS, install_cost_explorer

EPOCH = datetime.fromtimestamp(0, UTC)


def metrics(amount="0.1234567890123456789012345"):
    return {name: {"Amount": amount, "Unit": "USD"} for name in api.COST_METRICS}


def result(day, groups=None, amount="0.1234567890123456789012345"):
    start = date.fromisoformat(day)
    return {
        "TimePeriod": {"Start": day, "End": (start + timedelta(days=1)).isoformat()},
        "Estimated": True,
        "Total": metrics(amount) if groups is None else {},
        "Groups": (
            []
            if groups is None
            else [{"Keys": [key], "Metrics": metrics(amount)} for key in groups]
        ),
    }


def page(*results, **extra):
    return CostAndUsageResponse.model_validate(
        {"ResultsByTime": list(results), **extra}
    )


def poll(fetch, start, cursor=EPOCH):
    """Run one poll of `fetch`, returning everything it yields."""

    async def collect():
        return [item async for item in fetch(CREDENTIALS, start, getLogger(), cursor)]

    return asyncio.run(collect())


def freeze_clock(monkeypatch, instant):
    monkeypatch.setattr(api, "utc_now", lambda: instant)


def test_sdk_pagination_freezes_arguments_and_closes(monkeypatch):
    fake = install_cost_explorer(
        monkeypatch,
        [
            {"ResultsByTime": [], "NextPageToken": "page2"},
            {"ResultsByTime": [result("2026-09-29", ["S3"])]},
        ],
    )
    pages = asyncio.run(
        api.query_cost_and_usage(
            CREDENTIALS, date(2026, 9, 1), date(2026, 9, 30), "SERVICE"
        )
    )
    assert fake.calls[0] == {
        "TimePeriod": {"Start": "2026-09-01", "End": "2026-09-30"},
        "Granularity": "DAILY",
        "Metrics": list(api.COST_METRICS),
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
    }
    assert fake.calls[1] == {**fake.calls[0], "NextPageToken": "page2"}
    assert len(pages) == 2
    assert fake.closed


def test_failed_continuation_does_not_emit_or_checkpoint(monkeypatch):
    fake = install_cost_explorer(
        monkeypatch,
        [
            {"ResultsByTime": [result("2026-09-29")], "NextPageToken": "page2"},
            RuntimeError("failed continuation"),
        ],
    )

    with pytest.raises(RuntimeError):
        poll(api.fetch_daily_costs, date(2026, 9, 1))
    assert fake.closed


@pytest.mark.parametrize(
    "model,groups",
    [(DailyCost, None), (ServiceCost, ["S3"]), (AccountCost, ["123456789012"])],
)
def test_grain_and_precision(model, groups):
    pages = [page(result("2026-09-29", groups)), page(result("2026-09-28", groups))]
    rows = list(api.cost_rows(pages, model))
    assert [row.date for row in rows] == ["2026-09-29", "2026-09-28"]
    for row in rows:
        assert (
            row.period_end
            == (date.fromisoformat(row.date) + timedelta(days=1)).isoformat()
        )
        assert set(row.metrics) == set(api.COST_METRICS)
        assert row.metrics["UnblendedCost"].amount == "0.1234567890123456789012345"
        assert row.metrics["UnblendedCost"].unit == "USD"
        assert row.estimated


def test_fragmented_group_pages_and_later_account_labels():
    pages = [
        page(result("2026-09-29", ["222222222222"])),
        page(
            result("2026-09-29", ["111111111111"]),
            DimensionValueAttributes=[
                {"Value": "222222222222", "Attributes": {"description": "Example"}}
            ],
        ),
    ]
    names = {
        row.model_dump()["linked_account_id"]: row.model_dump()["linked_account_name"]
        for row in api.cost_rows(pages, AccountCost)
    }
    assert names == {"222222222222": "Example", "111111111111": None}


def test_empty_groups_and_missing_metric():
    assert list(api.cost_rows([page(result("2026-09-29", []))], ServiceCost)) == []
    with pytest.raises(KeyError):
        list(api.cost_rows([page({**result("2026-09-29"), "Total": {}})], DailyCost))


@pytest.mark.parametrize(
    "now,expected",
    [
        (date(2026, 9, 30), date(2025, 8, 1)),
        (date(2026, 1, 1), date(2024, 12, 1)),
        (date(2025, 3, 1), date(2024, 2, 1)),
    ],
)
def test_retention_calendar_months(now, expected):
    assert api.available_start(date(2020, 1, 1), now) == expected
    assert api.available_start(now, now) == now


def test_explicit_start_normalizes_utc():
    assert resources.configured_start(EndpointConfig(credentials=CREDENTIALS)) is None
    explicit = EndpointConfig(
        credentials=CREDENTIALS, start_date="2026-09-01T01:00:00+02:00"
    )
    assert resources.configured_start(explicit) == date(2026, 8, 31)


def test_default_window_rolls_with_each_poll(monkeypatch):
    queried = []

    async def query(_credentials, start, end, group_by):
        queried.append((start, end))
        return []

    monkeypatch.setattr(api, "query_cost_and_usage", query)
    freeze_clock(monkeypatch, datetime(2026, 9, 30, 12, tzinfo=UTC))
    poll(api.fetch_daily_costs, None)
    freeze_clock(monkeypatch, datetime(2026, 10, 1, 12, tzinfo=UTC))
    poll(api.fetch_daily_costs, None)
    assert queried == [
        (date(2026, 8, 31), date(2026, 9, 30)),
        (date(2026, 9, 1), date(2026, 10, 1)),
    ]


def test_interval_defaults_to_and_requires_one_day():
    assert CostResourceConfig(name="daily_costs").interval == timedelta(days=1)
    resources.validate_intervals([CostResourceConfig(name="daily_costs")])
    with pytest.raises(ValidationError, match="daily_costs_by_service"):
        resources.validate_intervals(
            [
                CostResourceConfig(name="daily_costs"),
                CostResourceConfig(name="daily_costs_by_service", interval="PT1H"),
            ]
        )


def test_later_polls_reread_from_month_before_last_poll(monkeypatch):
    queried = []

    async def query(_credentials, start, end, group_by):
        queried.append(start)
        return []

    monkeypatch.setattr(api, "query_cost_and_usage", query)
    freeze_clock(monkeypatch, datetime(2026, 10, 13, 12, tzinfo=UTC))
    start = date(2025, 9, 1)
    poll(api.fetch_daily_costs, start, datetime(2026, 10, 12, 12, tzinfo=UTC))
    # Paused since June: the window reaches back to cover the missed days.
    poll(api.fetch_daily_costs, start, datetime(2026, 6, 10, 12, tzinfo=UTC))
    # A backfill resets the cursor and rereads from the start date.
    poll(api.fetch_daily_costs, start, EPOCH)
    assert queried == [date(2026, 9, 1), date(2026, 5, 1), date(2025, 9, 1)]


def test_old_history_survives_retention_and_explicit_zero_updates(monkeypatch):
    queried, emitted = [], {}
    freeze_clock(monkeypatch, datetime(2026, 9, 30, tzinfo=UTC))

    async def query(_credentials, start, end, group_by):
        queried.append((start, end))
        if end.month == 9:
            return [page(result("2025-08-15", ["S3"]), result("2026-09-29", ["S3"]))]
        return [page(result("2026-09-29", ["S3"], amount="0"))]

    monkeypatch.setattr(api, "query_cost_and_usage", query)

    def apply(items):
        for item in items:
            if isinstance(item, DailyCost):
                assert item.meta_.op == "u"
                emitted[(item.date, item.model_dump()["service"])] = item
        assert isinstance(items[-1], datetime)

    apply(poll(api.fetch_daily_costs_by_service, date(2025, 8, 1)))
    freeze_clock(monkeypatch, datetime(2026, 10, 1, tzinfo=UTC))
    apply(poll(api.fetch_daily_costs_by_service, date(2025, 8, 1)))
    assert queried == [
        (date(2025, 8, 1), date(2026, 9, 30)),
        (date(2025, 9, 1), date(2026, 10, 1)),
    ]
    assert emitted[("2025-08-15", "S3")].metrics["UnblendedCost"].amount != "0"
    assert emitted[("2026-09-29", "S3")].metrics["UnblendedCost"].amount == "0"


def test_future_range_is_empty_without_aws_request(monkeypatch):
    freeze_clock(monkeypatch, datetime(2026, 9, 30, tzinfo=UTC))

    async def forbidden(*_):
        pytest.fail("empty range queried AWS")

    monkeypatch.setattr(api, "query_cost_and_usage", forbidden)
    items = poll(api.fetch_daily_costs, date(2026, 10, 1))
    assert items == [datetime(2026, 9, 30, tzinfo=UTC)]


def test_static_discovery_uses_natural_keys(monkeypatch):
    async def forbidden(*_):
        pytest.fail("discovery queried AWS")

    monkeypatch.setattr(api, "query_cost_and_usage", forbidden)
    discovered = asyncio.run(
        resources.all_resources(
            getLogger(), None, EndpointConfig(credentials=CREDENTIALS)
        )
    )
    assert [resource.key for resource in discovered] == [
        ["/date"],
        ["/date", "/service"],
        ["/date", "/linked_account_id"],
    ]
    assert all(
        not isinstance(resource, common.SnapshotResource) for resource in discovered
    )
