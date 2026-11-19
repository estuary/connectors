"""Offline tests for the report windows, pagination, and row keys.

A stub session records each request's query and answers with canned pages, and
the clock is patched, so nothing here calls Anthropic.
"""

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from logging import getLogger
from typing import Any, cast
from urllib.parse import parse_qsl, urlsplit

import pytest
from estuary_cdk.http import HTTPError, HTTPSession

from source_anthropic import api
from source_anthropic.api import (
    backfill_cost,
    backfill_usage,
    ceil_to,
    fetch_cost,
    fetch_usage,
    floor_to,
    previous_month_settled_at,
    previous_month_start,
)
from source_anthropic.models import (
    DAILY,
    GROUP_BY,
    HOURLY,
    MINUTELY,
    CostRow,
    UsageRow,
)

LOG = getLogger(__name__)
LOOKBACK = timedelta(hours=48)
CONSOLE = GROUP_BY["Claude Console"]
ENTERPRISE = GROUP_BY["Claude Enterprise"]
START = datetime(2026, 1, 1, tzinfo=UTC)


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _row(starting_at: str, ending_at: str, **group: Any) -> dict[str, Any]:
    base = {
        "actor": {"type": "user_actor", "user_id": "user_1"},
        "model": "claude-sonnet-5-5",
        "product": "messages_api",
        "api_key_id": None,
        "workspace_id": "wrkspc_1",
        "speed": None,
        "context_window": None,
    }
    return {
        "starting_at": starting_at,
        "ending_at": ending_at,
        "group": {**base, **group},
        "usage": {"requests": 1},
    }


def _page(
    rows: list[dict[str, Any]], next_page: str | None = None
) -> dict[str, Any]:
    return {
        "data": rows,
        "has_more": next_page is not None,
        "next_page": next_page,
        "data_refreshed_at": "2026-11-16T14:48:10Z",
    }


Responder = Callable[[dict[str, Any]], dict[str, Any] | HTTPError]


class Stub:
    """Records each request's path, query, and headers; answers with the
    responder's page (or raises the HTTPError it returns). The default answers
    every window with one row at its start."""

    def __init__(self, responder: Responder | None = None):
        self.requests: list[dict[str, Any]] = []
        self.responder = responder or (
            lambda q: _page([_row(q["starting_at"], q["ending_at"])])
        )

    async def request(
        self, log: Any, url: str, headers: dict[str, str] | None = None, **kwargs: Any
    ) -> bytes:
        parts = urlsplit(url)
        pairs = parse_qsl(parts.query)
        query: dict[str, Any] = {k: v for k, v in pairs if k != "group_by[]"}
        query["group_by[]"] = [v for k, v in pairs if k == "group_by[]"]
        query["path"] = parts.path
        query["headers"] = headers or {}
        self.requests.append(query)
        response = self.responder(query)
        if isinstance(response, HTTPError):
            raise response
        return json.dumps(response).encode()

    def windows(self) -> list[tuple[str, str]]:
        return [(r["starting_at"], r["ending_at"]) for r in self.requests]


def _session(stub: Stub) -> HTTPSession:
    return cast(HTTPSession, stub)


async def _drain(gen: Any) -> list[Any]:
    return [item async for item in gen]


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Callable[[datetime], None]:
    def set_now(now: datetime) -> None:
        monkeypatch.setattr(api, "utc_now", lambda: now)

    return set_now


def test_floor_and_ceil_to_buckets():
    t = datetime(2026, 11, 16, 15, 7, 30, tzinfo=UTC)
    assert floor_to(t, MINUTELY.tick) == datetime(2026, 11, 16, 15, 7, tzinfo=UTC)
    assert floor_to(t, HOURLY.tick) == datetime(2026, 11, 16, 15, tzinfo=UTC)
    assert floor_to(t, DAILY.tick) == datetime(2026, 11, 16, tzinfo=UTC)
    assert ceil_to(t, HOURLY.tick) == datetime(2026, 11, 16, 16, tzinfo=UTC)
    # An aligned time is its own ceiling.
    aligned = datetime(2026, 11, 16, 15, tzinfo=UTC)
    assert ceil_to(aligned, HOURLY.tick) == aligned


def test_previous_month_settles_on_the_ninth():
    assert previous_month_settled_at(datetime(2026, 12, 20, 8, tzinfo=UTC)) == datetime(
        2026, 12, 9, tzinfo=UTC
    )


def test_previous_month_start_crosses_the_year():
    assert previous_month_start(datetime(2026, 12, 3, 8, tzinfo=UTC)) == datetime(
        2026, 11, 1, tzinfo=UTC
    )
    assert previous_month_start(datetime(2027, 1, 2, tzinfo=UTC)) == datetime(
        2026, 12, 1, tzinfo=UTC
    )


@pytest.mark.asyncio
async def test_hourly_sweep_rereads_the_lookback_through_the_current_bucket(clock):
    now = datetime(2026, 11, 16, 15, 7, tzinfo=UTC)
    clock(now)
    stub = Stub()

    items = await _drain(
        fetch_usage(_session(stub), HOURLY, CONSOLE, START, LOOKBACK, LOG, now - timedelta(hours=1))
    )

    assert stub.windows() == [("2026-11-14T14:00:00Z", "2026-11-16T16:00:00Z")]
    # Rows first, then the sweep time as the new cursor.
    assert isinstance(items[0], UsageRow)
    assert items[-1] == now


@pytest.mark.asyncio
async def test_requests_carry_the_grouping_ordering_and_beta_headers(clock):
    now = datetime(2026, 11, 16, 15, 7, tzinfo=UTC)
    clock(now)
    stub = Stub()

    await _drain(
        fetch_usage(_session(stub), HOURLY, CONSOLE, START, LOOKBACK, LOG, now - timedelta(hours=1))
    )

    request = stub.requests[0]
    assert request["path"] == "/v1/organizations/analytics/usage"
    assert request["time_bucket_width"] == "1h"
    assert request["order_by"] == "time"
    assert request["order"] == "asc"
    assert request["limit"] == "1000"
    assert request["group_by[]"] == [
        "actor",
        "model",
        "product",
        "api_key_id",
        "workspace_id",
    ]
    assert request["headers"]["anthropic-beta"] == "analytics-usage-cost-2026-08-24"


@pytest.mark.asyncio
async def test_enterprise_groups_by_speed_and_context_window(clock):
    now = datetime(2026, 11, 16, 15, 7, tzinfo=UTC)
    clock(now)
    stub = Stub(lambda q: _page([]))

    await _drain(
        fetch_cost(
            _session(stub), HOURLY, ENTERPRISE, START, LOOKBACK, LOG,
            now - timedelta(hours=1),
        )
    )

    # api_key_id and workspace_id are always null for Enterprise, so their
    # slots go to the dimensions that explain price differences.
    assert stub.requests[0]["group_by[]"] == [
        "actor",
        "model",
        "product",
        "speed",
        "context_window",
        "cost_type",
        "token_type",
    ]


def test_group_key_distinguishes_speed_and_context_window():
    def key(speed: str, context_window: str) -> str:
        return UsageRow.model_validate(
            _row(
                "2026-11-12T21:00:00Z", "2026-11-12T22:00:00Z",
                speed=speed, context_window=context_window,
            )
        ).group_key

    assert key("fast", "200k-1M").endswith("|speed=fast|context_window=200k-1M")
    assert key("fast", "200k-1M") != key("standard", "200k-1M")
    assert key("fast", "200k-1M") != key("fast", "0-200k")


@pytest.mark.asyncio
async def test_cost_adds_cost_and_token_type_grouping(clock):
    clock(datetime(2026, 11, 16, 15, tzinfo=UTC))
    stub = Stub(
        lambda q: _page(
            [
                {
                    **_row(q["starting_at"], q["ending_at"], cost_type="tokens", token_type="output_tokens"),
                    "cost": {"amount": "6.487000", "list_amount": "6.487000", "currency": "USD"},
                }
            ]
        )
    )

    items = await _drain(
        backfill_cost(
            _session(stub), DAILY, CONSOLE, datetime(2026, 11, 10, tzinfo=UTC), LOG, None,
            datetime(2026, 11, 12, 9, tzinfo=UTC),
        )
    )

    assert stub.requests[0]["path"] == "/v1/organizations/analytics/cost"
    assert stub.requests[0]["group_by[]"][-2:] == ["cost_type", "token_type"]
    row = items[0]
    assert isinstance(row, CostRow)
    assert row.group_key.endswith("|cost_type=tokens|token_type=output_tokens")
    # Amounts stay decimal strings.
    assert row.model_dump()["cost"]["amount"] == "6.487000"


async def _hourly_windows(now: datetime, cursor: datetime) -> list[tuple[str, str]]:
    stub = Stub()
    await _drain(fetch_usage(_session(stub), HOURLY, CONSOLE, START, LOOKBACK, LOG, cursor))
    return stub.windows()


@pytest.mark.asyncio
async def test_first_sweep_after_the_ninth_rereads_the_previous_month(clock):
    now = datetime(2026, 12, 9, 0, 30, tzinfo=UTC)
    clock(now)

    # From Nov 1 through the current bucket, split at the 31-day span limit.
    assert await _hourly_windows(now, datetime(2026, 12, 8, 23, 30, tzinfo=UTC)) == [
        ("2026-11-01T00:00:00Z", "2026-12-02T00:00:00Z"),
        ("2026-12-02T00:00:00Z", "2026-12-09T01:00:00Z"),
    ]


@pytest.mark.asyncio
async def test_early_month_days_use_only_the_lookback(clock):
    now = datetime(2026, 12, 3, 8, 30, tzinfo=UTC)
    clock(now)

    # Before the 9th, November can still be revised, so it isn't re-read yet.
    assert await _hourly_windows(now, datetime(2026, 12, 2, 23, tzinfo=UTC)) == [
        ("2026-11-30T23:00:00Z", "2026-12-03T09:00:00Z")
    ]


@pytest.mark.asyncio
async def test_later_sweeps_after_the_ninth_use_only_the_lookback(clock):
    now = datetime(2026, 12, 9, 2, tzinfo=UTC)
    clock(now)

    # The 00:30 sweep already re-read November; this cursor is past the 9th.
    assert await _hourly_windows(now, datetime(2026, 12, 9, 1, tzinfo=UTC)) == [
        ("2026-12-07T01:00:00Z", "2026-12-09T02:00:00Z")
    ]


@pytest.mark.asyncio
async def test_a_missed_ninth_rereads_the_previous_month_on_the_next_sweep(clock):
    now = datetime(2026, 12, 12, 10, tzinfo=UTC)
    clock(now)

    # The task was down from the 5th to the 12th; the cursor predates the 9th.
    windows = await _hourly_windows(now, datetime(2026, 12, 5, tzinfo=UTC))
    assert windows[0][0] == "2026-11-01T00:00:00Z"


@pytest.mark.asyncio
async def test_minutely_rereads_the_lookback_split_at_the_24h_limit(clock):
    now = datetime(2026, 11, 16, 15, 7, 30, tzinfo=UTC)
    clock(now)
    stub = Stub()

    # The cursor plays no part in where a minutely sweep starts.
    await _drain(
        fetch_usage(
            _session(stub), MINUTELY, CONSOLE, START, LOOKBACK, LOG,
            datetime(2026, 11, 1, tzinfo=UTC),
        )
    )

    # 48h of lookback in 24h requests, through the current minute.
    assert stub.windows() == [
        ("2026-11-14T15:07:00Z", "2026-11-15T15:07:00Z"),
        ("2026-11-15T15:07:00Z", "2026-11-16T15:07:00Z"),
        ("2026-11-16T15:07:00Z", "2026-11-16T15:08:00Z"),
    ]
    assert {r["time_bucket_width"] for r in stub.requests} == {"1m"}


@pytest.mark.asyncio
async def test_minutely_rereads_at_least_a_day(clock):
    now = datetime(2026, 11, 16, 15, 7, 30, tzinfo=UTC)
    clock(now)
    stub = Stub()

    await _drain(
        fetch_usage(
            _session(stub), MINUTELY, CONSOLE, START, timedelta(hours=6), LOG,
            now - timedelta(hours=1),
        )
    )

    assert stub.windows()[0][0] == "2026-11-15T15:07:00Z"


@pytest.mark.asyncio
async def test_incremental_is_clamped_to_the_start_date(clock):
    now = datetime(2026, 11, 16, 15, tzinfo=UTC)
    clock(now)
    stub = Stub()

    await _drain(
        fetch_usage(
            _session(stub), HOURLY, CONSOLE, datetime(2026, 11, 16, 2, 30, tzinfo=UTC),
            LOOKBACK, LOG, now - timedelta(hours=1),
        )
    )

    assert stub.windows() == [("2026-11-16T02:00:00Z", "2026-11-16T15:00:00Z")]


@pytest.mark.asyncio
async def test_backfill_walks_31_day_windows_and_resumes_by_window_start(clock):
    clock(datetime(2026, 11, 16, tzinfo=UTC))
    cutoff = datetime(2026, 3, 15, 10, 30, tzinfo=UTC)
    stub = Stub()

    items = await _drain(
        backfill_usage(_session(stub), DAILY, CONSOLE, START, LOG, None, cutoff)
    )

    # The cutoff's own bucket belongs to incremental.
    assert stub.windows() == [
        ("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"),
        ("2026-02-01T00:00:00Z", "2026-03-04T00:00:00Z"),
        ("2026-03-04T00:00:00Z", "2026-03-15T00:00:00Z"),
    ]
    # A page cursor after each window but the last, after its rows.
    pages = [i for i in items if isinstance(i, str)]
    assert pages == ["2026-02-01T00:00:00Z", "2026-03-04T00:00:00Z"]
    assert isinstance(items[0], UsageRow) and items[1] == pages[0]

    resumed = Stub()
    await _drain(
        backfill_usage(_session(resumed), DAILY, CONSOLE, START, LOG, pages[1], cutoff)
    )
    assert resumed.windows() == [("2026-03-04T00:00:00Z", "2026-03-15T00:00:00Z")]


@pytest.mark.asyncio
async def test_backfill_start_is_clamped_to_the_data_floor_and_retention(clock):
    clock(datetime(2027, 6, 1, 12, tzinfo=UTC))
    stub = Stub()

    await _drain(
        backfill_usage(
            _session(stub), DAILY, CONSOLE, datetime(2025, 6, 1, tzinfo=UTC), LOG, None,
            datetime(2026, 6, 10, tzinfo=UTC),
        )
    )

    # 365 days before "now", ceiled to the day, is later than 2026-01-01.
    assert stub.windows()[0][0] == "2026-06-02T00:00:00Z"


@pytest.mark.asyncio
async def test_pages_are_followed_with_identical_parameters(clock):
    clock(datetime(2026, 11, 16, tzinfo=UTC))

    def responder(q: dict[str, Any]) -> dict[str, Any]:
        row = _row(q["starting_at"], q["ending_at"])
        return _page([row], None if q.get("page") == "page_2" else "page_2")

    stub = Stub(responder)
    items = await _drain(
        backfill_usage(
            _session(stub), DAILY, CONSOLE, datetime(2026, 11, 10, tzinfo=UTC), LOG, None,
            datetime(2026, 11, 12, tzinfo=UTC),
        )
    )

    assert [r.get("page") for r in stub.requests] == [None, "page_2"]
    first, second = ({k: v for k, v in r.items() if k != "page"} for r in stub.requests)
    assert first == second
    # Both pages carry the same bucket and group, so they merge into one row.
    rows = [i for i in items if isinstance(i, UsageRow)]
    assert len(rows) == 1
    assert rows[0].model_dump()["usage"]["requests"] == 2


@pytest.mark.asyncio
async def test_an_expired_page_cursor_restarts_the_window(clock):
    clock(datetime(2026, 11, 16, tzinfo=UTC))
    calls = {"n": 0}

    def responder(q: dict[str, Any]) -> dict[str, Any] | HTTPError:
        calls["n"] += 1
        if calls["n"] == 2:
            return HTTPError("cursor expired", 410)
        row = _row(q["starting_at"], q["ending_at"])
        return _page([row], "page_2" if calls["n"] == 1 else None)

    stub = Stub(responder)
    await _drain(
        backfill_usage(
            _session(stub), DAILY, CONSOLE, datetime(2026, 11, 10, tzinfo=UTC), LOG, None,
            datetime(2026, 11, 12, tzinfo=UTC),
        )
    )

    assert [r.get("page") for r in stub.requests] == [None, "page_2", None]


@pytest.mark.asyncio
async def test_a_window_whose_cursor_keeps_expiring_gives_up(clock):
    clock(datetime(2026, 11, 16, tzinfo=UTC))

    def responder(q: dict[str, Any]) -> dict[str, Any] | HTTPError:
        if q.get("page"):
            return HTTPError("cursor expired", 410)
        return _page([_row(q["starting_at"], q["ending_at"])], "page_2")

    stub = Stub(responder)
    with pytest.raises(HTTPError):
        await _drain(
            backfill_usage(
                _session(stub), DAILY, CONSOLE, datetime(2026, 11, 10, tzinfo=UTC), LOG, None,
                datetime(2026, 11, 12, tzinfo=UTC),
            )
        )

    # The first pass plus MAX_WINDOW_RESTARTS restarts, each reaching page 2.
    assert [r.get("page") for r in stub.requests] == [None, "page_2"] * (
        api.MAX_WINDOW_RESTARTS + 1
    )


@pytest.mark.asyncio
async def test_a_410_without_a_page_cursor_is_raised(clock):
    clock(datetime(2026, 11, 16, tzinfo=UTC))
    stub = Stub(lambda q: HTTPError("gone", 410))

    with pytest.raises(HTTPError):
        await _drain(
            backfill_usage(
                _session(stub), DAILY, CONSOLE, datetime(2026, 11, 10, tzinfo=UTC), LOG, None,
                datetime(2026, 11, 12, tzinfo=UTC),
            )
        )


def _null_actor_row(starting_at: str, ending_at: str, **usage: Any) -> dict[str, Any]:
    return {**_row(starting_at, ending_at, actor=None), "usage": usage}


@pytest.mark.asyncio
async def test_rows_sharing_a_key_are_summed(clock):
    clock(datetime(2026, 11, 16, tzinfo=UTC))
    start, end = "2026-11-12T00:00:00Z", "2026-11-13T00:00:00Z"
    # Anthropic returns each identity it can't render as its own null-actor row.
    stub = Stub(
        lambda q: _page(
            [
                _null_actor_row(start, end, requests=1, output_tokens=500,
                                cache_creation={"ephemeral_5m_input_tokens": 10}),
                _null_actor_row(start, end, requests=2, output_tokens=300,
                                cache_creation={"ephemeral_5m_input_tokens": 0}),
                _null_actor_row(start, end, requests=None, output_tokens=200,
                                cache_creation={"ephemeral_5m_input_tokens": 5}),
            ]
        )
    )

    items = await _drain(
        backfill_usage(
            _session(stub), DAILY, CONSOLE, datetime(2026, 11, 12, tzinfo=UTC), LOG, None,
            datetime(2026, 11, 13, tzinfo=UTC),
        )
    )

    rows = [i for i in items if isinstance(i, UsageRow)]
    assert len(rows) == 1
    assert rows[0].model_dump()["usage"] == {
        "requests": 3,
        "output_tokens": 1000,
        "cache_creation": {"ephemeral_5m_input_tokens": 15},
    }


@pytest.mark.asyncio
async def test_a_bucket_split_across_pages_is_summed_once(clock):
    clock(datetime(2026, 11, 16, tzinfo=UTC))
    day1 = ("2026-11-12T00:00:00Z", "2026-11-13T00:00:00Z")
    day2 = ("2026-11-13T00:00:00Z", "2026-11-14T00:00:00Z")

    def responder(q: dict[str, Any]) -> dict[str, Any]:
        if q.get("page") != "page_2":
            return _page([_null_actor_row(*day1, output_tokens=500)], "page_2")
        return _page(
            [
                _null_actor_row(*day1, output_tokens=300),
                _null_actor_row(*day2, output_tokens=7),
                _row(*day2),
            ]
        )

    stub = Stub(responder)
    items = await _drain(
        backfill_usage(
            _session(stub), DAILY, CONSOLE, datetime(2026, 11, 12, tzinfo=UTC), LOG, None,
            datetime(2026, 11, 14, tzinfo=UTC),
        )
    )

    rows = [i.model_dump() for i in items if isinstance(i, UsageRow)]
    assert [(str(r["starting_at"])[:10], r["group_key"][:7]) for r in rows] == [
        ("2026-11-12", "actor=|"),
        ("2026-11-13", "actor=|"),
        ("2026-11-13", "actor=u"),
    ]
    assert rows[0]["usage"]["output_tokens"] == 800
    assert rows[1]["usage"]["output_tokens"] == 7


@pytest.mark.asyncio
async def test_cost_amounts_sum_exactly(clock):
    clock(datetime(2026, 11, 16, tzinfo=UTC))
    start, end = "2026-11-12T00:00:00Z", "2026-11-13T00:00:00Z"

    def cost_row(amount: str) -> dict[str, Any]:
        return {
            **_row(start, end, actor=None, cost_type="tokens", token_type="output_tokens"),
            "cost": {"amount": amount, "list_amount": amount, "currency": "USD", "requests": None},
        }

    stub = Stub(lambda q: _page([cost_row("6.487000"), cost_row("0.017200"), cost_row("0.000100")]))
    items = await _drain(
        backfill_cost(
            _session(stub), DAILY, CONSOLE, datetime(2026, 11, 12, tzinfo=UTC), LOG, None,
            datetime(2026, 11, 13, tzinfo=UTC),
        )
    )

    rows = [i for i in items if isinstance(i, CostRow)]
    assert len(rows) == 1
    # Exact decimal sum, same six-place precision, never a float.
    assert rows[0].model_dump()["cost"] == {
        "amount": "6.504300",
        "list_amount": "6.504300",
        "currency": "USD",
        "requests": None,
    }


@pytest.mark.asyncio
async def test_rows_out_of_time_order_fail_loudly(clock):
    clock(datetime(2026, 11, 16, tzinfo=UTC))
    stub = Stub(
        lambda q: _page(
            [
                _row("2026-11-13T00:00:00Z", "2026-11-14T00:00:00Z"),
                _row("2026-11-12T00:00:00Z", "2026-11-13T00:00:00Z"),
            ]
        )
    )

    with pytest.raises(RuntimeError, match="out of time order"):
        await _drain(
            backfill_usage(
                _session(stub), DAILY, CONSOLE, datetime(2026, 11, 12, tzinfo=UTC), LOG, None,
                datetime(2026, 11, 14, tzinfo=UTC),
            )
        )


@pytest.mark.parametrize(
    "actor, expected",
    [
        (None, ""),
        ({"type": "user_actor", "user_id": "user_1", "deleted": False}, "user:user_1"),
        ({"type": "service_account_actor", "service_account_id": "svac_1"}, "service_account:svac_1"),
        ({"type": "api_actor", "api_key_id": "apikey_1"}, "api:apikey_1"),
        (
            {"type": "federated_actor", "provider": {"type": "aws", "account_id": "123"}, "subject": "role/x"},
            "federated:aws:123::role%2Fx",
        ),
        (
            {"type": "federated_actor", "provider": {"type": "oidc", "issuer": "https://idp"}, "subject": None},
            "federated:oidc:https%3A%2F%2Fidp::",
        ),
    ],
)
def test_group_key_per_actor_type(actor: dict[str, Any] | None, expected: str):
    row = UsageRow.model_validate(
        _row("2026-11-12T21:00:00Z", "2026-11-12T22:00:00Z", actor=actor, model=None)
    )
    assert row.group_key == (
        f"actor={expected}|model=|product=messages_api|api_key=|workspace=wrkspc_1"
        "|speed=|context_window="
    )


def test_federated_principals_in_one_account_get_distinct_keys():
    def key(role: str) -> str:
        actor = {
            "type": "federated_actor",
            "provider": {
                "type": "aws",
                "account_id": "123",
                "signed_principal": f"arn:aws:iam::123:role/{role}",
            },
            "subject": None,
        }
        return UsageRow.model_validate(
            _row("2026-11-12T21:00:00Z", "2026-11-12T22:00:00Z", actor=actor)
        ).group_key

    assert key("billing-bot") != key("search-bot")


def test_separator_characters_in_values_cannot_collide():
    def key(issuer: str, subject: str) -> str:
        actor = {
            "type": "federated_actor",
            "provider": {"type": "oidc", "issuer": issuer},
            "subject": subject,
        }
        return UsageRow.model_validate(
            _row("2026-11-12T21:00:00Z", "2026-11-12T22:00:00Z", actor=actor)
        ).group_key

    assert key("https://idp", "a") != key("https", "//idp:a")


def test_an_unknown_actor_type_fails_validation():
    with pytest.raises(ValueError):
        UsageRow.model_validate(
            _row("2026-11-12T21:00:00Z", "2026-11-12T22:00:00Z", actor={"type": "robot_actor"})
        )
