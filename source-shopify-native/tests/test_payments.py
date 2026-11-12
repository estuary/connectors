"""Tests for the Shopify Payments streams, which are read through `shopifyPaymentsAccount`.

Development stores can't generate payouts, so the capture snapshot holds none. These tests drive
the real `ShopifyGraphQLClient` and `_paginate_through_resources` with responses shaped like the
examples on Shopify's `shopifyPaymentsAccount` docs page; only the HTTP transport is mocked.
"""

import json
import re
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from logging import Logger
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import ResourceState

import source_shopify_native.api as api
from source_shopify_native.api import (
    _next_checkpoint,
    _paginate_through_resources,
    fetch_recent_payout_transactions,
    fetch_recent_payouts,
)
from source_shopify_native.graphql.client import ShopifyGraphQLClient
from source_shopify_native.graphql.payments.balance_transactions import BalanceTransactions
from source_shopify_native.graphql.payments.payouts import Payouts
from source_shopify_native.models import (
    EndpointConfig,
    StoreCapabilities,
    create_response_data_model,
)
from source_shopify_native.resources import (
    StoreContext,
    _build_incremental_resource,
    _get_available_resources,
)
from source_shopify_native.utils import dt_to_str, str_to_dt

START = datetime(2012, 11, 1, tzinfo=UTC)
END = datetime(2012, 12, 1, tzinfo=UTC)
STORE = "teststore"
CAPS = StoreCapabilities(scopes=frozenset())

PAYOUT_PAID = {
    "id": "gid://shopify/ShopifyPaymentsPayout/623721858",
    "legacyResourceId": "623721858",
    "issuedAt": "2012-11-12T00:00:00Z",
    "net": {"amount": "41.9", "currencyCode": "USD"},
    "status": "PAID",
    "transactionType": "DEPOSIT",
}
PAYOUT_SCHEDULED = {
    "id": "gid://shopify/ShopifyPaymentsPayout/631321250",
    "legacyResourceId": "631321250",
    "issuedAt": "2012-11-12T00:00:00Z",
    "net": {"amount": "41.9", "currencyCode": "USD"},
    "status": "SCHEDULED",
    "transactionType": "DEPOSIT",
}
PAYOUT_CANCELED = {
    "id": "gid://shopify/ShopifyPaymentsPayout/512467833",
    "legacyResourceId": "512467833",
    "issuedAt": "2012-11-12T00:00:00Z",
    "net": {"amount": "41.9", "currencyCode": "USD"},
    "status": "CANCELED",
    "transactionType": "DEPOSIT",
}
PAYOUT_FAILED = {
    "id": "gid://shopify/ShopifyPaymentsPayout/128753854",
    "legacyResourceId": "128753854",
    "issuedAt": "2012-11-12T00:00:00Z",
    "net": {"amount": "41.9", "currencyCode": "USD"},
    "status": "FAILED",
    "transactionType": "DEPOSIT",
}


def _transaction(
    number: int, payout: dict, transaction_date: str = "2012-11-12T00:00:00Z"
) -> dict:
    return {
        "id": f"gid://shopify/ShopifyPaymentsBalanceTransaction/{number}",
        "transactionDate": transaction_date,
        "type": "CHARGE",
        "test": False,
        "amount": {"amount": "11.5", "currencyCode": "USD"},
        "fee": {"amount": "0.65", "currencyCode": "USD"},
        "net": {"amount": "10.85", "currencyCode": "USD"},
        "associatedOrder": {"id": "gid://shopify/Order/450789469"},
        "associatedPayout": {"id": payout["id"]},
    }


class MockHTTP:
    """Stands in for estuary_cdk's HTTPSession, returning queued GraphQL `data` bodies in order."""

    def __init__(self, pages: list[dict]):
        self._pages = list(pages)
        self.queries: list[str] = []

    async def request(self, log, url, method="GET", **kwargs) -> bytes:
        self.queries.append((kwargs.get("json") or {}).get("query", ""))
        return json.dumps({"data": self._pages.pop(0)}).encode()


def _payouts_page(nodes: list[dict], has_next: bool = False, end_cursor: str | None = None) -> dict:
    return {
        "shopifyPaymentsAccount": {
            "payouts": {
                "edges": [{"node": n} for n in nodes],
                "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
            }
        }
    }


def _transactions_page(
    nodes: list[dict], has_next: bool = False, end_cursor: str | None = None
) -> dict:
    return {
        "shopifyPaymentsAccount": {
            "balanceTransactions": {
                "edges": [{"node": n} for n in nodes],
                "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
            }
        }
    }


async def _capture(http: MockHTTP, log: Logger) -> list[dict]:
    client = ShopifyGraphQLClient(http, STORE)  # type: ignore[arg-type]
    data_model = create_response_data_model(Payouts)
    return [
        doc.model_dump(by_alias=True)
        async for doc in _paginate_through_resources(
            client, Payouts, data_model, START, END, STORE, CAPS, log
        )
    ]


def _a_day_and_a_minute_ago() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=1, minutes=1)


async def _refresh_payouts(
    http: MockHTTP,
    log: Logger,
    start_date: datetime | None = START,
    last_refresh: datetime | None = None,
) -> list:
    client = ShopifyGraphQLClient(http, STORE)  # type: ignore[arg-type]
    return [
        item
        async for item in fetch_recent_payouts(
            client,
            create_response_data_model(Payouts),
            STORE,
            CAPS,
            start_date,
            log,
            last_refresh or _a_day_and_a_minute_ago(),
        )
    ]


def _transactions_refresh(
    http: MockHTTP,
    log: Logger,
    last_refresh: datetime | None = None,
    start_date: datetime | None = START,
):
    return fetch_recent_payout_transactions(
        ShopifyGraphQLClient(http, STORE),  # type: ignore[arg-type]
        create_response_data_model(Payouts),
        create_response_data_model(BalanceTransactions),
        STORE,
        CAPS,
        start_date,
        log,
        last_refresh or _a_day_and_a_minute_ago(),
    )


async def _refresh_transactions(
    http: MockHTTP,
    log: Logger,
    last_refresh: datetime | None = None,
    start_date: datetime | None = START,
) -> list:
    return [item async for item in _transactions_refresh(http, log, last_refresh, start_date)]


def _issued_at_bounds(query: str) -> tuple[datetime, datetime]:
    lower, upper = re.findall(r"issued_at:[<>]='([^']+)'", query)
    return str_to_dt(lower), str_to_dt(upper)


def _ids_and_checkpoints(items: list) -> list[str]:
    return ["checkpoint" if isinstance(i, datetime) else i.id for i in items]


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


@pytest.mark.asyncio
async def test_payouts_are_read_through_the_account(log):
    http = MockHTTP([_payouts_page([PAYOUT_PAID, PAYOUT_SCHEDULED])])

    docs = await _capture(http, log)

    assert [d["id"] for d in docs] == [PAYOUT_PAID["id"], PAYOUT_SCHEDULED["id"]]
    assert all(d["_meta"]["store"] == STORE for d in docs)
    assert "shopifyPaymentsAccount {" in http.queries[0]
    assert "issued_at:>='2012-11-01T00:00:00Z' AND issued_at:<='2012-12-01T00:00:00Z'" in http.queries[0]


@pytest.mark.asyncio
async def test_payouts_follow_the_page_cursor(log):
    http = MockHTTP(
        [
            _payouts_page([PAYOUT_PAID], has_next=True, end_cursor="C1"),
            _payouts_page([PAYOUT_SCHEDULED]),
        ]
    )

    docs = await _capture(http, log)

    assert [d["id"] for d in docs] == [PAYOUT_PAID["id"], PAYOUT_SCHEDULED["id"]]
    assert 'after: "C1"' in http.queries[1]


@pytest.mark.asyncio
async def test_null_account_reads_no_payouts(log):
    """The account query is nullable; a store without one has no payouts to read."""
    http = MockHTTP([{"shopifyPaymentsAccount": None}])

    docs = await _capture(http, log)

    assert docs == []
    assert len(http.queries) == 1


@pytest.mark.parametrize(
    "start_date",
    [pytest.param(START, id="earlier start_date"), pytest.param(None, id="blank start_date")],
)
@pytest.mark.asyncio
async def test_the_daily_refresh_emits_payouts_dated_in_the_30_days_before_it_fell_due(
    log, start_date
):
    """A payout's status can change after its date."""
    last_refresh = _a_day_and_a_minute_ago()
    http = MockHTTP([_payouts_page([PAYOUT_PAID, PAYOUT_SCHEDULED])])

    before = datetime.now(tz=UTC)
    *docs, cursor = await _refresh_payouts(
        http, log, start_date=start_date, last_refresh=last_refresh
    )
    after = datetime.now(tz=UTC)

    assert [d.id for d in docs] == [PAYOUT_PAID["id"], PAYOUT_SCHEDULED["id"]]
    log.info.assert_any_call(f"[{STORE}] Refreshed 2 recent payouts.")
    assert before <= cursor <= after
    lower, upper = _issued_at_bounds(http.queries[0])
    due = last_refresh + timedelta(days=1)
    assert lower == (due - timedelta(days=30)).replace(microsecond=0)
    assert before - timedelta(seconds=1) <= upper <= after


@pytest.mark.asyncio
async def test_the_payouts_refresh_waits_a_day_between_runs(log):
    http = MockHTTP([])

    items = await _refresh_payouts(
        http, log, last_refresh=datetime.now(tz=UTC) - timedelta(hours=23)
    )

    assert items == []
    assert http.queries == []


@pytest.mark.asyncio
async def test_the_refresh_window_starts_at_the_start_date_when_it_is_later(log):
    start_date = (datetime.now(tz=UTC) - timedelta(days=1)).replace(microsecond=0)
    http = MockHTTP([_payouts_page([])])

    await _refresh_payouts(http, log, start_date=start_date)

    lower, _ = _issued_at_bounds(http.queries[0])
    assert lower == start_date


@pytest.mark.asyncio
async def test_the_payouts_refresh_moves_the_cursor_when_the_account_is_null(log):
    http = MockHTTP([{"shopifyPaymentsAccount": None}])

    items = await _refresh_payouts(http, log)

    assert len(items) == 1 and isinstance(items[0], datetime)


def test_payout_cursor_is_issued_at():
    payout = Payouts.model_validate(PAYOUT_PAID)

    assert payout.get_cursor_value() == datetime(2012, 11, 12, tzinfo=UTC)


@pytest.mark.parametrize(
    "granted, available",
    [
        ({"read_shopify_payments_payouts", "read_shopify_payments_accounts"}, True),
        ({"read_shopify_payments_payouts", "read_shopify_payments"}, True),
        ({"read_shopify_payments_payouts"}, False),
        ({"read_shopify_payments_accounts"}, False),
    ],
)
def test_payments_streams_need_the_payouts_scope_and_account_access(granted, available):
    resources = _get_available_resources(
        granted, uses_custom_app_token=False, can_access_pii=True, is_plus_or_advanced=False
    )

    assert (Payouts in resources) == available
    assert (BalanceTransactions in resources) == available


def test_transaction_cursor_is_transaction_date():
    transaction = BalanceTransactions.model_validate(_transaction(1, PAYOUT_PAID))

    assert transaction.get_cursor_value() == datetime(2012, 11, 12, tzinfo=UTC)


def test_transactions_are_read_through_the_account_by_processed_at():
    query = BalanceTransactions.build_query(START, END, first=250)

    assert "shopifyPaymentsAccount {" in query
    assert "balanceTransactions(" in query
    assert "processed_at:>='2012-11-01T00:00:00Z' AND processed_at:<='2012-12-01T00:00:00Z'" in query


def test_transactions_reference_their_payout_by_id_only():
    """A payout's status changes for weeks after its transactions stop being read, so it's
    read from the payouts stream only."""
    assert re.search(r"associatedPayout\s*\{\s*id\s*\}", BalanceTransactions.QUERY)


@pytest.mark.asyncio
async def test_the_transactions_refresh_reads_payouts_dated_in_the_7_days_before_it_fell_due(
    log,
):
    """A transaction's payout doesn't change once set, so its payout's transactions are read
    for days, not for the weeks the payout's status can change."""
    last_refresh = _a_day_and_a_minute_ago()
    http = MockHTTP([_payouts_page([])])

    before = datetime.now(tz=UTC)
    await _refresh_transactions(http, log, last_refresh=last_refresh)
    after = datetime.now(tz=UTC)

    lower, upper = _issued_at_bounds(http.queries[0])
    due = last_refresh + timedelta(days=1)
    assert lower == (due - timedelta(days=7)).replace(microsecond=0)
    assert before - timedelta(seconds=1) <= upper <= after


@pytest.mark.parametrize(
    "refresh, window",
    [
        pytest.param(_refresh_payouts, timedelta(days=30), id="payouts"),
        pytest.param(_refresh_transactions, timedelta(days=7), id="balance_transactions"),
    ],
)
@pytest.mark.asyncio
async def test_a_refresh_after_downtime_reads_back_from_when_it_fell_due(log, refresh, window):
    """Measured back from now, the window would skip every payout dated in the first part of
    an outage longer than it, and those payouts would never be read again."""
    last_refresh = (datetime.now(tz=UTC) - timedelta(days=45)).replace(microsecond=0)
    http = MockHTTP([_payouts_page([])])

    await refresh(http, log, last_refresh=last_refresh)

    lower, _ = _issued_at_bounds(http.queries[0])
    assert lower == last_refresh + timedelta(days=1) - window


@pytest.mark.asyncio
async def test_the_daily_refresh_rereads_every_recent_payouts_transactions(log):
    paid_txn, scheduled_txn = _transaction(1, PAYOUT_PAID), _transaction(2, PAYOUT_SCHEDULED)
    http = MockHTTP(
        [
            _payouts_page([PAYOUT_PAID, PAYOUT_SCHEDULED]),
            _transactions_page([paid_txn]),
            _transactions_page([scheduled_txn]),
        ]
    )

    items = await _refresh_transactions(http, log)

    assert _ids_and_checkpoints(items) == [
        "checkpoint", paid_txn["id"], "checkpoint", scheduled_txn["id"], "checkpoint",
    ]
    assert 'query: "payments_transfer_id:623721858"' in http.queries[1]
    assert 'query: "payments_transfer_id:631321250"' in http.queries[2]
    cursors = [i for i in items if isinstance(i, datetime)]
    assert cursors == sorted(cursors) and len(set(cursors)) == len(cursors)
    log.info.assert_any_call(
        f"[{STORE}] Refreshed 2 balance transactions in 2 recent payouts."
    )


@pytest.mark.asyncio
async def test_the_transactions_refresh_waits_a_day_between_runs(log):
    http = MockHTTP([])

    items = await _refresh_transactions(
        http, log, last_refresh=datetime.now(tz=UTC) - timedelta(hours=23)
    )

    assert items == []
    assert http.queries == []


NONE_OF_ITS_OWN = [
    pytest.param([], id="empty"),
    pytest.param([_transaction(1, PAYOUT_SCHEDULED)], id="another payout's"),
]


@pytest.mark.parametrize("first_page", NONE_OF_ITS_OWN)
@pytest.mark.asyncio
async def test_a_payout_read_holding_none_of_its_own_transactions_fails_after_the_rest(
    log, first_page
):
    """A payout stays in the window for a week, so failing at once would leave every payout
    after it unrefreshed for as long."""
    scheduled_txn = _transaction(2, PAYOUT_SCHEDULED)
    http = MockHTTP(
        [
            _payouts_page([PAYOUT_PAID, PAYOUT_SCHEDULED]),
            _transactions_page(first_page),
            _transactions_page([scheduled_txn]),
        ]
    )

    items = []
    with pytest.raises(RuntimeError) as raised:
        async for item in _transactions_refresh(http, log):
            items.append(item)

    assert _ids_and_checkpoints(items) == [
        "checkpoint", "checkpoint", scheduled_txn["id"], "checkpoint",
    ]
    # The message tells a filter that doesn't match payouts from associatedPayout ids in
    # another form, by naming the payout ids the page held.
    message = str(raised.value)
    assert "The balance transactions read for 1 payout(s) held none of their own." in message
    assert (
        f"payout {PAYOUT_PAID['id']} (PAID DEPOSIT, issued 2012-11-12T00:00:00Z): "
        f"its first page held {len(first_page)} transaction(s)"
    ) in message
    for transaction in first_page:
        assert transaction["associatedPayout"]["id"] in message


@pytest.mark.asyncio
async def test_a_payout_dated_within_the_grace_period_may_hold_none_of_its_own_transactions(
    log,
):
    """Just after a payout's date, the payments_transfer_id search may not show its
    transactions yet. They're read again on later days."""
    an_hour_ago = dt_to_str(datetime.now(tz=UTC) - timedelta(hours=1))
    payout = {**PAYOUT_PAID, "issuedAt": an_hour_ago}
    http = MockHTTP([_payouts_page([payout]), _transactions_page([])])

    items = await _refresh_transactions(http, log)

    assert _ids_and_checkpoints(items) == ["checkpoint", "checkpoint"]


@pytest.mark.parametrize(
    "payout",
    [pytest.param(PAYOUT_FAILED, id="failed"), pytest.param(PAYOUT_CANCELED, id="canceled")],
)
@pytest.mark.parametrize("first_page", NONE_OF_ITS_OWN)
@pytest.mark.asyncio
async def test_a_failed_or_canceled_payout_may_hold_none_of_its_own_transactions(
    log, payout, first_page
):
    """Shopify doesn't say whether a failed or canceled payout keeps its transactions."""
    http = MockHTTP([_payouts_page([payout]), _transactions_page(first_page)])

    items = await _refresh_transactions(http, log)

    assert _ids_and_checkpoints(items) == ["checkpoint", "checkpoint"]


@pytest.mark.asyncio
async def test_transactions_that_moved_to_another_payout_are_still_written(log):
    own, moved = _transaction(1, PAYOUT_PAID), _transaction(2, PAYOUT_SCHEDULED)
    http = MockHTTP([_payouts_page([PAYOUT_PAID]), _transactions_page([own, moved])])

    items = await _refresh_transactions(http, log)

    assert _ids_and_checkpoints(items) == ["checkpoint", own["id"], moved["id"], "checkpoint"]


@pytest.mark.asyncio
async def test_only_a_payouts_first_page_must_hold_its_own_transactions(log):
    own, moved = _transaction(1, PAYOUT_PAID), _transaction(2, PAYOUT_SCHEDULED)
    http = MockHTTP(
        [
            _payouts_page([PAYOUT_PAID]),
            _transactions_page([own], has_next=True, end_cursor="T1"),
            _transactions_page([moved]),
        ]
    )

    items = await _refresh_transactions(http, log)

    assert _ids_and_checkpoints(items) == ["checkpoint", own["id"], moved["id"], "checkpoint"]


@pytest.mark.asyncio
async def test_a_payouts_transactions_follow_the_page_cursor_with_checkpoints(log, monkeypatch):
    monkeypatch.setattr(api, "CHECKPOINT_INTERVAL", 2)
    txns = [_transaction(n, PAYOUT_PAID) for n in (1, 2, 3)]
    http = MockHTTP(
        [
            _payouts_page([PAYOUT_PAID]),
            _transactions_page(txns[:2], has_next=True, end_cursor="T1"),
            _transactions_page(txns[2:]),
        ]
    )

    items = await _refresh_transactions(http, log)

    assert _ids_and_checkpoints(items) == [
        "checkpoint", txns[0]["id"], txns[1]["id"], "checkpoint", txns[2]["id"], "checkpoint",
    ]
    assert 'after: "T1"' in http.queries[2]


@pytest.mark.parametrize(
    "start_date, written",
    [
        pytest.param(
            datetime(2012, 11, 12, tzinfo=UTC), ["at start_date"], id="configured start_date"
        ),
        pytest.param(None, ["before start_date", "at start_date"], id="blank start_date"),
    ],
)
@pytest.mark.asyncio
async def test_transactions_processed_before_a_configured_start_date_are_not_written(
    log, start_date, written
):
    """A payout holds transactions processed days before its date, so one dated after the
    start date can hold some processed before it. The backfill doesn't write those either."""
    transactions = {
        "before start_date": _transaction(1, PAYOUT_PAID, "2012-11-11T23:59:59Z"),
        "at start_date": _transaction(2, PAYOUT_PAID, "2012-11-12T00:00:00Z"),
    }
    http = MockHTTP(
        [_payouts_page([PAYOUT_PAID]), _transactions_page(list(transactions.values()))]
    )

    items = await _refresh_transactions(http, log, start_date=start_date)

    assert [i.id for i in items if not isinstance(i, datetime)] == [
        transactions[name]["id"] for name in written
    ]


def test_checkpoints_keep_rising_if_the_clock_steps_back():
    ahead = datetime.now(tz=UTC) + timedelta(minutes=5)

    assert _next_checkpoint(ahead) == ahead + timedelta(microseconds=1)


def _config(stores: list[str], fields: dict | None = None) -> EndpointConfig:
    return EndpointConfig.model_validate(
        {
            "stores": [
                {
                    "store": store,
                    "credentials": {
                        "credentials_title": "Private App Credentials",
                        "access_token": "tok",
                    },
                }
                for store in stores
            ],
            **(fields or {}),
        }
    )


def _store_context(client=None, available_resources: set | None = None) -> StoreContext:
    return StoreContext(
        http=MagicMock(),
        client=client or MagicMock(),
        bulk_job_manager=MagicMock(),
        capabilities=CAPS,
        available_resources=available_resources or set(),
    )


async def _open(
    model,
    stores: list[str],
    state: ResourceState | None = None,
    config_fields: dict | None = None,
) -> dict:
    resource = _build_incremental_resource(
        model, stores, {s: _store_context() for s in stores}, _config(stores, config_fields)
    )
    task = MagicMock(spec=Task)
    task.log = MagicMock()
    task.checkpoint = AsyncMock()
    binding = MagicMock()
    binding.stateKey = model.NAME
    state = state or deepcopy(resource.initial_state)

    with patch("source_shopify_native.resources.open_binding") as open_binding:
        await resource.open(binding, 0, state, task, [])

    return {"resource": resource, "state": state, **open_binding.call_args.kwargs}


@pytest.mark.parametrize(
    "model, refresh",
    [
        (Payouts, api.fetch_recent_payouts),
        (BalanceTransactions, api.fetch_recent_payout_transactions),
    ],
)
@pytest.mark.asyncio
async def test_payments_streams_add_a_daily_refresh_subtask_per_store(model, refresh):
    opened = await _open(model, ["a", "b"])

    fetch_changes = opened["fetch_changes"]
    assert set(fetch_changes) == {"a", "b", "a:recent_payouts", "b:recent_payouts"}
    assert fetch_changes["a"].func is api.fetch_incremental_unsorted
    assert fetch_changes["a:recent_payouts"].func is refresh
    assert opened["fetch_page"]["a"].func is api.backfill_incremental_unsorted
    assert set(opened["fetch_page"]) == {"a", "b"}
    assert set(opened["state"].backfill) == {"a", "b"}
    assert opened["resource"].initial_config.schedule == ""


@pytest.mark.asyncio
async def test_a_new_store_gets_both_subtasks():
    cutoff = datetime(2026, 11, 1, tzinfo=UTC)
    state = ResourceState(
        inc={
            "a": ResourceState.Incremental(cursor=cutoff),
            "a:recent_payouts": ResourceState.Incremental(cursor=cutoff),
        },
        backfill={"a": None},
    )

    opened = await _open(BalanceTransactions, ["a", "b"], state)

    assert set(state.inc) == {"a", "b", "a:recent_payouts", "b:recent_payouts"}
    assert set(state.backfill) == {"a", "b"}
    assert state.inc["a"].cursor == cutoff
    assert set(opened["fetch_changes"]) == set(state.inc)


@pytest.mark.parametrize(
    "config_fields, refresh_start_date",
    [
        pytest.param({}, None, id="blank"),
        pytest.param(
            {"start_date": "2026-10-01T00:00:00Z"},
            datetime(2026, 10, 1, tzinfo=UTC),
            id="configured",
        ),
    ],
)
@pytest.mark.parametrize("model", [Payouts, BalanceTransactions])
@pytest.mark.asyncio
async def test_the_refreshes_ignore_a_blank_start_date(
    model, config_fields, refresh_start_date
):
    """A blank start_date is reset to 30 days back at every start, so it can pass
    transactions the capture already holds and must keep refreshing."""
    opened = await _open(model, ["a"], config_fields=config_fields)

    assert opened["fetch_changes"]["a:recent_payouts"].args[-1] == refresh_start_date
    assert isinstance(opened["fetch_page"]["a"].args[-1], datetime)
