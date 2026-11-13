"""Tests for the Shopify Payments streams, which are read through `shopifyPaymentsAccount`.

Development stores can't generate payouts, so the capture snapshot holds none. These tests drive
the real `ShopifyGraphQLClient` and fetch functions with responses shaped like the examples on
Shopify's `shopifyPaymentsAccount` docs page. Only the HTTP transport is mocked.
"""

import json
import re
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from logging import Logger
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from estuary_cdk.capture import Task
from estuary_cdk.capture.common import AssociatedDocument
from estuary_cdk.flow import ValidationError

import source_shopify_native.api as api
from source_shopify_native import _validate_bindings
from source_shopify_native.api import (
    PayoutTransactionsMismatchError,
    _paginate_through_resources,
    fetch_payouts,
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


def _transaction(
    number: int, payout: dict | None, transaction_date: str = "2012-11-12T00:00:00Z"
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
        "associatedPayout": {"id": payout["id"] if payout else None},
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


def _payouts_read(
    http: MockHTTP,
    log: Logger,
    last_read: datetime,
    start_date: datetime | None = None,
    balance_transactions_binding: int | None = None,
):
    return fetch_payouts(
        ShopifyGraphQLClient(http, STORE),  # type: ignore[arg-type]
        create_response_data_model(Payouts),
        create_response_data_model(BalanceTransactions),
        STORE,
        CAPS,
        start_date,
        balance_transactions_binding,
        log,
        last_read,
    )


async def _read_payouts(
    http: MockHTTP,
    log: Logger,
    last_read: datetime,
    start_date: datetime | None = None,
    balance_transactions_binding: int | None = None,
) -> list:
    return [
        item
        async for item in _payouts_read(
            http, log, last_read, start_date, balance_transactions_binding
        )
    ]


def _payout(number: int, issued_at: datetime, status: str = "PAID") -> dict:
    return {
        "id": f"gid://shopify/ShopifyPaymentsPayout/{number}",
        "legacyResourceId": str(number),
        "issuedAt": dt_to_str(issued_at),
        "net": {"amount": "41.9", "currencyCode": "USD"},
        "status": status,
        "transactionType": "DEPOSIT",
    }


def _node_selection(query: str) -> str:
    match = re.search(r"node \{(.*?)\n\s*\}\s*\}\s*pageInfo", query, re.S)
    assert match is not None
    return match.group(1)


def _whole_seconds_ago(delta: timedelta) -> datetime:
    # Query bounds are written to the second.
    return datetime.now(tz=UTC).replace(microsecond=0) - delta


def _issued_at_bounds(query: str) -> tuple[datetime, datetime]:
    lower, upper = re.findall(r"issued_at:[<>]='([^']+)'", query)
    return str_to_dt(lower), str_to_dt(upper)


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


def _store_context() -> StoreContext:
    return StoreContext(
        http=MagicMock(),
        client=MagicMock(),
        bulk_job_manager=MagicMock(),
        capabilities=CAPS,
        available_resources=set(),
    )


async def _open(
    model,
    stores: list[str],
    config_fields: dict | None = None,
    bound_names: list[str] | None = None,
) -> dict:
    resource = _build_incremental_resource(
        model, stores, {s: _store_context() for s in stores}, _config(stores, config_fields)
    )
    task = MagicMock(spec=Task)
    task.log = MagicMock()
    task.checkpoint = AsyncMock()
    binding = MagicMock()
    binding.stateKey = model.NAME
    state = deepcopy(resource.initial_state)

    with patch("source_shopify_native.resources.open_binding") as open_binding:
        all_bindings = [(MagicMock(), SimpleNamespace(name=name)) for name in bound_names or []]
        opening = resource.open(binding, 0, state, task, all_bindings)  # type: ignore[arg-type]
        assert opening is not None
        await opening

    return dict(open_binding.call_args.kwargs)


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


@pytest.mark.parametrize(
    "granted, available",
    [
        pytest.param({"read_shopify_payments_payouts"}, False, id="payouts scope only"),
        pytest.param({"read_shopify_payments_accounts"}, False, id="account scope only"),
        pytest.param(
            {"read_shopify_payments_payouts", "read_shopify_payments_accounts"},
            True,
            id="payouts and accounts scopes",
        ),
        pytest.param(
            {"read_shopify_payments_payouts", "read_shopify_payments"},
            True,
            id="payouts and payments scopes",
        ),
    ],
)
@pytest.mark.parametrize("model", [Payouts, BalanceTransactions])
def test_payments_streams_need_a_scope_from_each_set(model, granted, available):
    resources = _get_available_resources(
        granted, uses_custom_app_token=False, can_access_pii=True, is_plus_or_advanced=True
    )

    assert (model in resources) is available


@pytest.mark.asyncio
async def test_payouts_are_read_through_the_account(log):
    http = MockHTTP([_payouts_page([PAYOUT_PAID, PAYOUT_SCHEDULED])])

    docs = await _capture(http, log)

    assert [d["id"] for d in docs] == [PAYOUT_PAID["id"], PAYOUT_SCHEDULED["id"]]
    assert all(d["_meta"]["store"] == STORE for d in docs)
    assert re.search(r"shopifyPaymentsAccount \{\s*payouts\(", http.queries[0])
    assert _issued_at_bounds(http.queries[0]) == (START, END)


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
    """The account query is nullable. A store without an account has no payouts to read."""
    http = MockHTTP([{"shopifyPaymentsAccount": None}])

    docs = await _capture(http, log)

    assert docs == []
    assert len(http.queries) == 1


@pytest.mark.asyncio
async def test_balance_transactions_are_read_through_the_account_by_processed_at(log):
    http = MockHTTP([_transactions_page([_transaction(1, PAYOUT_PAID), _transaction(2, None)])])
    client = ShopifyGraphQLClient(http, STORE)  # type: ignore[arg-type]

    docs = [
        doc.model_dump(by_alias=True)
        async for doc in _paginate_through_resources(
            client,
            BalanceTransactions,
            create_response_data_model(BalanceTransactions),
            START,
            END,
            STORE,
            CAPS,
            log,
        )
    ]

    assert [d["associatedPayout"]["id"] for d in docs] == [PAYOUT_PAID["id"], None]
    assert re.search(r"shopifyPaymentsAccount \{\s*balanceTransactions\(", http.queries[0])
    assert (
        "processed_at:>='2012-11-01T00:00:00Z' AND processed_at:<='2012-12-01T00:00:00Z'"
        in http.queries[0]
    )


@pytest.mark.parametrize(
    "days_since_last_read",
    [pytest.param(1, id="daily"), pytest.param(5, id="after downtime")],
)
@pytest.mark.asyncio
async def test_a_payouts_read_covers_30_days_before_the_last_one(log, days_since_last_read):
    last_read = _whole_seconds_ago(timedelta(days=days_since_last_read, minutes=1))
    http = MockHTTP([_payouts_page([PAYOUT_PAID, PAYOUT_SCHEDULED])])
    before = _whole_seconds_ago(timedelta())

    items = await _read_payouts(http, log, last_read)

    lower, upper = _issued_at_bounds(http.queries[0])
    assert lower == last_read - Payouts.STATUS_CHANGE_WINDOW
    assert upper >= before
    # The cursor comes after every payout, so a read that fails partway is retried in full.
    assert [i.id for i in items[:-1]] == [PAYOUT_PAID["id"], PAYOUT_SCHEDULED["id"]]
    assert items[-1].replace(microsecond=0) == upper


@pytest.mark.asyncio
async def test_a_payouts_read_starts_no_earlier_than_a_configured_start_date(log):
    last_read = _whole_seconds_ago(timedelta(days=1, minutes=1))
    start_date = last_read - timedelta(days=10)
    http = MockHTTP([_payouts_page([])])

    await _read_payouts(http, log, last_read, start_date)

    assert _issued_at_bounds(http.queries[0])[0] == start_date


@pytest.mark.parametrize(
    "config_fields, read_start_date",
    [
        pytest.param({}, None, id="blank"),
        pytest.param(
            {"start_date": "2026-10-01T00:00:00Z"},
            datetime(2026, 10, 1, tzinfo=UTC),
            id="configured",
        ),
    ],
)
@pytest.mark.asyncio
async def test_payouts_reads_ignore_a_blank_start_date(config_fields, read_start_date):
    opened = await _open(Payouts, ["a"], config_fields)

    read = opened["fetch_changes"]["a"]
    assert read.func is api.fetch_payouts
    assert read.args[-2] == read_start_date
    assert opened["fetch_page"]["a"].func is api.backfill_incremental_unsorted


@pytest.mark.parametrize(
    "bound_names, balance_transactions_binding",
    [
        pytest.param(["payouts", "orders", "balance_transactions"], 2, id="enabled"),
        pytest.param(["payouts", "orders"], None, id="not enabled"),
    ],
)
@pytest.mark.asyncio
async def test_payouts_reads_write_to_the_balance_transactions_binding(
    bound_names, balance_transactions_binding
):
    opened = await _open(Payouts, ["a"], bound_names=bound_names)

    assert opened["fetch_changes"]["a"].args[-1] == balance_transactions_binding


def test_both_balance_transaction_queries_select_the_same_fields():
    processed_at_query = BalanceTransactions.build_query(START, END, first=250)
    payout_query = BalanceTransactions.build_payout_query("623721858", first=250)

    assert _node_selection(processed_at_query) == _node_selection(payout_query)
    assert processed_at_query.count("fragment ") == payout_query.count("fragment ") == 1


@pytest.mark.asyncio
async def test_a_payouts_read_writes_recent_payouts_transactions_to_balance_transactions(log):
    last_read = _whole_seconds_ago(timedelta(days=1, minutes=1))
    start_date = last_read - timedelta(days=20)
    older = _payout(1, last_read - timedelta(days=10))
    recent = _payout(2, last_read - timedelta(days=2))
    after_start_date = dt_to_str(last_read - timedelta(days=3))
    before_start_date = dt_to_str(start_date - timedelta(days=1))
    http = MockHTTP(
        [
            _payouts_page([older, recent]),
            _transactions_page(
                [
                    _transaction(1, recent, after_start_date),
                    _transaction(2, recent, before_start_date),
                ]
            ),
        ]
    )

    items = await _read_payouts(http, log, last_read, start_date, balance_transactions_binding=3)

    # Only the payout issued within 7 days of the last read has its transactions read, and a
    # transaction processed before the configured start_date is skipped.
    assert len(http.queries) == 2
    assert "payments_transfer_id:2" in http.queries[1]
    assert [i.id for i in items[:2]] == [older["id"], recent["id"]]
    transactions = items[2:-1]
    assert all(isinstance(t, AssociatedDocument) and t.binding == 3 for t in transactions)
    assert [(t.doc.id, t.doc.associatedPayout.id) for t in transactions] == [
        ("gid://shopify/ShopifyPaymentsBalanceTransaction/1", recent["id"])
    ]
    assert isinstance(items[-1], datetime)


@pytest.mark.asyncio
async def test_a_payouts_read_without_the_balance_transactions_binding_reads_no_transactions(log):
    last_read = _whole_seconds_ago(timedelta(days=1, minutes=1))
    http = MockHTTP([_payouts_page([_payout(1, last_read - timedelta(days=2))])])

    items = await _read_payouts(http, log, last_read)

    assert len(http.queries) == 1
    assert len(items) == 2


@pytest.mark.asyncio
async def test_only_a_payouts_first_page_must_hold_its_own_transactions(log):
    last_read = _whole_seconds_ago(timedelta(days=1, minutes=1))
    payout = _payout(1, last_read - timedelta(days=2))
    elsewhere = _payout(9, last_read - timedelta(days=1))
    http = MockHTTP(
        [
            _payouts_page([payout]),
            _transactions_page([_transaction(1, payout)], has_next=True, end_cursor="T1"),
            _transactions_page([_transaction(2, elsewhere)]),
        ]
    )

    items = await _read_payouts(http, log, last_read, balance_transactions_binding=0)

    assert 'after: "T1"' in http.queries[2]
    assert [i.doc.associatedPayout.id for i in items if isinstance(i, AssociatedDocument)] == [
        payout["id"],
        elsewhere["id"],
    ]


@pytest.mark.asyncio
async def test_a_payout_whose_transactions_hold_none_of_its_own_fails_the_read(log):
    last_read = _whole_seconds_ago(timedelta(days=1, minutes=1))
    payout = _payout(1, last_read - timedelta(days=2))
    elsewhere = _payout(9, last_read - timedelta(days=1))
    http = MockHTTP([_payouts_page([payout]), _transactions_page([_transaction(1, elsewhere)])])
    items = []

    with pytest.raises(PayoutTransactionsMismatchError):
        async for item in _payouts_read(http, log, last_read, balance_transactions_binding=0):
            items.append(item)

    # No cursor, so the whole read is retried when the task restarts.
    assert not any(isinstance(i, datetime) for i in items)


@pytest.mark.parametrize(
    "status, issued_ago",
    [
        pytest.param("FAILED", timedelta(days=3), id="failed"),
        pytest.param("CANCELED", timedelta(days=3), id="canceled"),
        pytest.param("PAID", timedelta(hours=1), id="issued within a day"),
    ],
)
@pytest.mark.asyncio
async def test_some_payouts_may_hold_none_of_their_own_transactions(log, status, issued_ago):
    last_read = _whole_seconds_ago(timedelta(days=1, minutes=1))
    payout = _payout(1, _whole_seconds_ago(issued_ago), status)
    http = MockHTTP([_payouts_page([payout]), _transactions_page([])])

    items = await _read_payouts(http, log, last_read, balance_transactions_binding=0)

    assert [i.id for i in items[:-1]] == [payout["id"]]
    assert isinstance(items[-1], datetime)


@pytest.mark.parametrize(
    "bound_names, is_valid",
    [
        pytest.param(["payouts", "balance_transactions"], True, id="both"),
        pytest.param(["payouts"], True, id="payouts only"),
        pytest.param(["balance_transactions"], False, id="balance_transactions only"),
    ],
)
def test_balance_transactions_requires_payouts(bound_names, is_valid):
    bindings = [SimpleNamespace(resourceConfig=SimpleNamespace(name=n)) for n in bound_names]

    if is_valid:
        _validate_bindings(bindings)  # type: ignore[arg-type]
    else:
        with pytest.raises(ValidationError):
            _validate_bindings(bindings)  # type: ignore[arg-type]

