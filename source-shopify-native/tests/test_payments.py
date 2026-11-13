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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from estuary_cdk.capture import Task

import source_shopify_native.api as api
from source_shopify_native.api import _paginate_through_resources, fetch_payouts
from source_shopify_native.graphql.client import ShopifyGraphQLClient
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
from source_shopify_native.utils import str_to_dt

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


async def _capture(http: MockHTTP, log: Logger) -> list[dict]:
    client = ShopifyGraphQLClient(http, STORE)  # type: ignore[arg-type]
    data_model = create_response_data_model(Payouts)
    return [
        doc.model_dump(by_alias=True)
        async for doc in _paginate_through_resources(
            client, Payouts, data_model, START, END, STORE, CAPS, log
        )
    ]


async def _read_payouts(
    http: MockHTTP,
    log: Logger,
    last_read: datetime,
    start_date: datetime | None = None,
) -> list:
    client = ShopifyGraphQLClient(http, STORE)  # type: ignore[arg-type]
    return [
        item
        async for item in fetch_payouts(
            client,
            create_response_data_model(Payouts),
            STORE,
            CAPS,
            start_date,
            log,
            last_read,
        )
    ]


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


async def _open(model, stores: list[str], config_fields: dict | None = None) -> dict:
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
        opening = resource.open(binding, 0, state, task, [])
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
def test_payouts_need_a_scope_from_each_set(granted, available):
    resources = _get_available_resources(
        granted, uses_custom_app_token=False, can_access_pii=True, is_plus_or_advanced=True
    )

    assert (Payouts in resources) is available


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
    assert read.args[-1] == read_start_date
    assert opened["fetch_page"]["a"].func is api.backfill_incremental_unsorted
