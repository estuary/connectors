"""
Tests for `product_variant_contextual_pricing`: one document per variant per configured country,
holding Shopify's price and compare-at price for that country. The query prices every country
through an alias, the backfill reads every variant from the epoch, and configured countries are
checked against Shopify's `CountryCode` enum before any query runs.
"""

import functools
import re
from datetime import UTC, datetime
from logging import Logger
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from estuary_cdk.capture.common import is_recurring_fetch_page_fn
from estuary_cdk.flow import ValidationError
from pydantic import ValidationError as PydanticValidationError

import source_shopify_native.graphql as gql
from source_shopify_native.api import (
    PAGE_SIZE,
    backfill_incremental_unsorted,
    fetch_incremental_unsorted,
)
from source_shopify_native.models import (
    MAX_CONTEXTUAL_PRICING_COUNTRIES,
    EndpointConfig,
    StoreCapabilities,
    StoreValidationContext,
)
from source_shopify_native.resources import (
    StoreContext,
    bound_resources,
    discovered_resources,
)

MODEL = gql.ProductVariantContextualPricing
DATA_MODEL = gql.ProductVariantContextualPricingPage
CUTOFF = datetime(2026, 11, 13, tzinfo=UTC)


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


def _money(amount: str, currency: str) -> dict:
    return {"amount": amount, "currencyCode": currency}


def _variant(variant_id: int, countries: dict[str, dict]) -> dict:
    """A productVariants node as the API returns it, with one alias per country."""
    node = {
        "id": f"gid://shopify/ProductVariant/{variant_id}",
        "legacyResourceId": str(variant_id),
        "updatedAt": "2026-09-02T09:40:53Z",
        "product": {"id": "gid://shopify/Product/1"},
    }
    for country, pricing in countries.items():
        node[f"contextualPricing_{country}"] = pricing
    return node


def _page(nodes: list[dict], has_next: bool, end_cursor: str | None = None) -> dict:
    return {
        "productVariants": {
            "edges": [{"node": n} for n in nodes],
            "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
        }
    }


class _MockClient:
    """Returns queued pages in order and records each query."""

    url = "https://store-a.myshopify.com/admin/api/graphql.json"

    def __init__(self, pages: list[dict]):
        self._pages = list(pages)
        self.queries: list[str] = []

    async def request(self, query, data_model, log, context=None):
        self.queries.append(query)
        return data_model.model_validate(self._pages.pop(0), context=context)


def _config(countries: list[str]) -> EndpointConfig:
    return EndpointConfig.model_validate(
        {
            "stores": [
                {
                    "store": "store-a",
                    "credentials": {
                        "credentials_title": "Private App Credentials",
                        "access_token": "tok",
                    },
                }
            ],
            "advanced": {"contextual_pricing_countries": countries},
        }
    )


# --- query --------------------------------------------------------------------------------------


def test_query_prices_each_country_once_in_code_order():
    query = MODEL.build_query(CUTOFF, CUTOFF, first=250, countries=["MX", "US", "MX", "DE"])

    aliases = re.findall(r"contextualPricing_(\w+): contextualPricing\(context: \{country: (\w+)\}\)", query)
    assert aliases == [("DE", "DE"), ("MX", "MX"), ("US", "US")]
    assert "# {{ contextualPricing }}" not in query


def test_query_without_countries_raises():
    with pytest.raises(ValueError, match="at least one country"):
        MODEL.build_query(CUTOFF, CUTOFF, first=250)


def test_incremental_query_filters_on_updated_at_unsorted():
    query = MODEL.build_query(
        datetime(2026, 11, 1, tzinfo=UTC), CUTOFF, first=250, countries=["US"]
    )

    assert "productVariants(" in query
    assert "updated_at:>='2026-11-01T00:00:00Z' AND updated_at:<='2026-11-13T00:00:00Z'" in query
    assert "sortKey" not in query
    assert "id:>" not in query
    assert "createdAt" not in query


# --- documents ----------------------------------------------------------------------------------


def test_page_yields_one_document_per_variant_per_country():
    us = {"price": _money("699.95", "USD"), "compareAtPrice": _money("799.95", "USD")}
    mx = {"price": _money("12501.0", "MXN"), "compareAtPrice": _money("14287.0", "MXN")}
    # Shopify hides some compare-at prices: DE returns null.
    de = {"price": _money("699.95", "USD"), "compareAtPrice": None}
    page = _page([_variant(1, {"US": us, "MX": mx, "DE": de}), _variant(2, {"US": us})], False)

    data = DATA_MODEL.model_validate(page, context=StoreValidationContext(store="store-a"))
    dumped = [doc.model_dump(by_alias=True, exclude_unset=True) for doc in data.nodes]

    variant_1 = _variant(1, {})
    variant_2 = _variant(2, {})
    meta = {"store": "store-a"}
    assert dumped == [
        {**variant_1, "_meta": meta, "country": "DE", "contextualPricing": de},
        {**variant_1, "_meta": meta, "country": "MX", "contextualPricing": mx},
        {**variant_1, "_meta": meta, "country": "US", "contextualPricing": us},
        {**variant_2, "_meta": meta, "country": "US", "contextualPricing": us},
    ]


def test_page_leaves_the_response_untouched():
    page = _page([_variant(1, {"US": {"price": _money("1.0", "USD"), "compareAtPrice": None}})], False)
    before = repr(page)

    DATA_MODEL.model_validate(page, context=StoreValidationContext(store="store-a"))

    assert repr(page) == before


def test_full_page_at_max_countries_parses():
    countries = [f"{a}{b}" for a in "ABCDEFGHIJ" for b in "ABCDE"][:MAX_CONTEXTUAL_PRICING_COUNTRIES]
    pricing = {"price": _money("12501.0", "MXN"), "compareAtPrice": _money("14287.0", "MXN")}
    page = _page(
        [_variant(i, {c: pricing for c in countries}) for i in range(1, PAGE_SIZE + 1)],
        has_next=False,
    )

    data = DATA_MODEL.model_validate(page, context=StoreValidationContext(store="store-a"))

    assert len(data.nodes) == PAGE_SIZE * len(countries)
    assert data.nodes[-1].country == max(countries)


# --- backfill -----------------------------------------------------------------------------------


async def _drain(gen) -> list:
    return [item async for item in gen]


async def _bound_backfill(client: "_MockClient", countries: list[str], log):
    """The backfill `bound_resources` hands the CDK for store-a."""
    with _patch_store_init(client), _patch_country_codes(set(countries)):
        [resource] = await bound_resources(log, MagicMock(), _config(countries), {MODEL.NAME})
    with (
        patch("source_shopify_native.resources._reconcile_connector_state", new=AsyncMock()),
        patch("source_shopify_native.resources.open_binding") as open_binding,
    ):
        await resource.open(MagicMock(), 0, resource.initial_state, MagicMock(), [])
    return open_binding.call_args.kwargs["fetch_page"]["store-a"]


@pytest.mark.asyncio
async def test_backfill_reads_every_variant_in_the_configured_countries(log):
    pricing = {
        "GB": {"price": _money("1.0", "USD"), "compareAtPrice": None},
        "MX": {"price": _money("18.0", "MXN"), "compareAtPrice": None},
    }
    client = _MockClient(
        [
            _page([_variant(7, pricing)], has_next=True, end_cursor="c1"),
            _page([_variant(9, pricing)], has_next=False),
        ]
    )
    backfill = await _bound_backfill(client, ["GB", "MX"], log)

    first = await _drain(backfill(log, None, CUTOFF))
    rest = await _drain(backfill(log, "c1", CUTOFF))

    assert [(d.id, d.country) for d in first[:2]] == [
        ("gid://shopify/ProductVariant/7", "GB"),
        ("gid://shopify/ProductVariant/7", "MX"),
    ]
    assert first[2] == "c1"
    assert [(d.id, d.country) for d in rest] == [
        ("gid://shopify/ProductVariant/9", "GB"),
        ("gid://shopify/ProductVariant/9", "MX"),
    ]
    # Every variant, however long ago it changed: the range starts at the epoch.
    assert (
        "updated_at:>='1970-01-01T00:00:00Z' AND updated_at:<='2026-11-13T00:00:00Z'"
        in client.queries[0]
    )
    assert "contextualPricing_GB:" in client.queries[0]
    assert "contextualPricing_MX:" in client.queries[0]
    assert 'after: "c1"' in client.queries[1]


@pytest.mark.asyncio
async def test_backfill_is_not_mistaken_for_a_recurring_one(log):
    """The CDK passes a fourth positional argument, a bool, to a `fetch_page` that accepts one."""
    # How every other unsorted stream binds the shared backfill.
    plain = functools.partial(
        backfill_incremental_unsorted,
        MagicMock(),
        MODEL,
        DATA_MODEL,
        "store-a",
        StoreCapabilities(scopes=frozenset()),
        CUTOFF,
    )
    priced = await _bound_backfill(_MockClient([]), ["GB"], log)

    for backfill in (plain, priced):
        assert not is_recurring_fetch_page_fn(backfill, log, None, CUTOFF, False)


# --- incremental --------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_incremental_queries_the_configured_countries(log):
    pricing = {
        "GB": {"price": _money("1.0", "USD"), "compareAtPrice": None},
        "MX": {"price": _money("18.0", "MXN"), "compareAtPrice": None},
    }
    client = _MockClient([_page([_variant(3, pricing)], has_next=False)])

    out = await _drain(
        fetch_incremental_unsorted(
            client,
            MODEL,
            DATA_MODEL,
            "store-a",
            StoreCapabilities(scopes=frozenset()),
            log,
            datetime(2026, 9, 1, tzinfo=UTC),
            build_query=functools.partial(MODEL.build_query, countries=["GB", "MX"]),
        )
    )

    assert "contextualPricing_GB:" in client.queries[0]
    assert "contextualPricing_MX:" in client.queries[0]
    assert [(d.id, d.country) for d in out[:2]] == [
        ("gid://shopify/ProductVariant/3", "GB"),
        ("gid://shopify/ProductVariant/3", "MX"),
    ]
    assert out[1].model_dump()["contextualPricing"] == pricing["MX"]
    assert out[2] == datetime(2026, 9, 2, 9, 40, 53, tzinfo=UTC)


# --- config -------------------------------------------------------------------------------------


def test_countries_default_to_none():
    assert _config([]).advanced.contextual_pricing_countries == []
    assert EndpointConfig.model_validate(
        {"stores": _config([]).model_dump()["stores"]}
    ).advanced.contextual_pricing_countries == []


@pytest.mark.parametrize("country", ["uk", "GBR", "G", "G1"])
def test_countries_must_be_two_capital_letters(country):
    with pytest.raises(PydanticValidationError):
        _config([country])


def test_countries_are_capped():
    countries = [f"{a}{b}" for a in "ABCDEFGHIJ" for b in "ABCDEF"]
    with pytest.raises(PydanticValidationError):
        _config(countries[: MAX_CONTEXTUAL_PRICING_COUNTRIES + 1])


def test_countries_schema_flags_duplicates_while_typing():
    schema = EndpointConfig.model_json_schema()
    field = schema["$defs"]["Advanced"]["properties"]["contextual_pricing_countries"]
    assert field["uniqueItems"] is True
    assert field["items"]["pattern"] == "^[A-Z]{2}$"


# --- discovery and validation -------------------------------------------------------------------


def _store_context(client=None) -> StoreContext:
    return StoreContext(
        http=MagicMock(),
        client=client or MagicMock(),
        bulk_job_manager=MagicMock(),
        capabilities=StoreCapabilities(scopes=frozenset({"read_products"})),
        available_resources={MODEL, gql.ProductVariants},
    )


def _patch_store_init(client=None):
    async def fake_initialize(log, http, config, should_cancel_ongoing_job):
        return {"store-a": _store_context(client)}

    return patch(
        "source_shopify_native.resources._initialize_store_contexts",
        side_effect=fake_initialize,
    )


def _patch_country_codes(codes: set[str]):
    async def fake_get_country_codes(http, url, log):
        return codes

    return patch(
        "source_shopify_native.resources._get_country_codes",
        side_effect=fake_get_country_codes,
    )


@pytest.mark.asyncio
async def test_discovery_needs_a_country(log):
    with _patch_store_init(), _patch_country_codes({"GB"}) as codes:
        names = {r.name for r in await discovered_resources(log, MagicMock(), _config([]))}

    assert MODEL.NAME not in names
    assert gql.ProductVariants.NAME in names
    codes.assert_not_called()


@pytest.mark.asyncio
async def test_discovery_with_valid_countries(log):
    with _patch_store_init(), _patch_country_codes({"GB", "MX"}):
        resources = await discovered_resources(log, MagicMock(), _config(["GB", "MX"]))

    resource = next(r for r in resources if r.name == MODEL.NAME)
    assert resource.key == ["/_meta/store", "/id", "/country"]
    assert {"id", "country"} <= set(resource.model.model_json_schema()["required"])


@pytest.mark.asyncio
async def test_invalid_country_is_rejected(log):
    with _patch_store_init(), _patch_country_codes({"GB", "MX"}):
        with pytest.raises(ValidationError) as err:
            await bound_resources(log, MagicMock(), _config(["UK", "MX"]), {MODEL.NAME})

    assert "UK is not a country code Shopify accepts" in str(err.value)


@pytest.mark.asyncio
async def test_bound_without_countries_is_idle(log):
    with _patch_store_init(), _patch_country_codes({"GB"}) as codes:
        resources = await bound_resources(log, MagicMock(), _config([]), {MODEL.NAME})

    assert [r.name for r in resources] == [MODEL.NAME]
    assert "Its binding will be idle" in log.warning.call_args.args[0]
    codes.assert_not_called()
