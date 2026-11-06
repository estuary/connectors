"""
Tests for product_variant_market_prices: the configured countries drive the query's
contextualPricing aliases, each variant fans out into one document per country, countries are
split across bulk queries in batches, and the stream is only available once countries are listed.
"""

import json
from datetime import UTC, datetime
from logging import Logger
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

import source_shopify_native.graphql as gql
from source_shopify_native.api import bulk_fetch_product_variant_market_prices
from source_shopify_native.graphql.products.variant_market_prices import (
    ProductVariantMarketPrices,
)
from source_shopify_native.models import EndpointConfig, StoreCapabilities
from source_shopify_native.resources import (
    StoreContext,
    bound_resources,
    discovered_resources,
)

START = datetime(2024, 1, 1, tzinfo=UTC)
END = datetime(2024, 2, 1, tzinfo=UTC)


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


def _config(countries: list[str] | None = None) -> EndpointConfig:
    advanced = {} if countries is None else {"market_price_countries": countries}
    return EndpointConfig.model_validate(
        {
            "stores": [
                {
                    "store": "store",
                    "credentials": {
                        "credentials_title": "Private App Credentials",
                        "access_token": "tok",
                    },
                }
            ],
            "advanced": advanced,
        }
    )


def _patch_store_init():
    context = StoreContext(
        http=MagicMock(),
        client=MagicMock(),
        bulk_job_manager=MagicMock(),
        capabilities=StoreCapabilities(scopes=frozenset({"read_products"})),
        available_resources={gql.ProductVariants, gql.ProductVariantMarketPrices},
    )

    async def fake_create_store_context(log, http, store_config, should_cancel_ongoing_job, max_concurrent_bulk_ops):
        return context

    return patch(
        "source_shopify_native.resources._create_store_context",
        side_effect=fake_create_store_context,
    )


async def _lines(*records: dict):
    for record in records:
        yield json.dumps(record).encode()


def _variant_line(**prices) -> dict:
    return {
        "id": "gid://shopify/ProductVariant/2",
        "updatedAt": "2024-01-15T00:00:00Z",
        "product": {"id": "gid://shopify/Product/1"},
        **prices,
        "__parentId": "gid://shopify/Product/1",
    }


def test_countries_are_upper_cased_and_deduplicated():
    config = _config(["gb", "DE", "GB"])

    assert config.advanced.market_price_countries == ["GB", "DE"]


@pytest.mark.parametrize("country", ["GBR", "G", "G1", ""])
def test_malformed_country_is_rejected(country):
    with pytest.raises(ValidationError):
        _config([country])


def test_query_requests_one_contextual_pricing_alias_per_country():
    query = ProductVariantMarketPrices.build_query(START, END, countries=["GB", "MX"])

    assert "products(" in query
    assert "updated_at:>=" in query
    assert "country_GB: contextualPricing(context: {country: GB})" in query
    assert "country_MX: contextualPricing(context: {country: MX})" in query
    assert "{{ contextualPricing }}" not in query


@pytest.mark.asyncio
async def test_each_variant_fans_out_into_one_document_per_country(log):
    gb = {
        "price": {"amount": "699.95", "currencyCode": "USD"},
        "compareAtPrice": {"amount": "799.95", "currencyCode": "USD"},
    }
    mx = {
        "price": {"amount": "12648.0", "currencyCode": "MXN"},
        "compareAtPrice": None,
    }
    lines = _lines(
        {"id": "gid://shopify/Product/1"},
        _variant_line(country_GB=gb, country_MX=mx),
    )

    docs = [doc async for doc in ProductVariantMarketPrices.process_result(log, lines)]

    base = {
        "id": "gid://shopify/ProductVariant/2",
        "updatedAt": "2024-01-15T00:00:00Z",
        "product": {"id": "gid://shopify/Product/1"},
    }
    assert docs == [
        {**base, "country": "GB", **gb},
        {**base, "country": "MX", **mx},
    ]


@pytest.mark.asyncio
async def test_unidentified_line_raises(log):
    lines = _lines({"id": "gid://shopify/Collection/1"})

    with pytest.raises(RuntimeError):
        _ = [doc async for doc in ProductVariantMarketPrices.process_result(log, lines)]


@pytest.mark.asyncio
async def test_countries_are_split_into_batched_bulk_queries(log):
    countries = [f"A{chr(ord('A') + i)}" for i in range(26)] + ["BA", "BB", "BC", "BD"]
    bulk_job_manager = MagicMock()
    bulk_job_manager.execute = AsyncMock(return_value="https://results")
    http = MagicMock()

    async def request_lines(log, url):
        variant = _variant_line(
            **{
                f"country_{c}": {
                    "price": {"amount": "1.0", "currencyCode": "USD"},
                    "compareAtPrice": None,
                }
                for c in countries
            }
        )
        return None, lambda: _lines(variant)

    http.request_lines = request_lines

    results = [
        item
        async for item in bulk_fetch_product_variant_market_prices(
            http, END - START, bulk_job_manager, countries, "store", log, START
        )
    ]

    queries = [call.args[1] for call in bulk_job_manager.execute.call_args_list]
    assert len(queries) == 2
    assert all(f"country_{c}:" in queries[0] for c in countries[:25])
    assert all(f"country_{c}:" not in queries[0] for c in countries[25:])
    assert all(f"country_{c}:" in queries[1] for c in countries[25:])

    # The cursor advances once, after every batch.
    assert results[-1] == END
    assert all(isinstance(doc, ProductVariantMarketPrices) for doc in results[:-1])


@pytest.mark.asyncio
async def test_no_results_skips_remaining_batches(log):
    countries = [f"A{chr(ord('A') + i)}" for i in range(26)]
    bulk_job_manager = MagicMock()
    bulk_job_manager.execute = AsyncMock(return_value=None)

    results = [
        item
        async for item in bulk_fetch_product_variant_market_prices(
            MagicMock(), END - START, bulk_job_manager, countries, "store", log, START
        )
    ]

    assert bulk_job_manager.execute.call_count == 1
    assert results == [END]


@pytest.mark.asyncio
async def test_stream_is_not_discovered_without_countries(log):
    with _patch_store_init():
        resources = await discovered_resources(log, MagicMock(), _config())

    names = {r.name for r in resources}
    assert "product_variants" in names
    assert "product_variant_market_prices" not in names


@pytest.mark.asyncio
async def test_stream_is_keyed_on_variant_and_country(log):
    with _patch_store_init():
        resources = await discovered_resources(log, MagicMock(), _config(["GB"]))

    resource = next(r for r in resources if r.name == "product_variant_market_prices")
    assert resource.key == ["/_meta/store", "/id", "/country"]
    assert isinstance(resource.model, type)
    assert "country" in resource.model.model_json_schema()["properties"]


@pytest.mark.asyncio
async def test_bound_stream_without_countries_opens_idle(log):
    with _patch_store_init():
        resources = await bound_resources(
            log, MagicMock(), _config(), {"product_variant_market_prices"}
        )

    assert [r.name for r in resources] == ["product_variant_market_prices"]
    assert resources[0].initial_state.inc == {}
    log.warning.assert_called_once()
