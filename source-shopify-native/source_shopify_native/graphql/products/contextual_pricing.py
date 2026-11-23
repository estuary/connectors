from collections.abc import Sequence
from datetime import datetime
from typing import Any

from pydantic import model_validator

from source_shopify_native.models import (
    BaseResponseData,
    Edge,
    EdgesData,
    PageInfo,
    ShopifyGraphQLResource,
    StoreCapabilities,
)

# Each country's pricing is requested under its own alias, ex: `contextualPricing_GB`, so one query
# prices a variant in every configured country.
_ALIAS_PREFIX = "contextualPricing_"
_COUNTRY_PRICING = """
{alias}: contextualPricing(context: {{country: {country}}}) {{
    price {{ amount currencyCode }}
    compareAtPrice {{ amount currencyCode }}
}}
"""


class ProductVariantContextualPricing(ShopifyGraphQLResource):
    """A product variant's final price and compare-at price in one configured country, after its
    market's price adjustment, fixed prices, currency conversion and rounding.

    There is one document per variant per country: `country` is the country the variant was
    priced in, and `contextualPricing` is Shopify's pricing for it.
    """

    NAME = "product_variant_contextual_pricing"
    QUERY_ROOT = "productVariants"
    SHOULD_USE_BULK_QUERIES = False
    # productVariants can filter on updated_at but offers no updated_at sortKey, so the connector
    # routes this stream to the unsorted incremental path.
    SORT_KEY = None
    QUALIFYING_SCOPES = {"read_products"}
    # A change to a market's price adjustment, fixed prices, rounding or exchange rate doesn't
    # advance a variant's updatedAt. To capture those changes, we re-backfill the binding every
    # day; the incremental stream keeps capturing variant edits while the backfill runs.
    BACKFILL_SCHEDULE = "55 23 * * *"
    QUERY = """
    product {
        id
    }
    # {{ contextualPricing }}
    """

    country: str

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
        countries: Sequence[str] = (),
    ) -> str:
        if not countries:
            raise ValueError("Contextual pricing needs at least one country.")

        query = ProductVariantContextualPricing.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
            includeCreatedAt=False,
            capabilities=capabilities,
        )
        pricing = "".join(
            _COUNTRY_PRICING.format(alias=f"{_ALIAS_PREFIX}{country}", country=country)
            for country in sorted(set(countries))
        )
        return query.replace("# {{ contextualPricing }}", pricing)


class ProductVariantContextualPricingPage(BaseResponseData[ProductVariantContextualPricing]):
    """A page of `productVariants`, read as one document per variant per country it was priced in."""

    productVariants: EdgesData[ProductVariantContextualPricing]

    @property
    def edges(self) -> list[Edge[ProductVariantContextualPricing]]:
        return self.productVariants.edges

    @property
    def page_info(self) -> PageInfo:
        return self.productVariants.pageInfo

    @model_validator(mode="before")
    @classmethod
    def _one_node_per_country(cls, data: Any) -> Any:
        if not isinstance(data, dict) or not isinstance(data.get("productVariants"), dict):
            return data

        connection = data["productVariants"]
        edges: list[dict[str, Any]] = []
        for edge in connection.get("edges", []):
            variant = edge["node"]
            aliases = sorted(key for key in variant if key.startswith(_ALIAS_PREFIX))
            fields = {key: value for key, value in variant.items() if key not in aliases}
            for alias in aliases:
                node = {
                    **fields,
                    "country": alias.removeprefix(_ALIAS_PREFIX),
                    "contextualPricing": variant[alias],
                }
                edges.append({"node": node})

        return {**data, "productVariants": {**connection, "edges": edges}}
