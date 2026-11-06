from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator, ClassVar, Sequence

from source_shopify_native.models import ShopifyGraphQLResource, SortKey, StoreCapabilities

# Each country's contextualPricing is requested under an alias of this prefix plus the country
# code (ex: `country_GB`), so a single query resolves several countries per variant.
_ALIAS_PREFIX = "country_"


def _contextual_pricing_selection(countries: Sequence[str]) -> str:
    return "\n".join(
        f"{_ALIAS_PREFIX}{country}: contextualPricing(context: {{country: {country}}}) {{\n"
        "    price { amount currencyCode }\n"
        "    compareAtPrice { amount currencyCode }\n"
        "}"
        for country in countries
    )


class ProductVariantMarketPrices(ShopifyGraphQLResource):
    """A product variant's final price and compare-at price in one country, after the price
    adjustments, rounding, and fixed prices of the market that country belongs to."""

    NAME = "product_variant_market_prices"
    QUERY_ROOT = "products"
    SORT_KEY = SortKey.UPDATED_AT
    QUALIFYING_SCOPES = {"read_products"}
    # Each variant fans out into one document per configured country.
    KEY = ["/id", "/country"]
    # Products are filtered on updatedAt, which a change to a market's price adjustment,
    # rounding, fixed prices, or exchange rate doesn't advance. To capture those changes, we
    # re-backfill the binding every day.
    BACKFILL_SCHEDULE = "55 23 * * *"
    QUERY = """
    variants {
        edges {
            node {
                id
                updatedAt
                product {
                    id
                }
                # {{ contextualPricing }}
            }
        }
    }
    """
    # Each alias is another price resolution per variant, so batching bounds how much work a
    # single bulk operation does.
    COUNTRIES_PER_QUERY: ClassVar[int] = 25

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
        query = ProductVariantMarketPrices.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
            includeLegacyId=False,
            includeCreatedAt=False,
            includeUpdatedAt=False,
            capabilities=capabilities,
        )
        return query.replace(
            "# {{ contextualPricing }}", _contextual_pricing_selection(countries)
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            # Product lines only parent the variants; each variant carries its product's id.
            if "gid://shopify/Product/" in id:
                continue

            if "gid://shopify/ProductVariant/" not in id:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

            record.pop("__parentId", None)
            variant = {k: v for k, v in record.items() if not k.startswith(_ALIAS_PREFIX)}

            for key, pricing in record.items():
                if key.startswith(_ALIAS_PREFIX):
                    yield {
                        **variant,
                        "country": key.removeprefix(_ALIAS_PREFIX),
                        "price": pricing["price"],
                        "compareAtPrice": pricing["compareAtPrice"],
                    }
