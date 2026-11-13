from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from pydantic import ValidationInfo, model_validator

from ..models import (
    ShopifyGraphQLResource,
    StoreCapabilities,
    StoreValidationContext,
)


@dataclass(frozen=True, slots=True)
class PriceListValidationContext(StoreValidationContext):
    """Validation context that also carries the price list whose prices are being read."""

    price_list_id: str


class PriceLists(ShopifyGraphQLResource):
    """Price lists of a Shopify store, each with its currency, its adjustment of the store's
    prices, and the catalog it prices.

    PriceList has no `updatedAt` and the `priceLists` connection cannot filter by date, so there
    is no usable incremental cursor and the full set is re-fetched each snapshot.

    A price list's catalog is a market, company location or app catalog. The connector captures
    none of them in a stream of its own, so the catalog is embedded rather than referenced by id.
    A price list can have no catalog.
    """

    NAME = "price_lists"
    QUERY_ROOT = "priceLists"
    SHOULD_USE_BULK_QUERIES = False
    QUALIFYING_SCOPES = [{"read_products"}]
    QUERY = """
    id
    name
    currency
    fixedPricesCount
    parent {
        adjustment {
            type
            value
        }
        settings {
            compareAtMode
        }
    }
    catalog {
        __typename
        id
        title
        status
    }
    """

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        # An unfiltered, cursor-paginated query in id order: PriceList has no updated_at and the
        # priceLists connection cannot filter by date, so `start`/`end` are unused.
        return f"""
        {{
            priceLists(
                sortKey: ID
                {f"first: {first}" if first else ""}
                {f'after: "{after}"' if after else ""}
            ) {{
                edges {{
                    node {{
                        {PriceLists.QUERY}
                    }}
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
        """


class PriceListPrices(ShopifyGraphQLResource):
    """A product variant's price on one price list, with the price list's id.

    Every price list prices every variant. FIXED prices are set on the list, in its currency.
    RELATIVE prices apply the list's adjustment to the variant's price and stay in the store's
    currency, so they don't move with exchange rates.

    A price has no id or timestamps and `prices` cannot filter by date, so every price list's
    prices are re-fetched each snapshot.
    """

    NAME = "price_list_prices"
    QUERY_ROOT = "priceList"
    CONNECTION_PATH = ["prices"]
    SHOULD_USE_BULK_QUERIES = False
    QUALIFYING_SCOPES = [{"read_products"}]
    # A snapshot reads one price per variant per price list, and a change to any price re-emits
    # them all, so it runs once a day by default.
    SNAPSHOT_INTERVAL = timedelta(days=1)
    QUERY = """
    originType
    price {
        amount
        currencyCode
    }
    compareAtPrice {
        amount
        currencyCode
    }
    variant {
        id
    }
    """

    # A price has no id of its own: its price list and variant identify it.
    id: str | None = None  # type: ignore[assignment]

    @model_validator(mode="before")
    @classmethod
    def _inject_price_list_from_context(cls, data: Any, info: ValidationInfo) -> Any:
        """A price doesn't name its price list, so its id comes from the validation context."""
        if isinstance(data, dict) and isinstance(info.context, PriceListValidationContext):
            data["priceList"] = {"id": info.context.price_list_id}
        return data

    @staticmethod
    def build_query_for_price_list(
        price_list_id: str, first: int, after: str | None = None
    ) -> str:
        return f"""
        {{
            priceList(id: "{price_list_id}") {{
                prices(
                    first: {first}
                    {f'after: "{after}"' if after else ""}
                ) {{
                    edges {{
                        node {{
                            {PriceListPrices.QUERY}
                        }}
                    }}
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                }}
            }}
        }}
        """
