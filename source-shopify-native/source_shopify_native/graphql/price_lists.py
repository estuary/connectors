from datetime import datetime, timedelta
from logging import Logger
import json
from typing import Any, AsyncGenerator

from ..models import ShopifyGraphQLResource, StoreCapabilities


class PriceListPrices(ShopifyGraphQLResource):
    """A product variant's price on one price list, with the price list's currency, adjustment,
    and catalog."""

    NAME = "price_list_prices"
    QUERY_ROOT = "priceLists"
    QUALIFYING_SCOPES = {"read_products"}
    # Each sweep fetches every price on every price list, so sweep less often than the snapshot
    # default.
    SNAPSHOT_INTERVAL = timedelta(hours=6)
    QUERY = """
    id
    name
    currency
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
    prices {
        edges {
            node {
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
                    product {
                        id
                    }
                }
            }
        }
    }
    """

    # A price has no id of its own; it is identified by its price list and variant.
    id: str | None = None  # type: ignore[assignment]

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        # PriceList has no updated_at and priceLists cannot filter by date, so `start`/`end` are
        # unused.
        return f"""
        {{
            priceLists {{
                edges {{
                    node {{
                        {PriceListPrices.QUERY}
                    }}
                }}
            }}
        }}
        """

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        price_lists: dict[str, dict[str, Any]] = {}

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/PriceList/" in id:
                price_lists[id] = record
                continue

            # Prices have no id, so they're recognized by the price list they belong to.
            parent_id: str = record.pop("__parentId", "")
            price_list = price_lists.get(parent_id)
            if id or price_list is None:
                log.error(
                    "Unidentified line in JSONL response. Check if the JSONL response from Shopify is not ordered correctly.",
                    {"record": record, "__parentId": parent_id},
                )
                raise RuntimeError()

            yield {"priceList": price_list, **record}
