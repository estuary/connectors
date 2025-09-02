from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ...models import ShopifyGraphQLResource


class InventoryItems(ShopifyGraphQLResource):
    NAME = "inventory_items"
    QUERY_ROOT = "inventoryItems"
    SHOULD_USE_BULK_QUERIES = False
    QUERY = """
    id
    legacyResourceId
    createdAt
    updatedAt
    countryCodeOfOrigin
    harmonizedSystemCode
    provinceCodeOfOrigin
    sku
    tracked
    requiresShipping
    unitCost {
        amount
        currencyCode
    }
    variant {
        id
        legacyResourceId
        product {
            id
            legacyResourceId
            title
        }
    }
    inventoryHistoryUrl
    """

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return InventoryItems.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in InventoryItems._process_result(
            log, lines, "gid://shopify/InventoryItem/"
        ):
            yield record
