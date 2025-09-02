from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ...models import ShopifyGraphQLResource, SortKey


class Fulfillments(ShopifyGraphQLResource):
    NAME = "fulfillments"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT
    SHOULD_USE_BULK_QUERIES = False
    QUERY = """
    fulfillments {
        id
        legacyResourceId
        createdAt
        updatedAt
        deliveredAt
        displayStatus
        estimatedDeliveryAt
        inTransitAt
        location {
            id
            legacyResourceId
        }
        name
        order {
            id
            legacyResourceId
        }
        originAddress {
            address1
            address2
            city
            countryCode
            provinceCode
            zip
        }
        requiresShipping
        service {
            handle
            id
            inventoryManagement
            location {
                id
                legacyResourceId
            }
            serviceName
            trackingSupport
            type
        }
        status
        totalQuantity
        trackingInfo {
            company
            number
            url
        }
    }
    """

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return Fulfillments.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in Fulfillments._process_result(
            log, lines, "gid://shopify/Order/"
        ):
            yield record
