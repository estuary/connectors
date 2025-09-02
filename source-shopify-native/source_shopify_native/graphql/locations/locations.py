from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ...models import ShopifyGraphQLResource


class Locations(ShopifyGraphQLResource):
    NAME = "locations"
    QUERY_ROOT = "locations"
    SHOULD_USE_BULK_QUERIES = False
    QUERY = """
    id
    name
    legacyResourceId
    updatedAt
    createdAt
    address {
        address1
        address2
        city
        country
        countryCode
        phone
        province
        provinceCode
        zip
    }
    addressVerified
    deactivatedAt
    deactivatable
    deletable
    fulfillsOnlineOrders
    hasActiveInventory
    isActive
    activatable
    shipsInventory
    fulfillmentService {
        id
        handle
        serviceName
        type
    }
    hasUnfulfilledOrders
    """

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return Locations.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in Locations._process_result(
            log, lines, "gid://shopify/Location/"
        ):
            yield record
