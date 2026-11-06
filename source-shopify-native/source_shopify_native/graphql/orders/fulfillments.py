from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ..common import money_bag_fragment
from ...models import (
    NestedConnection,
    ShopifyGraphQLResource,
    SortKey,
    StoreCapabilities,
)


_FULFILLMENT_LINE_ITEM_SELECTION = """
id
quantity
discountedTotalSet {
    ..._MoneyBagFields
}
originalTotalSet {
    ..._MoneyBagFields
}
lineItem {
    id
}
"""


class Fulfillments(ShopifyGraphQLResource):
    NAME = "fulfillments"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT
    SHOULD_USE_BULK_QUERIES = False
    QUALIFYING_SCOPES = {
        "read_orders",
        "read_marketplace_orders",
        "read_assigned_fulfillment_orders",
        "read_merchant_managed_fulfillment_orders",
        "read_third_party_fulfillment_orders",
        "read_marketplace_fulfillment_orders",
    }
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
        # {{ fulfillmentLineItems }}
    }
    """
    NESTED_CONNECTIONS = [
        NestedConnection(
            parent_path=["fulfillments"],
            parent_typename="Fulfillment",
            field_name="fulfillmentLineItems",
            node_selection=_FULFILLMENT_LINE_ITEM_SELECTION,
            page_size=100,
            overflow_page_size=250,
            fragments=[money_bag_fragment],
        ),
    ]
    OUTER_PAGE_SIZE = 100

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        return Fulfillments.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
            capabilities=capabilities,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in Fulfillments._process_result(
            log, lines, "gid://shopify/Order/"
        ):
            yield record
