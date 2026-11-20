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


_REFUND_LINE_ITEM_SELECTION = """
id
location {
    id
}
priceSet {
    ..._MoneyBagFields
}
quantity
restocked
restockType
subtotalSet {
    ..._MoneyBagFields
}
totalTaxSet {
    ..._MoneyBagFields
}
lineItem {
    id
}
"""

_ORDER_ADJUSTMENT_SELECTION = """
id
reason
amountSet {
    ..._MoneyBagFields
}
taxAmountSet {
    ..._MoneyBagFields
}
"""

_REFUND_SHIPPING_LINE_SELECTION = """
id
shippingLine {
    id
}
subtotalAmountSet {
    ..._MoneyBagFields
}
taxAmountSet {
    ..._MoneyBagFields
}
"""


class OrderRefunds(ShopifyGraphQLResource):
    NAME = "order_refunds"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT
    SHOULD_USE_BULK_QUERIES = False
    QUALIFYING_SCOPES = {"read_orders", "read_marketplace_orders", "read_quick_sale"}
    QUERY = """
    displayFinancialStatus
    displayFulfillmentStatus
    refunds {
        id
        legacyResourceId
        note
        createdAt
        updatedAt
        duties {
            amountSet {
                ..._MoneyBagFields
            }
            originalDuty {
                id
                countryCodeOfOrigin
                harmonizedSystemCode
                price {
                    ..._MoneyBagFields
                }
                taxLines {
                    channelLiable
                    priceSet {
                        ..._MoneyBagFields
                    }
                    rate
                    title
                    source
                }
            }
        }
        order {
            id
            legacyResourceId
        }
        totalRefundedSet {
            ..._MoneyBagFields
        }
        # {{ refundLineItems }}
        # {{ orderAdjustments }}
        # {{ refundShippingLines }}
        # {{ transactions }}
    }
    """

    FRAGMENTS = [money_bag_fragment]
    NESTED_CONNECTIONS = [
        NestedConnection(
            parent_path=["refunds"],
            parent_typename="Refund",
            field_name="refundLineItems",
            node_selection=_REFUND_LINE_ITEM_SELECTION,
            page_size=10,
            overflow_page_size=250,
            fragments=[money_bag_fragment],
        ),
        NestedConnection(
            parent_path=["refunds"],
            parent_typename="Refund",
            field_name="orderAdjustments",
            node_selection=_ORDER_ADJUSTMENT_SELECTION,
            page_size=10,
            overflow_page_size=250,
            fragments=[money_bag_fragment],
        ),
        NestedConnection(
            parent_path=["refunds"],
            parent_typename="Refund",
            field_name="refundShippingLines",
            node_selection=_REFUND_SHIPPING_LINE_SELECTION,
            page_size=10,
            overflow_page_size=250,
            fragments=[money_bag_fragment],
        ),
        NestedConnection(
            parent_path=["refunds"],
            parent_typename="Refund",
            field_name="transactions",
            node_selection="id",
            page_size=10,
            overflow_page_size=250,
        ),
    ]
    # 100 orders with these four inline connections request 380 of the 1,000 points Shopify
    # allows per query (requestedQueryCost on a development store, API 2026-01).
    OUTER_PAGE_SIZE = 100

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        return OrderRefunds.build_query_with_fragment(
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
        async for record in OrderRefunds._process_result(
            log, lines, "gid://shopify/Order/"
        ):
            yield record
