from datetime import datetime

from ..common import money_bag_fragment
from ...models import (
    NestedConnection,
    ShopifyGraphQLResource,
    SortKey,
    StoreCapabilities,
)


_SHIPPING_LINE_SELECTION = """
id
title
code
source
carrierIdentifier
deliveryCategory
custom
isRemoved
originalPriceSet {
    ..._MoneyBagFields
}
discountedPriceSet {
    ..._MoneyBagFields
}
currentDiscountedPriceSet {
    ..._MoneyBagFields
}
"""


class OrderShippingLines(ShopifyGraphQLResource):
    NAME = "order_shipping_lines"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT
    # Could run in bulk, but pages to leave the store's five bulk query slots to the streams
    # that need them.
    SHOULD_USE_BULK_QUERIES = False
    QUALIFYING_SCOPES = [{"read_orders", "read_marketplace_orders", "read_quick_sale"}]
    QUERY = """
    # {{ shippingLines }}
    """
    NESTED_CONNECTIONS = [
        NestedConnection(
            parent_path=[],
            parent_typename="Order",
            field_name="shippingLines",
            node_selection=_SHIPPING_LINE_SELECTION,
            # 250 orders with 5 inline lines each request 68 of the 1,000 points Shopify allows
            # per query (Bruno's `Orders / cost at full page`), so the default outer page size fits.
            page_size=5,
            overflow_page_size=250,
            # includeRemovals defaults to false, which drops shipping lines an order edit removed.
            extra_args="includeRemovals: true",
            fragments=[money_bag_fragment],
        ),
    ]

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        return OrderShippingLines.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
            capabilities=capabilities,
        )
