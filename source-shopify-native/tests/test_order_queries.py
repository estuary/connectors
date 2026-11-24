from datetime import UTC, datetime

from source_shopify_native.graphql.orders.orders import Orders
from source_shopify_native.graphql.orders.refunds import OrderRefunds

START = datetime(2024, 1, 1, tzinfo=UTC)
END = datetime(2024, 2, 1, tzinfo=UTC)


def test_order_refunds_query_selects_refund_details():
    query = OrderRefunds.build_query(START, END, first=100)

    assert "orderAdjustments(first: 10)" in query
    assert "refundShippingLines(first: 10)" in query
    assert "transactions(first: 10)" in query
    # Several connections spread _MoneyBagFields, but the query may define it only once.
    assert query.count("fragment _MoneyBagFields on MoneyBag") == 1


def test_orders_query_selects_line_item_titles():
    query = Orders.build_query(START, END)
    # Other parts of an order also select `title`, so check only the line item's own
    # fields, which come before its `image`.
    line_item_fields = query.split("lineItems {", 1)[1].split("image {", 1)[0].split()

    assert "title" in line_item_fields
    assert "variantTitle" in line_item_fields
