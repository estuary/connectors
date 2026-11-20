import re
from datetime import UTC, datetime

from source_shopify_native.graphql.orders.orders import Orders
from source_shopify_native.graphql.orders.refunds import OrderRefunds

START = datetime(2024, 1, 1, tzinfo=UTC)
END = datetime(2024, 2, 1, tzinfo=UTC)


def test_order_refunds_query_includes_refund_details():
    query = " ".join(OrderRefunds.build_query(START, END, first=100).split())

    assert (
        "orderAdjustments(first: 10) { edges { node { id reason "
        "amountSet { ..._MoneyBagFields } taxAmountSet { ..._MoneyBagFields } } }"
    ) in query
    assert (
        "refundShippingLines(first: 10) { edges { node { id shippingLine { id } "
        "subtotalAmountSet { ..._MoneyBagFields } taxAmountSet { ..._MoneyBagFields } } }"
    ) in query
    assert "transactions(first: 10) { edges { node { id } }" in query
    assert query.count("fragment _MoneyBagFields on MoneyBag") == 1


def test_orders_query_includes_line_item_titles():
    query = re.sub(r"(?m)#.*$", "", Orders.build_query(START, END))
    query = " ".join(query.split())

    assert "lineItems { edges { node { id name title variantTitle sku vendor" in query
