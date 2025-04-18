from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from .common import dt_to_str, round_to_latest_midnight
from ..models import ShopifyGraphQlResource


class Orders(ShopifyGraphQlResource):
    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        lower_bound = dt_to_str(start)
        upper_bound = dt_to_str(round_to_latest_midnight(end))

        query = f"""
        {{
            orders(
                query: "updated_at:>={lower_bound} AND updated_at:<={upper_bound}"
                sortKey: UPDATED_AT
            ) {{
                edges {{
                    node {{
                        id
                        name
                        email
                        createdAt
                        updatedAt
                        closedAt
                        processedAt
                        cancelledAt
                        cancelReason
                        currencyCode
                        displayFinancialStatus
                        displayFulfillmentStatus
                        fullyPaid
                        note
                        phone
                        subtotalPrice
                        totalPrice
                        totalShippingPrice
                        totalTax
                        totalRefunded
                        customer {{
                            id
                        }}
                        shippingAddress {{
                            address1
                            address2
                            city
                            company
                            country
                            countryCodeV2
                            firstName
                            lastName
                            name
                            phone
                            province
                            provinceCode
                            zip
                        }}
                        billingAddress {{
                            address1
                            address2
                            city
                            company
                            country
                            countryCodeV2
                            firstName
                            lastName
                            name
                            phone
                            province
                            provinceCode
                            zip
                        }}
                        lineItems {{
                            edges {{
                                node {{
                                    id
                                    name
                                    quantity
                                    originalUnitPrice
                                    discountedUnitPrice
                                    variant {{
                                        id
                                        title
                                        sku
                                        price
                                        product {{
                                            id
                                            title
                                        }}
                                    }}
                                }}
                            }}
                        }}
                        transactions {{
                            id
                            amount
                            createdAt
                            gateway
                            kind
                            status
                            test
                            processedAt
                            paymentDetails {{
                                ... on CardPaymentDetails {{
                                    avsResultCode
                                    bin
                                    company
                                    number
                                    cvvResultCode
                                    paymentMethodName
                                }}
                                ... on LocalPaymentMethodsPaymentDetails {{
                                    paymentMethodName
                                    paymentDescriptor
                                }}
                                ... on ShopPayInstallmentsPaymentDetails {{
                                    paymentMethodName
                                }}
                            }}
                        }}
                        fulfillments {{
                            id
                            createdAt
                            updatedAt
                            deliveredAt
                            estimatedDeliveryAt
                            displayStatus
                            status
                            trackingInfo {{
                                company
                                number
                                url
                            }}
                        }}
                        metafields {{
                            edges {{
                                node {{
                                    id
                                    namespace
                                    key
                                    value
                                    type
                                    createdAt
                                    updatedAt
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
        """

        return query

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[Any, None]:
        LINE_ITEMS_KEY = "lineItems"
        METAFIELDS_KEY = "metafields"

        current_order = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/Order/" in id:
                if current_order:
                    yield current_order

                current_order = record
                current_order[LINE_ITEMS_KEY] = []
                current_order[METAFIELDS_KEY] = []

                # Convert transactions and fulfillments from objects to lists if they're not already lists
                if current_order.get("transactions") and not isinstance(
                    current_order.get("transactions"), list
                ):
                    current_order["transactions"] = [current_order["transactions"]]
                if current_order.get("fulfillments") and not isinstance(
                    current_order.get("fulfillments"), list
                ):
                    current_order["fulfillments"] = [current_order["fulfillments"]]

            elif "gid://shopify/LineItem/" in id:
                if not current_order:
                    log.error("Found a line item before finding an order.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_order.get("id", ""):
                    log.error(
                        "Line item's parent id does not match the current order's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "lineItem.id": id,
                            "lineItem.__parentId": record.get("__parentId"),
                            "current_order.id": current_order.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_order[LINE_ITEMS_KEY].append(record)

            elif "gid://shopify/Metafield/" in id:
                if not current_order:
                    log.error("Found a metafield before finding an order.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_order.get("id", ""):
                    log.error(
                        "Metafield's parent id does not match the current order's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "metafield.id": id,
                            "metafield.__parentId": record.get("__parentId"),
                            "current_order.id": current_order.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_order[METAFIELDS_KEY].append(record)

        if current_order:
            yield current_order
