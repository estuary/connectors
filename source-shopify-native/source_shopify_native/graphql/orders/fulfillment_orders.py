import json
from datetime import datetime
from logging import Logger
from typing import AsyncGenerator, Any

from ...models import ShopifyGraphQLResource


class FulfillmentOrders(ShopifyGraphQLResource):
    NAME = "fulfillment_orders"
    QUERY = """
    id
    fulfillmentOrders {
        edges {
            node {
                __typename
                id
                channelId
                assignedLocation {
                    location {
                        id
                        legacyResourceId
                    }
                    address1
                    address2
                    city
                    countryCode
                    name
                    phone
                    province
                    zip
                }
                destination {
                    id
                    address1
                    address2
                    city
                    company
                    countryCode
                    email
                    firstName
                    lastName
                    phone
                    province
                    zip
                }
                deliveryMethod {
                    id
                    methodType
                    minDeliveryDateTime
                    maxDeliveryDateTime
                }
                fulfillAt
                fulfillBy
                internationalDuties {
                    incoterm
                }
                fulfillmentHolds {
                    reason
                    reasonNotes
                }
                lineItems {
                    edges {
                        node {
                            __typename
                            id
                            inventoryItemId
                            lineItem {
                                id
                                fulfillableQuantity
                                currentQuantity
                                product {
                                    id
                                    legacyResourceId
                                }
                                variant {
                                    id
                                    legacyResourceId
                                }
                            }
                        }
                    }
                }
                createdAt
                updatedAt
                requestStatus
                status
                supportedActions {
                    action
                    externalUrl
                }
                merchantRequests {
                    edges {
                        node {
                            __typename
                            id
                            message
                            kind
                            requestOptions
                            sentAt
                        }
                    }
                }
            }
        }
    }
    """

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return FulfillmentOrders.build_query_with_fragment(
            "orders",
            "UPDATED_AT",
            start,
            end,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        current_order = None
        current_fulfillment_order = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")
            typename: str = record.get("__typename", "")
            parent_id: str = record.get("__parentId", "")

            if "gid://shopify/Order/" in id:
                if current_order:
                    yield current_order
                current_order = record
                current_order["fulfillmentOrders"] = []
                current_fulfillment_order = None

            elif "gid://shopify/FulfillmentOrder/" in id:
                if not current_order:
                    log.error("Found a fulfillment order before finding an order.")
                    raise RuntimeError()
                current_fulfillment_order = record
                current_fulfillment_order["merchantRequests"] = []
                current_fulfillment_order["lineItems"] = []
                current_order["fulfillmentOrders"].append(current_fulfillment_order)

            elif "gid://shopify/FulfillmentOrderMerchantRequest/" in id:
                if not current_fulfillment_order:
                    log.error(
                        "Found a merchant request before finding a fulfillment order."
                    )
                    raise RuntimeError()
                current_fulfillment_order["merchantRequests"].append(record)

            elif (
                typename == "FulfillmentOrderLineItem"
                or "gid://shopify/FulfillmentOrderLineItem/" in id
            ):
                if not current_fulfillment_order:
                    if parent_id and current_order:
                        for fo in current_order["fulfillmentOrders"]:
                            if fo["id"] == parent_id:
                                current_fulfillment_order = fo
                                break

                if not current_fulfillment_order:
                    log.error(
                        "Found a fulfillment order line item before finding a fulfillment order."
                    )
                    raise RuntimeError()

                current_fulfillment_order["lineItems"].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_order:
            yield current_order
