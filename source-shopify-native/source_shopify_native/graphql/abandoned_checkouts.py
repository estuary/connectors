from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from .common import money_bag_fragment
from ..models import ShopifyGraphQLResource, SortKey


class AbandonedCheckouts(ShopifyGraphQLResource):
    NAME = "abandoned_checkouts"
    QUERY_ROOT = "abandonedCheckouts"
    SORT_KEY = SortKey.CREATED_AT
    # Requires read_orders scope AND the user must have manage_abandoned_checkouts
    # staff permission in Shopify admin. The staff permission cannot be verified
    # programmatically - only the OAuth scope can be checked.
    # See: https://shopify.dev/docs/api/admin-graphql/latest/objects/AbandonedCheckout
    QUALIFYING_SCOPES = {"read_orders"}
    QUERY = """
    id
    abandonedCheckoutUrl
    completedAt
    createdAt
    updatedAt
    note
    taxesIncluded
    customer {
        id
        firstName
        lastName
        email
        note
        numberOfOrders
        state
        updatedAt
        tags
        defaultEmailAddress {
            marketingOptInLevel
            marketingState
            marketingUnsubscribeUrl
            marketingUpdatedAt
        }
        defaultPhoneNumber {
            marketingOptInLevel
            marketingState
            marketingCollectedFrom
            marketingUpdatedAt
        }
    }
    discountCodes
    lineItems {
        edges {
            node {
                id
                title
                quantity
                variant {
                    id
                    title
                    price
                    sku
                    product {
                        id
                        title
                    }
                }
            }
        }
    }
    shippingAddress {
        firstName
        lastName
        address1
        address2
        city
        province
        provinceCode
        country
        countryCode
        zip
        phone
    }
    billingAddress {
        firstName
        lastName
        address1
        address2
        city
        province
        provinceCode
        country
        countryCode
        zip
        phone
    }
    subtotalPriceSet {
        ..._MoneyBagFields
    }
    totalDiscountSet {
        ..._MoneyBagFields
    }
    totalLineItemsPriceSet {
        ..._MoneyBagFields
    }
    totalPriceSet {
        ..._MoneyBagFields
    }
    totalTaxSet {
        ..._MoneyBagFields
    }
    """
    FRAGMENTS = [money_bag_fragment]

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return AbandonedCheckouts.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
            includeLegacyId=False,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        LINE_ITEMS_KEY = "lineItems"

        current_checkout = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/AbandonedCheckout/" in id:
                if current_checkout:
                    yield current_checkout

                current_checkout = record
                current_checkout[LINE_ITEMS_KEY] = []

            elif "gid://shopify/LineItem/" in id:
                if not current_checkout:
                    log.error("Found a line item before finding an abandoned checkout.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_checkout.get("id", ""):
                    log.error(
                        "Line item's parent id does not match the current abandoned checkout's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "lineItem.id": id,
                            "lineItem.__parentId": record.get("__parentId"),
                            "current_checkout.id": current_checkout.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_checkout[LINE_ITEMS_KEY].append(record)

        if current_checkout:
            yield current_checkout
