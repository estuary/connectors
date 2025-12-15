import json
from datetime import datetime
from logging import Logger
from typing import Any, AsyncGenerator

from ...models import ShopifyGraphQLResource, SortKey


class SubscriptionContracts(ShopifyGraphQLResource):
    NAME = "subscription_contracts"
    QUERY_ROOT = "subscriptionContracts"
    SORT_KEY = SortKey.UPDATED_AT
    QUALIFYING_SCOPES = {"read_own_subscription_contracts"}
    QUERY = """
    status
    nextBillingDate
    currencyCode
    note
    revisionId
    app {
        id
    }
    appAdminUrl
    customer {
        id
        legacyResourceId
    }
    customerPaymentMethod {
        id
    }
    billingPolicy {
        interval
        intervalCount
    }
    deliveryPolicy {
        interval
        intervalCount
    }
    deliveryPrice {
        amount
        currencyCode
    }
    deliveryMethod {
        __typename
        ... on SubscriptionDeliveryMethodPickup {
            pickupOption {
                code
                description
                location {
                    id
                    createdAt
                    address {
                        address1
                        address2
                        city
                        country
                        countryCode
                        formatted
                        latitude
                        longitude
                        phone
                        province
                        provinceCode
                        zip
                    }
                }
                presentmentTitle
                title
            }
        }
        ... on SubscriptionDeliveryMethodLocalDelivery {
            address {
                address1
                address2
                city
                company
                country
                firstName
                lastName
                name
                phone
                province
                provinceCode
                zip
            }
            localDeliveryOption {
                code
                description
                instructions
                phone
                presentmentTitle
                title
            }
        }
        ... on SubscriptionDeliveryMethodShipping {
            address {
                address1
                address2
                city
                company
                country
                firstName
                lastName
                name
                phone
                province
                provinceCode
                zip
            }
            shippingOption {
                code
                description
                presentmentTitle
                title
            }
        }
    }
    customAttributes {
        key
        value
    }
    lines {
        edges {
            node {
                id
                quantity
                variantId
                productId
                sellingPlanId
                sellingPlanName
                currentPrice {
                    amount
                    currencyCode
                }
                lineDiscountedPrice {
                    amount
                    currencyCode
                }
                pricingPolicy {
                    basePrice {
                        amount
                        currencyCode
                    }
                }
            }
        }
    }
    linesCount {
        count
    }
    lastBillingAttemptErrorType
    lastPaymentStatus
    originOrder {
        id
    }
    """

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return SubscriptionContracts.build_query_with_fragment(
            start,
            end,
            includeLegacyId=False,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        LINES_KEY = "lines"
        current_contract = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/SubscriptionContract/" in id:
                if current_contract:
                    yield current_contract

                current_contract = record
                current_contract[LINES_KEY] = []

            elif "gid://shopify/SubscriptionLine/" in id:
                if not current_contract:
                    log.error("Found a subscription line before finding a contract.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_contract.get("id", ""):
                    log.error(
                        "Subscription line's parent id does not match the current contract's id.",
                        {
                            "subscriptionLine.id": id,
                            "subscriptionLine.__parentId": record.get("__parentId"),
                            "current_contract.id": current_contract.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_contract[LINES_KEY].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_contract:
            yield current_contract
