from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ...models import ShopifyGraphQLResource, SortKey


class SubscriptionContracts(ShopifyGraphQLResource):
    NAME = "subscription_contracts"
    QUERY_ROOT = "subscriptionContracts"
    SORT_KEY = SortKey.UPDATED_AT
    SHOULD_USE_BULK_QUERIES = False
    QUERY = """
    id
    createdAt
    updatedAt
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
        instrument
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
                    createdAt
                    id
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
                coordinatesValidated
                country
                countryCodeV2
                firstName
                formatted
                formattedArea
                id
                lastName
                latitude
                longitude
                name
                phone
                province
                provinceCode
                timeZone
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
                coordinatesValidated
                country
                countryCodeV2
                firstName
                formatted
                formattedArea
                id
                lastName
                latitude
                longitude
                name
                phone
                province
                provinceCode
                timeZone
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
    lines(first: 250) {
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
        name
        number
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
            first=first,
            after=after,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in SubscriptionContracts._process_result(
            log, lines, "gid://shopify/SubscriptionContract/"
        ):
            yield record
