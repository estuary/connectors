from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator, Dict

from ..common import money_bag_fragment
from ...models import ShopifyGraphQLResource, SortKey


class Orders(ShopifyGraphQLResource):
    NAME = "orders"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT
    QUERY = """
    app {
        id
    }
    billingAddress {
        id
        address1
        address2
        city
        company
        country
        countryCodeV2
        firstName
        lastName
        latitude
        longitude
        name
        phone
        province
        provinceCode
        zip
    }
    shippingAddress {
        id
        address1
        address2
        city
        company
        country
        countryCodeV2
        firstName
        lastName
        latitude
        longitude
        name
        phone
        province
        provinceCode
        zip
    }
    clientIp
    customerAcceptsMarketing
    cancelReason
    cancelledAt
    closedAt
    purchasingEntity {
        __typename
        ... on PurchasingCompany {
            company {
                id
                name
                externalId
            }
        }
    }
    confirmationNumber
    confirmed
    createdAt
    updatedAt
    currencyCode
    currentSubtotalPriceSet {
        ..._MoneyBagFields
    }
    currentTotalAdditionalFeesSet {
        ..._MoneyBagFields
    }
    currentTotalDiscountsSet {
        ..._MoneyBagFields
    }
    currentTotalDutiesSet {
        ..._MoneyBagFields
    }
    currentTotalPriceSet {
        ..._MoneyBagFields
    }
    currentTotalTaxSet {
        ..._MoneyBagFields
    }
    customer {
        id
        legacyResourceId
    }
    customerJourneySummary {
        customerOrderIndex
        daysToConversion
        firstVisit {
            id
            landingPage
            marketingEvent {
                id
                description
                manageUrl
                previewUrl
                type
            }
            occurredAt
            referralCode
            referrerUrl
            source
            sourceDescription
            sourceType
        }
        lastVisit {
            id
            landingPage
            marketingEvent {
                id
                description
                manageUrl
                previewUrl
                type
            }
            occurredAt
            referralCode
            referrerUrl
            source
            sourceDescription
            sourceType
        }
        momentsCount {
            count
            precision
        }
        ready
    }
    customerLocale
    discountApplications {
        edges {
            node {
                __typename
                allocationMethod
                index
                targetSelection
                targetType
                value
            }
        }
    }
    discountCode
    discountCodes
    email
    estimatedTaxes
    displayFinancialStatus
    displayFulfillmentStatus
    merchantOfRecordApp {
        id
        name
    }
    name
    note
    customAttributes {
        key
        value
    }
    statusPageUrl
    originalTotalAdditionalFeesSet {
        ..._MoneyBagFields
    }
    originalTotalDutiesSet {
        ..._MoneyBagFields
    }
    paymentGatewayNames
    paymentTerms {
        id
        dueInDays
        overdue
        paymentTermsName
        paymentTermsType
        paymentSchedules {
            edges {
                node {
                    id
                    completedAt
                    dueAt
                    issuedAt
                }
            }
        }
    }
    lineItems {
        edges {
            node {
                # identifiers
                id
                sku
                vendor
                image {
                    originalSrc
                }

                # quantities and statuses useful for reporting
                quantity
                currentQuantity
                unfulfilledQuantity
                refundableQuantity
                nonFulfillableQuantity
                requiresShipping
                isGiftCard
                taxable

                # unit-level prices
                originalUnitPriceSet {
                    ..._MoneyBagFields
                }
                discountedUnitPriceSet {
                    ..._MoneyBagFields
                }
                discountedUnitPriceAfterAllDiscountsSet {
                    ..._MoneyBagFields
                }

                # total-level prices (quantity already factored)
                originalTotalSet {
                    ..._MoneyBagFields
                }
                discountedTotalSet {
                    ..._MoneyBagFields
                }
                totalDiscountSet {
                    ..._MoneyBagFields
                }

                # unfulfilled totals
                unfulfilledDiscountedTotalSet {
                    ..._MoneyBagFields
                }
                unfulfilledOriginalTotalSet {
                    ..._MoneyBagFields
                }

                # promotions, taxes, duties
                discountAllocations {
                    allocatedAmountSet {
                        ..._MoneyBagFields
                    }
                }
                taxLines {
                    title
                    ratePercentage
                    priceSet {
                        ..._MoneyBagFields
                    }
                }
                duties {
                    id
                    price {
                        ..._MoneyBagFields
                    }
                }

                # subscription & bundle information
                sellingPlan {
                    name
                    sellingPlanId
                }
                lineItemGroup {
                    title
                    quantity
                }

                # product and variant information
                variant {
                    id
                    legacyResourceId
                    sku
                }
                product {
                    id
                    legacyResourceId
                }
            }
        }
    }
    phone
    poNumber
    presentmentCurrencyCode
    processedAt
    referrerUrl
    sourceIdentifier
    sourceName
    registeredSourceUrl
    subtotalPrice
    tags
    taxExempt
    taxLines {
        channelLiable
        priceSet {
            ..._MoneyBagFields
        }
        rate
        title
        source
    }
    taxesIncluded
    test
    totalDiscountsSet {
        ..._MoneyBagFields
    }
    subtotalPriceSet {
        ..._MoneyBagFields
    }
    totalOutstandingSet {
        ..._MoneyBagFields
    }
    totalPriceSet {
        ..._MoneyBagFields
    }
    totalShippingPriceSet {
        ..._MoneyBagFields
    }
    totalTaxSet {
        ..._MoneyBagFields
    }
    totalTipReceivedSet {
        ..._MoneyBagFields
    }
    totalWeight
    """
    FRAGMENTS = [money_bag_fragment]

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return Orders.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        DISCOUNT_APPLICATIONS_KEY = "discountApplications"
        PAYMENT_TERMS_KEY = "paymentTerms"
        PAYMENT_SCHEDULES_KEY = "paymentSchedules"
        LINE_ITEMS_KEY = "lineItems"
        current_order = None

        async for line in lines:
            record: Dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")
            typename: str = record.get("__typename", "")

            if "gid://shopify/Order/" in id:
                if current_order:
                    yield current_order

                current_order = record
                current_order[DISCOUNT_APPLICATIONS_KEY] = []
                current_order[PAYMENT_TERMS_KEY] = (
                    {}
                    if not record.get("paymentTerms", None)
                    else record["paymentTerms"]
                )
                current_order[PAYMENT_TERMS_KEY][PAYMENT_SCHEDULES_KEY] = []
                current_order[LINE_ITEMS_KEY] = []

            elif (
                "gid://shopify/AutomaticDiscountApplication/" in id
                or typename == "AutomaticDiscountApplication"
            ):
                if not current_order:
                    log.error("Found a discount application before finding an order.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_order.get("id", ""):
                    log.error(
                        "Discount application's parent id does not match the current order's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "discountApplication.id": id,
                            "discountApplication.__parentId": record.get("__parentId"),
                            "current_order.id": current_order.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_order[DISCOUNT_APPLICATIONS_KEY].append(record)

            elif (
                "gid://shopify/DiscountCodeApplication/" in id
                or typename == "DiscountCodeApplication"
            ):
                if not current_order:
                    log.error("Found a discount application before finding an order.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_order.get("id", ""):
                    log.error(
                        "Discount application's parent id does not match the current order's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "discountApplication.id": id,
                            "discountApplication.__parentId": record.get("__parentId"),
                            "current_order.id": current_order.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_order[DISCOUNT_APPLICATIONS_KEY].append(record)

            elif (
                "gid://shopify/ManualDiscountApplication/" in id
                or typename == "ManualDiscountApplication"
            ):
                if not current_order:
                    log.error("Found a discount application before finding an order.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_order.get("id", ""):
                    log.error(
                        "Discount application's parent id does not match the current order's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "discountApplication.id": id,
                            "discountApplication.__parentId": record.get("__parentId"),
                            "current_order.id": current_order.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_order[DISCOUNT_APPLICATIONS_KEY].append(record)

            elif (
                "gid://shopify/ScriptDiscountApplication/" in id
                or typename == "ScriptDiscountApplication"
            ):
                if not current_order:
                    log.error("Found a discount application before finding an order.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_order.get("id", ""):
                    log.error(
                        "Discount application's parent id does not match the current order's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "discountApplication.id": id,
                            "discountApplication.__parentId": record.get("__parentId"),
                            "current_order.id": current_order.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_order[DISCOUNT_APPLICATIONS_KEY].append(record)

            elif "gid://shopify/PaymentSchedule/" in id:
                if not current_order:
                    log.error("Found a payment schedule before finding an order.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_order.get("id", ""):
                    log.error(
                        "Payment schedule's parent id does not match the current order's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "paymentSchedule.id": id,
                            "paymentSchedule.__parentId": record.get("__parentId"),
                            "current_order.id": current_order.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_order[PAYMENT_TERMS_KEY][PAYMENT_SCHEDULES_KEY].append(record)

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

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_order:
            yield current_order
