from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from .common import dt_to_str, round_to_latest_midnight
from ..models import ShopifyGraphQlResource


class AbandonedCheckouts(ShopifyGraphQlResource):
    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        lower_bound = dt_to_str(start)
        upper_bound = dt_to_str(round_to_latest_midnight(end))

        query = f"""
        {{
            abandonedCheckouts(
                query: "updated_at:>={lower_bound} AND updated_at:<={upper_bound}"
                sortKey: CREATED_AT
            ) {{
                edges {{
                    node {{
                        id
                        abandonedCheckoutUrl
                        completedAt
                        createdAt
                        updatedAt
                        note
                        taxesIncluded
                        customer {{
                            id
                            firstName
                            lastName
                            email
                        }}
                        discountCodes
                        lineItems {{
                            edges {{
                                node {{
                                    id
                                    title
                                    quantity
                                    variant {{
                                        id
                                        title
                                        price
                                        sku
                                        product {{
                                            id
                                            title
                                        }}
                                    }}
                                }}
                            }}
                        }}
                        shippingAddress {{
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
                        }}
                        billingAddress {{
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
                        }}
                        subtotalPriceSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        totalDiscountSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        totalLineItemsPriceSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        totalPriceSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
                            }}
                        }}
                        totalTaxSet {{
                            presentmentMoney {{
                                amount
                                currencyCode
                            }}
                            shopMoney {{
                                amount
                                currencyCode
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
