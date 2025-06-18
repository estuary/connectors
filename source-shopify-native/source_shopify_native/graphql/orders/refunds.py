from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ..common import money_bag_fragment
from ...models import ShopifyGraphQLResource


class OrderRefunds(ShopifyGraphQLResource):
    NAME = "order_refunds"
    QUERY = """
    displayFinancialStatus
    displayFulfillmentStatus
    refunds {
        id
        legacyResourceId
        note
        createdAt
        updatedAt
        duties {
            amountSet {
                ..._MoneyBagFields
            }
            originalDuty {
                id
                countryCodeOfOrigin
                harmonizedSystemCode
                price {
                    ..._MoneyBagFields
                }
                taxLines {
                    channelLiable
                    priceSet {
                        ..._MoneyBagFields
                    }
                    rate
                    title
                    source
                }
            }
        }
        order {
            id
            legacyResourceId
        }
        totalRefundedSet {
            ..._MoneyBagFields
        }
    }
    """

    FRAGMENTS = [money_bag_fragment]

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return OrderRefunds.build_query_with_fragment(
            "orders",
            "UPDATED_AT",
            start,
            end,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in OrderRefunds._process_result(
            log, lines, "gid://shopify/Order/"
        ):
            yield record
