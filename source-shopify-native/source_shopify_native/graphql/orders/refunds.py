from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ..common import money_bag_fragment
from ...models import ShopifyGraphQLResource, SortKey


class OrderRefunds(ShopifyGraphQLResource):
    NAME = "order_refunds"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT
    SHOULD_USE_BULK_QUERIES = False
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
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return OrderRefunds.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in OrderRefunds._process_result(
            log, lines, "gid://shopify/Order/"
        ):
            yield record
