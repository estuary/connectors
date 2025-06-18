from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ...models import ShopifyGraphQLResource


class Customers(ShopifyGraphQLResource):
    NAME = "customers"
    QUERY = """
    displayName
    email
    firstName
    lastName
    phone
    note
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
    verifiedEmail
    state
    taxExempt
    taxExemptions
    locale
    defaultAddress {
        id
    }
    addresses {
        address1
        address2
        city
        company
        country
        countryCodeV2
        firstName
        id
        lastName
        name
        phone
        province
        provinceCode
        zip
    }
    statistics {
        predictedSpendTier
    }
    """

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return Customers.build_query_with_fragment(
            "customers",
            "UPDATED_AT",
            start,
            end,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in Customers._process_result(
            log, lines, "gid://shopify/Customer/"
        ):
            yield record
