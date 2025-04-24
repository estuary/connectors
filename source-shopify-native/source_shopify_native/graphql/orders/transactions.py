from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ..common import money_bag_fragment
from ...models import ShopifyGraphQLResource


class OrderTransactions(ShopifyGraphQLResource):
    QUERY = """
    transactions {
        id
        createdAt
        accountNumber
        amountSet {
            ..._MoneyBagFields
        }
        amountRoundingSet {
            ..._MoneyBagFields
        }
        authorizationCode
        authorizationExpiresAt
        errorCode
        formattedGateway
        gateway
        kind
        manuallyCapturable
        manualPaymentGateway
        multiCapturable
        paymentId
        processedAt
        receiptJson
        settlementCurrency
        settlementCurrencyRate
        status
        test
        paymentDetails {
            ... on CardPaymentDetails {
                avsResultCode
                bin
                company
                cvvResultCode
                number
            }
            ... on LocalPaymentMethodsPaymentDetails {
                paymentDescriptor
                paymentMethodName
            }
            ... on ShopPayInstallmentsPaymentDetails {
                paymentMethodName
            }
        }
        totalUnsettledSet {
            ..._MoneyBagFields
        }
    }
    """
    FRAGMENTS = [money_bag_fragment]

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return OrderTransactions.build_query_with_fragment(
            "orders",
            "UPDATED_AT",
            start,
            end,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in OrderTransactions._process_result(
            log, lines, "gid://shopify/Order/"
        ):
            yield record
