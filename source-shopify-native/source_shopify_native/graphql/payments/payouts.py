from datetime import datetime, timedelta
from typing import ClassVar

from pydantic import AwareDatetime

from ..common import money_v2_fragment
from ...models import (
    ShopifyGraphQLResource,
    StoreCapabilities,
)
from ...utils import dt_to_str, str_to_dt


class Payouts(ShopifyGraphQLResource):
    NAME = "payouts"
    QUERY_ROOT = "shopifyPaymentsAccount"
    CONNECTION_PATH = ["payouts"]
    SHOULD_USE_BULK_QUERIES = False
    # Reading through shopifyPaymentsAccount needs one of the scopes in the second set.
    QUALIFYING_SCOPES = [
        {"read_shopify_payments_payouts"},
        {"read_shopify_payments_accounts", "read_shopify_payments"},
    ]
    # How long a payout's status can keep changing after it's issued. Shopify staff say a paid
    # payout "can take 10 business days to come back as failed"
    # (https://community.shopify.com/t/47794). We can't check this on a development store, so
    # 30 days leaves a margin of about twice that.
    STATUS_CHANGE_WINDOW: ClassVar[timedelta] = timedelta(days=30)
    # Shopify's docs say a payout's transactions are assigned when the payout is created, so a
    # few daily reads after it's issued fill in their payout ids. 7 days leaves several reads to
    # spare, in case one read misses a transaction.
    TRANSACTIONS_WINDOW: ClassVar[timedelta] = timedelta(days=7)
    QUERY = """
    id
    legacyResourceId
    issuedAt
    status
    transactionType
    externalTraceId
    net {
        ..._MoneyV2Fields
    }
    businessEntity {
        id
    }
    summary {
        adjustmentsFee {
            ..._MoneyV2Fields
        }
        adjustmentsGross {
            ..._MoneyV2Fields
        }
        advanceFees {
            ..._MoneyV2Fields
        }
        advanceGross {
            ..._MoneyV2Fields
        }
        chargesFee {
            ..._MoneyV2Fields
        }
        chargesGross {
            ..._MoneyV2Fields
        }
        refundsFee {
            ..._MoneyV2Fields
        }
        refundsFeeGross {
            ..._MoneyV2Fields
        }
        reservedFundsFee {
            ..._MoneyV2Fields
        }
        reservedFundsGross {
            ..._MoneyV2Fields
        }
        retriedPayoutsFee {
            ..._MoneyV2Fields
        }
        retriedPayoutsGross {
            ..._MoneyV2Fields
        }
        usdcRebateCreditAmount {
            ..._MoneyV2Fields
        }
    }
    """

    legacyResourceId: str
    issuedAt: str
    status: str
    transactionType: str

    def get_cursor_value(self) -> AwareDatetime:
        return str_to_dt(self.issuedAt)

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        lower_bound = dt_to_str(start)
        upper_bound = dt_to_str(end)
        return f"""
        {{
            shopifyPaymentsAccount {{
                payouts(
                    query: "issued_at:>='{lower_bound}' AND issued_at:<='{upper_bound}'"
                    {f"first: {first}" if first else ""}
                    {f'after: "{after}"' if after else ""}
                ) {{
                    edges {{
                        node {{
                            {Payouts.QUERY}
                        }}
                    }}
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                }}
            }}
        }}
        {money_v2_fragment}
        """
