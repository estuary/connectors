from datetime import datetime

from pydantic import AwareDatetime

from ..common import money_v2_fragment
from ...models import (
    ShopifyGraphQLResource,
    StoreCapabilities,
)
from ...utils import dt_to_str, str_to_dt


class BalanceTransactions(ShopifyGraphQLResource):
    NAME = "balance_transactions"
    QUERY_ROOT = "shopifyPaymentsAccount"
    CONNECTION_PATH = ["balanceTransactions"]
    SHOULD_USE_BULK_QUERIES = False
    # Shopify can sort by PROCESSED_AT, but nothing relies on the order. The backfill already
    # checkpoints after every page, and an incremental read usually covers only a few
    # minutes, so sorting would add little.
    SORT_KEY = None
    # Reading through shopifyPaymentsAccount needs one of the scopes in the second set.
    QUALIFYING_SCOPES = [
        {"read_shopify_payments_payouts"},
        {"read_shopify_payments_accounts", "read_shopify_payments"},
    ]
    QUERY = """
    id
    transactionDate
    type
    sourceType
    sourceId
    sourceOrderTransactionId
    test
    adjustmentReason
    amount {
        ..._MoneyV2Fields
    }
    fee {
        ..._MoneyV2Fields
    }
    net {
        ..._MoneyV2Fields
    }
    associatedOrder {
        id
    }
    associatedPayout {
        id
    }
    """

    transactionDate: str

    def get_cursor_value(self) -> AwareDatetime:
        return str_to_dt(self.transactionDate)

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
                balanceTransactions(
                    query: "processed_at:>='{lower_bound}' AND processed_at:<='{upper_bound}'"
                    {f"first: {first}" if first else ""}
                    {f'after: "{after}"' if after else ""}
                ) {{
                    edges {{
                        node {{
                            {BalanceTransactions.QUERY}
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
