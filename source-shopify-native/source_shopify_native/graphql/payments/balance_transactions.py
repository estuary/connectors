from datetime import datetime, timedelta
from typing import ClassVar

from pydantic import AwareDatetime, BaseModel, ConfigDict

from ..common import money_v2_fragment
from ...models import (
    ShopifyGraphQLResource,
    StoreCapabilities,
)
from ...utils import dt_to_str, str_to_dt
from .account import SHOPIFY_PAYMENTS_ACCOUNT_SCOPES, build_account_query


class AssociatedPayout(BaseModel):
    model_config = ConfigDict(extra="allow")

    # Null until the transaction is assigned to a payout.
    id: str | None


class BalanceTransactions(ShopifyGraphQLResource):
    NAME = "balance_transactions"
    QUERY_ROOT_PARENT = "shopifyPaymentsAccount"
    QUERY_ROOT = "balanceTransactions"
    SHOULD_USE_BULK_QUERIES = False
    # No sort key is sent, like disputes, and nothing relies on the order (Shopify's default is
    # PROCESSED_AT): the backfill already checkpoints after every page, and an incremental
    # read usually covers only the minutes since the last one, so the checkpoints partway
    # through a read that sorting would allow gain little.
    SORT_KEY = None
    QUALIFYING_SCOPES = {"read_shopify_payments_payouts"}
    ADDITIONAL_REQUIRED_SCOPES = [SHOPIFY_PAYMENTS_ACCOUNT_SCOPES]
    # How long after a payout's date its transactions are read again. A transaction gets its
    # payout when the payout is created and, per Shopify's docs, keeps it (no development
    # store can show this). A payout may wait out a weekend or public holiday before it's
    # sent, so 7 days still leaves several daily reads after that, in case one read misses a
    # transaction.
    RECENT_PAYOUTS_WINDOW: ClassVar[timedelta] = timedelta(days=7)
    FRAGMENTS = [money_v2_fragment]
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
    associatedPayout: AssociatedPayout

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
        return build_account_query(
            BalanceTransactions,
            f"processed_at:>='{dt_to_str(start)}' AND processed_at:<='{dt_to_str(end)}'",
            first,
            after,
        )

    # A payout's transactions, which the transaction filter calls a transfer. Takes the
    # payout's legacyResourceId.
    @staticmethod
    def build_payout_query(
        payout_legacy_resource_id: str,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return build_account_query(
            BalanceTransactions,
            f"payments_transfer_id:{payout_legacy_resource_id}",
            first,
            after,
        )
