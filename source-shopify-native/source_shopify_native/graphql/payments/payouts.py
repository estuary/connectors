from datetime import datetime, timedelta
from typing import ClassVar

from pydantic import AwareDatetime

from ..common import money_v2_fragment
from ...models import (
    ShopifyGraphQLResource,
    StoreCapabilities,
)
from ...utils import dt_to_str, str_to_dt
from .account import SHOPIFY_PAYMENTS_ACCOUNT_SCOPES, build_account_query


class Payouts(ShopifyGraphQLResource):
    NAME = "payouts"
    QUERY_ROOT_PARENT = "shopifyPaymentsAccount"
    QUERY_ROOT = "payouts"
    SHOULD_USE_BULK_QUERIES = False
    # No sort key is sent and nothing relies on the order (Shopify's default is ISSUED_AT): a
    # read holds few payouts, so the checkpoints partway through it that sorting would allow
    # gain nothing.
    SORT_KEY = None
    QUALIFYING_SCOPES = {"read_shopify_payments_payouts"}
    ADDITIONAL_REQUIRED_SCOPES = [SHOPIFY_PAYMENTS_ACCOUNT_SCOPES]
    # How long a payout's status can keep changing after its date. Shopify staff say a paid
    # payout "can take 10 business days to come back as failed"
    # (https://community.shopify.com/t/47794); 30 days is about twice that, because no
    # development store can show it.
    RECENT_PAYOUTS_WINDOW: ClassVar[timedelta] = timedelta(days=30)
    FRAGMENTS = [money_v2_fragment]
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
        return build_account_query(
            Payouts,
            f"issued_at:>='{dt_to_str(start)}' AND issued_at:<='{dt_to_str(end)}'",
            first,
            after,
        )
