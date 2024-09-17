from datetime import datetime
from enum import StrEnum, auto
from pydantic import BaseModel, Field, AwareDatetime, model_validator, AliasChoices
from typing import Literal, Generic, TypeVar, Annotated, ClassVar, TYPE_CHECKING, Dict, List
import urllib.parse

from estuary_cdk.flow import AccessToken

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    AccessToken,
    BaseDocument,
    ResourceConfig,
    ResourceState,
)


class EndpointConfig(BaseModel):
    credentials: AccessToken = Field(
        title="API Key"        )
    stop_date: AwareDatetime = Field(
        description="Replication Stop Date. Records will only be considered for backfilling "\
                    "before the stop_date, similar to a start date",
        default=datetime.fromisoformat("2010-01-01T00:00:00Z".replace('Z', '+00:00'))
    )


# We use ResourceState directly, without extending it.
ConnectorState = GenericConnectorState[ResourceState]

Item = TypeVar("Item")


class EventResult(BaseModel,  Generic[Item], extra="forbid"):
    class Data(BaseModel):

        class CLSDATA(BaseModel):
            object: Item
            previous_attributes: Dict | None = None

        id: str
        object: Literal["event"]
        api_version: str
        created: int
        data: CLSDATA

    object: str
    url: str
    has_more: bool
    data: List[Data]


class ListResult(BaseModel,  Generic[Item], extra="forbid"):
    object: str
    url: str
    has_more: bool
    data: List[Item]


class BackfillResult(BaseModel, Generic[Item], extra="allow"):
    # Set extra as allow since Refunds has one aditional field
    object: str
    url: str
    has_more: bool
    data: List[Item]


class Files(BaseDocument, extra="allow"):
    """
    Incremental stream with no Events
    """
    NAME: ClassVar[str] = "Files"
    SEARCH_NAME: ClassVar[str] = "files"

    id: str


class FilesLink(BaseDocument, extra="allow"):
    """
    Incremental stream with no Events
    """
    NAME: ClassVar[str] = "FilesLink"
    SEARCH_NAME: ClassVar[str] = "file_links"

    id: str


class BalanceTransactions(BaseDocument, extra="allow"):
    """
    Incremental stream with no Events
    """
    NAME: ClassVar[str] = "BalanceTransactions"
    SEARCH_NAME: ClassVar[str] = "balance_transactions"

    id: str


class Accounts(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Accounts"
    TYPES: ClassVar[str] =  "accounts.updated"
    SEARCH_NAME: ClassVar[str] = "accounts"

    id: str

class Persons(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "Persons"
    SEARCH_NAME: ClassVar[str] = "persons"

    id: str


class ExternalAccountCards(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "ExternalAccountCards"
    SEARCH_NAME: ClassVar[str] = "external_accounts"

    id: str


class ExternalBankAccount(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "ExternalBankAccount"
    SEARCH_NAME: ClassVar[str] = "external_accounts"

    id: str


class ApplicationFees(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "ApplicationFees"
    TYPES: ClassVar[str] =  "application_fee.refunded"
    SEARCH_NAME: ClassVar[str] = "application_fees"

    id: str


class ApplicationFeesRefunds(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: ApplicationFees
    """
    NAME: ClassVar[str] = "ApplicationFeesRefunds"
    SEARCH_NAME: ClassVar[str] = "refunds"

    id: str


class Authorizations(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Authorizations"
    TYPES: ClassVar[str] =  "issuing_authorization.updated"
    SEARCH_NAME: ClassVar[str] = "issuing/authorizations"

    id: str


class Customers(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Customers"
    TYPES: ClassVar[str] =  "customer.updated"
    SEARCH_NAME: ClassVar[str] = "customers"

    id: str


class Cards(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "Cards"
    SEARCH_NAME: ClassVar[str] = "cards"

    id: str


class Bank_Accounts(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "BankAccounts"
    SEARCH_NAME: ClassVar[str] = "bank_accounts"

    id: str


class CustomerBalanceTransaction(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "CustomerBalanceTransaction"
    SEARCH_NAME: ClassVar[str] = "balance_transactions"

    id: str


class PaymentMethods(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "PaymentMethods"
    SEARCH_NAME: ClassVar[str] = "payment_methods"

    id: str


class CardHolders(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "CardHolders"
    TYPES: ClassVar[str] =  "issuing_cardholder.updated"
    SEARCH_NAME: ClassVar[str] = "issuing/cardholders"

    id: str


class Charges(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Charges"
    TYPES: ClassVar[str] =  "charge.updated"
    SEARCH_NAME: ClassVar[str] = "charges"

    id: str


class CheckoutSessions(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "CheckoutSessions"
    TYPES: ClassVar[str] =  "checkout.session.*"
    SEARCH_NAME: ClassVar[str] = "checkout/sessions"

    id: str


class CheckoutSessionsLine(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: CheckoutSessions
    """
    NAME: ClassVar[str] = "CheckoutSessionsLine"
    SEARCH_NAME: ClassVar[str] = "line_items"

    id: str


class Coupons(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Coupons"
    TYPES: ClassVar[str] =  "coupon.updated"
    SEARCH_NAME: ClassVar[str] = "coupons"

    id: str


class CreditNotes(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "CreditNotes"
    TYPES: ClassVar[str] =  "credit_note.updated"
    SEARCH_NAME: ClassVar[str] = "credit_notes"

    id: str


class CreditNotesLines(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: CreditNotes
    """
    NAME: ClassVar[str] = "CreditNotesLines"
    SEARCH_NAME: ClassVar[str] = "lines"

    id: str


class Disputes(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Disputes"
    TYPES: ClassVar[str] =  "charge.dispute.updated"
    SEARCH_NAME: ClassVar[str] = "disputes"

    id: str


class EarlyFraudWarning(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "EarlyFraudWarning"
    TYPES: ClassVar[str] =  "radar.early_fraud_warning.*"
    SEARCH_NAME: ClassVar[str] = "radar/early_fraud_warnings"

    id: str


class InvoiceItems(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "InvoiceItems"
    TYPES: ClassVar[str] =  "invoiceitem.*"
    SEARCH_NAME: ClassVar[str] = "invoiceitems"
    
    id: str
    created: int = Field(validation_alias=AliasChoices('date'))


class Invoices(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Invoices"
    TYPES: ClassVar[str] =  "invoice.updated"
    SEARCH_NAME: ClassVar[str] = "invoices"

    id: str


class InvoiceLineItems(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Invoices
    """
    NAME: ClassVar[str] = "InvoiceLineItems"
    SEARCH_NAME: ClassVar[str] = "lines"

    id: str


class PaymentIntent(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "PaymentIntents"
    TYPES: ClassVar[str] =  "payment_intent.*"
    SEARCH_NAME: ClassVar[str] = "payment_intents"

    id: str


class Payouts(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Payouts"
    TYPES: ClassVar[str] =  "payout.updated"
    SEARCH_NAME: ClassVar[str] = "payouts"

    id: str


class Plans(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Plans"
    TYPES: ClassVar[str] =  "plan.updated"
    SEARCH_NAME: ClassVar[str] = "plans"

    id: str

class Products(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Products"
    TYPES: ClassVar[str] =  "product.updated"
    SEARCH_NAME: ClassVar[str] = "products"

    id: str


class PromotionCode(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "PromotionCode"
    TYPES: ClassVar[str] =  "promotion_code.updated"
    SEARCH_NAME: ClassVar[str] = "promotion_codes"

    id: str


class Refunds(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Refunds"
    TYPES: ClassVar[str] =  "refund.updated"
    SEARCH_NAME: ClassVar[str] = "refunds"

    id: str


class Reviews(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Reviews"
    TYPES: ClassVar[str] =  "review.*"
    SEARCH_NAME: ClassVar[str] = "reviews"

    id: str


class SetupIntents(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "SetupIntents"
    TYPES: ClassVar[str] =  "setup_intent.*"
    SEARCH_NAME: ClassVar[str] = "setup_intents"

    id: str


class SetupAttempts(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: SetupIntents
    """
    NAME: ClassVar[str] = "SetupAttempts"
    SEARCH_NAME: ClassVar[str] = "setup_attempts"

    id: str


class Subscriptions(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Subscriptions"
    TYPES: ClassVar[str] =  "customer.subscription.*"
    SEARCH_NAME: ClassVar[str] = "subscriptions"

    id: str


class SubscriptionItems(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "SubscriptionItems"
    TYPES: ClassVar[str] =  "customer.subscription.*"
    SEARCH_NAME: ClassVar[str] = "subscriptions"

    class Items(BaseModel, extra="allow"):
        class Values(BaseModel, extra="allow"):
            id: str

        data: list[Values]

    id: str
    items: Items


class UsageRecords(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: SubscriptionItems
    """
    NAME: ClassVar[str] = "UsageRecords"
    SEARCH_NAME: ClassVar[str] = "usage_record_summaries"

    id: str
    subscription_item: str


class SubscriptionsSchedule(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "SubscriptionSchedule"
    TYPES: ClassVar[str] =  "subscription_schedule.updated"
    SEARCH_NAME: ClassVar[str] = "subscription_schedules"

    id: str


class TopUps(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "TopUps"
    TYPES: ClassVar[str] =  "topup.*"
    SEARCH_NAME: ClassVar[str] = "topups"

    id: str


class Transactions(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Transactions"
    TYPES: ClassVar[str] =  "issuing_transaction.updated"
    SEARCH_NAME: ClassVar[str] = "issuing/transactions"

    id: str


class Transfers(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Transfers"
    TYPES: ClassVar[str] =  "transfer.updated"
    SEARCH_NAME: ClassVar[str] = "transfers"

    id: str


class TransferReversals(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Transfers
    """
    NAME: ClassVar[str] = "TransferReversals"
    SEARCH_NAME: ClassVar[str] = "reversals"

    id: str

# Streams can have 0 or more children. In most cases, child streams are accessible if their parent stream is accessible,
# and they are inaccessible if their parent is inaccessible. 
# However, some child streams can be inaccessible when their parent stream is accessible. In these situations, the discoverPath
# property is set on the child stream. discoverPath must be structured correctly but should use an invalid parent id. Since the
# Stripe API checks whether the stream is accessible before checking if the parent & child resources actually exist, we can
# determine if the child stream is accessible by the API response's status code.
STREAMS = [
    {"stream": Accounts, "children": [
            {"stream": Persons},
            {"stream": ExternalAccountCards},
            {"stream": ExternalBankAccount},
        ]
    },
    {"stream": ApplicationFees, "children": [
            {"stream": ApplicationFeesRefunds},
        ]
    },
    {"stream": Customers, "children": [
            {"stream": Bank_Accounts},
            {"stream": Cards},
            {"stream": CustomerBalanceTransaction},
            {"stream": PaymentMethods},
        ]
    },
    {"stream": Charges},
    {"stream": CheckoutSessions, "children": [
            {"stream": CheckoutSessionsLine},
        ]
    },
    {"stream": Coupons},
    {"stream": CreditNotes, "children": [
            {"stream": CreditNotesLines},
        ]
    },
    {"stream": Disputes},
    {"stream": EarlyFraudWarning},
    {"stream": InvoiceItems},
    {"stream": Invoices, "children": [
            {"stream": InvoiceLineItems},
        ]
    },
    {"stream": PaymentIntent},
    {"stream": Payouts},
    {"stream": Plans},
    {"stream": Products},
    {"stream": PromotionCode},
    {"stream": Refunds},
    {"stream": Reviews},
    {"stream": SetupIntents, "children": [
            {"stream": SetupAttempts},
        ]
    },
    {"stream": Subscriptions},
    {"stream": SubscriptionsSchedule},
    {"stream": SubscriptionItems, "children": [
            {"stream": UsageRecords, "discoverPath": f"subscription_items/invalid_id/{UsageRecords.SEARCH_NAME}"},
        ]
    }, 
    {"stream": TopUps},
    {"stream": Transfers, "children": [
        {"stream": TransferReversals},
    ]},
    {"stream": Files},
    {"stream": FilesLink},
    {"stream": BalanceTransactions},
]

# Regional streams are streams that don't have any children and are only accessible in certain regions.
# This means we always have to check if these streams are accessible during discovery.
REGIONAL_STREAMS = [
    Authorizations,
    CardHolders,
    Transactions,
]
