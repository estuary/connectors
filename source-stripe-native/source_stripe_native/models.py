from datetime import datetime, timezone, timedelta
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


def default_start_date():
    dt = datetime.now(timezone.utc) - timedelta(days=30)
    return dt

class EndpointConfig(BaseModel):
    credentials: AccessToken = Field(
        title="API Key"
    )
    start_date: AwareDatetime = Field(
        description="UTC data and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date. ",
        default_factory=default_start_date,
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


class BaseStripeObject(BaseDocument, extra="allow"):
    """
    Shared properties between all Stripe objects.
    """
    NAME: ClassVar[str]
    SEARCH_NAME: ClassVar[str]

    id: str


class BaseStripeObjectNoEvents(BaseStripeObject):
    """
    Stream that does not have any corresponding events 
    generated at the /events endpoint.
    """
    created: int


class BaseStripeObjectWithEvents(BaseStripeObject):
    """
    Stream that has corresponding events
    generated at the /events endpoint.
    """
    TYPES: ClassVar[str]

    created: int


# Note: BaseStripeChildObject is prepositioning for later when we have separate 
# TYPES class attributes for parent and child classes.
class BaseStripeChildObject(BaseStripeObject):
    """
    Child stream that can only be accessed by using an
    ID from a parent stream.
    """
    pass


StripeObject = TypeVar("StripeObject", bound=BaseStripeObject)
StripeObjectNoEvents = TypeVar("StripeObjectNoEvents", bound=BaseStripeObjectNoEvents)
StripeObjectWithEvents = TypeVar("StripeObjectWithEvents", bound=BaseStripeObjectWithEvents)
StripeChildObject = TypeVar("StripeChildObject", bound=BaseStripeChildObject)


class Files(BaseStripeObjectNoEvents):
    """
    Incremental stream with no Events
    """
    NAME: ClassVar[str] = "Files"
    SEARCH_NAME: ClassVar[str] = "files"


class FilesLink(BaseStripeObjectNoEvents):
    """
    Incremental stream with no Events
    """
    NAME: ClassVar[str] = "FilesLink"
    SEARCH_NAME: ClassVar[str] = "file_links"


class BalanceTransactions(BaseStripeObjectNoEvents):
    """
    Incremental stream with no Events
    """
    NAME: ClassVar[str] = "BalanceTransactions"
    SEARCH_NAME: ClassVar[str] = "balance_transactions"


class Accounts(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Accounts"
    TYPES: ClassVar[str] =  "accounts.updated"
    SEARCH_NAME: ClassVar[str] = "accounts"


class Persons(BaseStripeChildObject):
    """
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "Persons"
    SEARCH_NAME: ClassVar[str] = "persons"


class ExternalAccountCards(BaseStripeChildObject):
    """
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "ExternalAccountCards"
    SEARCH_NAME: ClassVar[str] = "external_accounts"


class ExternalBankAccount(BaseStripeChildObject):
    """
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "ExternalBankAccount"
    SEARCH_NAME: ClassVar[str] = "external_accounts"


class ApplicationFees(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "ApplicationFees"
    TYPES: ClassVar[str] =  "application_fee.refunded"
    SEARCH_NAME: ClassVar[str] = "application_fees"


class ApplicationFeesRefunds(BaseStripeChildObject):
    """
    Parent Stream: ApplicationFees
    """
    NAME: ClassVar[str] = "ApplicationFeesRefunds"
    SEARCH_NAME: ClassVar[str] = "refunds"


class Authorizations(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Authorizations"
    TYPES: ClassVar[str] =  "issuing_authorization.updated"
    SEARCH_NAME: ClassVar[str] = "issuing/authorizations"


class Customers(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Customers"
    TYPES: ClassVar[str] =  "customer.updated"
    SEARCH_NAME: ClassVar[str] = "customers"


class Cards(BaseStripeChildObject):
    """
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "Cards"
    SEARCH_NAME: ClassVar[str] = "cards"


class Bank_Accounts(BaseStripeChildObject):
    """
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "BankAccounts"
    SEARCH_NAME: ClassVar[str] = "bank_accounts"


class CustomerBalanceTransaction(BaseStripeChildObject):
    """
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "CustomerBalanceTransaction"
    SEARCH_NAME: ClassVar[str] = "balance_transactions"


class PaymentMethods(BaseStripeChildObject):
    """
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "PaymentMethods"
    SEARCH_NAME: ClassVar[str] = "payment_methods"


class CardHolders(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "CardHolders"
    TYPES: ClassVar[str] =  "issuing_cardholder.updated"
    SEARCH_NAME: ClassVar[str] = "issuing/cardholders"


class Charges(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Charges"
    TYPES: ClassVar[str] =  "charge.updated"
    SEARCH_NAME: ClassVar[str] = "charges"


class CheckoutSessions(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "CheckoutSessions"
    TYPES: ClassVar[str] =  "checkout.session.*"
    SEARCH_NAME: ClassVar[str] = "checkout/sessions"


class CheckoutSessionsLine(BaseStripeChildObject):
    """
    Parent Stream: CheckoutSessions
    """
    NAME: ClassVar[str] = "CheckoutSessionsLine"
    SEARCH_NAME: ClassVar[str] = "line_items"


class Coupons(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Coupons"
    TYPES: ClassVar[str] =  "coupon.updated"
    SEARCH_NAME: ClassVar[str] = "coupons"


class CreditNotes(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "CreditNotes"
    TYPES: ClassVar[str] =  "credit_note.updated"
    SEARCH_NAME: ClassVar[str] = "credit_notes"


class CreditNotesLines(BaseStripeChildObject):
    """
    Parent Stream: CreditNotes
    """
    NAME: ClassVar[str] = "CreditNotesLines"
    SEARCH_NAME: ClassVar[str] = "lines"


class Disputes(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Disputes"
    TYPES: ClassVar[str] =  "charge.dispute.updated"
    SEARCH_NAME: ClassVar[str] = "disputes"


class EarlyFraudWarning(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "EarlyFraudWarning"
    TYPES: ClassVar[str] =  "radar.early_fraud_warning.*"
    SEARCH_NAME: ClassVar[str] = "radar/early_fraud_warnings"


class InvoiceItems(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "InvoiceItems"
    TYPES: ClassVar[str] =  "invoiceitem.*"
    SEARCH_NAME: ClassVar[str] = "invoiceitems"

    created: int = Field(validation_alias=AliasChoices('date'))


class Invoices(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Invoices"
    TYPES: ClassVar[str] =  "invoice.updated"
    SEARCH_NAME: ClassVar[str] = "invoices"


class InvoiceLineItems(BaseStripeChildObject):
    """
    Parent Stream: Invoices
    """
    NAME: ClassVar[str] = "InvoiceLineItems"
    SEARCH_NAME: ClassVar[str] = "lines"


class PaymentIntent(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "PaymentIntents"
    TYPES: ClassVar[str] =  "payment_intent.*"
    SEARCH_NAME: ClassVar[str] = "payment_intents"


class Payouts(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Payouts"
    TYPES: ClassVar[str] =  "payout.updated"
    SEARCH_NAME: ClassVar[str] = "payouts"


class Plans(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Plans"
    TYPES: ClassVar[str] =  "plan.updated"
    SEARCH_NAME: ClassVar[str] = "plans"


class Products(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Products"
    TYPES: ClassVar[str] =  "product.updated"
    SEARCH_NAME: ClassVar[str] = "products"


class PromotionCode(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "PromotionCode"
    TYPES: ClassVar[str] =  "promotion_code.updated"
    SEARCH_NAME: ClassVar[str] = "promotion_codes"


class Refunds(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Refunds"
    TYPES: ClassVar[str] =  "refund.updated"
    SEARCH_NAME: ClassVar[str] = "refunds"


class Reviews(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Reviews"
    TYPES: ClassVar[str] =  "review.*"
    SEARCH_NAME: ClassVar[str] = "reviews"


class SetupIntents(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "SetupIntents"
    TYPES: ClassVar[str] =  "setup_intent.*"
    SEARCH_NAME: ClassVar[str] = "setup_intents"


class SetupAttempts(BaseStripeChildObject):
    """
    Parent Stream: SetupIntents
    """
    NAME: ClassVar[str] = "SetupAttempts"
    SEARCH_NAME: ClassVar[str] = "setup_attempts"


class Subscriptions(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Subscriptions"
    TYPES: ClassVar[str] =  "customer.subscription.*"
    SEARCH_NAME: ClassVar[str] = "subscriptions"


class SubscriptionItems(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "SubscriptionItems"
    TYPES: ClassVar[str] =  "customer.subscription.*"
    SEARCH_NAME: ClassVar[str] = "subscriptions"

    class Items(BaseModel, extra="allow"):
        class Values(BaseModel, extra="allow"):
            id: str

        data: list[Values]

    items: Items


class UsageRecords(BaseStripeChildObject):
    """
    Parent Stream: SubscriptionItems
    """
    NAME: ClassVar[str] = "UsageRecords"
    SEARCH_NAME: ClassVar[str] = "usage_record_summaries"

    subscription_item: str


class SubscriptionsSchedule(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "SubscriptionSchedule"
    TYPES: ClassVar[str] =  "subscription_schedule.updated"
    SEARCH_NAME: ClassVar[str] = "subscription_schedules"


class TopUps(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "TopUps"
    TYPES: ClassVar[str] =  "topup.*"
    SEARCH_NAME: ClassVar[str] = "topups"


class Transactions(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Transactions"
    TYPES: ClassVar[str] =  "issuing_transaction.updated"
    SEARCH_NAME: ClassVar[str] = "issuing/transactions"


class Transfers(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Transfers"
    TYPES: ClassVar[str] =  "transfer.updated"
    SEARCH_NAME: ClassVar[str] = "transfers"


class TransferReversals(BaseStripeChildObject):
    """
    Parent Stream: Transfers
    """
    NAME: ClassVar[str] = "TransferReversals"
    SEARCH_NAME: ClassVar[str] = "reversals"


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
