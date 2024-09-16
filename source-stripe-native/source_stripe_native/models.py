from datetime import datetime
from enum import StrEnum, auto
from pydantic import BaseModel, Field, AwareDatetime, model_validator, AliasChoices
from typing import Literal, Generic, TypeVar, Annotated, ClassVar, TYPE_CHECKING, Dict, List, Any
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
    links: Dict | None = Field(exclude=True)

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
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str


class ExternalAccountCards(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "ExternalAccountCards"
    SEARCH_NAME: ClassVar[str] = "external_accounts"
    PARAMETERS: ClassVar[Dict] = {"limit": 100, "object":"card"}   

    id: str


class ExternalBankAccount(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "ExternalBankAccount"
    SEARCH_NAME: ClassVar[str] = "external_accounts"
    PARAMETERS: ClassVar[Dict] = {"limit": 100, "object":"bank_account"} 

    
    id: str


class ApplicationFees(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "ApplicationFees"
    TYPES: ClassVar[str] =  "application_fee.refunded"
    SEARCH_NAME: ClassVar[str] = "application_fees"

    id: str
    refunds: Dict = Field(exclude=True)

class ApplicationFeesRefunds(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: ApplicationFees
    """
    NAME: ClassVar[str] = "ApplicationFeesRefunds"
    SEARCH_NAME: ClassVar[str] = "refunds"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None # Custom Added Field


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
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None # Custom Added Field



class Bank_Accounts(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "BankAccounts"
    SEARCH_NAME: ClassVar[str] = "bank_accounts"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    
    id: str
    parent_id: str | None = None # Custom Added Field


class CustomerBalanceTransaction(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "CustomerBalanceTransaction"
    SEARCH_NAME: ClassVar[str] = "balance_transactions"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}


    id: str
    parent_id: str | None = None # Custom Added Field


class PaymentMethods(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "PaymentMethods"
    SEARCH_NAME: ClassVar[str] = "payment_methods"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}
    
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
    client_secret: str | None = Field(exclude=True) # Sensible Field


class CheckoutSessionsLine(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: CheckoutSessions
    """

    NAME: ClassVar[str] = "CheckoutSessionsLine"
    SEARCH_NAME: ClassVar[str] = "line_items"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

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
    lines: Dict = Field(exclude=True)


class CreditNotesLines(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: CreditNotes
    """

    NAME: ClassVar[str] = "CreditNotesLines"
    SEARCH_NAME: ClassVar[str] = "lines"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

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
    discounts: List[str] | None
    created: int = Field(validation_alias=AliasChoices('date'))


class Invoices(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Invoices"
    TYPES: ClassVar[str] =  "invoice.updated"
    SEARCH_NAME: ClassVar[str] = "invoices"

    id: str
    lines: Dict = Field(exclude=True)

class InvoiceLineItems(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Invoices
    """
    NAME: ClassVar[str] = "InvoiceLineItems"
    SEARCH_NAME: ClassVar[str] = "lines"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None # Custom Added Field


class PaymentIntent(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "PaymentIntents"
    TYPES: ClassVar[str] =  "payment_intent.*"
    SEARCH_NAME: ClassVar[str] = "payment_intents"

    id: str
    client_secret: str | None = Field(exclude=True) # Sensible Field


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
    client_secret: str | None = Field(exclude=True) # Sensible Field


class SetupAttempts(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: SetupIntents
    """
    NAME: ClassVar[str] = "SetupAttempts"
    SEARCH_NAME: ClassVar[str] = "setup_attempts"
    PARAMETERS: ClassVar[Dict] = {"limit": 100, "setup_intent": None}

    id: str



class Subscriptions(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "Subscriptions"
    TYPES: ClassVar[str] =  "customer.subscription.*"
    SEARCH_NAME: ClassVar[str] = "subscriptions"

    id: str
    items: Dict = Field(exclude=True)


class SubscriptionItems(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "SubscriptionItems"
    TYPES: ClassVar[str] =  "customer.subscription.*"
    SEARCH_NAME: ClassVar[str] = "subscriptions"
    class Items(BaseModel, extra="allow"):
        class Values(BaseModel, extra="allow"):
            id: str
            parent_id: str | None = None
            object: Literal["subscription_item"]
            billing_thresholds: Dict | None = None
            created: int
            metadata: Dict | None = None
            discounts: List[str] | None = None
            plan: Dict | None = None
            price: Dict
            quantity: int | None = None
            subscription: str
            tax_rates: List[Dict] | None = None

        object: Literal["list"]
        data: list[Values]
        has_more: bool = Field(exclude=True)
        total_count: int = Field(exclude=True)
        url: str = Field(exclude=True)

    id: str
    billing_cycle_anchor: int  = Field(exclude=True)
    billing_cycle_anchor_config: Dict | None = Field(exclude=True)
    billing_thresholds: Dict | None  = Field(exclude=True)
    cancel_at: int | None  = Field(exclude=True)
    cancel_at_period_end: bool  = Field(exclude=True)
    discounts: list[str] | None = Field(exclude=True)
    discount: Dict | None = Field(exclude=True)
    items: Items



class UsageRecords(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: SubscriptionItems
    """
    NAME: ClassVar[str] = "UsageRecords"
    SEARCH_NAME: ClassVar[str] = "usage_record_summaries"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

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
    reversals: Dict = Field(exclude=True)


class TransferReversals(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Transfers
    """
    NAME: ClassVar[str] = "TransferReversals"
    SEARCH_NAME: ClassVar[str] = "reversals"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
