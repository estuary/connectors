from datetime import datetime, timezone, timedelta
from pydantic import BaseModel, Field, AwareDatetime, AliasChoices
from typing import (
    Literal,
    Generic,
    TypeVar,
    ClassVar,
    Dict,
    List,
)

from estuary_cdk.flow import AccessToken

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    BaseDocument,
    ResourceState,
)


def default_start_date():
    dt = datetime.now(timezone.utc) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    credentials: AccessToken = Field(title="API Key")
    start_date: AwareDatetime = Field(
        description="UTC data and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date. ",
        default_factory=default_start_date,
    )
    capture_connected_accounts: bool = Field(
        description="Whether to capture data from connected accounts.",
        default=False,
    )


# We use ResourceState directly, without extending it.
ConnectorState = GenericConnectorState[ResourceState]

Item = TypeVar("Item")


class EventResult(BaseModel, Generic[Item], extra="forbid"):
    class Data(BaseModel):
        class CLSDATA(BaseModel):
            object: Item  # type: ignore
            previous_attributes: Dict | None = None

        id: str
        object: Literal["event"]
        api_version: str
        created: int
        type: str
        data: CLSDATA

    object: str
    url: str
    has_more: bool
    data: List[Data]


class ListResult(BaseModel, Generic[Item], extra="forbid"):
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
    object: str
    account_id: str | None = Field(
        default=None,
        description="The account ID associated with the resource.",
        # Don't schematize the default value.
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )


class BaseStripeObjectNoEvents(BaseStripeObject):
    """
    Stream that does not have any corresponding events
    generated at the /events endpoint.
    """

    created: int


# EVENT_TYPES is a dict where keys are specific types of events generated by
# the Stripe API when a resource is modified. The associated values are what
# the event respresents (c = created, u = updated, d = deleted).
class BaseStripeObjectWithEvents(BaseStripeObject):
    """
    Stream that has corresponding events
    generated at the /events endpoint.
    """

    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]]

    created: int


class BaseStripeChildObject(BaseStripeObject):
    """
    Child stream that can only be accessed by using an
    ID from a parent stream. EVENT_TYPES can be different
    from its parent, and the listed events may return either a parent
    document or a child document. Streams whose events return a child
    document directly are listed in SPLIT_CHILD_STREAM_NAMES.
    """

    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]]


StripeObject = TypeVar("StripeObject", bound=BaseStripeObject)
StripeObjectNoEvents = TypeVar("StripeObjectNoEvents", bound=BaseStripeObjectNoEvents)
StripeObjectWithEvents = TypeVar(
    "StripeObjectWithEvents", bound=BaseStripeObjectWithEvents
)
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


class Events(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Events"
    SEARCH_NAME: ClassVar[str] = "events"
    # EVENT_TYPES is left empty so all event types are captured.
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {}


# Could not verify Accounts events are generated in test mode, but suspect
# they are generated in Stripe's live mode.
class Accounts(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Accounts"
    SEARCH_NAME: ClassVar[str] = "accounts"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "account.updated": "u",
    }

    # Accounts docs returned in account.updated events may not have a created field.
    created: int = Field(
        default=None,
        # Don't schematize the default value.
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )


# Could not verify Persons events are generated in test mode, but suspect
# they are generated in Stripe's live mode.
class Persons(BaseStripeChildObject):
    """
    Parent Stream: Accounts
    """

    NAME: ClassVar[str] = "Persons"
    SEARCH_NAME: ClassVar[str] = "persons"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "person.created": "c",
        "person.updated": "u",
        "person.deleted": "d",
    }


# Could not verify the ExternalAccountCards event types are actually generated
# by the Stripe API.
class ExternalAccountCards(BaseStripeChildObject):
    """
    Parent Stream: Accounts
    """

    NAME: ClassVar[str] = "ExternalAccountCards"
    SEARCH_NAME: ClassVar[str] = "external_accounts"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "account.external_account.created": "c",
        "account.external_account.updated": "u",
        "account.external_account.deleted": "d",
    }


# Could not verify the ExternalBankAccounts event types are actually generated
# by the Stripe API.
class ExternalBankAccount(BaseStripeChildObject):
    """
    Parent Stream: Accounts
    """

    NAME: ClassVar[str] = "ExternalBankAccount"
    SEARCH_NAME: ClassVar[str] = "external_accounts"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "account.external_account.created": "c",
        "account.external_account.updated": "u",
        "account.external_account.deleted": "d",
    }


class ApplicationFees(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "ApplicationFees"
    SEARCH_NAME: ClassVar[str] = "application_fees"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "application_fee.created": "c",
        "application_fee.refunded": "u",
    }


class ApplicationFeesRefunds(BaseStripeChildObject):
    """
    Parent Stream: ApplicationFees
    """

    NAME: ClassVar[str] = "ApplicationFeesRefunds"
    SEARCH_NAME: ClassVar[str] = "refunds"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        # Application fees can be refunded entirely or partially, and potentially multiple times.
        # So we treat each refund as an update since we can't distinguish whether or not this is
        # the first refund.
        "application_fee.refunded": "u",
    }


# Could not verify the Authorizations event types are actually generated
# by the Stripe API.
class Authorizations(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Authorizations"
    SEARCH_NAME: ClassVar[str] = "issuing/authorizations"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "issuing_authorization.created": "c",
        "issuing_authorization.updated": "u",
    }


class Customers(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Customers"
    SEARCH_NAME: ClassVar[str] = "customers"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "customer.created": "c",
        "customer.updated": "u",
        # When a Customer is deleted in Stripe, they are still accessible in order to track their history.
        # So we treat a customer.deleted event as an update rather than a delete.
        "customer.deleted": "u",
    }


class Cards(BaseStripeChildObject):
    """
    Parent Stream: Customers

    When backfilling, Cards behaves as a child stream. But during incremental replication,
    Cards listens to distinct events that contain Card documents, meaning it isn't a child stream.
    """

    NAME: ClassVar[str] = "Cards"
    SEARCH_NAME: ClassVar[str] = "cards"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "customer.source.created": "c",
        "customer.source.updated": "u",
        "customer.source.deleted": "d",
    }


class Bank_Accounts(BaseStripeChildObject):
    """
    Parent Stream: Customers
    """

    NAME: ClassVar[str] = "BankAccounts"
    SEARCH_NAME: ClassVar[str] = "bank_accounts"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = Customers.EVENT_TYPES


class CustomerBalanceTransaction(BaseStripeChildObject):
    """
    Parent Stream: Customers
    """

    NAME: ClassVar[str] = "CustomerBalanceTransaction"
    SEARCH_NAME: ClassVar[str] = "balance_transactions"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = Customers.EVENT_TYPES


class PaymentMethods(BaseStripeChildObject):
    """
    Parent Stream: Customers

    When backfilling, PaymentMethods behaves as a child stream. But during incremental replication,
    PaymentMethods listens to distinct events that contain PaymentMethods documents, meaning it
    isn't a child stream.
    """

    NAME: ClassVar[str] = "PaymentMethods"
    SEARCH_NAME: ClassVar[str] = "payment_methods"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "payment_method.attached": "u",
        "payment_method.automatically_updated": "u",
        "payment_method.updated": "u",
        "payment_method.detached": "u",
    }


# Could not verify the Authorizations event types are actually generated
# by the Stripe API.
class CardHolders(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "CardHolders"
    SEARCH_NAME: ClassVar[str] = "issuing/cardholders"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "issuing_cardholder.created": "c",
        "issuing_cardholder.updated": "u",
    }


class Charges(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Charges"
    SEARCH_NAME: ClassVar[str] = "charges"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "charge.pending": "u",
        "charge.succeeded": "u",
        "charge.captured": "u",
        "charge.updated": "u",
        "charge.failed": "u",
        "charge.refunded": "u",
        "charge.expired": "u",
    }


class CheckoutSessions(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "CheckoutSessions"
    SEARCH_NAME: ClassVar[str] = "checkout/sessions"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "checkout.session.completed": "u",
        "checkout.session.expired": "u",
        "checkout.session.async_payment_succeeded": "u",
        "checkout.session.async_payment_failed": "u",
    }


class CheckoutSessionsLine(BaseStripeChildObject):
    """
    Parent Stream: CheckoutSessions
    """

    NAME: ClassVar[str] = "CheckoutSessionsLine"
    SEARCH_NAME: ClassVar[str] = "line_items"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = (
        CheckoutSessions.EVENT_TYPES
    )


class Coupons(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Coupons"
    SEARCH_NAME: ClassVar[str] = "coupons"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "coupon.created": "c",
        "coupon.updated": "u",
        "coupon.deleted": "d",
    }


class CreditNotes(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "CreditNotes"
    SEARCH_NAME: ClassVar[str] = "credit_notes"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "credit_note.created": "c",
        "credit_note.updated": "u",
        "credit_note.voided": "u",
    }


class CreditNotesLines(BaseStripeChildObject):
    """
    Parent Stream: CreditNotes
    """

    NAME: ClassVar[str] = "CreditNotesLines"
    SEARCH_NAME: ClassVar[str] = "lines"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = CreditNotes.EVENT_TYPES


class Disputes(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Disputes"
    SEARCH_NAME: ClassVar[str] = "disputes"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "charge.dispute.created": "c",
        "charge.dispute.updated": "u",
        "charge.dispute.closed": "u",
        # I could not confirm Stripe generates the events below.
        "charge.dispute.funds_reinstated": "u",
        "charge.dispute.funds_withdrawn": "u",
    }


class EarlyFraudWarning(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "EarlyFraudWarning"
    SEARCH_NAME: ClassVar[str] = "radar/early_fraud_warnings"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "radar.early_fraud_warning.created": "c",
        "radar.early_fraud_warning.updated": "u",
    }


class InvoiceItems(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "InvoiceItems"
    SEARCH_NAME: ClassVar[str] = "invoiceitems"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "invoiceitem.created": "c",
        "invoiceitem.deleted": "u",
        "invoiceitem.updated": "u",
    }

    created: int = Field(validation_alias=AliasChoices("date"))


class Invoices(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Invoices"
    SEARCH_NAME: ClassVar[str] = "invoices"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "invoice.created": "c",
        "invoice.updated": "u",
        "invoice.finalized": "u",
        "invoice.marked_uncollectible": "u",
        "invoice.voided": "u",
        "invoice.sent": "u",
        "invoice.finalization_failed": "u",
        "invoice.overdue": "u",
        "invoice.paid": "u",
        "invoice.deleted": "d",
        "invoice.payment_succeeded": "u",
        "invoice.payment_failed": "u",
        # I have not been able to confirm the events below
        # are actually generated by Stripe.
        "invoice.payment_action_required": "u",
        "invoice.will_be_due": "u",
    }


class InvoiceLineItems(BaseStripeChildObject):
    """
    Parent Stream: Invoices
    """

    NAME: ClassVar[str] = "InvoiceLineItems"
    SEARCH_NAME: ClassVar[str] = "lines"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "invoice.created": "c",
        "invoice.updated": "u",
    }


class PaymentIntent(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "PaymentIntents"
    SEARCH_NAME: ClassVar[str] = "payment_intents"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "payment_intent.created": "c",
        "payment_intent.updated": "u",
        "payment_intent.canceled": "u",
        "payment_intent.amount_capturable_updated": "u",
        "payment_intent.partially_funded": "u",
        "payment_intent.payment_failed": "u",
        "payment_intent.processing": "u",
        "payment_intent.requires_action": "u",
        "payment_intent.succeeded": "u",
    }


class Payouts(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Payouts"
    SEARCH_NAME: ClassVar[str] = "payouts"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "payout.created": "c",
        "payout.canceled": "u",
        "payout.failed": "u",
        "payout.paid": "u",
        "payout.reconciliation_completed": "u",
        "payout.updated": "u",
    }


class Plans(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Plans"
    SEARCH_NAME: ClassVar[str] = "plans"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "plan.created": "c",
        "plan.updated": "u",
        "plan.deleted": "d",
    }


class Products(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Products"
    SEARCH_NAME: ClassVar[str] = "products"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "product.created": "c",
        "product.updated": "u",
        "product.deleted": "d",
    }


class PromotionCode(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "PromotionCode"
    SEARCH_NAME: ClassVar[str] = "promotion_codes"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "promotion_code.created": "c",
        "promotion_code.deleted": "d",
    }


# Despite listing various refund.* event types, Stripe does not generate
# these events. So we use charge.refund.updated events instead.
class Refunds(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Refunds"
    SEARCH_NAME: ClassVar[str] = "refunds"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "refund.created": "c",
        "charge.refund.updated": "u",
        "refund.updated": "u",
    }


# Could not verify the Reviews event types are actually generated
# by the Stripe API.
class Reviews(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Reviews"
    SEARCH_NAME: ClassVar[str] = "reviews"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "review.opened": "c",
        "review.closed": "u",
    }


class SetupIntents(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "SetupIntents"
    SEARCH_NAME: ClassVar[str] = "setup_intents"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "setup_intent.created": "c",
        "setup_intent.canceled": "u",
        "setup_intent.succeeded": "u",
        "setup_intent.setup_failed": "u",
        "setup_intent.requires_action": "u",
    }


class SetupAttempts(BaseStripeChildObject):
    """
    Parent Stream: SetupIntents
    """

    NAME: ClassVar[str] = "SetupAttempts"
    SEARCH_NAME: ClassVar[str] = "setup_attempts"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "setup_intent.succeeded": "c",
        "setup_intent.setup_failed": "c",
    }


class Subscriptions(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Subscriptions"
    SEARCH_NAME: ClassVar[str] = "subscriptions"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "customer.subscription.created": "c",
        "customer.subscription.updated": "u",
        "customer.subscription.paused": "u",
        "customer.subscription.resumed": "u",
        "customer.subscription.pending_update_expired": "u",
        "customer.subscription.trial_will_end": "u",
        "customer.subscription.deleted": "d",
    }


class SubscriptionItems(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "SubscriptionItems"
    SEARCH_NAME: ClassVar[str] = "subscriptions"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "customer.subscription.created": "c",
        "customer.subscription.updated": "u",
        "customer.subscription.paused": "u",
        "customer.subscription.resumed": "u",
        "customer.subscription.pending_update_expired": "u",
        "customer.subscription.trial_will_end": "u",
        "customer.subscription.deleted": "d",
    }

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
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = (
        SubscriptionItems.EVENT_TYPES
    )

    subscription_item: str


class SubscriptionsSchedule(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "SubscriptionSchedule"
    SEARCH_NAME: ClassVar[str] = "subscription_schedules"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "subscription_schedule.created": "c",
        "subscription_schedule.updated": "u",
        "subscription_schedule.released": "u",
        "subscription_schedule.aborted": "u",
        "subscription_schedule.canceled": "u",
        "subscription_schedule.completed": "u",
        "subscription_schedule.expiring": "u",
    }


# Could not verify the TopUps event types are actually generated
# by the Stripe API.
class TopUps(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "TopUps"
    SEARCH_NAME: ClassVar[str] = "topups"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "topup.created": "c",
        "topup.canceled": "u",
        "topup.failed": "u",
        "topup.reversed": "u",
        "topup.succeeded": "u",
    }


# Could not verify the Transactions event types are actually generated
# by the Stripe API.
class Transactions(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Transactions"
    SEARCH_NAME: ClassVar[str] = "issuing/transactions"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "issuing_transaction.created": "c",
        "issuing_transaction.updated": "u",
    }


class Transfers(BaseStripeObjectWithEvents):
    NAME: ClassVar[str] = "Transfers"
    SEARCH_NAME: ClassVar[str] = "transfers"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        "transfer.created": "c",
        "transfer.updated": "u",
        "transfer.reversed": "u",
    }


class TransferReversals(BaseStripeChildObject):
    """
    Parent Stream: Transfers
    """

    NAME: ClassVar[str] = "TransferReversals"
    SEARCH_NAME: ClassVar[str] = "reversals"
    EVENT_TYPES: ClassVar[dict[str, Literal["c", "u", "d"]]] = {
        # Transfers can be reversed entirely or partially, and potentially multiple times.
        # So we treat each reversal as an update since we can't distinguish whether or not this is
        # the first reversal.
        "transfer.reversed": "u",
    }


# Streams can have 0 or more children. In most cases, child streams are accessible if their parent stream is accessible,
# and they are inaccessible if their parent is inaccessible.
# However, some child streams can be inaccessible when their parent stream is accessible. In these situations, the discoverPath
# property is set on the child stream. discoverPath must be structured correctly but should use an invalid parent id. Since the
# Stripe API checks whether the stream is accessible before checking if the parent & child resources actually exist, we can
# determine if the child stream is accessible by the API response's status code.
STREAMS = [
    {
        "stream": Accounts,
        "children": [
            {"stream": Persons},
            {"stream": ExternalAccountCards},
            {"stream": ExternalBankAccount},
        ],
    },
    {
        "stream": ApplicationFees,
        "children": [
            {"stream": ApplicationFeesRefunds},
        ],
    },
    {
        "stream": Customers,
        "children": [
            {"stream": Bank_Accounts},
            {"stream": Cards},
            {"stream": CustomerBalanceTransaction},
            {"stream": PaymentMethods},
        ],
    },
    {"stream": Charges},
    {
        "stream": CheckoutSessions,
        "children": [
            {"stream": CheckoutSessionsLine},
        ],
    },
    {"stream": Coupons},
    {
        "stream": CreditNotes,
        "children": [
            {"stream": CreditNotesLines},
        ],
    },
    {"stream": Disputes},
    {"stream": EarlyFraudWarning},
    {"stream": Events},
    {"stream": InvoiceItems},
    {
        "stream": Invoices,
        "children": [
            {"stream": InvoiceLineItems},
        ],
    },
    {"stream": PaymentIntent},
    {"stream": Payouts},
    {"stream": Plans},
    {"stream": Products},
    {"stream": PromotionCode},
    {"stream": Refunds},
    {"stream": Reviews},
    {
        "stream": SetupIntents,
        "children": [
            {"stream": SetupAttempts},
        ],
    },
    {"stream": Subscriptions},
    {"stream": SubscriptionsSchedule},
    {
        "stream": SubscriptionItems,
        "children": [
            {
                "stream": UsageRecords,
                "discoverPath": f"subscription_items/invalid_id/{UsageRecords.SEARCH_NAME}",
            },
        ],
    },
    {"stream": TopUps},
    {
        "stream": Transfers,
        "children": [
            {"stream": TransferReversals},
        ],
    },
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

# Split child streams are streams that are accessed like a child stream during
# a backfill but have corresponding events generated at the /events endpoint
# that return the modified child object directly. Meaning, they behave like a child
# stream when backfilling but like a base stream during incremental replication.
SPLIT_CHILD_STREAM_NAMES = [
    Cards.NAME,
    PaymentMethods.NAME,
    Persons.NAME,
    ExternalAccountCards.NAME,
    ExternalBankAccount.NAME,
]

# Streams that should not use the Stripe-Account header or have account_id set from the parent account
# These streams either represent accounts themselves or are directly related to accounts in a way
# that they should be accessed from the platform account context rather than a connected account
CONNECTED_ACCOUNT_EXEMPT_STREAMS = [
    "Accounts",
    "ExternalAccountCards",
    "ExternalBankAccount",
    "Persons",
]
