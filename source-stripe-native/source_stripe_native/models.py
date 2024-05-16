from datetime import datetime
from enum import StrEnum, auto
from pydantic import BaseModel, Field, AwareDatetime, model_validator, AliasChoices
from typing import Literal, Generic, TypeVar, Annotated, ClassVar, TYPE_CHECKING, Dict, List
import urllib.parse

from estuary_cdk.flow import BasicAuth

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    AccessToken,
    BaseDocument,
    ResourceConfig,
    ResourceState,
)


class EndpointConfig(BaseModel):
    credentials: BasicAuth


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

class Files(BaseDocument, extra="forbid"):
    """
    Incremental stream with no Events
    """
    NAME: ClassVar[str] = "Files"
    SEARCH_NAME: ClassVar[str] = "files"

    id: str
    object: Literal["file"]
    created: int
    expires_at: int | None = None 
    filename: str | None = None
    links: Dict | None = Field(exclude=True)
    purpose: str | None = None
    size: int
    title: str | None = None
    type: str | None = None
    url: str | None = None

class FilesLink(BaseDocument, extra="forbid"):
    """
    Incremental stream with no Events
    """
    NAME: ClassVar[str] = "FilesLink"
    SEARCH_NAME: ClassVar[str] = "file_links"

    id: str
    object: Literal["file_link"]
    created: int
    expired: bool
    expires_at: int | None = None
    file: str
    livemode: bool
    metadata: Dict | None = None
    url: str | None = None

class BalanceTransactions(BaseDocument, extra="forbid"):
    """
    Incremental stream with no Events
    """
    NAME: ClassVar[str] = "BalanceTransactions"
    SEARCH_NAME: ClassVar[str] = "balance_transactions"

    id: str
    object: Literal["balance_transaction"]
    amount: int
    available_on: int
    created: int
    currency: str | None = None
    description: str | None = None
    exchange_rate: float | None = None
    fee: int
    fee_details: list[Dict]
    net: int
    reporting_category: str
    source: str | None = None
    status: str
    type: str | None = None

class Accounts(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Accounts"
    TYPES: ClassVar[str] =  "accounts.updated"
    SEARCH_NAME: ClassVar[str] = "accounts"

    id: str
    object: Literal["account"]
    business_profile: Dict | None = None
    business_type: str | None = None
    capabilities: Dict | None = None
    charges_enabled: bool
    controller: Dict | None = None
    country: str
    company: Dict | None = None
    created: int
    default_currency: str
    details_submitted: bool
    email: str | None = None
    external_accounts: Dict
    future_requirements: Dict | None = None
    metadata: Dict | None = None
    payouts_enabled: bool
    requirements: Dict | None = None
    settings: Dict | None = None
    tos_acceptance: Dict
    login_links: Dict | None = None
    type: str

class Persons(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "Persons"
    SEARCH_NAME: ClassVar[str] = "persons"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["person"]
    account: str
    created: int
    dob: Dict | None = None
    first_name: str | None = None
    future_requirements: Dict | None = None
    id_number_provided: bool
    last_name: str | None = None
    metadata: Dict | None = None
    relationship: Dict
    requirements: Dict | None = None
    ssn_last_4_provided: bool
    verification: Dict

class ExternalAccountCards(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "External_Account_Cards"
    SEARCH_NAME: ClassVar[str] = "external_accounts"
    PARAMETERS: ClassVar[Dict] = {"limit": 100, "object":"card"}   

    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["card"]
    address_city: str | None = None
    address_country: str | None = None
    address_line1: str | None = None
    address_line1_check: str | None = None
    address_line2: str | None = None
    address_state: str | None = None
    address_zip: str | None = None
    address_zip_check: str | None = None
    brand: str | None = None
    country: str | None = None
    customer: Dict | None = None
    cvc_check: str | None = None
    dynamic_last4: str | None = None
    exp_month: int
    exp_year: int
    fingerprint: str | None = None
    funding: str
    last4: str
    metadata: Dict | None = None
    name: str | None = None
    tokenization_method: str | None = None
    wallet: Dict | None = None

class ExternalBankAccount(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "External_Bank_Account"
    SEARCH_NAME: ClassVar[str] = "external_accounts"
    PARAMETERS: ClassVar[Dict] = {"limit": 100, "object":"bank_account"} 

    
    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["bank_account"]
    account_holder_name: str | None = None
    account_holder_type: str | None = None
    account_type: str | None = None
    available_payout_methods: List[str] | None = None
    bank_name: str | None = None
    country: str 
    currency: str
    customer: Dict | None = None
    fingerprint: str | None = None
    last4: str
    metadata: Dict | None = None
    routing_number: str | None = None
    status: str

class ApplicationFees(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Application_Fees"
    TYPES: ClassVar[str] =  "application_fee.refunded"
    SEARCH_NAME: ClassVar[str] = "application_fees"

    id: str
    object: Literal["application_fee"]
    account: str
    amount: int
    amount_refunded: int
    application: str
    balance_transaction: str | None = None
    charge: str
    created: int
    currency: str
    livemode: bool
    originating_transaction: str | None = None
    refunded: bool
    refunds: Dict = Field(exclude=True)

class ApplicationFeesRefunds(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: ApplicationFees
    """
    NAME: ClassVar[str] = "Application_Fees_Refunds"
    SEARCH_NAME: ClassVar[str] = "refunds"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["fee_refund"]
    amount: int
    balance_transaction: str | None = None
    created: int
    currency: str
    fee: str
    metadata: Dict | None = None

class Authorizations(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Authorizations"
    TYPES: ClassVar[str] =  "issuing_authorization.updated"
    SEARCH_NAME: ClassVar[str] = "issuing/authorizations"

    id: str
    object: Literal["issuing.authorization"]
    amount: int
    amount_details: Dict | None = None
    approved: bool
    authorization_method: str
    balance_transactions: list[Dict]
    card: Dict
    cardholder: str | None = None
    created: int
    currency: str
    livemode: bool
    merchant_amount: int
    merchant_currency: str
    merchant_data: Dict
    metadata: Dict | None = None
    network_data: Dict | None = None
    pending_request: Dict | None = None
    redaction: str | None = None
    request_history: list[Dict]
    status: str
    transactions: list[Dict]
    verification_data: Dict
    wallet: str | None = None

class Customers(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Customers"
    TYPES: ClassVar[str] =  "customer.updated"
    SEARCH_NAME: ClassVar[str] = "customers"

    id: str
    object: Literal["customer"]
    address: Dict | None = None
    balance: int
    created: int
    currency: str | None = None 
    default_source: str | None = None
    default_currency: str | None = None
    delinquent: bool | None = None
    description: str | None = None
    discount: Dict | None = None
    email: str | None = None
    invoice_prefix: str | None = None
    invoice_settings: Dict
    livemode: bool
    metadata: Dict
    name: str | None = None
    next_invoice_sequence: int | None = None
    phone: str | None = None
    preferred_locales: List[str]
    shipping: Dict | None = None
    tax_exempt: str | None = None
    test_clock: str | None = None

class Cards(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "Cards"
    SEARCH_NAME: ClassVar[str] = "cards"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["card"]
    address_city: str | None = None
    address_country: str | None = None
    address_line1: str | None = None
    address_line1_check: str | None = None
    address_line2: str | None = None
    address_state: str | None = None
    address_zip: str | None = None
    address_zip_check: str | None = None
    brand: str | None = None
    country: str | None = None
    customer: str | None = None
    cvc_check: str | None = None
    dynamic_last4: str | None = None
    exp_month: int
    exp_year: int
    fingerprint: str | None = None
    funding: str
    last4: str
    metadata: Dict | None = None
    name: str | None = None
    tokenization_method: str | None = None
    wallet: Dict | None = None


class Bank_Accounts(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "Bank_Accounts"
    SEARCH_NAME: ClassVar[str] = "bank_accounts"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    
    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["bank_account"]
    account_holder_name: str | None = None
    account_holder_type: str | None = None
    account_type: str | None = None
    bank_name: str | None = None
    country: str 
    currency: str
    customer: str
    fingerprint: str | None = None
    last4: str
    metadata: Dict | None = None
    routing_number: str | None = None
    status: str

class CustomerBalanceTransaction(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "Customer_Balance_Transaction"
    SEARCH_NAME: ClassVar[str] = "balance_transactions"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}


    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["customer_balance_transaction"]
    amount: int
    available_on: int | None = None
    created: int
    customer: str
    ending_balance: int
    currency: str | None = None
    description: str | None = None
    exchange_rate: float | None = None
    fee: int | None = None
    fee_details: list[Dict] | None = None
    net: int | None = None
    reporting_category: str | None = None
    source: str | None = None
    status: str | None = None
    invoice: str | None = None
    livemode: bool
    metadata: Dict | None = None
    credit_note: str | None = None
    type: str

class PaymentMethods(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: Customers
    """
    NAME: ClassVar[str] = "Payment_Methods"
    SEARCH_NAME: ClassVar[str] = "payment_methods"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}
    
    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["payment_method"]
    billing_details: Dict
    card: Dict | None = None
    created: int
    customer: str | None = None
    link: Dict | None = None
    livemode: bool
    allow_redisplay: str | None = None
    metadata: Dict | None = None
    type: str | None = None

class CardHolders(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "CardHolders"
    TYPES: ClassVar[str] =  "issuing_cardholder.updated"
    SEARCH_NAME: ClassVar[str] = "issuing/cardholders"

    id: str
    object: Literal["issuing.cardholder"]
    billing: Dict
    company: Dict | None = None
    created: int
    email: str | None = None
    individual: Dict | None = None
    livemode: bool
    metadata: Dict | None = None
    name: str
    phone_number: str | None = None
    redaction: str | None = None
    requirements: Dict
    spending_controls: Dict
    status: str
    type: str

class Charges(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Charges"
    TYPES: ClassVar[str] =  "charge.updated"
    SEARCH_NAME: ClassVar[str] = "charges"

    id: str
    object: Literal["charge"]
    amount: int
    amount_captured: int
    amount_refunded: int
    application: str | None = None
    application_fee: str | None = None
    application_fee_amount: str | None = None
    balance_transaction: str | None = None
    billing_details: Dict
    calculated_statement_descriptor: str | None = None
    captured: bool
    created: int
    currency: str
    customer: str | None = None
    refunds: Dict | None = None
    description: str | None = None
    disputed: bool
    destination: str | None = None
    dispute: str | None = None
    order: str | None = None
    radar_options: Dict | None = None
    source: Dict | None = None
    failure_balance_transaction: str | None = None
    failure_code: str | None = None
    failure_message: str | None = None
    fraud_details: Dict | None = None
    invoice: str | None = None
    livemode: bool
    metadata: Dict | None = None
    on_behalf_of: str | None = None
    outcome: Dict
    paid: bool
    payment_intent: str | None = None
    payment_method: str | None = None
    payment_method_details: Dict
    receipt_email: str | None = None
    receipt_number: str | None = None
    receipt_url: str | None = None
    refunded: bool
    review: str | None = None
    shipping: Dict | None = None 
    source_transfer: str | None = None
    statement_descriptor: str | None = None
    statement_descriptor_suffix: str | None = None
    status: str
    transfer_data: Dict | None = None
    transfer_group: str | None = None

class CheckoutSessions(BaseDocument, extra="forbid"):

    NAME: ClassVar[str] = "CheckoutSessions"
    TYPES: ClassVar[str] =  "checkout.session.*"
    SEARCH_NAME: ClassVar[str] = "checkout/sessions"

    id: str
    object: Literal["checkout.session"]
    after_expiration: Dict | None = None
    allow_promotion_codes: bool | None = None
    amount_subtotal: int
    amount_total: int
    automatic_tax: Dict
    billing_address_collection: str | None = None
    cancel_url: str | None = None
    client_reference_id: str | None = None
    consent: Dict | None = None
    consent_collection: Dict | None = None
    created: int
    currency: str | None = None
    custom_fields: list[Dict]
    custom_text: Dict
    customer: str | None = None
    customer_creation: str | None = None
    customer_details: Dict | None = None
    customer_email: str | None = None
    expires_at: int
    invoice: str | None = None
    invoice_creation: Dict | None = None
    client_secret: str | None = Field(exclude=True) # Sensible Field
    currency_conversion: Dict | None = None
    payment_method_configuration_details: Dict | None = None
    saved_payment_method_options: Dict | None = None
    ui_mode: str | None = None
    livemode: bool
    locale: str | None = None
    metadata: Dict | None = None
    mode: str | None = None
    payment_intent: str | None = None
    payment_link: str | None = None
    payment_method_collection: str | None = None
    payment_method_options: Dict | None = None
    payment_method_types: list[str]
    payment_status: str
    phone_number_collection: Dict | None = None
    recovered_from: str | None = None
    setup_intent: str | None = None
    shipping: str | None = None
    shipping_rate: int | None = None
    shipping_address_collection: Dict | None = None
    shipping_cost: Dict | None = None
    shipping_details: Dict | None = None
    shipping_options: list[Dict]
    status: str | None = None
    submit_type: str | None = None
    subscription: str | None = None
    success_url: str | None = None
    total_details: Dict | None = None
    url: str | None = None

class CheckoutSessionsLine(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: CheckoutSessions
    """

    NAME: ClassVar[str] = "CheckoutSessionsLine"
    SEARCH_NAME: ClassVar[str] = "line_items"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None
    object: Literal["item"]
    amount_discount: int
    amount_subtotal: int
    amount_tax: int
    amount_total: int
    currency: str | None = None
    description: str | None = None
    price: Dict
    quantity: int | None = None

class Coupons(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Coupons"
    TYPES: ClassVar[str] =  "coupon.updated"
    SEARCH_NAME: ClassVar[str] = "coupons"


    id: str
    object: Literal["coupon"]
    amount_off: int | None = None
    created: int
    currency: str | None = None
    duration: str
    duration_in_months: int | None = None
    livemode: bool
    max_redemptions: int | None = None
    metadata: Dict | None = None
    name: str | None = None
    percent_off: float | None = None
    redeem_by: int | None = None
    times_redeemed: int
    valid: bool

class CreditNotes(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "CreditNotes"
    TYPES: ClassVar[str] =  "credit_note.updated"
    SEARCH_NAME: ClassVar[str] = "credit_notes"

    id: str
    object: Literal["credit_note"]
    amount: int
    amount_shipping: int
    created: int
    currency: str | None = None
    customer: str
    customer_balance_transaction: str | None = None
    discount_amount: int | None = None
    effective_at: int | None = None
    discount_amounts: list[Dict]
    invoice: str
    lines: Dict = Field(exclude=True)
    livemode: bool
    memo: str | None = None
    metadata: Dict | None = None
    number: str
    out_of_band_amount: int | None = None
    pdf: str
    reason: str | None = None
    refund: str | None = None
    shipping_cost: Dict | None = None
    status: str
    subtotal: int
    subtotal_excluding_tax: int | None = None
    tax_amounts: list[Dict]
    total: int
    total_excluding_tax: int | None = None
    type: str
    voided_at: int | None = None

class CreditNotesLines(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: CreditNotes
    """

    NAME: ClassVar[str] = "CreditNotesLines"
    SEARCH_NAME: ClassVar[str] = "lines"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["credit_note_line_item"]
    amount: int
    amount_excluding_tax: int | None = None
    description: str | None = None
    discount_amounts: list[Dict] | None = None
    discount_amount: int | None = None
    invoice_line_item: str | None = None
    livemode: bool
    quantity: int | None = None
    tax_amounts: list[Dict]
    tax_rates: list[Dict]
    type: str | None = None
    unit_amount: int | None = None
    unit_amount_decimal: str | None = None
    unit_amount_excluding_tax: str | None = None

class Disputes(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Disputes"
    TYPES: ClassVar[str] =  "charge.dispute.updated"
    SEARCH_NAME: ClassVar[str] = "disputes"


    id: str
    object: Literal["dispute"]
    amount: int
    balance_transactions: list[Dict] | None = None
    balance_transaction: str | None = None
    payment_method_details: Dict | None = None
    network_details: Dict | None = None
    charge: str
    count: int | None = None
    created: int
    currency: str | None = None
    evidence: Dict
    evidence_details: Dict
    is_charge_refundable: bool
    livemode: bool
    metadata: Dict | None = None
    payment_intent: str | None = None
    reason: str
    status: str

class EarlyFraudWarning(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Early_Fraud_Warning"
    TYPES: ClassVar[str] =  "radar.early_fraud_warning.*"
    SEARCH_NAME: ClassVar[str] = "radar/early_fraud_warnings"


    id: str
    object: Literal["radar.early_fraud_warning"]
    actionable: bool
    charge: str
    created: int
    fraud_type: str
    payment_intent: str | None = None
    livemode: bool

class InvoiceItems(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "InvoiceItems"
    TYPES: ClassVar[str] =  "invoiceitem.*"
    SEARCH_NAME: ClassVar[str] = "invoiceitems"
    
    id: str
    object: Literal["invoiceitem"]
    amount: int
    currency: str | None = None
    customer: str
    created: int = Field(validation_alias=AliasChoices('date'))
    description: str | None = None
    discountable: bool
    discounts: list[Dict] | None = None
    invoice: str | None = None
    livemode: bool
    metadata: Dict | None = None
    period: Dict
    plan: Dict | None = None
    price: Dict
    proration: bool
    quantity: int
    subscription: str | None = None
    subscription_item: str | None = None
    tax_rates: list[Dict] | None = None
    test_clock: str | None = None
    unit_amount: int | None = None
    unit_amount_decimal: str | None = None

class Invoices(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Invoices"
    TYPES: ClassVar[str] =  "invoice.updated"
    SEARCH_NAME: ClassVar[str] = "invoices"

    id: str
    object: Literal["invoice"]
    account_country: str | None = None
    account_name: str | None = None
    account_tax_ids: list[str] | None = None
    amount_due: int
    amount_paid: int
    amount_remaining: int
    amount_shipping: int
    application: str | None = None
    application_fee_amount: int | None = None
    attempt_count: int
    attempted: bool
    auto_advance: bool
    automatic_tax: Dict
    billing_reason: str | None = None
    charge: str | None = None
    collection_method: str | None = None
    created: int
    currency: str | None = None
    custom_fields: list[Dict] | None = None
    customer: str | None = None
    customer_address: Dict | None = None
    customer_email: str | None = None
    customer_name: str | None = None
    customer_phone: str | None = None
    customer_shipping: Dict | None = None
    customer_tax_exempt: str | None = None
    customer_tax_ids: list[Dict] | None = None
    default_payment_method: str | None = None
    default_source: str | None = None
    default_tax_rates: list[Dict]
    description: str | None = None
    discount: Dict | None = None
    discounts: list[str] | None = None
    due_date: int | None = None
    ending_balance: int | None = None
    effective_at: int | None = None
    rendering: Dict | None = None
    footer: str | None = None
    from_invoice: Dict | None = None
    hosted_invoice_url: str | None = None
    invoice_pdf: str | None = None
    issuer: Dict | None = None
    last_finalization_error: Dict | None = None
    latest_revision: str | None = None
    lines: Dict = Field(exclude=True)
    livemode: bool
    metadata: Dict | None = None
    next_payment_attempt: int | None = None
    number: str | None = None
    on_behalf_of: str | None = None
    paid: bool
    paid_out_of_band: bool
    payment_intent: str | None = None
    payment_settings: Dict
    period_end: int 
    period_start: int 
    post_payment_credit_notes_amount: int
    pre_payment_credit_notes_amount: int
    quote: str | None = None
    receipt_number: str | None = None
    rendering_options: str | None = None
    shipping_cost: Dict | None = None
    shipping_details: Dict | None = None
    starting_balance: int | None = None
    statement_descriptor: str | None = None
    status: str | None = None
    status_transitions: Dict
    subscription: str | None = None
    subscription_details: Dict | None = None
    subtotal: int
    subtotal_excluding_tax: int | None = None
    tax: int | None = None
    test_clock: str | None = None
    total: int
    total_discount_amounts: list[Dict] | None = None
    total_excluding_tax: int | None = None
    total_tax_amounts: list[Dict]
    transfer_data: Dict | None = None
    webhooks_delivered_at: int | None = None

class InvoiceLineItems(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: Invoices
    """
    NAME: ClassVar[str] = "InvoiceLineItems"
    SEARCH_NAME: ClassVar[str] = "lines"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["line_item"]
    amount: int
    amount_excluding_tax: int | None = None
    currency: str | None = None
    description: str | None = None
    discount_amounts: list[Dict] | None = None
    discountable: bool
    discounts: list[Dict] | None = None
    invoice_item: str | None = None
    livemode: bool
    metadata: Dict | None = None
    period: Dict
    price: Dict
    invoice: str | None = None
    proration: bool
    plan: Dict | None = None
    subscription_item: str | None = None
    proration_details: Dict | None = None
    quantity: int | None = None
    subscription: str | None = None
    tax_amounts: list[Dict]
    tax_rates: list[Dict]
    type: str | None = None
    unit_amount_excluding_tax: str | None = None

class PaymentIntent(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "PaymentIntents"
    TYPES: ClassVar[str] =  "payment_intent.*"
    SEARCH_NAME: ClassVar[str] = "payment_intents"

    id: str
    object: Literal["payment_intent"]
    amount: int | None = None
    amount_capturable: int
    amount_details: Dict | None = None
    amount_received: int
    application: str | None = None
    application_fee_amount: int | None = None
    automatic_payment_methods: Dict | None = None
    canceled_at: int | None = None
    cancellation_reason: str | None = None
    capture_method: str
    client_secret: str | None = Field(exclude=True) # Sensible Field
    charges: Dict | None = None
    confirmation_method: str | None = None
    created: int
    currency: str
    customer: str | None = None
    description: str | None = None
    invoice: str | None = None
    last_payment_error: Dict | None = None
    latest_charge: str | None = None
    livemode: bool
    metadata: Dict | None = None
    next_action: Dict | None = None
    on_behalf_of: str | None = None
    payment_method: str | None = None
    payment_method_options: Dict | None = None
    payment_method_configuration_details: Dict | None = None
    payment_method_types: list[str] | None = None
    processing: Dict | None = None
    receipt_email: str | None = None
    review: str | None = None
    setup_future_usage: str | None = None
    shipping: Dict | None = None
    source: str | None = None
    statement_descriptor: str | None = None
    statement_descriptor_suffix: str | None = None
    status: str | None = None
    transfer_data: Dict | None = None
    transfer_group: str | None = None

class Payouts(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Payouts"
    TYPES: ClassVar[str] =  "payout.updated"
    SEARCH_NAME: ClassVar[str] = "payouts"

    id: str
    object: Literal["payout"]
    amount: int
    application_fee: str | None = None 
    application_fee_amount: int | None = None
    arrival_date: int
    automatic: bool
    balance_transaction: str | None = None
    created: int
    currency: str | None = None
    description: str | None = None
    destination: str | None = None
    failure_balance_transaction: str | None = None
    failure_code: str | None = None
    failure_message: str | None = None
    livemode: bool
    metadata: Dict | None = None
    method: str
    original_payout: str | None = None
    reconciliation_status: str
    reversed_by: str | None = None
    source_type: str
    statement_descriptor: str | None = None
    status: str
    type: str | None = None

class Plans(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Plans"
    TYPES: ClassVar[str] =  "plan.updated"
    SEARCH_NAME: ClassVar[str] = "plans"

    id: str
    object: Literal["plan"]
    active: bool
    aggregate_usage: str | None = None
    amount: int | None = None
    amount_decimal: str | None = None
    billing_scheme: str | None = None
    meter: str | None = None
    created: int
    currency: str | None = None
    interval: str
    interval_count: int | None = None
    livemode: bool
    metadata: Dict | None = None
    nickname: str | None = None
    product: str | None = None
    tiers_mode: str | None = None
    transform_usage: Dict | None = None
    trial_period_days: int | None = None
    usage_type: str

class Products(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Products"
    TYPES: ClassVar[str] =  "product.updated"
    SEARCH_NAME: ClassVar[str] = "products"

    id: str
    object: Literal["product"]
    active: bool
    attributes: list[str] | None = None
    marketing_features: list[str] | None = None
    type: str
    created: int
    default_price: str | None = None 
    description: str | None = None
    images: list[str]
    features: list
    livemode: bool
    metadata: Dict | None = None
    name: str
    package_dimensions: Dict | None = None
    shippable: bool | None = None
    statement_descriptor: str | None = None
    tax_code: str | None = None
    unit_label: str | None = None
    updated: int
    caption: str | None = None
    deactivate_on: list[str] | None = None
    url: str | None = None

class PromotionCode(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "PromotionCode"
    TYPES: ClassVar[str] =  "promotion_code.updated"
    SEARCH_NAME: ClassVar[str] = "promotion_codes"

    id: str
    object: Literal["promotion_code"]
    active: bool
    code: str
    coupon: Dict
    created: int
    customer: str | None = None
    expires_at: int | None = None
    livemode: bool
    max_redemptions: int | None = None
    metadata: Dict | None = None
    restrictions: Dict
    times_redeemed: int

class Refunds(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Refunds"
    TYPES: ClassVar[str] =  "refund.updated"
    SEARCH_NAME: ClassVar[str] = "refunds"

    id: str
    object: Literal["refund"]
    amount: int
    balance_transaction: str | None = None
    charge: str | None = None
    count: int | None = None
    created: int
    currency: str | None = None
    destination_details: Dict | None = None
    failure_balance_transaction: str | None = None
    failure_reason: str | None = None
    metadata: Dict | None = None
    payment_intent: str | None = None
    reason: str | None = None
    receipt_number: str | None = None
    source_transfer_reversal: str | None = None
    status: str | None = None
    transfer_reversal: str | None = None

class Reviews(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Reviews"
    TYPES: ClassVar[str] =  "review.*"
    SEARCH_NAME: ClassVar[str] = "reviews"

    id: str
    object: Literal["review"]
    billing_zip: str | None = None
    charge: str | None = None 
    closed_reason: str | None = None
    created: int
    ip_address: str | None = None
    ip_address_location: Dict | None = None
    livemode: bool
    open: bool
    opened_reason: str
    payment_intent: str | None = None
    reason: str
    session: Dict | None = None

class SetupIntents(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Setup_Intents"
    TYPES: ClassVar[str] =  "setup_intent.*"
    SEARCH_NAME: ClassVar[str] = "setup_intents"

    id: str
    object: Literal["setup_intent"]
    application: str | None = None
    automatic_payment_methods: Dict | None = None
    cancellation_reason: str | None = None
    client_secret: str | None = Field(exclude=True) # Sensible Field
    created: int
    customer: str | None = None
    description: str | None = None
    flow_directions: list[str] | None = None
    last_setup_error: Dict | None = None
    latest_attempt: str | None = None
    livemode: bool
    mandate: str | None = None
    metadata: Dict | None = None
    next_action: Dict | None = None
    on_behalf_of: str | None = None
    payment_method: str | None = None
    payment_method_configuration_details: Dict | None = None
    payment_method_options: Dict | None = None
    payment_method_types: list[str] | None = None
    single_use_mandate: str | None = None
    status: str | None = None
    usage: str | None = None

class SetupAttempts(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: SetupIntents
    """
    NAME: ClassVar[str] = "Setup_Attempts"
    SEARCH_NAME: ClassVar[str] = "setup_attempts"
    PARAMETERS: ClassVar[Dict] = {"limit": 100, "setup_intent": None} 

    id: str
    parent_id: str | None = None
    object: Literal["setup_attempt"]
    application: str | None = None
    created: int
    customer: str | None = None
    flow_directions: list[str] | None = None
    livemode: bool
    on_behalf_of: str | None = None
    payment_method: str
    payment_method_details: Dict
    setup_error: Dict | None = None
    setup_intent: str
    status: str
    usage: str


class Subscriptions(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Subscriptions"
    TYPES: ClassVar[str] =  "customer.subscription.updated"
    SEARCH_NAME: ClassVar[str] = "subscriptions"

    id: str
    object: Literal["subscription"]
    application: str | None = None
    application_fee_percent: float | None = None
    automatic_tax: Dict
    billing_cycle_anchor: int
    billing_cycle_anchor_config: Dict | None = None
    billing_thresholds: Dict | None = None
    cancel_at: int | None = None
    cancel_at_period_end: bool
    canceled_at: int | None = None
    cancellation_details: Dict
    collection_method: str | None = None
    created: int
    currency: str | None = None
    current_period_end: int | None = None
    current_period_start: int | None = None
    customer: str
    days_until_due: int | None = None
    default_payment_method: str | None = None
    default_source: str | None = None
    default_tax_rates: list[Dict] | None = None
    description: str | None = None
    discounts: list[str] | None = None
    discount: Dict | None = None
    ended_at: int | None = None
    invoice_settings: Dict
    items: Dict = Field(exclude=True)
    latest_invoice: str | None = None
    livemode: bool
    metadata: Dict | None = None
    next_pending_invoice_item_invoice: int | None = None
    on_behalf_of: str | None = None
    pause_collection: Dict | None = None
    payment_settings: Dict | None = None
    pending_invoice_item_interval: Dict | None = None
    pending_setup_intent: str | None = None
    pending_update: Dict | None = None
    schedule: str | None = None
    start_date: int
    status: str
    test_clock: str | None = None
    transfer_data: Dict | None = None
    trial_end: int | None = None
    trial_settings: Dict
    trial_start: int | None = None
    plan: Dict
    quantity: int

class SubscriptionItems(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: Subscriptions
    """
    NAME: ClassVar[str] = "SubscriptionItems"
    SEARCH_NAME: ClassVar[str] = "subscription_items"
    PARAMETERS: ClassVar[Dict] = {"limit": 100, "subscription": None} 

    id: str
    parent_id: str | None = None
    object: Literal["subscription_item"]
    billing_thresholds: Dict | None = None
    created: int
    metadata: Dict | None = None
    discounts: list[Dict] | None = None
    plan: Dict | None = None
    price: Dict
    quantity: int | None = None
    subscription: str
    tax_rates: list[Dict] | None = None

class UsageRecords(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: SubscriptionItems
    """
    NAME: ClassVar[str] = "UsageRecords"
    SEARCH_NAME: ClassVar[str] = "usage_record_summaries"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["usage_record_summary"]
    livemode: bool
    quantity: int | None = None
    subscription_item: str
    timestamp: int | None = None
    invoice: str | None = None
    period: Dict | None = None
    total_usage: int | None = None

class SubscriptionsSchedule(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Subscription_Schedule"
    TYPES: ClassVar[str] =  "subscription_schedule.updated"
    SEARCH_NAME: ClassVar[str] = "subscription_schedules"

    id: str
    object: Literal["subscription_schedule"]
    application: str | None = None
    canceled_at: int | None = None
    completed_at: int | None = None
    created: int
    current_phase: Dict | None = None
    customer: str
    default_settings: Dict
    end_behavior: str | None = None
    livemode: bool
    metadata: Dict | None = None
    phases: list[Dict]
    released_at: int | None = None
    released_subscription: str | None = None
    renewal_interval: str | None = None
    status: str | None = None
    subscription: str | None = None
    test_clock: str | None = None

class TopUps(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "TopUps"
    TYPES: ClassVar[str] =  "topup.*"
    SEARCH_NAME: ClassVar[str] = "topups"

    id: str
    object: Literal["topup"]
    amount: int
    balance_transaction: str | None = None
    created: int
    currency: str | None = None
    description: str | None = None
    expected_availability_date: int | None = None
    failure_code: str | None = None
    failure_message: str | None = None
    livemode: bool
    #"source": null, docs set as deprecated
    statement_descriptor: str | None = None
    status: str
    transfer_group: str | None = None

class Transactions(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Transactions"
    TYPES: ClassVar[str] =  "issuing_transaction.updated"
    SEARCH_NAME: ClassVar[str] = "issuing/transactions"

    id: str
    object: Literal["issuing.transaction"]
    amount: int
    amount_details: Dict | None = None
    authorization: str | None = None
    balance_transaction: str | None = None
    card: str
    cardholder: str | None = None
    created: int
    currency: str | None = None
    dispute: str | None = None
    livemode: bool
    merchant_amount: int
    merchant_currency: str | None = None
    merchant_data: Dict
    metadata: Dict | None = None
    type: str | None = None
    wallet: str | None = None

class Transfers(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Transfers"
    TYPES: ClassVar[str] =  "transfer.updated"
    SEARCH_NAME: ClassVar[str] = "transfers"

    id: str
    object: Literal["transfer"]
    amount: int
    amount_reversed: int
    balance_transaction: str | None = None
    created: int
    currency: str | None = None
    description: str | None = None
    destination: str | None = None
    destination_payment: str | None = None
    livemode: bool
    metadata: str | None = None
    reversals: Dict = Field(exclude=True)
    reversed: bool
    source_transaction: str | None = None
    source_type: str | None = None
    transfer_group: str | None = None
    metadata: Dict | None = None

class TransferReversals(BaseDocument, extra="forbid"):
    """
    Child Stream
    Parent Stream: Transfers
    """
    NAME: ClassVar[str] = "TransferReversals"
    SEARCH_NAME: ClassVar[str] = "reversals"
    PARAMETERS: ClassVar[Dict] = {"limit": 100}

    id: str
    parent_id: str | None = None # Custom Added Field
    object: Literal["transfer_reversal"]
    amount: int
    balance_transaction: str | None = None
    created: int
    currency: str | None = None
    destination_payment_refund: str | None = None
    metadata: Dict | None = None
    source_refund: str | None = None
    transfer: str