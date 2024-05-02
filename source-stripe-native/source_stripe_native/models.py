from datetime import datetime
from enum import StrEnum, auto
from pydantic import BaseModel, Field, AwareDatetime, model_validator
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

#TODO add Balance Transaction Stream
class EventResult(BaseModel,  Generic[Item], extra="forbid"):
    class Data(BaseModel):
        class CLSDATA(BaseModel):
            object: Item
            previous_attributes: Dict
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

class BackfillResult(BaseModel, Generic[Item], extra="forbid"):
    object: str
    url: str
    has_more: bool
    data: List[Item]

class Accounts(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Accounts"
    TYPES: ClassVar[list[str]] =  ["accounts.updated"]
    SEARCH_NAME: ClassVar[str] = "accounts"

    id: str
    object: Literal["account"]
    business_profile: Dict | None = None
    business_type: str | None = None
    capabilities: Dict | None = None
    charges_enabled: bool
    controller: Dict | None = None
    country: str
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
    type: str

class Persons(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "Persons"
    SEARCH_NAME: ClassVar[str] = "persons"

    id: str
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
    SEARCH_NAME: ClassVar[str] = "external_account"
    # TODO add Params: {object=”card”}

    id: str
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

class ExternalBankAccount(BaseDocument, extra="allow"):
    """
    Child Stream
    Parent Stream: Accounts
    """
    NAME: ClassVar[str] = "External_Bank_Account"
    SEARCH_NAME: ClassVar[str] = "external_account"
    # TODO add Params: {object=bank_account”}

    
    id: str
    object: Literal["bank_account"]
    account_holder_name: str | None = None
    account_holder_type: str | None = None
    account_type: str | None = None
    available_payout_methods: List[str] | None = None
    bank_name: str | None = None
    country: str 
    currency: str
    customer: str
    fingerprint: str | None = None
    last4: str
    metadata: Dict | None = None
    routing_number: str | None = None
    status: str

class ApplicationFees(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Application_Fees"
    TYPES: ClassVar[list[str]] =  ["application_fee.refunded"]
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

    id: str
    object: Literal["fee_refund"]
    amount: int
    balance_transaction: str | None = None
    created: int
    currency: str
    fee: str
    metadata: Dict | None = None

class Authorizations(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "Authorizations"
    TYPES: ClassVar[list[str]] =  ["issuing_authorization.updated"]
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
    TYPES: ClassVar[list[str]] =  ["customer.updated"]
    SEARCH_NAME: ClassVar[str] = "customers"

    id: str
    object: Literal["customer"]
    address: Dict | None = None
    balance: int
    created: int
    currency: str | None = None 
    default_source: str | None = None
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

    id: str
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

    
    id: str
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

class CardHolders(BaseDocument, extra="forbid"):
    NAME: ClassVar[str] = "CardHolders"
    TYPES: ClassVar[list[str]] =  ["issuing_cardholder.updated"]
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
