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
