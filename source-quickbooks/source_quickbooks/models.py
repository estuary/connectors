from __future__ import annotations

import urllib.parse
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, ClassVar

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import (
    OAuth2Spec,
    RotatingOAuth2Credentials,
)
from pydantic import AwareDatetime, BaseModel, Field

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


scopes = [
    "com.intuit.quickbooks.accounting",
    "com.intuit.quickbooks.payment",
    "openid",
    "profile",
    "email",
    "phone",
    "address",
]

OAUTH2_SPEC = OAuth2Spec(
    provider="intuit",
    accessTokenBody=(
        "grant_type=authorization_code"
        r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
    ),
    authUrlTemplate=(
        "https://appcenter.intuit.com/connect/oauth2?"
        r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&scope="
        + urllib.parse.quote(" ".join(scopes))
        + r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        + r"&response_type=code"
        + r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate="https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
    accessTokenResponseMap={
        "access_token": "/access_token",
        "refresh_token": "/refresh_token",
        "access_token_expires_at": r"{{#now_plus}}{{ expires_in }}{{/now_plus}}",
    },
    accessTokenHeaders={
        "Content-Type": "application/x-www-form-urlencoded",
    },
)


if TYPE_CHECKING:
    OAuth2Credentials = RotatingOAuth2Credentials.with_client_credentials_placement(
        "headers"
    )
else:
    OAuth2Credentials = RotatingOAuth2Credentials.with_client_credentials_placement(
        "headers"
    ).for_provider(OAUTH2_SPEC.provider)


class EndpointConfig(BaseModel):
    # Though technically a numerical value, realm IDs tend to be large enough that precision loss
    # can alter what's actually used.
    # This value is presented to end users as their company ID
    realm_id: str = Field(
        description="ID for the Company to Request Data From",
        title="Company ID",
        json_schema_extra={"order": 0},
    )
    credentials: OAuth2Credentials = Field(
        title="Authentication",
        json_schema_extra={"order": 1},
    )
    start_date: AwareDatetime = Field(
        description=(
            "UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. "
            "Any data generated before this date will not be replicated. "
            "If left blank, the start date will be set to 30 days before the present."
        ),
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
        json_schema_extra={"order": 2},
    )


ConnectorState = GenericConnectorState[ResourceState]


class QuickBooksEntityMetadata(BaseModel, extra="allow"):
    LastUpdatedTime: AwareDatetime


class QuickBooksEntity(BaseDocument, extra="allow"):
    resource_name: ClassVar[str]
    table_name: ClassVar[str]

    Id: str
    MetaData: QuickBooksEntityMetadata


class Account(QuickBooksEntity):
    resource_name: ClassVar[str] = "Accounts"
    table_name: ClassVar[str] = "Account"


class BillPayment(QuickBooksEntity):
    resource_name: ClassVar[str] = "Bill Payments"
    table_name: ClassVar[str] = "BillPayment"


class Budget(QuickBooksEntity):
    resource_name: ClassVar[str] = "Budgets"
    table_name: ClassVar[str] = "Budget"


class Bill(QuickBooksEntity):
    resource_name: ClassVar[str] = "Bills"
    table_name: ClassVar[str] = "Bill"


class Class(QuickBooksEntity):
    resource_name: ClassVar[str] = "Classes"
    table_name: ClassVar[str] = "Class"


class CreditMemo(QuickBooksEntity):
    resource_name: ClassVar[str] = "Credit Memos"
    table_name: ClassVar[str] = "CreditMemo"


class Customer(QuickBooksEntity):
    resource_name: ClassVar[str] = "Customers"
    table_name: ClassVar[str] = "Customer"


class Department(QuickBooksEntity):
    resource_name: ClassVar[str] = "Departments"
    table_name: ClassVar[str] = "Department"


class Deposit(QuickBooksEntity):
    resource_name: ClassVar[str] = "Deposits"
    table_name: ClassVar[str] = "Deposit"


class Employee(QuickBooksEntity):
    resource_name: ClassVar[str] = "Employees"
    table_name: ClassVar[str] = "Employee"


class Estimate(QuickBooksEntity):
    resource_name: ClassVar[str] = "Estimates"
    table_name: ClassVar[str] = "Estimate"


class Invoice(QuickBooksEntity):
    resource_name: ClassVar[str] = "Invoices"
    table_name: ClassVar[str] = "Invoice"


class Item(QuickBooksEntity):
    resource_name: ClassVar[str] = "Items"
    table_name: ClassVar[str] = "Item"


class JournalEntry(QuickBooksEntity):
    resource_name: ClassVar[str] = "Journal Entries"
    table_name: ClassVar[str] = "JournalEntry"


class Payment(QuickBooksEntity):
    resource_name: ClassVar[str] = "Payments"
    table_name: ClassVar[str] = "Payment"


class PaymentMethod(QuickBooksEntity):
    resource_name: ClassVar[str] = "Payment Methods"
    table_name: ClassVar[str] = "PaymentMethod"


class Purchase(QuickBooksEntity):
    resource_name: ClassVar[str] = "Purchases"
    table_name: ClassVar[str] = "Purchase"


class PurchaseOrder(QuickBooksEntity):
    resource_name: ClassVar[str] = "Purchase Orders"
    table_name: ClassVar[str] = "PurchaseOrder"


class RefundReceipt(QuickBooksEntity):
    resource_name: ClassVar[str] = "Refund Receipts"
    table_name: ClassVar[str] = "RefundReceipt"


class SalesReceipt(QuickBooksEntity):
    resource_name: ClassVar[str] = "Sales Receipts"
    table_name: ClassVar[str] = "SalesReceipt"


class TaxAgency(QuickBooksEntity):
    resource_name: ClassVar[str] = "Tax Agencies"
    table_name: ClassVar[str] = "TaxAgency"


class TaxCode(QuickBooksEntity):
    resource_name: ClassVar[str] = "Tax Codes"
    table_name: ClassVar[str] = "TaxCode"


class TaxRate(QuickBooksEntity):
    resource_name: ClassVar[str] = "Tax Rates"
    table_name: ClassVar[str] = "TaxRate"


class Term(QuickBooksEntity):
    resource_name: ClassVar[str] = "Terms"
    table_name: ClassVar[str] = "Term"


class TimeActivity(QuickBooksEntity):
    resource_name: ClassVar[str] = "Time Activities"
    table_name: ClassVar[str] = "TimeActivity"


class Transfer(QuickBooksEntity):
    resource_name: ClassVar[str] = "Transfers"
    table_name: ClassVar[str] = "Transfer"


class VendorCredit(QuickBooksEntity):
    resource_name: ClassVar[str] = "Vendor Credits"
    table_name: ClassVar[str] = "VendorCredit"


class Vendor(QuickBooksEntity):
    resource_name: ClassVar[str] = "Vendors"
    table_name: ClassVar[str] = "Vendor"


ALL_RESOURCES = [
    Account,
    BillPayment,
    Budget,
    Bill,
    Class,
    CreditMemo,
    Customer,
    Department,
    Deposit,
    Employee,
    Estimate,
    Invoice,
    Item,
    JournalEntry,
    Payment,
    PaymentMethod,
    Purchase,
    PurchaseOrder,
    RefundReceipt,
    SalesReceipt,
    TaxAgency,
    TaxCode,
    TaxRate,
    Term,
    TimeActivity,
    Transfer,
    VendorCredit,
    Vendor,
]
