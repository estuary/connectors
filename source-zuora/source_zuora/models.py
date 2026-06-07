from datetime import UTC, datetime
from decimal import Decimal
from pydantic import AwareDatetime, BaseModel, Field

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import ClientCredentialsOAuth2Credentials


def _default_start_date():
    return datetime(2000, 1, 1, tzinfo=UTC)


class EndpointConfig(BaseModel):
    credentials: ClientCredentialsOAuth2Credentials = Field(
        title="Authentication",
        description="OAuth2 Client ID and Client Secret for Zuora API access.",
    )
    tenant_endpoint: str = Field(
        title="Tenant Endpoint",
        description=(
            "Your Zuora REST API base URL. "
            "Use https://rest.zuora.com for production, "
            "https://rest.apisandbox.zuora.com for sandbox, "
            "or https://rest.eu.zuora.com for EU production."
        ),
        default="https://rest.zuora.com",
        examples=["https://rest.zuora.com", "https://rest.apisandbox.zuora.com"],
    )
    start_date: AwareDatetime = Field(
        title="Start Date",
        description=(
            "UTC date and time from which to start replicating data. "
            "Defaults to January 1, 2000, which will capture all available history."
        ),
        default_factory=_default_start_date,
    )


ConnectorState = GenericConnectorState[ResourceState]


class ZuoraDocument(BaseDocument, extra="allow"):
    """Base class for all Zuora objects. All Zuora objects have Id and UpdatedDate."""
    Id: str = ""
    UpdatedDate: AwareDatetime | None = None


# --- Account ---
class Account(ZuoraDocument, extra="allow"):
    AccountNumber: str | None = None
    AdditionalEmailAddresses: str | None = None
    AllowInvoiceEdit: bool | None = None
    AutoPay: bool | None = None
    Balance: Decimal | None = None
    Batch: str | None = None
    BcdSettingOption: str | None = None
    BillCycleDay: int | None = None
    BillToContactId: str | None = None
    CommunicationProfileId: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    CreditBalance: Decimal | None = None
    CrmId: str | None = None
    Currency: str | None = None
    CustomerServiceRepName: str | None = None
    DefaultPaymentMethodId: str | None = None
    Deleted: bool | None = None
    InvoiceDeliveryPrefsEmail: bool | None = None
    InvoiceDeliveryPrefsPrint: bool | None = None
    InvoiceTemplateId: str | None = None
    LastInvoiceDate: AwareDatetime | None = None
    Name: str | None = None
    Notes: str | None = None
    ParentAccountId: str | None = None
    PaymentGateway: str | None = None
    PaymentTerm: str | None = None
    PurchaseOrderNumber: str | None = None
    SalesRepName: str | None = None
    SoldToContactId: str | None = None
    Status: str | None = None
    TaxExemptCertificateId: str | None = None
    TaxExemptCertificateType: str | None = None
    TaxExemptDescription: str | None = None
    TaxExemptEffectiveDate: AwareDatetime | None = None
    TaxExemptExpirationDate: AwareDatetime | None = None
    TaxExemptIssuingJurisdiction: str | None = None
    TaxExemptStatus: str | None = None
    TotalInvoiceBalance: Decimal | None = None
    UpdatedById: str | None = None


# --- AccountingCode ---
class AccountingCode(ZuoraDocument, extra="allow"):
    Category: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Deleted: bool | None = None
    GlAccountName: str | None = None
    GlAccountNumber: str | None = None
    Name: str | None = None
    Notes: str | None = None
    Status: str | None = None
    Type: str | None = None
    UpdatedById: str | None = None


# --- AccountingPeriod ---
class AccountingPeriod(ZuoraDocument, extra="allow"):
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    EndDate: AwareDatetime | None = None
    FiscalQuarter: str | None = None
    FiscalYear: str | None = None
    Name: str | None = None
    Notes: str | None = None
    RunTrialBalanceEnd: AwareDatetime | None = None
    RunTrialBalanceErrorMessage: str | None = None
    RunTrialBalanceStart: AwareDatetime | None = None
    RunTrialBalanceStatus: str | None = None
    StartDate: AwareDatetime | None = None
    Status: str | None = None
    UpdatedById: str | None = None


# --- Amendment ---
class Amendment(ZuoraDocument, extra="allow"):
    AutoRenew: bool | None = None
    Code: str | None = None
    ContractEffectiveDate: AwareDatetime | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    CurrentTerm: int | None = None
    CurrentTermPeriodType: str | None = None
    CustomerAcceptanceDate: AwareDatetime | None = None
    Deleted: bool | None = None
    Description: str | None = None
    EffectiveDate: AwareDatetime | None = None
    Name: str | None = None
    RenewalSetting: str | None = None
    RenewalTerm: str | None = None
    RenewalTermPeriodType: str | None = None
    ServiceActivationDate: AwareDatetime | None = None
    SpecificUpdateDate: AwareDatetime | None = None
    Status: str | None = None
    SubscriptionId: str | None = None
    TermStartDate: str | None = None
    Type: str | None = None
    UpdatedById: str | None = None


# --- BillingRun ---
class BillingRun(ZuoraDocument, extra="allow"):
    AutoEmail: bool | None = None
    AutoPost: bool | None = None
    AutoRenewal: bool | None = None
    Batch: str | None = None
    BillCycleDay: str | None = None
    BillRunNumber: str | None = None
    ChargeTypeToExclude: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Deleted: bool | None = None
    ErrorMessage: str | None = None
    ExecutedDate: AwareDatetime | None = None
    InvoiceDate: AwareDatetime | None = None
    InvoicesEmailed: bool | None = None
    LastEmailSentTime: AwareDatetime | None = None
    NoEmailForZeroAmountInvoice: bool | None = None
    NumberOfAccounts: int | None = None
    NumberOfInvoices: int | None = None
    Status: str | None = None
    TargetDate: AwareDatetime | None = None
    UpdatedById: str | None = None


# --- CommunicationProfile ---
class CommunicationProfile(ZuoraDocument, extra="allow"):
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Deleted: bool | None = None
    Description: str | None = None
    ProfileName: str | None = None
    UpdatedById: str | None = None


# --- Contact ---
class Contact(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    Address1: str | None = None
    Address2: str | None = None
    City: str | None = None
    Country: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Deleted: bool | None = None
    Description: str | None = None
    Fax: str | None = None
    FirstName: str | None = None
    HomePhone: str | None = None
    LastName: str | None = None
    MobilePhone: str | None = None
    NickName: str | None = None
    OtherPhone: str | None = None
    OtherPhoneType: str | None = None
    PersonalEmail: str | None = None
    PostalCode: str | None = None
    State: str | None = None
    TaxRegion: str | None = None
    UpdatedById: str | None = None
    WorkEmail: str | None = None
    WorkPhone: str | None = None


# --- ContactSnapshot ---
class ContactSnapshot(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    Address1: str | None = None
    Address2: str | None = None
    City: str | None = None
    ContactId: str | None = None
    Country: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Description: str | None = None
    Fax: str | None = None
    FirstName: str | None = None
    HomePhone: str | None = None
    LastName: str | None = None
    MobilePhone: str | None = None
    NickName: str | None = None
    OtherPhone: str | None = None
    OtherPhoneType: str | None = None
    PersonalEmail: str | None = None
    PostalCode: str | None = None
    State: str | None = None
    TaxRegion: str | None = None
    UpdatedById: str | None = None
    WorkEmail: str | None = None
    WorkPhone: str | None = None


# --- CreditBalanceAdjustment ---
class CreditBalanceAdjustment(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    AccountReceivableAccountingCodeId: str | None = None
    AccountingCode: str | None = None
    AccountingPeriodId: str | None = None
    AdjustmentDate: AwareDatetime | None = None
    Amount: float | None = None
    BillToContactId: str | None = None
    CancelledOn: AwareDatetime | None = None
    CashOnAccountAccountingCodeId: str | None = None
    Comment: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    DefaultPaymentMethodId: str | None = None
    Deleted: bool | None = None
    InvoiceId: str | None = None
    JournalEntryId: str | None = None
    JournalRunId: str | None = None
    Number: str | None = None
    ParentAccountId: str | None = None
    PaymentId: str | None = None
    PaymentMethodId: str | None = None
    PaymentMethodSnapshotId: str | None = None
    ReasonCode: str | None = None
    ReferenceId: str | None = None
    RefundId: str | None = None
    SoldToContactId: str | None = None
    SourceTransactionId: str | None = None
    SourceTransactionNumber: str | None = None
    SourceTransactionType: str | None = None
    Status: str | None = None
    TransferredToAccounting: str | None = None
    UpdatedById: str | None = None


# --- DiscountAppliedMetrics ---
class DiscountAppliedMetrics(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    AmendmentId: str | None = None
    BillToContactId: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    DefaultPaymentMethodId: str | None = None
    DiscountRatePlanChargeId: str | None = None
    Mrr: str | None = None
    ParentAccountId: str | None = None
    ProductId: str | None = None
    ProductRatePlanChargeId: str | None = None
    ProductRatePlanId: str | None = None
    RatePlanChargeId: str | None = None
    RatePlanId: str | None = None
    SoldToContactId: str | None = None
    StartDate: AwareDatetime | None = None
    SubscriptionId: str | None = None
    Tcv: str | None = None
    UpdatedById: str | None = None


# --- Export ---
class Export(ZuoraDocument, extra="allow"):
    ConvertToCurrencies: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Deleted: bool | None = None
    Encrypted: bool | None = None
    FileId: str | None = None
    Format: str | None = None
    Name: str | None = None
    Query: str | None = None
    Size: int | None = None
    Status: str | None = None
    StatusReason: str | None = None
    UpdatedById: str | None = None
    Zip: bool | None = None


# --- Import ---
class Import(ZuoraDocument, extra="allow"):
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Deleted: bool | None = None
    ImportType: str | None = None
    ImportedCount: int | None = None
    Md5: str | None = None
    Name: str | None = None
    OriginalResourceUrl: str | None = None
    ResultResourceUrl: str | None = None
    Status: str | None = None
    StatusReason: str | None = None
    TotalCount: int | None = None
    UpdatedById: str | None = None


# --- Invoice ---
class Invoice(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    AdjustmentAmount: Decimal | None = None
    Amount: Decimal | None = None
    AmountWithoutTax: Decimal | None = None
    Balance: Decimal | None = None
    BillToContactId: str | None = None
    BillToContactSnapshotId: str | None = None
    Comments: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Currency: str | None = None
    DefaultPaymentMethodId: str | None = None
    Deleted: bool | None = None
    DueDate: AwareDatetime | None = None
    InvoiceDate: AwareDatetime | None = None
    InvoiceNumber: str | None = None
    LastEmailSentDate: AwareDatetime | None = None
    ParentAccountId: str | None = None
    PaymentAmount: Decimal | None = None
    PostedBy: str | None = None
    PostedDate: AwareDatetime | None = None
    RefundAmount: Decimal | None = None
    SoldToContactId: str | None = None
    SoldToContactSnapshotId: str | None = None
    Source: str | None = None
    SourceId: str | None = None
    Status: str | None = None
    TargetDate: AwareDatetime | None = None
    TaxAmount: Decimal | None = None
    TaxExemptAmount: Decimal | None = None
    TransferredToAccounting: str | None = None
    UpdatedById: str | None = None


# --- InvoiceItem ---
class InvoiceItem(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    AccountingCode: str | None = None
    AccountingPeriodId: str | None = None
    AmendmentId: str | None = None
    AmendmentType: str | None = None
    AppliedToInvoiceItemId: str | None = None
    BillToContactId: str | None = None
    ChargeAmount: Decimal | None = None
    ChargeDate: AwareDatetime | None = None
    ChargeName: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    DefaultPaymentMethodId: str | None = None
    Deleted: bool | None = None
    InvoiceId: str | None = None
    ParentAccountId: str | None = None
    ProcessingType: str | None = None
    ProductDescription: str | None = None
    ProductId: str | None = None
    ProductName: str | None = None
    ProductRatePlanChargeId: str | None = None
    Quantity: Decimal | None = None
    RatePlanChargeId: str | None = None
    RevRecCode: str | None = None
    RevRecStartDate: AwareDatetime | None = None
    RevRecTriggerCondition: str | None = None
    ServiceEndDate: AwareDatetime | None = None
    ServiceStartDate: AwareDatetime | None = None
    SKU: str | None = None
    SoldToContactId: str | None = None
    SubscriptionId: str | None = None
    SubscriptionName: str | None = None
    TaxAmount: Decimal | None = None
    TaxCode: str | None = None
    TaxExemptAmount: Decimal | None = None
    TaxMode: str | None = None
    UnitOfMeasure: str | None = None
    UnitPrice: Decimal | None = None
    UpdatedById: str | None = None


# --- InvoiceItemAdjustment ---
class InvoiceItemAdjustment(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    AccountingCode: str | None = None
    AccountingPeriodId: str | None = None
    AdjustmentDate: AwareDatetime | None = None
    AdjustmentNumber: str | None = None
    Amount: Decimal | None = None
    BillToContactId: str | None = None
    CancelledById: str | None = None
    CancelledDate: AwareDatetime | None = None
    Comment: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    CustomerName: str | None = None
    CustomerNumber: str | None = None
    DefaultPaymentMethodId: str | None = None
    Deleted: bool | None = None
    InvoiceId: str | None = None
    InvoiceItemId: str | None = None
    InvoiceItemName: str | None = None
    InvoiceNumber: str | None = None
    JournalEntryId: str | None = None
    JournalRunId: str | None = None
    ParentAccountId: str | None = None
    ReasonCode: str | None = None
    ReferenceId: str | None = None
    ServiceEndDate: AwareDatetime | None = None
    ServiceStartDate: AwareDatetime | None = None
    SoldToContactId: str | None = None
    SourceId: str | None = None
    SourceType: str | None = None
    Status: str | None = None
    SubType: str | None = None
    TransferredToAccounting: str | None = None
    Type: str | None = None
    UpdatedById: str | None = None


# --- JournalEntry ---
class JournalEntry(ZuoraDocument, extra="allow"):
    AccountingPeriodId: str | None = None
    AggregateCurrency: bool | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Currency: str | None = None
    HomeCurrency: str | None = None
    JournalEntryDate: AwareDatetime | None = None
    JournalRunId: str | None = None
    Notes: str | None = None
    Number: str | None = None
    Segments: str | None = None
    Status: str | None = None
    TimePeriodEnd: AwareDatetime | None = None
    TimePeriodStart: AwareDatetime | None = None
    TransactionType: str | None = None
    TransferDateTime: AwareDatetime | None = None
    TransferredBy: str | None = None
    TransferredToAccounting: str | None = None
    UpdatedById: str | None = None


# --- JournalEntryItem ---
class JournalEntryItem(ZuoraDocument, extra="allow"):
    AccountingCodeId: str | None = None
    AccountingCodeName: str | None = None
    AccountingCodeType: str | None = None
    AccountingPeriodId: str | None = None
    Amount: Decimal | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Currency: str | None = None
    GlAccountName: str | None = None
    GlAccountNumber: str | None = None
    HomeCurrency: str | None = None
    JournalEntryId: str | None = None
    Type: str | None = None
    UpdatedById: str | None = None


# --- JournalRun ---
class JournalRun(ZuoraDocument, extra="allow"):
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    ExecutedOn: AwareDatetime | None = None
    InitializedOn: AwareDatetime | None = None
    Name: str | None = None
    Number: str | None = None
    SegmentedTransactionsExecutedOn: AwareDatetime | None = None
    SegmentedTransactionsInitializedOn: AwareDatetime | None = None
    SegmentedTransactionsStatus: str | None = None
    Status: str | None = None
    Success: bool | None = None
    TargetEndDate: AwareDatetime | None = None
    TargetStartDate: AwareDatetime | None = None
    TotalAccountCount: int | None = None
    TotalJournalEntryCount: int | None = None
    UpdatedById: str | None = None


# --- Payment ---
class Payment(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    AccountingCode: str | None = None
    AccountingPeriodId: str | None = None
    Amount: Decimal | None = None
    AppliedAmount: Decimal | None = None
    AppliedCreditBalanceAmount: Decimal | None = None
    AuthTransactionId: str | None = None
    BankIdentificationNumber: str | None = None
    BillToContactId: str | None = None
    CancelledOn: AwareDatetime | None = None
    Comment: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Currency: str | None = None
    DefaultPaymentMethodId: str | None = None
    Deleted: bool | None = None
    EffectiveDate: AwareDatetime | None = None
    GatewayId: str | None = None
    GatewayOrderId: str | None = None
    GatewayResponse: str | None = None
    GatewayResponseCode: str | None = None
    GatewayState: str | None = None
    MarkedForSubmissionOn: AwareDatetime | None = None
    ParentAccountId: str | None = None
    PaymentMethodId: str | None = None
    PaymentMethodSnapshotId: str | None = None
    PaymentNumber: str | None = None
    ReferenceId: str | None = None
    RefundAmount: Decimal | None = None
    SecondPaymentReferenceId: str | None = None
    SettledOn: AwareDatetime | None = None
    SoldToContactId: str | None = None
    Source: str | None = None
    SourceId: str | None = None
    SourceName: str | None = None
    Status: str | None = None
    SubmittedOn: AwareDatetime | None = None
    TransferredToAccounting: str | None = None
    Type: str | None = None
    UnappliedAmount: Decimal | None = None
    UnappliedCreditBalanceAmount: Decimal | None = None
    UpdatedById: str | None = None


# --- PaymentMethod ---
class PaymentMethod(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    Active: bool | None = None
    AchAbaCode: str | None = None
    AchAccountName: str | None = None
    AchAccountType: str | None = None
    AchBankName: str | None = None
    BankBranchCode: str | None = None
    BankCheckDigit: str | None = None
    BankCity: str | None = None
    BankCode: str | None = None
    BankIdentificationNumber: str | None = None
    BankName: str | None = None
    BankPostalCode: str | None = None
    BankStreetName: str | None = None
    BankStreetNumber: str | None = None
    BankTransferAccountName: str | None = None
    BankTransferAccountType: str | None = None
    BankTransferType: str | None = None
    BusinessIdentificationCode: str | None = None
    City: str | None = None
    Country: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    CreditCardAddress1: str | None = None
    CreditCardAddress2: str | None = None
    CreditCardCity: str | None = None
    CreditCardCountry: str | None = None
    CreditCardExpirationMonth: int | None = None
    CreditCardExpirationYear: int | None = None
    CreditCardHolderName: str | None = None
    CreditCardMaskNumber: str | None = None
    CreditCardPostalCode: str | None = None
    CreditCardState: str | None = None
    CreditCardType: str | None = None
    Deleted: bool | None = None
    DeviceSessionId: str | None = None
    Email: str | None = None
    ExistingMandate: str | None = None
    FirstName: str | None = None
    Iban: str | None = None
    IpAddress: str | None = None
    IsCompany: bool | None = None
    LastFailedSaleTransactionDate: AwareDatetime | None = None
    LastName: str | None = None
    LastTransactionDateTime: AwareDatetime | None = None
    LastTransactionStatus: str | None = None
    MandateCreationDate: AwareDatetime | None = None
    MandateId: str | None = None
    MandateReceived: str | None = None
    MandateUpdateDate: AwareDatetime | None = None
    MaxConsecutivePaymentFailures: int | None = None
    Name: str | None = None
    NumConsecutiveFailures: int | None = None
    PaymentMethodStatus: str | None = None
    PaymentRetryWindow: int | None = None
    PaypalBaid: str | None = None
    PaypalEmail: str | None = None
    PaypalPreapprovalKey: str | None = None
    PaypalType: str | None = None
    Phone: str | None = None
    PostalCode: str | None = None
    SecondTokenId: str | None = None
    State: str | None = None
    StreetName: str | None = None
    StreetNumber: str | None = None
    TokenId: str | None = None
    TotalNumberOfErrorPayments: int | None = None
    TotalNumberOfProcessedPayments: int | None = None
    Type: str | None = None
    UpdatedById: str | None = None
    UseDefaultRetryRule: bool | None = None


# --- PaymentMethodSnapshot ---
class PaymentMethodSnapshot(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    Active: bool | None = None
    AchAbaCode: str | None = None
    AchAccountName: str | None = None
    AchAccountType: str | None = None
    AchBankName: str | None = None
    BankBranchCode: str | None = None
    BankCheckDigit: str | None = None
    BankCity: str | None = None
    BankCode: str | None = None
    BankIdentificationNumber: str | None = None
    BankName: str | None = None
    BankPostalCode: str | None = None
    BankStreetName: str | None = None
    BankStreetNumber: str | None = None
    BankTransferAccountName: str | None = None
    BankTransferAccountType: str | None = None
    BankTransferType: str | None = None
    BusinessIdentificationCode: str | None = None
    City: str | None = None
    Country: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    CreditCardAddress1: str | None = None
    CreditCardAddress2: str | None = None
    CreditCardCity: str | None = None
    CreditCardCountry: str | None = None
    CreditCardExpirationMonth: int | None = None
    CreditCardExpirationYear: int | None = None
    CreditCardHolderName: str | None = None
    CreditCardMaskNumber: str | None = None
    CreditCardPostalCode: str | None = None
    CreditCardState: str | None = None
    CreditCardType: str | None = None
    DeviceSessionId: str | None = None
    Email: str | None = None
    ExistingMandate: str | None = None
    FirstName: str | None = None
    Iban: str | None = None
    IpAddress: str | None = None
    IsCompany: bool | None = None
    LastName: str | None = None
    MandateCreationDate: AwareDatetime | None = None
    MandateId: str | None = None
    MandateReceived: str | None = None
    MandateUpdateDate: AwareDatetime | None = None
    MaxConsecutivePaymentFailures: int | None = None
    Name: str | None = None
    NumConsecutiveFailures: int | None = None
    PaymentMethodId: str | None = None
    PaymentMethodStatus: str | None = None
    PaymentRetryWindow: int | None = None
    PaypalBaid: str | None = None
    PaypalEmail: str | None = None
    PaypalPreapprovalKey: str | None = None
    PaypalType: str | None = None
    Phone: str | None = None
    PostalCode: str | None = None
    SecondTokenId: str | None = None
    State: str | None = None
    StreetName: str | None = None
    StreetNumber: str | None = None
    TokenId: str | None = None
    TotalNumberOfErrorPayments: int | None = None
    TotalNumberOfProcessedPayments: int | None = None
    Type: str | None = None
    UpdatedById: str | None = None
    UseDefaultRetryRule: bool | None = None


# --- PaymentRun ---
class PaymentRun(ZuoraDocument, extra="allow"):
    BillingRunId: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    ExecutedOn: AwareDatetime | None = None
    NumberOfErrors: int | None = None
    NumberOfInvoices: int | None = None
    NumberOfPayments: int | None = None
    NumberOfUnprocessedDebitMemos: int | None = None
    NumberOfUnprocessedInvoices: int | None = None
    RunDate: AwareDatetime | None = None
    Status: str | None = None
    TargetDate: AwareDatetime | None = None
    UpdatedById: str | None = None


# --- ProcessedUsage ---
class ProcessedUsage(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    AccountNumber: str | None = None
    BillToContactId: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    InvoiceId: str | None = None
    InvoiceItemId: str | None = None
    Month: str | None = None
    ParentAccountId: str | None = None
    ProductRatePlanChargeId: str | None = None
    Quantity: Decimal | None = None
    RatePlanChargeId: str | None = None
    RatePlanChargeName: str | None = None
    SoldToContactId: str | None = None
    SubscriptionId: str | None = None
    SubscriptionNumber: str | None = None
    UnitOfMeasure: str | None = None
    UpdatedById: str | None = None


# --- Product ---
class Product(ZuoraDocument, extra="allow"):
    AllowFeatureChanges: bool | None = None
    Category: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Deleted: bool | None = None
    Description: str | None = None
    EffectiveEndDate: AwareDatetime | None = None
    EffectiveStartDate: AwareDatetime | None = None
    Name: str | None = None
    SKU: str | None = None
    UpdatedById: str | None = None


# --- ProductRatePlan ---
class ProductRatePlan(ZuoraDocument, extra="allow"):
    ActiveCurrencies: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Deleted: bool | None = None
    Description: str | None = None
    EffectiveEndDate: AwareDatetime | None = None
    EffectiveStartDate: AwareDatetime | None = None
    Name: str | None = None
    ProductId: str | None = None
    UpdatedById: str | None = None


# --- ProductRatePlanCharge ---
class ProductRatePlanCharge(ZuoraDocument, extra="allow"):
    AccountingCode: str | None = None
    ApplyDiscountTo: str | None = None
    BillCycleDay: str | None = None
    BillCycleType: str | None = None
    BillingPeriod: str | None = None
    BillingPeriodAlignment: str | None = None
    BillingTiming: str | None = None
    ChargeModel: str | None = None
    ChargeType: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    DefaultQuantity: Decimal | None = None
    Deleted: bool | None = None
    Description: str | None = None
    DiscountClass: str | None = None
    DiscountLevel: str | None = None
    EndDateCondition: str | None = None
    IncludedUnits: Decimal | None = None
    LegacyRevenueReporting: bool | None = None
    ListPriceBase: str | None = None
    MaxQuantity: Decimal | None = None
    MinQuantity: Decimal | None = None
    Name: str | None = None
    NumberOfPeriods: int | None = None
    OverageCalculationOption: str | None = None
    OverageUnusedUnitsCreditOption: str | None = None
    PriceChangeOption: str | None = None
    PriceIncreasePercentage: Decimal | None = None
    ProductRatePlanId: str | None = None
    RatingGroup: str | None = None
    RecognizedRevenueAccount: str | None = None
    RevRecCode: str | None = None
    RevRecTriggerCondition: str | None = None
    RevenueRecognitionRuleName: str | None = None
    SmoothingModel: str | None = None
    SpecificBillingPeriod: int | None = None
    TaxCode: str | None = None
    TaxMode: str | None = None
    Taxable: bool | None = None
    TriggerEvent: str | None = None
    UOM: str | None = None
    UpToPeriodsType: str | None = None
    UpdatedById: str | None = None
    UseDiscountSpecificAccountingCode: bool | None = None
    UseTenantDefaultForPriceChange: bool | None = None
    WeeklyBillCycleDay: str | None = None


# --- ProductRatePlanChargeTier ---
class ProductRatePlanChargeTier(ZuoraDocument, extra="allow"):
    Active: bool | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Currency: str | None = None
    Deleted: bool | None = None
    DiscountAmount: Decimal | None = None
    DiscountPercentage: Decimal | None = None
    EndingUnit: Decimal | None = None
    IncludedUnits: Decimal | None = None
    OveragePrice: Decimal | None = None
    Price: Decimal | None = None
    PriceFormat: str | None = None
    ProductRatePlanChargeId: str | None = None
    StartingUnit: Decimal | None = None
    Tier: int | None = None
    UpdatedById: str | None = None


# --- RatePlan ---
class RatePlan(ZuoraDocument, extra="allow"):
    AmendmentId: str | None = None
    AmendmentSubscriptionRatePlanId: str | None = None
    AmendmentType: str | None = None
    BillToContactId: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    DefaultPaymentMethodId: str | None = None
    Deleted: bool | None = None
    Name: str | None = None
    ParentAccountId: str | None = None
    ProductId: str | None = None
    ProductRatePlanId: str | None = None
    SoldToContactId: str | None = None
    SubscriptionId: str | None = None
    UpdatedById: str | None = None


# --- RatePlanCharge ---
class RatePlanCharge(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    ApplyDiscountTo: str | None = None
    BillCycleDay: str | None = None
    BillCycleType: str | None = None
    BilledPeriod: str | None = None
    BillingPeriodAlignment: str | None = None
    BillingTiming: str | None = None
    BillToContactId: str | None = None
    ChargeModel: str | None = None
    ChargeNumber: str | None = None
    ChargeType: str | None = None
    ChargedThroughDate: AwareDatetime | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    DMRC: Decimal | None = None
    DTCV: Decimal | None = None
    DefaultPaymentMethodId: str | None = None
    Deleted: bool | None = None
    Description: str | None = None
    DiscountAmount: Decimal | None = None
    DiscountClass: str | None = None
    DiscountLevel: str | None = None
    DiscountPercentage: Decimal | None = None
    EffectiveEndDate: AwareDatetime | None = None
    EffectiveStartDate: AwareDatetime | None = None
    EndDateCondition: str | None = None
    IsLastSegment: bool | None = None
    ListPriceBase: str | None = None
    MRR: Decimal | None = None
    Name: str | None = None
    NumberOfPeriods: int | None = None
    OriginalId: str | None = None
    OriginalOrderDate: AwareDatetime | None = None
    OverageCalculationOption: str | None = None
    OveragePrice: Decimal | None = None
    OverageUnusedUnitsCreditOption: str | None = None
    ParentAccountId: str | None = None
    Price: Decimal | None = None
    PriceChangeOption: str | None = None
    PriceIncreasePercentage: Decimal | None = None
    ProcessedThroughDate: AwareDatetime | None = None
    ProductRatePlanChargeId: str | None = None
    Quantity: Decimal | None = None
    RatePlanId: str | None = None
    RatingGroup: str | None = None
    RevRecCode: str | None = None
    RevRecTriggerCondition: str | None = None
    RevenueRecognitionRuleName: str | None = None
    Segment: int | None = None
    SoldToContactId: str | None = None
    SpecificBillingPeriod: int | None = None
    SpecificEndDate: AwareDatetime | None = None
    TCV: Decimal | None = None
    TaxCode: str | None = None
    TaxMode: str | None = None
    Taxable: bool | None = None
    TriggerDate: AwareDatetime | None = None
    TriggerEvent: str | None = None
    UOM: str | None = None
    UnbilledMRR: Decimal | None = None
    UpToPeriods: int | None = None
    UpToPeriodsType: str | None = None
    UpdatedById: str | None = None
    UseDiscountSpecificAccountingCode: bool | None = None
    Version: int | None = None
    WeeklyBillCycleDay: str | None = None


# --- RatePlanChargeTier ---
class RatePlanChargeTier(ZuoraDocument, extra="allow"):
    Active: bool | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Currency: str | None = None
    Deleted: bool | None = None
    DiscountAmount: Decimal | None = None
    DiscountPercentage: Decimal | None = None
    EndingUnit: Decimal | None = None
    IncludedUnits: Decimal | None = None
    OveragePrice: Decimal | None = None
    Price: Decimal | None = None
    PriceFormat: str | None = None
    RatePlanChargeId: str | None = None
    StartingUnit: Decimal | None = None
    Tier: int | None = None
    UpdatedById: str | None = None


# --- Refund ---
class Refund(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    AccountingCode: str | None = None
    AccountingPeriodId: str | None = None
    Amount: Decimal | None = None
    BillToContactId: str | None = None
    CancelledOn: AwareDatetime | None = None
    Comment: str | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    DefaultPaymentMethodId: str | None = None
    Deleted: bool | None = None
    GatewayResponse: str | None = None
    GatewayResponseCode: str | None = None
    GatewayState: str | None = None
    MarkedForSubmissionOn: AwareDatetime | None = None
    MethodType: str | None = None
    ParentAccountId: str | None = None
    PaymentId: str | None = None
    PaymentMethodId: str | None = None
    PaymentMethodSnapshotId: str | None = None
    ReasonCode: str | None = None
    ReferenceId: str | None = None
    RefundDate: AwareDatetime | None = None
    RefundNumber: str | None = None
    RefundTransactionTime: AwareDatetime | None = None
    SecondRefundReferenceId: str | None = None
    SettledOn: AwareDatetime | None = None
    SoftDescriptor: str | None = None
    SoftDescriptorPhone: str | None = None
    SoldToContactId: str | None = None
    SourceType: str | None = None
    Status: str | None = None
    SubmittedOn: AwareDatetime | None = None
    TransferredToAccounting: str | None = None
    Type: str | None = None
    UpdatedById: str | None = None


# --- Revenue objects share a common structure ---
class _RevenueItem(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    AccountingCodeId: str | None = None
    AccountingPeriodId: str | None = None
    Amount: Decimal | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    Currency: str | None = None
    IsAccountingPeriodClosed: bool | None = None
    Notes: str | None = None
    UpdatedById: str | None = None


class RevenueChargeSummaryItem(_RevenueItem, extra="allow"):
    RevenueScheduleItemId: str | None = None


class RevenueEventItem(_RevenueItem, extra="allow"):
    RevenueEventItemId: str | None = None
    RevenueEventType: str | None = None


class RevenueEventItemInvoiceItem(_RevenueItem, extra="allow"):
    RevenueEventItemId: str | None = None
    RevenueEventType: str | None = None


class RevenueEventItemInvoiceItemAdjustment(_RevenueItem, extra="allow"):
    RevenueEventItemId: str | None = None
    RevenueEventType: str | None = None


class RevenueScheduleItem(_RevenueItem, extra="allow"):
    RevenueScheduleId: str | None = None


class RevenueScheduleItemInvoiceItem(_RevenueItem, extra="allow"):
    RevenueScheduleId: str | None = None


class RevenueScheduleItemInvoiceItemAdjustment(_RevenueItem, extra="allow"):
    RevenueScheduleId: str | None = None


# --- Subscription ---
class Subscription(ZuoraDocument, extra="allow"):
    AccountId: str | None = None
    AncestorAccountId: str | None = None
    AutoRenew: bool | None = None
    BillToContactId: str | None = None
    CancelledDate: AwareDatetime | None = None
    ContractAcceptanceDate: AwareDatetime | None = None
    ContractEffectiveDate: AwareDatetime | None = None
    CreatedById: str | None = None
    CreatedDate: AwareDatetime | None = None
    CreatorAccountId: str | None = None
    CreatorInvoiceOwnerId: str | None = None
    CurrentTermPeriodType: str | None = None
    DefaultPaymentMethodId: str | None = None
    Deleted: bool | None = None
    InitialTermPeriodType: str | None = None
    InvoiceOwnerId: str | None = None
    IsInvoiceSeparate: bool | None = None
    IsLatestVersion: bool | None = None
    Name: str | None = None
    Notes: str | None = None
    OriginalCreatedDate: AwareDatetime | None = None
    OriginalId: str | None = None
    ParentAccountId: str | None = None
    PaymentTerm: str | None = None
    PreviousSubscriptionId: str | None = None
    RenewalSetting: str | None = None
    RenewalTermPeriodType: str | None = None
    ServiceActivationDate: AwareDatetime | None = None
    SoldToContactId: str | None = None
    Status: str | None = None
    SubscriptionEndDate: AwareDatetime | None = None
    SubscriptionStartDate: AwareDatetime | None = None
    TermEndDate: AwareDatetime | None = None
    TermStartDate: AwareDatetime | None = None
    TermType: str | None = None
    UpdatedById: str | None = None
    Version: int | None = None


# Map from Zuora object name to its Pydantic model class
OBJECT_MODELS: dict[str, type[ZuoraDocument]] = {
    "Account": Account,
    "AccountingCode": AccountingCode,
    "AccountingPeriod": AccountingPeriod,
    "Amendment": Amendment,
    "BillingRun": BillingRun,
    "CommunicationProfile": CommunicationProfile,
    "Contact": Contact,
    "ContactSnapshot": ContactSnapshot,
    "CreditBalanceAdjustment": CreditBalanceAdjustment,
    "DiscountAppliedMetrics": DiscountAppliedMetrics,
    "Export": Export,
    "Import": Import,
    "Invoice": Invoice,
    "InvoiceItem": InvoiceItem,
    "InvoiceItemAdjustment": InvoiceItemAdjustment,
    "JournalEntry": JournalEntry,
    "JournalEntryItem": JournalEntryItem,
    "JournalRun": JournalRun,
    "Payment": Payment,
    "PaymentMethod": PaymentMethod,
    "PaymentMethodSnapshot": PaymentMethodSnapshot,
    "PaymentRun": PaymentRun,
    "ProcessedUsage": ProcessedUsage,
    "Product": Product,
    "ProductRatePlan": ProductRatePlan,
    "ProductRatePlanCharge": ProductRatePlanCharge,
    "ProductRatePlanChargeTier": ProductRatePlanChargeTier,
    "RatePlan": RatePlan,
    "RatePlanCharge": RatePlanCharge,
    "RatePlanChargeTier": RatePlanChargeTier,
    "Refund": Refund,
    "RevenueChargeSummaryItem": RevenueChargeSummaryItem,
    "RevenueEventItem": RevenueEventItem,
    "RevenueEventItemInvoiceItem": RevenueEventItemInvoiceItem,
    "RevenueEventItemInvoiceItemAdjustment": RevenueEventItemInvoiceItemAdjustment,
    "RevenueScheduleItem": RevenueScheduleItem,
    "RevenueScheduleItemInvoiceItem": RevenueScheduleItemInvoiceItem,
    "RevenueScheduleItemInvoiceItemAdjustment": RevenueScheduleItemInvoiceItemAdjustment,
    "Subscription": Subscription,
}
