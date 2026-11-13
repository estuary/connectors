from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, ClassVar, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    ResourceConfigWithSchedule,
    ResourceState,
)
from estuary_cdk.flow import BaseOAuth2Credentials, OAuth2Spec


# Login with Amazon (LWA) issues the short-lived access tokens that SP-API
# requests carry in the `x-amz-access-token` header.
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

OAUTH2_SPEC = OAuth2Spec(
    provider="amazon-selling-partner",
    accessTokenUrlTemplate=LWA_TOKEN_URL,
    # Seller consent goes through Seller Central and returns `spapi_oauth_code`
    # rather than `code`, which the dashboard's OAuth flow does not handle yet.
    # Until it does, the spec isn't advertised and refresh tokens are entered by hand.
    authUrlTemplate="",
    accessTokenBody="",
    accessTokenHeaders={},
    accessTokenResponseMap={},
)


class Region(StrEnum):
    NORTH_AMERICA = "North America"
    EUROPE = "Europe"
    FAR_EAST = "Far East"

    def host(self, sandbox: bool) -> str:
        suffix = {
            Region.NORTH_AMERICA: "na",
            Region.EUROPE: "eu",
            Region.FAR_EAST: "fe",
        }[self]
        prefix = "sandbox." if sandbox else ""
        return f"https://{prefix}sellingpartnerapi-{suffix}.amazon.com"


def default_start_date() -> datetime:
    # A UTC midnight, so it is the same at every start within a day.
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    return today - timedelta(days=30)


class LoginWithAmazonCredentials(BaseModel):
    credentials_title: Literal["Login with Amazon"] = Field(
        default="Login with Amazon", json_schema_extra={"type": "string"}
    )
    client_id: str = Field(
        title="LWA Client ID",
        json_schema_extra={"secret": True},
    )
    client_secret: str = Field(
        title="LWA Client Secret",
        json_schema_extra={"secret": True},
    )
    refresh_token: str = Field(
        title="Refresh Token",
        description="The refresh token issued when the selling account authorized the application.",
        json_schema_extra={"secret": True},
    )


class _LWARefreshCredentials(BaseOAuth2Credentials):
    """The refresh-token grant the CDK's TokenSource exchanges with LWA."""

    def _you_must_build_oauth2_credentials_for_a_provider(self) -> None: ...


def lwa_credentials(credentials: LoginWithAmazonCredentials) -> BaseOAuth2Credentials:
    return _LWARefreshCredentials(
        client_id=credentials.client_id,
        client_secret=credentials.client_secret,
        refresh_token=credentials.refresh_token,
    )


class EndpointConfig(BaseModel):
    credentials: LoginWithAmazonCredentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    region: Region = Field(
        description="The SP-API region of the selling account. Capture each region separately.",
        title="Region",
    )
    marketplace_ids: list[str] = Field(
        description="Amazon marketplace IDs to capture, such as ATVPDKIKX0DER for Amazon.com. All must belong to the selected region.",
        title="Marketplace IDs",
        min_length=1,
        max_length=50,
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Orders created, financial transactions posted, settlement reports created and all orders lines last updated before this date are not captured, nor Sales and Traffic days before the UTC day it falls in. Amazon serves about two years of order history, and of all orders and Sales and Traffic reports, but keeps settlement reports for only 90 days, so older data is not captured whatever this date. If left blank, the start date is set to 30 days before the date each stream first starts backfilling.",
        title="Start Date",
        default_factory=default_start_date,
    )

    class Advanced(BaseModel):
        sandbox: bool = Field(
            description="Send requests to Amazon's SP-API sandbox, which returns fixed example responses. For testing only.",
            title="Use Sandbox",
            default=False,
        )
        window_size: Annotated[timedelta, Field(
            description="Size of the date windows orders and financial transactions are requested in, in ISO 8601 format. E.g. P1D means 1 day, PT6H means 6 hours. Each order window must be paged through within 24 hours, so busy accounts may need smaller windows.",
            title="Window Size",
            default_factory=lambda: timedelta(days=1),
            ge=timedelta(hours=1),
            le=timedelta(days=30),
        )]
        overlap: Annotated[timedelta, Field(
            description="How far each incremental request reaches back before the previous one ended, to catch orders Amazon makes searchable late, in ISO 8601 format.",
            title="Incremental Overlap",
            default_factory=lambda: timedelta(minutes=10),
            ge=timedelta(),
            le=timedelta(hours=1),
        )]
        financial_transactions_lookback: Annotated[timedelta, Field(
            description="How far behind the present financial transactions are read a final time, to catch deferred transactions Amazon has since released, in ISO 8601 format. Deferred transactions, such as invoiced (B2B) orders, are released on their maturity date, so set this past the longest deferral the account sees. Changes to a transaction after this point are not captured. Raising it re-reads the newly covered time.",
            title="Financial Transactions Lookback",
            default_factory=lambda: timedelta(days=90),
            # Transactions are already re-read at 72 hours for late postings,
            # so a final read must come later than that.
            ge=timedelta(days=4),
            le=timedelta(days=180),
        )]

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

    @model_validator(mode="after")
    def _check_marketplaces(self) -> "EndpointConfig":
        if len(set(self.marketplace_ids)) != len(self.marketplace_ids):
            raise ValueError("Marketplace IDs must not repeat.")
        return self


ConnectorState = GenericConnectorState[ResourceState]


class OrdersResourceConfig(ResourceConfigWithSchedule):
    PATH_POINTERS: ClassVar[list[str]] = ["/name"]


def sortable_utc(value: datetime) -> str:
    """
    `value` in UTC with six fractional digits, so the strings of a maximize
    reduction key, which Flow compares as text, sort as times whatever
    offset and precision Amazon gave.
    """
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# The backfill and the incremental task commit separately, so an older copy of
# an order can reach the collection after a newer one. Keeping the copy with the
# latest lastUpdatedTime makes the result independent of commit order; on equal
# lastUpdatedTime Flow deep-merges the copies, so a field missing from the later
# copy is kept. Assumes Amazon changes lastUpdatedTime whenever an order's
# content changes. The CDK writes only `if` and `then`, so this `else` is the
# non-delete reduction.
class Order(
    BaseDocument,
    extra="allow",
    json_schema_extra={
        "else": {"reduce": {"strategy": "maximize", "key": ["/lastUpdatedTime"]}}
    },
):
    PATH: ClassVar[str] = "/orders/2026-01-01/orders"
    PAGE_SIZE: ClassVar[int] = 100
    # Extra order sections to request. BUYER and RECIPIENT are left out: they
    # carry buyer personal data, which Amazon only releases to applications
    # approved for restricted roles. FULFILLMENT_ORDERS is also not requested,
    # for no recorded reason. Amazon's model says it holds only fulfillment
    # order ids, for Easy Ship and Seller Flex orders; requesting it is an open
    # decision.
    INCLUDED_DATA: ClassVar[list[str]] = [
        "PROCEEDS",
        "EXPENSE",
        "PROMOTION",
        "CANCELLATION",
        "FULFILLMENT",
        "PACKAGES",
        "TAX",
        "PAYMENT",
    ]

    orderId: str
    createdTime: AwareDatetime
    lastUpdatedTime: AwareDatetime

    @field_serializer("lastUpdatedTime")
    def _sortable_last_updated_time(self, value: datetime):
        return sortable_utc(value)


class SearchOrdersResponse(BaseModel, extra="allow"):
    """What remains of a searchOrders page once its orders are streamed out."""

    class Pagination(BaseModel, extra="allow"):
        nextToken: str | None = None

    # The streamed-out array remains as `[]`. Requiring it makes a response
    # that moved its orders elsewhere fail instead of advancing the cursor.
    orders: list[Any]
    pagination: Pagination | None = None
    lastUpdatedBefore: AwareDatetime | None = None
    createdBefore: AwareDatetime | None = None


class MarketplaceParticipationsResponse(BaseModel, extra="allow"):
    class Participation(BaseModel, extra="allow"):
        class Marketplace(BaseModel, extra="allow"):
            id: str

        marketplace: Marketplace

    payload: list[Participation] = []


class FinancialTransaction(BaseDocument, extra="allow"):
    PATH: ClassVar[str] = "/finances/2024-06-19/transactions"
    ITEMS_PREFIX: ClassVar[str] = "payload.transactions.item"
    # Bounds on postedDate: SINCE_PARAM is inclusive, BEFORE_PARAM exclusive.
    SINCE_PARAM: ClassVar[str] = "postedAfter"
    BEFORE_PARAM: ClassVar[str] = "postedBefore"

    # The model file marks no field required. transactionId is described as
    # the transaction's unique identifier and is the collection key, so a
    # transaction without one fails the capture rather than being keyed on
    # something that changes when it is released.
    transactionId: str


class ListTransactionsResponse(BaseModel, extra="allow"):
    """What remains of a listTransactions page once its transactions are streamed out."""

    class Payload(BaseModel, extra="allow"):
        nextToken: str | None = None
        # The model file makes the array optional and says pages can be
        # empty, so its absence, or a null, is not an error.
        transactions: list[Any] = []

        @field_validator("transactions", mode="before")
        @classmethod
        def _null_is_empty(cls, value: Any) -> Any:
            return [] if value is None else value

        @model_validator(mode="after")
        def _check_no_moved_array(self) -> "ListTransactionsResponse.Payload":
            # A non-empty array under another key means the transactions
            # moved; fail instead of advancing the cursor past them.
            moved = [k for k, v in (self.model_extra or {}).items() if isinstance(v, list) and v]
            if moved:
                raise ValueError(f"unexpected arrays in listTransactions payload: {moved}")
            return self

    payload: Payload


class ProcessingStatus(StrEnum):
    CANCELLED = "CANCELLED"
    DONE = "DONE"
    FATAL = "FATAL"
    IN_PROGRESS = "IN_PROGRESS"
    IN_QUEUE = "IN_QUEUE"


class Report(BaseModel, extra="allow"):
    # getReports lists reports, and createReport requests one, at LIST_PATH.
    LIST_PATH: ClassVar[str] = "/reports/2021-06-30/reports"
    PATH: ClassVar[str] = "/reports/2021-06-30/reports/{reportId}"
    # getReports' maximum.
    PAGE_SIZE: ClassVar[int] = 100
    # Bounds on createdTime. Their inclusivity is unverified.
    SINCE_PARAM: ClassVar[str] = "createdSince"
    BEFORE_PARAM: ClassVar[str] = "createdUntil"

    reportId: str
    reportType: str
    processingStatus: ProcessingStatus
    createdTime: AwareDatetime
    dataStartTime: AwareDatetime | None = None
    dataEndTime: AwareDatetime | None = None
    marketplaceIds: list[str] | None = None
    reportDocumentId: str | None = None


class CreateReportResponse(BaseModel, extra="allow"):
    reportId: str


class GetReportsResponse(BaseModel, extra="allow"):
    reports: list[Report]
    nextToken: str | None = None


class ReportDocument(BaseModel, extra="allow"):
    PATH: ClassVar[str] = "/reports/2021-06-30/documents/{reportDocumentId}"

    reportDocumentId: str
    # Pre-signed, and expires five minutes after it is issued.
    url: str
    # An algorithm other than GZIP fails validation rather than being read
    # as plain text.
    compressionAlgorithm: Literal["GZIP"] | None = None


class ReportRow(BaseDocument, extra="allow"):
    """
    One data row of a report document. The row's own columns are carried as
    extra fields, as the strings the file holds; the declared fields come
    from the report's metadata.
    """

    REPORT_TYPE: ClassVar[str]

    reportId: str
    reportDocumentId: str
    reportCreatedTime: AwareDatetime
    dataStartTime: AwareDatetime | None
    dataEndTime: AwareDatetime | None
    marketplaceIds: list[str] | None
    # 1-based, counting data rows only. With reportId it keys the row, so
    # reading a report again overwrites what the first read captured.
    rowNumber: int


class SettlementReportRow(ReportRow):
    REPORT_TYPE: ClassVar[str] = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"

    # The first data row of a V2 settlement file carries the settlement's
    # totals and dates, with its transaction columns blank.
    summaryRow: bool


class AllOrdersRow(ReportRow):
    """
    One order item line of a report of orders by last update. Each report's
    version of a line is kept; the rows of an order's newest report are its
    current lines.
    """

    # Keyed by report and row number, as every report row is, not by an order
    # or item column: the same order and SKU can repeat within a file.
    # Amazon's column list names an order-item-id; whether every file carries
    # it is unverified.
    REPORT_TYPE: ClassVar[str] = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL"


# The backfill and the incremental task read the same recent days from
# separate reports and commit separately, so an older report's copy of a day
# can reach the collection after a newer one. Keeping the copy from the newest
# report makes the result independent of commit order. The CDK writes only
# `if` and `then`, so this `else` is the non-delete reduction.
class SalesAndTrafficByDate(
    BaseDocument,
    extra="allow",
    json_schema_extra={
        "else": {"reduce": {"strategy": "maximize", "key": ["/reportCreatedTime"]}}
    },
):
    """
    One day of a marketplace's catalog-wide sales and traffic totals: an
    entry of a Sales and Traffic report's salesAndTrafficByDate section,
    with the marketplace and report it was read from. A day read from
    several reports keeps the copy from the newest report.
    """

    REPORT_TYPE: ClassVar[str] = "GET_SALES_AND_TRAFFIC_REPORT"
    ITEMS_PREFIX: ClassVar[str] = "salesAndTrafficByDate.item"
    # CHILD, though this stream reads no ASIN entries, so a stream of those
    # could read the same reports.
    REPORT_OPTIONS: ClassVar[dict[str, str]] = {
        "dateGranularity": "DAY",
        "asinGranularity": "CHILD",
    }

    marketplaceId: str
    # The day the totals cover, as YYYY-MM-DD.
    date: str
    reportId: str
    reportCreatedTime: AwareDatetime

    @field_serializer("reportCreatedTime")
    def _sortable_report_created_time(self, value: datetime):
        return sortable_utc(value)


class SalesAndTrafficSpecification(BaseModel, extra="allow"):
    """The reportSpecification section of a Sales and Traffic report document."""

    reportOptions: dict[str, str]
    marketplaceIds: list[str]
    # Amazon ignores the time of day of a requested range.
    dataStartTime: date
    dataEndTime: date
