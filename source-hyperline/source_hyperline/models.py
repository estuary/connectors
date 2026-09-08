from datetime import UTC, datetime, timedelta
from typing import ClassVar, Literal

import xxhash
from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken
from pydantic import AwareDatetime, BaseModel, Field, model_validator


class ApiKey(AccessToken):
    credentials_title: Literal["API Key"] = Field(
        default="API Key",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="API Key",
        description="Hyperline API key. Keys prefixed with test_ are routed to the sandbox environment (sandbox.api.hyperline.co).",
        json_schema_extra={"secret": True, "order": 1},
    )


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


def format_rfc3339_ms(dt: datetime) -> str:
    """Render a datetime the way Hyperline stamps and filters timestamps:
    RFC3339, UTC `Z`, exactly millisecond precision."""
    dt = dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


class PageMeta(BaseModel):
    # `taken` inconsistently echoes the requested `take` on empty result sets,
    # so it must never serve as a completion signal; a short page does.
    total: int
    taken: int
    skipped: int


class OffsetRemainder(BaseModel):
    meta: PageMeta


class CursorRemainder(BaseModel):
    next_cursor: str | None
    has_more: bool


class BaseIncrementalResource(BaseDocument, extra="allow"):
    NAME: ClassVar[str]
    PATH: ClassVar[str]
    ENGINE: ClassVar[Literal["cursor", "offset"]]
    KEY: ClassVar[list[str]] = ["/id"]

    # All incremental endpoints share one filter grammar over `updated_at`.
    # Inclusivity per operator: __gte/__lte inclusive, __gt/__lt exclusive.
    SINCE_EXCLUSIVE: ClassVar[str] = "updated_at__gt"
    SINCE_INCLUSIVE: ClassVar[str] = "updated_at__gte"
    UNTIL_INCLUSIVE: ClassVar[str] = "updated_at__lte"
    UNTIL_EXCLUSIVE: ClassVar[str] = "updated_at__lt"


class Customer(BaseIncrementalResource):
    NAME: ClassVar[str] = "customers"
    PATH: ClassVar[str] = "/v2/customers"
    ENGINE: ClassVar[Literal["cursor", "offset"]] = "cursor"

    id: str
    created_at: AwareDatetime
    updated_at: AwareDatetime


class Invoice(BaseIncrementalResource):
    NAME: ClassVar[str] = "invoices"
    PATH: ClassVar[str] = "/v2/invoices"
    ENGINE: ClassVar[Literal["cursor", "offset"]] = "cursor"

    id: str
    # Spec-nullable. A null-updated_at invoice would be invisible to every
    # window; none observed live, and the null partition can't be queried
    # (updated_at__isNull is rejected). Revisit with a scheduled backfill if
    # null rows ever surface in production accounts.
    updated_at: AwareDatetime | None


class Subscription(BaseIncrementalResource):
    NAME: ClassVar[str] = "subscriptions"
    PATH: ClassVar[str] = "/v2/subscriptions"
    ENGINE: ClassVar[Literal["cursor", "offset"]] = "offset"

    id: str
    created_at: AwareDatetime
    updated_at: AwareDatetime


class Quote(BaseIncrementalResource):
    NAME: ClassVar[str] = "quotes"
    PATH: ClassVar[str] = "/v1/quotes"
    ENGINE: ClassVar[Literal["cursor", "offset"]] = "offset"

    id: str
    updated_at: AwareDatetime


class CustomerCredit(BaseIncrementalResource):
    NAME: ClassVar[str] = "customer_credits"
    PATH: ClassVar[str] = "/v1/customers/credits"
    ENGINE: ClassVar[Literal["cursor", "offset"]] = "offset"
    # Credits have no id of their own; a credit balance is identified by the
    # (customer, product) pair it belongs to.
    KEY: ClassVar[list[str]] = ["/customer_id", "/product_id"]

    customer_id: str
    product_id: str
    updated_at: AwareDatetime


INCREMENTAL_RESOURCES: list[type[BaseIncrementalResource]] = [
    Customer,
    Invoice,
    Subscription,
    Quote,
    CustomerCredit,
]


class BaseSnapshotResource(BaseDocument, extra="allow"):
    """Full-refresh streams declare no required document fields: snapshot
    bindings write against `BaseDocument` with the default `/_meta/row_id` key,
    so an unexpected shape widens the inferred schema instead of failing
    validation."""

    NAME: ClassVar[str]
    PATH: ClassVar[str]
    EXTRA_QUERY: ClassVar[dict[str, str]] = {}


class Product(BaseSnapshotResource):
    NAME: ClassVar[str] = "products"
    PATH: ClassVar[str] = "/v1/products"
    # Cover the archived partition too; the bare default is unobservable.
    EXTRA_QUERY: ClassVar[dict[str, str]] = {"status": "all"}


class PriceBook(BaseSnapshotResource):
    NAME: ClassVar[str] = "price_books"
    PATH: ClassVar[str] = "/v1/price-books"


class PriceConfiguration(BaseSnapshotResource):
    NAME: ClassVar[str] = "price_configurations"
    PATH: ClassVar[str] = "/v1/price-configurations"


class Feature(BaseSnapshotResource):
    NAME: ClassVar[str] = "features"
    PATH: ClassVar[str] = "/v1/features"
    EXTRA_QUERY: ClassVar[dict[str, str]] = {"status": "all"}


class Coupon(BaseSnapshotResource):
    NAME: ClassVar[str] = "coupons"
    PATH: ClassVar[str] = "/v1/coupons"


class PromotionCode(BaseSnapshotResource):
    NAME: ClassVar[str] = "promotion_codes"
    PATH: ClassVar[str] = "/v1/promotion-codes"


class TaxRate(BaseSnapshotResource):
    NAME: ClassVar[str] = "tax_rates"
    PATH: ClassVar[str] = "/v1/taxes/rates"


class InvoicingEntity(BaseSnapshotResource):
    NAME: ClassVar[str] = "invoicing_entities"
    PATH: ClassVar[str] = "/v1/invoicing-entities"


class User(BaseSnapshotResource):
    NAME: ClassVar[str] = "users"
    PATH: ClassVar[str] = "/v1/users"


class CustomerSegment(BaseSnapshotResource):
    NAME: ClassVar[str] = "customer_segments"
    PATH: ClassVar[str] = "/v1/customers/segments"


class Transaction(BaseSnapshotResource):
    # Snapshot is the only viable strategy: documents mutate in place (status,
    # refunded_at, last_refreshed_at) and the endpoint honors no filter or sort
    # that could serve as a cursor. Unbounded growth is a conscious acceptance;
    # the engine logs meta.total each sweep so growth is observable.
    NAME: ClassVar[str] = "transactions"
    PATH: ClassVar[str] = "/v1/transactions"


class Wallet(BaseSnapshotResource):
    NAME: ClassVar[str] = "wallets"
    PATH: ClassVar[str] = "/v2/wallets"


class SubscriptionTransition(BaseSnapshotResource):
    # No updated_at exists; transition_date/transitioned_at are nullable and
    # date-granularity, and rows mutate while pending rows carry null
    # timestamps — any range filter silently drops them. The dataset is
    # intrinsically tiny (transitions are rare events), and a snapshot also
    # infers deletions of cancelled scheduled transitions.
    NAME: ClassVar[str] = "subscription_transitions"
    PATH: ClassVar[str] = "/v2/subscriptions/transitions"


SNAPSHOT_RESOURCES: list[type[BaseSnapshotResource]] = [
    Product,
    PriceBook,
    PriceConfiguration,
    Feature,
    Coupon,
    PromotionCode,
    TaxRate,
    InvoicingEntity,
    User,
    CustomerSegment,
    Transaction,
    Wallet,
    SubscriptionTransition,
]


class AuditLog(BaseDocument, extra="allow"):
    NAME: ClassVar[str] = "audit_logs"
    PATH: ClassVar[str] = "/v1/audit-logs"
    KEY: ClassVar[list[str]] = ["/_meta/estuary_id"]

    class Meta(BaseDocument.Meta):
        estuary_id: str = Field(
            default="",
            description="Surrogate key derived from the event's characteristic fields; audit-log events carry no id on the wire.",
        )

    meta_: Meta = Field(  # pyright: ignore[reportIncompatibleVariableOverride]
        default_factory=lambda: AuditLog.Meta(op="u"),
        alias="_meta",
        description="Document metadata",
    )

    type: str
    happened_at: AwareDatetime
    customer_id: str | None
    subscription_id: str | None
    invoice_id: str | None
    quote_id: str | None
    # email_status (present on email.created events) is deliberately excluded
    # from the model and the surrogate hash: if it mutates in place, hashing it
    # would mint a new identity per status flip while the walk-back can't
    # re-observe old rows. extra="allow" still carries it on the document.

    @model_validator(mode="after")
    def _compute_surrogate_key(self) -> "AuditLog":
        parts = [
            self.type,
            format_rfc3339_ms(self.happened_at),
            self.customer_id or "",
            self.subscription_id or "",
            self.invoice_id or "",
            self.quote_id or "",
        ]
        self.meta_.estuary_id = xxhash.xxh128("|".join(parts).encode()).hexdigest()
        return self
