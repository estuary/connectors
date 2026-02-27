import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from logging import Logger
from typing import (
    Annotated,
    Any,
    AsyncGenerator,
    ClassVar,
    Generic,
    Literal,
    TypeVar,
)

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken, ClientCredentialsOAuth2Credentials
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    model_validator,
)

from .utils import dt_to_str, str_to_dt

# OAuth scopes requested during authorization.
# These are verified against Shopify's GraphQL Admin API 2025-04 documentation.
# See: https://shopify.dev/docs/api/usage/access-scopes#authenticated-access-scopes
scopes = [
    "read_locales",
    "read_products",  # Products, ProductVariants, Collections, InventoryItems
    "read_orders",  # Orders, AbandonedCheckouts, Fulfillments, Transactions, Refunds, Risks
    "read_locations",  # Locations
    "read_inventory",  # InventoryItems, InventoryLevels, Locations
    "read_customers",  # Customers
    "read_own_subscription_contracts",  # SubscriptionContracts
    # FulfillmentOrder requires one of the following 4 scopes (not read_fulfillments).
    # See: https://shopify.dev/docs/api/admin-graphql/latest/objects/FulfillmentOrder
    "read_assigned_fulfillment_orders",
    "read_merchant_managed_fulfillment_orders",
    "read_third_party_fulfillment_orders",
    "read_marketplace_fulfillment_orders",
]

# TODO(justin): OAuth support is temporarily removed for multi-store. Will revisit once the
# OAuth app is approved and UI/runtime is ready to support OAuth inside array items with credentials.
# See git history for the previous OAUTH2_SPEC implementation.


class ShopifyClientCredentials(ClientCredentialsOAuth2Credentials):
    """Credentials for the Shopify client_credentials grant flow.

    Used by Dev Dashboard custom apps (post-Jan 2026) where the app developer
    and store owner are the same organization. The CDK's TokenSource exchanges
    client_id + client_secret for a short-lived access token (~24 hours)
    at runtime, with automatic refresh at 75% of the token lifetime.

    Inherits client_id, client_secret, and grant_type="client_credentials"
    from ClientCredentialsOAuth2Credentials / _BaseOAuth2CredentialsData.

    References:
        https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/client-credentials-grant
    """

    # This configuration provides a "title" annotation for the UI to display
    # instead of the class name.
    model_config = ConfigDict(
        title="Client Credentials",
    )

    credentials_title: Literal["Client Credentials"] = Field(  # type: ignore[assignment]
        default="Client Credentials",
        json_schema_extra={"type": "string"},
    )


def default_start_date() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=30)


class StoreConfig(BaseModel):
    """Configuration for a single Shopify store."""

    store: str = Field(
        title="Store Name",
        description="Shopify store name (the prefix of your admin URL, e.g., 'mystore' for mystore.myshopify.com)",
        min_length=1,
    )
    credentials: ShopifyClientCredentials | AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


class EndpointConfig(BaseModel):
    stores: list[StoreConfig] = Field(
        title="Shopify Stores",
        description="One or more Shopify stores to capture. Each store requires its own credentials.",
        min_length=1,
    )

    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )

    class Advanced(BaseModel):
        window_size: Annotated[
            timedelta,
            Field(
                description="Window size for incremental streams in ISO 8601 format. ex: P30D means 30 days, PT6H means 6 hours.",
                title="Window Size",
                default=timedelta(days=30),
                ge=timedelta(minutes=1),
            ),
        ]
        should_use_composite_key: bool = Field(
            default=False,
            title="Use Composite Key",
            description=(
                "Controls whether collection keys include the store identifier (/_meta/store). "
                "Enabled by default for new captures. Automatically set to false for legacy "
                "single-store captures during migration. Must be set to true (with a full "
                "backfill / dataflow reset of all bindings) before adding additional stores "
                "to a legacy capture."
            ),
        )

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

    _was_migrated: bool = PrivateAttr(default=False)
    _legacy_store: str | None = PrivateAttr(default=None)

    @model_validator(mode="wrap")
    @classmethod
    def transform_legacy_config(
        cls,
        data: Any,
        handler: ValidatorFunctionWrapHandler,
    ) -> "EndpointConfig":
        was_migrated = False
        legacy_store: str | None = None

        if isinstance(data, dict):
            # Migrate flat config to stores list
            if "stores" not in data and "store" in data and "credentials" in data:
                data = data.copy()
                legacy_store = data.pop("store")
                credentials = data.pop("credentials")
                data["stores"] = [{"store": legacy_store, "credentials": credentials}]
                advanced = data.get("advanced", {})
                if isinstance(advanced, dict):
                    advanced["should_use_composite_key"] = False
                    data["advanced"] = advanced
                was_migrated = True

        instance = handler(data)
        instance._was_migrated = was_migrated
        instance._legacy_store = legacy_store

        return instance

    @model_validator(mode="after")
    def validate_unique_store_names(self) -> "EndpointConfig":
        counts = Counter(s.store for s in self.stores)
        duplicates = [name for name, count in counts.items() if count > 1]
        if duplicates:
            raise ValueError(
                f"Duplicate store names are not allowed: {', '.join(duplicates)}"
            )
        return self


ConnectorState = GenericConnectorState[ResourceState]


T = TypeVar("T")
TShopifyGraphQLResource = TypeVar(
    "TShopifyGraphQLResource", bound="ShopifyGraphQLResource"
)


class GraphQLErrorCode(StrEnum):
    UNDEFINED_FIELD = "undefinedField"
    THROTTLED = "THROTTLED"
    ACCESS_DENIED = "ACCESS_DENIED"
    SHOP_INACTIVE = "SHOP_INACTIVE"
    INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"
    MAX_COST_EXCEEDED = "MAX_COST_EXCEEDED"
    BAD_REQUEST = "BAD_REQUEST"


class GraphQLError(BaseModel, extra="allow"):
    class Extensions(BaseModel, extra="allow"):
        code: GraphQLErrorCode
        typeName: str | None = None
        fieldName: str | None = None
        cost: int | None = None
        maxCost: int | None = None
        documentation: str | None = None

    message: str
    extensions: Extensions


class GraphQLResponse(BaseModel, Generic[T], extra="allow"):
    class Extensions(BaseModel, extra="allow"):
        class Cost(BaseModel, extra="allow"):
            class ThrottleStatus(BaseModel, extra="allow"):
                maximumAvailable: int
                currentlyAvailable: int
                restoreRate: int

            throttleStatus: ThrottleStatus

        cost: Cost

    data: T | None = None
    extensions: Extensions | None = None
    errors: list[GraphQLError] | None = None


class BulkOperationTypes(StrEnum):
    MUTATION = "MUTATION"
    QUERY = "QUERY"


class BulkOperationStatuses(StrEnum):
    CANCELED = "CANCELED"
    CANCELING = "CANCELING"
    COMPLETED = "COMPLETED"
    CREATED = "CREATED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"
    RUNNING = "RUNNING"


class BulkOperationErrorCodes(StrEnum):
    ACCESS_DENIED = "ACCESS_DENIED"
    INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"
    TIMEOUT = "TIMEOUT"


class BulkOperationUserErrorCodes(StrEnum):
    INVALID = "INVALID"
    OPERATION_IN_PROGRESS = "OPERATION_IN_PROGRESS"


class BulkOperationDetails(BaseModel, extra="allow"):
    type: BulkOperationTypes
    id: str
    status: BulkOperationStatuses
    createdAt: str
    completedAt: str | None
    url: str | None
    errorCode: BulkOperationErrorCodes | None


class UserErrors(BaseModel, extra="allow"):
    field: str | list[str] | None
    message: str


class BulkOperationUserErrors(UserErrors):
    code: BulkOperationUserErrorCodes


class BulkOperationCancel(BaseModel, extra="forbid"):
    bulkOperation: BulkOperationDetails
    userErrors: list[UserErrors] | None


class BulkOperationRunQuery(BaseModel, extra="forbid"):
    bulkOperation: BulkOperationDetails | None
    userErrors: list[BulkOperationUserErrors]


class BulkCancelData(BaseModel, extra="forbid"):
    bulkOperationCancel: BulkOperationCancel


class BulkCurrentData(BaseModel, extra="forbid"):
    currentBulkOperation: BulkOperationDetails | None


class BulkSpecificData(BaseModel, extra="forbid"):
    node: BulkOperationDetails


class BulkOperationEdge(BaseModel, extra="forbid"):
    node: BulkOperationDetails


class BulkOperationsConnection(BaseModel, extra="forbid"):
    edges: list[BulkOperationEdge]


class BulkOperationsData(BaseModel, extra="forbid"):
    """Response model for the bulkOperations query (API 2026-01+)."""

    bulkOperations: BulkOperationsConnection


class BulkSubmitData(BaseModel, extra="forbid"):
    bulkOperationRunQuery: BulkOperationRunQuery


# Names of Shopify plan types. Some plan types do not have access
# to certain resources (ex: BASIC and STARTER plans cannot access PII, like customer data).
class PlanName(StrEnum):
    STARTER = "Starter"
    BASIC = "Basic"
    SHOPIFY = "Shopify"
    ADVANCED = "Advanced"
    PLUS = "Plus"
    SHOPIFY_PLUS = "Shopify Plus"


class ShopDetails(BaseModel, extra="allow"):
    class Data(BaseModel, extra="forbid"):
        class Shop(BaseModel, extra="forbid"):
            class Plan(BaseModel, extra="forbid"):
                # The displayName field will be deprecated in the future,
                # but its replacement publicDisplayName is not available
                # on the current API version 2025-04.
                displayName: str
                partnerDevelopment: bool
                shopifyPlus: bool

            plan: Plan

        shop: Shop

    data: Data

    @staticmethod
    def query() -> str:
        return """
        {
            shop {
                plan {
                    displayName
                    partnerDevelopment
                    shopifyPlus
                }
            }
        }
        """


class AccessScopes(BaseModel, extra="allow"):
    """Model for querying currentAppInstallation to determine granted scopes."""

    class Data(BaseModel, extra="forbid"):
        class AppInstallation(BaseModel, extra="forbid"):
            class AccessScope(BaseModel, extra="forbid"):
                handle: str

            accessScopes: list[AccessScope]

        currentAppInstallation: AppInstallation

    data: Data

    @staticmethod
    def query() -> str:
        return """
        {
            currentAppInstallation {
                accessScopes {
                    handle
                }
            }
        }
        """

    def get_scope_handles(self) -> set[str]:
        return {s.handle for s in self.data.currentAppInstallation.accessScopes}


class SortKey(StrEnum):
    CREATED_AT = "CREATED_AT"
    UPDATED_AT = "UPDATED_AT"


@dataclass(frozen=True, slots=True)
class StoreValidationContext:
    """Validation context carrying the store name for document construction."""

    store: str


class ShopifyGraphQLResource(BaseDocument):
    QUERY: ClassVar[str] = ""
    QUERY_ROOT: ClassVar[str] = ""
    FRAGMENTS: ClassVar[list[str]] = []
    NAME: ClassVar[str] = ""
    SORT_KEY: ClassVar[SortKey | None] = None
    SHOULD_USE_BULK_QUERIES: ClassVar[bool] = True
    QUALIFYING_SCOPES: ClassVar[set[str]] = set()

    model_config = ConfigDict(extra="allow", validate_assignment=True)

    class Meta(BaseDocument.Meta):
        model_config = ConfigDict(validate_assignment=True)

        store: str = Field(
            default="",
            description="The Shopify store this document belongs to",
        )

    meta_: Meta = Field(  # type: ignore[override]
        default_factory=lambda: ShopifyGraphQLResource.Meta(op="u"),
        alias="_meta",
        description="Document metadata",
    )

    @model_validator(mode="before")
    @classmethod
    def _inject_store_from_context(cls, data: Any, info: ValidationInfo) -> Any:
        """Inject store name from validation context into document metadata.

        Uses mode="before" so the store is set on the raw dict before Pydantic constructs
        the model. Combined with validate_assignment=True on Meta, this ensures the store
        field is tracked in model_fields_set and survives model_dump(exclude_unset=True).
        """
        if not isinstance(data, dict):
            return data
        if info.context and isinstance(info.context, StoreValidationContext):
            meta = data.get("_meta") or {}
            meta["store"] = info.context.store
            data["_meta"] = meta
        return data

    id: str

    def get_cursor_value(self) -> AwareDatetime:
        if self.SORT_KEY is None or self.SORT_KEY == SortKey.UPDATED_AT:
            field_name = "updatedAt"
        else:
            field_name = "createdAt"

        raw_value = getattr(self, field_name)

        if isinstance(raw_value, str):
            return str_to_dt(raw_value)
        elif isinstance(raw_value, datetime):
            return raw_value
        else:
            raise ValueError(f"Expected datetime string, got {type(raw_value)}")

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        raise NotImplementedError("build_query method must be implemented")

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        raise NotImplementedError("process_result method must be implemented")

    @staticmethod
    async def _process_result(
        log: Logger, lines: AsyncGenerator[bytes, None], parent_id: str
    ) -> AsyncGenerator[dict, None]:
        current_record = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if parent_id in id:
                if current_record:
                    yield current_record

                current_record = record

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_record:
            yield current_record

    @classmethod
    def build_query_with_fragment(
        cls,
        start: datetime,
        end: datetime,
        query: str = "",
        first: int | None = None,
        after: str | None = None,
        includeLegacyId: bool = True,
        includeCreatedAt: bool = True,
        includeUpdatedAt: bool = True,
    ) -> str:
        lower_bound = dt_to_str(start)
        upper_bound = dt_to_str(end)

        query = f"""
        {{
            {cls.QUERY_ROOT}(
                query: "updated_at:>='{lower_bound}' AND updated_at:<='{upper_bound}' {
            "AND " + query.strip() if query else ""
        }"
                {f"sortKey: {cls.SORT_KEY}" if cls.SORT_KEY else ""}
                {f"first: {first}" if first else ""}
                {f'after: "{after}"' if after else ""}
            ) {{
                edges {{
                    node {{
                        id
                        {"legacyResourceId" if includeLegacyId else ""}
                        {"createdAt" if includeCreatedAt else ""}
                        {"updatedAt" if includeUpdatedAt else ""}
                        {cls.QUERY}
                    }}
                }}
                {
            f'''pageInfo {{
                    hasNextPage
                    endCursor
                }}'''
            if first
            else ""
        }
            }}
        }}
        """

        for fragment in cls.FRAGMENTS:
            query += fragment

        return query


class PageInfo(BaseModel):
    endCursor: str | None = None
    hasNextPage: bool = False


class Edge(BaseModel, Generic[TShopifyGraphQLResource]):
    node: TShopifyGraphQLResource


class EdgesData(BaseModel, Generic[TShopifyGraphQLResource]):
    edges: list[Edge[TShopifyGraphQLResource]]
    pageInfo: PageInfo = Field(default_factory=PageInfo)


class BaseResponseData(BaseModel, Generic[TShopifyGraphQLResource]):
    model_config = {"extra": "allow"}

    @property
    def edges(self) -> list[Edge[TShopifyGraphQLResource]]:
        raise NotImplementedError("edges property must be implemented by subclass")

    @property
    def page_info(self) -> PageInfo:
        raise NotImplementedError("page_info property must be implemented by subclass")

    @property
    def nodes(self) -> list[TShopifyGraphQLResource]:
        return [edge.node for edge in self.edges]


# create_response_data_model dynamically creates a model for the data field of
# each response. Models are created dynamically since the query root/field name
# under the "data" field is different for each query.
def create_response_data_model(
    resource_type: type[TShopifyGraphQLResource],
) -> type[BaseResponseData[TShopifyGraphQLResource]]:
    """Factory function to create typed GraphQL data field models."""
    query_root = resource_type.QUERY_ROOT

    def get_edges(self) -> list[Edge[TShopifyGraphQLResource]]:
        return getattr(self, query_root).edges

    def get_page_info(self) -> PageInfo:
        return getattr(self, query_root).pageInfo

    class_dict = {
        "__annotations__": {query_root: EdgesData[resource_type]},
        "edges": property(get_edges),
        "page_info": property(get_page_info),
    }

    DataModel = type(
        f"{query_root.title()}Data", (BaseResponseData[resource_type],), class_dict
    )

    return DataModel
