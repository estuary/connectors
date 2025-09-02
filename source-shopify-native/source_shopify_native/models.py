import json
from datetime import datetime, UTC, timedelta
from enum import StrEnum
from logging import Logger
from typing import (
    Annotated,
    AsyncGenerator,
    TYPE_CHECKING,
    ClassVar,
    Literal,
    Any,
    TypeVar,
    Generic,
)
from pydantic import AwareDatetime, BaseModel, Field

from estuary_cdk.capture.common import (
    AccessToken,
    BaseDocument,
    LongLivedClientCredentialsOAuth2Credentials,
    OAuth2Spec,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from .graphql.common import dt_to_str


scopes = [
    "read_locales",
    "read_products",
    "read_orders",
    "read_checkouts",
    "read_locations",
    "read_inventory",
    "read_fulfillments",
    "read_customers",
    "read_publications",
]

OAUTH2_SPEC = OAuth2Spec(
    provider="shopify",
    authUrlTemplate=(
        r"https://{{{ config.store }}}.myshopify.com/admin/oauth/authorize"
        r"?client_id={{{#urlencode}}}{{{ client_id }}}{{{/urlencode}}}"
        "&scope="
        + ",".join(scopes)
        + r"&state={{{#urlencode}}}{{{ state }}}{{{/urlencode}}}"
        r"&redirect_uri={{{#urlencode}}}{{{ redirect_uri }}}{{{/urlencode}}}"
    ),
    accessTokenUrlTemplate=(
        r"https://{{{ config.store }}}.myshopify.com/admin/oauth/access_token"
        r"?client_id={{{#urlencode}}}{{{ client_id }}}{{{/urlencode}}}"
        r"&client_secret={{{#urlencode}}}{{{ client_secret }}}{{{/urlencode}}}"
        r"&code={{{#urlencode}}}{{{ code }}}{{{/urlencode}}}"
    ),
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        r"client_id={{{#urlencode}}}{{{ client_id }}}{{{/urlencode}}}"
        r"&client_secret={{{#urlencode}}}{{{ client_secret }}}{{{/urlencode}}}"
        r"&code={{{#urlencode}}}{{{ code }}}{{{/urlencode}}}"
    ),
    accessTokenResponseMap={
        "access_token": "/access_token",
    },
)


if TYPE_CHECKING:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials.for_provider(
        OAUTH2_SPEC.provider
    )


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    store: str = Field(
        title="Shopify Store",
        description="Shopify store ID. Use the prefix of your admin URL e.g. https://{YOUR_STORE}.myshopify.com/admin",
    )
    credentials: AccessToken | OAuth2Credentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )

    class Advanced(BaseModel):
        window_size: Annotated[
            int,
            Field(
                description="Window size in days for incremental streams.",
                title="Window Size",
                default=30,
                gt=0,
            ),
        ]

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


T = TypeVar('T')


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


class BulkSubmitData(BaseModel, extra="forbid"):
    bulkOperationRunQuery: BulkOperationRunQuery


BulkJobCancelResponse = GraphQLResponse[BulkCancelData]
BulkCurrentJobResponse = GraphQLResponse[BulkCurrentData]  
BulkSpecificJobResponse = GraphQLResponse[BulkSpecificData]
BulkJobSubmitResponse = GraphQLResponse[BulkSubmitData]


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


class SortKey(StrEnum):
    CREATED_AT = "CREATED_AT"
    UPDATED_AT = "UPDATED_AT"


class ShopifyGraphQLResource(BaseDocument, extra="allow"):
    QUERY: ClassVar[str] = ""
    QUERY_ROOT: ClassVar[str] = ""
    FRAGMENTS: ClassVar[list[str]] = []
    NAME: ClassVar[str] = ""
    SORT_KEY: ClassVar[SortKey | None] = None

    id: str

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
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
        includeLegacyId: bool = True,
        includeCreatedAt: bool = True,
        includeUpdatedAt: bool = True,
    ) -> str:
        lower_bound = dt_to_str(start)
        upper_bound = dt_to_str(end)

        query = f"""
        {{
            {cls.QUERY_ROOT}(
                query: "updated_at:>='{lower_bound}' AND updated_at:<='{upper_bound}' {"AND " + query.strip() if query else ""}"
                {f"sortKey: {cls.SORT_KEY}" if cls.SORT_KEY else ""}
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
            }}
        }}
        """

        for fragment in cls.FRAGMENTS:
            query += fragment

        return query
