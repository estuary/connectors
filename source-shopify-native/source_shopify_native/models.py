from datetime import datetime, UTC, timedelta
from enum import StrEnum
from typing import Annotated, TYPE_CHECKING, AsyncGenerator
from logging import Logger

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
from pydantic import AwareDatetime, BaseModel, Field


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
    credentials: OAuth2Credentials | AccessToken = Field(
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


class UserErrors(BaseModel, extra="forbid"):
    field: str | None
    message: str
    code: BulkOperationUserErrorCodes


class BulkJobCancelResponse(BaseModel, extra="allow"):
    class Data(BaseModel, extra="forbid"):
        class BulkOperationCancel(BaseModel, extra="forbid"):
            bulkOperation: BulkOperationDetails
            userErrors: list[UserErrors]

        bulkOperationCancel: BulkOperationCancel

    data: Data


class BulkCurrentJobResponse(BaseModel, extra="allow"):
    class Data(BaseModel, extra="forbid"):
        currentBulkOperation: BulkOperationDetails | None

    data: Data


class BulkSpecificJobResponse(BaseModel, extra="allow"):
    class Data(BaseModel, extra="forbid"):
        node: BulkOperationDetails

    data: Data


class BulkJobSubmitResponse(BaseModel, extra="allow"):
    class Data(BaseModel, extra="forbid"):
        class BulkOperationRunQuery(BaseModel, extra="forbid"):
            bulkOperation: BulkOperationDetails | None
            userErrors: list[UserErrors]

        bulkOperationRunQuery: BulkOperationRunQuery

    data: Data


class ShopifyGraphQlResource(BaseDocument, extra="allow"):
    @staticmethod
    def build_query(start: datetime, end: datetime) -> str | list[str]:
        raise NotImplementedError("build_query method must be implemented")

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        raise NotImplementedError("process_result method must be implemented")


class FullRefreshResource(ShopifyGraphQlResource, extra="allow"):
    pass


class IncrementalResource(ShopifyGraphQlResource, extra="allow"):
    id: str
    updatedAt: AwareDatetime
