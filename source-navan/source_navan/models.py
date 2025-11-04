from typing import (
    Generic,
    TypeVar,
)

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import (
    ClientCredentialsOAuth2Credentials,
    OAuth2TokenFlowSpec,
)
from pydantic import (
    BaseModel,
    Field,
)
from pydantic.types import AwareDatetime


ConnectorState = GenericConnectorState[ResourceState]

OAUTH2_SPEC = OAuth2TokenFlowSpec(
    accessTokenUrlTemplate="https://api.navan.com/ta-auth/oauth/token",
    accessTokenResponseMap={"access_token": "/access_token"},
)


class EndpointConfig(BaseModel):
    credentials: ClientCredentialsOAuth2Credentials = Field(
        title="Authentication",
        description="See https://app.navan.com/app/helpcenter/articles/travel/admin/other-integrations/booking-data-integration",
    )


TData = TypeVar("TData", bound=BaseModel)


class ApiResponse(BaseModel, Generic[TData], extra="allow"):
    class Page(BaseModel, extra="allow"):
        totalPages: int
        currentPage: int
        pageSize: int
        totalElements: int

    data: list[TData]
    page: Page | None = None


class Booking(BaseDocument, extra="allow"):
    uuid: str
    lastModified: AwareDatetime
