from datetime import datetime, UTC
from typing import TYPE_CHECKING, ClassVar, Literal

from pydantic import AwareDatetime, BaseModel, Field

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import (
    ClientCredentialsOAuth2Credentials,
    OAuth2ClientCredentialsPlacement,
    OAuth2TokenFlowSpec,
)


ConnectorState = GenericConnectorState[ResourceState]


# commercetools was founded in 2006, so a start date before then captures all available data.
def default_start_date():
    dt = datetime(year=2006, month=1, day=1, tzinfo=UTC)
    return dt


Region = Literal[
    "us-central1.gcp",
    "us-east-2.aws",
    "europe-west1.gcp",
    "eu-central-1.aws",
    "australia-southeast1.gcp",
]


# commercetools sends client credentials as HTTP Basic on the token request, which is
# what the HEADERS placement produces.
if TYPE_CHECKING:
    OAuth2Credentials = ClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = (
        ClientCredentialsOAuth2Credentials.with_client_credentials_placement(
            OAuth2ClientCredentialsPlacement.HEADERS
        )
    )


def oauth2_spec(region: Region) -> OAuth2TokenFlowSpec:
    return OAuth2TokenFlowSpec(
        accessTokenUrlTemplate=f"https://auth.{region}.commercetools.com/oauth/token",
        accessTokenResponseMap={"access_token": "/access_token"},
    )


class EndpointConfig(BaseModel):
    region: Region = Field(
        description="The region hosting your commercetools Project. Visible in the API URL shown in the Merchant Center under Settings > Developer settings.",
        title="Region",
    )
    project_key: str = Field(
        description="The key of your commercetools Project. Visible in the Merchant Center under Settings > Project settings.",
        title="Project Key",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data modified before this date will not be replicated. If left blank, all available data will be captured.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: OAuth2Credentials = Field(
        title="Authentication",
        description="commercetools API Client credentials, created in the Merchant Center under Settings > Developer settings.",
    )


class TokenIntrospection(BaseModel, extra="allow"):
    """A token introspection response (RFC 7662).

    `scope` is space-delimited and absent altogether on an inactive token.
    """

    active: bool
    scope: str = ""


class CommercetoolsResource(BaseDocument, extra="allow"):
    """A resource returned by one of commercetools' Query endpoints.

    Every Query endpoint uses the same `PagedQueryResponse` envelope and gives its
    resources the same `id` / `lastModifiedAt` pair, so one implementation serves all
    of them. Only those two are declared; the rest is left to schema inference.
    """

    # Path segment under the Project, which is also the binding name.
    PATH: ClassVar[str]

    id: str
    lastModifiedAt: AwareDatetime

    def get_cursor(self) -> datetime:
        return self.lastModifiedAt


class Order(CommercetoolsResource):
    PATH: ClassVar[str] = "orders"


class Customer(CommercetoolsResource):
    PATH: ClassVar[str] = "customers"


class Payment(CommercetoolsResource):
    PATH: ClassVar[str] = "payments"


STREAMS: list[type[CommercetoolsResource]] = [Order, Customer, Payment]


class PagedQueryResponse(BaseModel, extra="allow"):
    """What a Query response carries outside its streamed `results` array."""

    limit: int
    offset: int
    count: int
