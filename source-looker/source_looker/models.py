from typing import ClassVar

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceOwnerPasswordOAuth2Credentials,
    OAuth2TokenFlowSpec,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    LogCursor,
    Logger,
)
from estuary_cdk.http import HTTPSession, TokenSource

from pydantic import BaseModel, Field

API_VERSION = "4.0"

SUBDOMAIN_REGEX = r"^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$"


# Unlike the "Bearer" header used by most other APIs when authenitcating with
# an access token received via an OAuth2 process, Looker uses the "Authorization"
# header and requires "token " to be prepended before the actual access token.
# This does not seem to be the standard, so I didn't build support for this into
# the CDK. If other APIs follow a similar pattern, we can abstract this into the
# CDK somehow.
class LookerTokenSource(TokenSource):
    async def fetch_token(self, log: Logger, session: HTTPSession) -> tuple[str, str]:
        _, access_token = await super().fetch_token(log, session)
        return ("Authorization", f"token {access_token}")

    # Similarly, Looker returns a `null` refresh_token instead of an empty string like the CDK expects.
    class AccessTokenResponse(TokenSource.AccessTokenResponse):
        refresh_token: None = None


OAUTH2_SPEC = OAuth2TokenFlowSpec(
    # The access token URL requires the user's Looker account domain. DOMAIN is replaced
    # at runtime with the domain configured by the user.
    accessTokenUrlTemplate=f"https://DOMAIN/api/{API_VERSION}/login",
    accessTokenResponseMap={
        "access_token": "/access_token",
        "expires_in": "/expires_in",
    },
)


# The class name appears in the UI's Authentication section, so we wrap the non-user friendly name in a slighly better name.
# TODO(alex): figure out why the class name is appearing in the UI & determine if there's some property to set that overrides it.
class OAuth2(ResourceOwnerPasswordOAuth2Credentials):
    pass


class EndpointConfig(BaseModel):
    subdomain: str = Field(
        description="The subdomain for your Looker account. For example in https://estuarydemo.cloud.looker.com/folders/home, estuarydemo.cloud.looker.com is the subdomain.",
        title="Looker Subdomain",
        pattern=SUBDOMAIN_REGEX,
    )
    credentials: OAuth2 = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

ConnectorState = GenericConnectorState[ResourceState]


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class LookMLModel(BaseModel, extra="allow"):
    name: str

    class LookmlModelNavExplore(BaseModel, extra="allow"):
        name: str

    explores: list[LookmlModelNavExplore]


class LookerStream():
    name: ClassVar[str]
    path: ClassVar[str]


class LookerChildStream(LookerStream):
    parent: ClassVar[type[LookerStream]]
    # If the connector's credentials do not have permission to view the child resource of a specific parent
    # resource, the parent's `can` property will contain another property indiciating this. For example,
    # a user's can.show_creds dictates whether or not the connector can successfully read the user's
    # embedded credentials.
    required_can_permission: ClassVar[str | None] = None 


class Dashboards(LookerStream):
    name: ClassVar[str] = "dashboards"
    path: ClassVar[str] = "dashboards"


class DashboardElements(LookerChildStream):
    name: ClassVar[str] = "dashboards_elements"
    path: ClassVar[str] = "dashboard_elements"
    parent: ClassVar[type[LookerStream]] = Dashboards


class Folders(LookerStream):
    name: ClassVar[str] = "folders"
    path: ClassVar[str] = "folders"


class Groups(LookerStream):
    name: ClassVar[str] = "groups"
    path: ClassVar[str] = "groups"


class LookMLModels(LookerStream):
    name: ClassVar[str] = "lookml_models"
    path: ClassVar[str] = "lookml_models"


class LookMLModelExplores(LookerChildStream):
    name: ClassVar[str] = "lookml_model_explores"
    path: ClassVar[str] = "lookml_model_explores"
    parent: ClassVar[type[LookerStream]] = LookMLModels


class Roles(LookerStream):
    name: ClassVar[str] = "roles"
    path: ClassVar[str] = "roles"


class Users(LookerStream):
    name: ClassVar[str] = "users"
    path: ClassVar[str] = "users"


class UserCredentialsEmbed(LookerStream):
    name: ClassVar[str] = "user_credentials_embed"
    path: ClassVar[str] = "credentials_embed"
    parent: ClassVar[type[LookerStream]] = Users
    required_can_permission: ClassVar[str] = "show_creds"


class UserRoles(LookerStream):
    name: ClassVar[str] = "user_roles"
    path: ClassVar[str] = "roles"
    parent: ClassVar[type[LookerStream]] = Users
    required_can_permission: ClassVar[str] = "show_details"


STREAMS = [
    {
        "stream": Dashboards,
        "children": [
            {"stream": DashboardElements}
        ]
    },
    {
        "stream": Folders,
    },
    {
        "stream": Groups,
    },
    {
        "stream": LookMLModels,
        "children": [
            {"stream": LookMLModelExplores}
        ]
    },
    {
        "stream": Roles,
    },
    {
        "stream": Users,
        "children": [
            {"stream": UserCredentialsEmbed},
            {"stream": UserRoles},
        ]
    }
]
