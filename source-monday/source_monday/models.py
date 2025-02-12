from pydantic import BaseModel, Field

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    BaseDocument,
    ClientCredentialsOAuth2Credentials,
    ClientCredentialsOAuth2Spec,
    AccessToken,
    ResourceState,
)


def urlencode_field(field: str):
    return "{{#urlencode}}{{{ " + field + " }}}{{/urlencode}}"


OAUTH2_SPEC = ClientCredentialsOAuth2Spec(
    accessTokenUrlTemplate="https://auth.monday.com/oauth2/token",
    accessTokenResponseMap={"access_token": "/access_token"},
)


# The class name appears in the UI's Authentication section, so we wrap the non-user friendly name in a slighly better name.
# TODO(alex): figure out why the class name is appearing in the UI & determine if there's some property to set that overrides it.
class OAuth(ClientCredentialsOAuth2Credentials):
    pass


class EndpointConfig(BaseModel):
    subdomain: str = Field(
        title="Subdomain",
        description="This is your Monday subdomain that can be found in your Monday account URL. For example, in https://{MY_SUBDOMAIN}.monday.com, where MY_SUBDOMAIN is the value of your subdomain.",
    )
    credentials: OAuth | AccessToken = Field(
        title="Authentication",
        discriminator="credentials_title",
    )


ConnectorState = GenericConnectorState[ResourceState]


class Board(BaseDocument, extra="allow"):
    pass
