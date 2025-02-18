from pydantic import BaseModel, Field
from typing import TYPE_CHECKING, Optional

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    BaseDocument,
    AuthorizationCodeFlowOAuth2Credentials,
    OAuth2Spec,
    AccessToken,
    ResourceState,
)


OAUTH2_SPEC = OAuth2Spec(
    provider="monday",
    authUrlTemplate=(
        "https://auth.monday.com/oauth2/authorize?"
        r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate="https://auth.monday.com/oauth2/token",
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
    ),
    accessTokenResponseMap={
        "access_token": "/access_token",
    },
)


if TYPE_CHECKING:
    OAuth2Credentials = AuthorizationCodeFlowOAuth2Credentials
else:
    OAuth2Credentials = AuthorizationCodeFlowOAuth2Credentials.for_provider(
        OAUTH2_SPEC.provider
    )


class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials | AccessToken = Field(
        title="Authentication",
        discriminator="credentials_title",
    )


ConnectorState = GenericConnectorState[ResourceState]


class GraphQLResponse(BaseModel):
    data: Optional[dict] = None
    errors: Optional[list[dict]] = None


class GraphQLDocument(BaseDocument, extra="allow"):
    pass
