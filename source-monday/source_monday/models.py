from logging import Logger
from typing import TYPE_CHECKING, Annotated, AsyncGenerator, Callable, Optional

from estuary_cdk.capture.common import (
    AccessToken,
    AuthorizationCodeFlowOAuth2Credentials,
    BaseDocument,
    LogCursor,
    OAuth2Spec,
    PageCursor,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.http import HTTPSession
from pydantic import AwareDatetime, BaseModel, Field

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

    class Advanced(BaseModel):
        limit: Annotated[
            int,
            Field(
                description="Limit used in queries for incremental streams. This should be left as the default value unless connector errors indicate a smaller limit is required.",
                title="Limit",
                default=1,
                gt=0,
            ),
        ]

    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class GraphQLResponse(BaseModel):
    data: Optional[dict] = None
    errors: Optional[list[dict]] = None


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class IncrementalResource(BaseDocument, extra="allow"):
    id: str
    updated_at: AwareDatetime


FullRefreshResourceFetchFn = Callable[
    [HTTPSession, Logger], AsyncGenerator[FullRefreshResource, None]
]

IncrementalResourceFetchChangesFn = Callable[
    [HTTPSession, Logger, LogCursor],
    AsyncGenerator[IncrementalResource | LogCursor, None],
]

IncrementalResourceFetchPageFn = Callable[
    [HTTPSession, Logger, PageCursor, LogCursor],
    AsyncGenerator[IncrementalResource | PageCursor, None],
]
