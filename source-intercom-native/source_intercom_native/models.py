from datetime import datetime, UTC, timedelta
from typing import Annotated, AsyncGenerator, Callable, Literal, TYPE_CHECKING, Optional

from estuary_cdk.capture.common import (
    AccessToken,
    BaseDocument,
    LongLivedClientCredentialsOAuth2Credentials,
    OAuth2Spec,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    LogCursor,
    Logger,
)
from estuary_cdk.http import HTTPSession

from pydantic import AwareDatetime, BaseModel, Field


OAUTH2_SPEC = OAuth2Spec(
    provider="intercom",
    authUrlTemplate=(
        "https://app.intercom.com/oauth?"
        r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate="https://api.intercom.io/auth/eagle/token",
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
    ),
    accessTokenResponseMap={
        "access_token": "/token",
    },
)


if TYPE_CHECKING:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
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
        window_size: Annotated[int, Field(
            description="Window size in days for incremental streams.",
            title="Window Size",
            default=5,
            gt=0,
        )]

    advanced: Advanced = Field(
        default_factory=Advanced, #type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class IntercomResource(BaseDocument, extra="allow"):
    pass


class TimestampedResource(IntercomResource):
    id: str
    updated_at: int


class SearchResponse(BaseModel, extra="allow"):
    type: str
    total_count: int

    class Pagination(BaseModel, extra="forbid"):
        type: str
        page: int
        per_page: int
        total_pages: int

        class Next(BaseModel, extra="forbid"):
            page: int
            starting_after: str

        next: Optional[Next] = None # If next is not present, this is the last page.

    pages: Pagination


class ContactsSearchResponse(SearchResponse):
    data: list[TimestampedResource]


class ConversationsSearchResponse(SearchResponse):
    conversations: list[TimestampedResource]


class ConversationResponse(BaseModel, extra="allow"):
    id: str
    updated_at: int

    class ConversationPartsWrapper(BaseModel):
        total_count: int
        type: str
        conversation_parts: list[TimestampedResource]

    conversation_parts: ConversationPartsWrapper


class SegmentsResponse(BaseModel, extra="forbid"):
    # The API docs state this response may contain a pagination object, but I haven't
    # observed that when checking actual API responses. It's possible that the Intercom
    # accounts I tested with don't have enough segments to trigger pagination. If pagination
    # does exist, we'll fail when someone with a large number of segments uses this connector
    # and we can add pagination then.
    type: str
    data: list[TimestampedResource] = Field(..., alias="segments")


class CompanySegmentsResponse(BaseModel, extra="forbid"):
    type: str
    data: list[TimestampedResource]


class CompanyListResponse(BaseModel, extra="allow"):
    total_count: int
    data: list[TimestampedResource]

    class Pagination(BaseModel, extra="forbid"):
        type: str
        page: int
        per_page: int
        total_pages: int
        next: Optional[str] # If next is None, this is the last page.

    pages: Pagination


ClientSideFilteringResourceFetchChangesFn = Callable[
    [HTTPSession, Logger, LogCursor],
    AsyncGenerator[TimestampedResource | LogCursor, None],
]

IncrementalDateWindowResourceFetchChangesFn = Callable[
    [HTTPSession, int, Logger, LogCursor],
    AsyncGenerator[TimestampedResource | LogCursor, None],
]
