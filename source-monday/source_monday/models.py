from logging import Logger
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Generic,
    TypeVar,
    Literal,
)
import json

from estuary_cdk.flow import (
    AccessToken,
    AuthorizationCodeFlowOAuth2Credentials,
    OAuth2Spec,
)

from estuary_cdk.capture.common import (
    BaseDocument,
    LogCursor,
    PageCursor,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.http import HTTPSession
from pydantic import AwareDatetime, BaseModel, Field, field_validator

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


# TODO(justin): Remove AccessToken and add OAuth2Credentials to EndpointConfig
# when the OAuth app is created and approved by Monday.com for public use.
class EndpointConfig(BaseModel):
    credentials: AccessToken = Field(
        title="Authentication",
        discriminator="credentials_title",
    )


ConnectorState = GenericConnectorState[ResourceState]
ResponseObject = TypeVar("ResponseObject", bound=BaseModel)


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class IncrementalResource(BaseDocument, extra="allow"):
    id: str
    updated_at: AwareDatetime


class GraphQLErrorLocation(BaseModel, extra="forbid"):
    line: int
    column: int


class GraphQLError(BaseModel, extra="allow"):
    class Extensions(BaseModel, extra="allow"):
        code: str = Field(
            default="INTERNAL_SERVER_ERROR"
        )  # Default code for errors if the API does not specify one.

    message: str
    locations: list[GraphQLErrorLocation] | None = None
    path: list[Any] | None = None
    extensions: Extensions | None = None


class GraphQLResponse(BaseModel, Generic[ResponseObject], extra="allow"):
    data: ResponseObject | None = None
    errors: list[GraphQLError] | None = None


class ActivityLog(BaseModel, extra="allow"):
    resource_id: str | None = None
    entity: Literal["board", "pulse"]
    event: str
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: str

    @field_validator("data", mode="before")
    @classmethod
    def parse_json_data(cls, v: Any) -> dict[str, Any]:
        if isinstance(v, str):
            return json.loads(v)
        return v

    def __init__(self, **data):
        super().__init__(**data)
        self.resource_id = self._get_resource_id()

    def _get_resource_id(self) -> str | None:
        """
        Extract the primary ID of the entity being acted upon from the data field.

        Note: when an item (pulse) is created we do not receive a pulse_id. The incremental stream will
        rely on querying for recently updated items to find updated items, which should capture these events.
        Alternatively, we could extract the board_id and backfill the board items again.
        """
        if not self.data:
            return None

        if self.entity == "pulse":
            pulse_id = self.data.get("pulse_id")
            return str(pulse_id) if pulse_id else None
        elif self.entity == "board":
            board_id = self.data.get("board_id")
            return str(board_id) if board_id else None

        return None


class BoardActivityLogs(BaseModel, extra="allow"):
    activity_logs: list[ActivityLog] | None


class ActivityLogsResponse(BaseModel, extra="forbid"):
    boards: list[BoardActivityLogs]


class Tag(BaseDocument, extra="allow"):
    pass


class Board(BaseDocument, extra="allow"):
    class Workspace(BaseModel, extra="allow"):
        kind: str | None = None

    id: str
    updated_at: AwareDatetime
    workspace: Workspace
    state: Literal["all", "active", "archived", "deleted"]


class DeletionRecord(BaseDocument, extra="forbid"):
    id: str
    updated_at: AwareDatetime

    meta_: "DeletionRecord.Meta" = Field(
        default_factory=lambda: DeletionRecord.Meta(op="d")
    )


class BoardsResponse(BaseModel, extra="forbid"):
    boards: list[Board] | None = None


class ParentItemRef(BaseModel, extra="allow"):
    id: str


class Item(BaseDocument, extra="allow"):
    id: str
    state: Literal["all", "active", "archived", "deleted"]
    parent_item: ParentItemRef | None = None
    updated_at: AwareDatetime


class ItemsPage(BaseModel, extra="allow"):
    cursor: str | None = None
    items: list[Item]


class BoardItems(BaseModel, extra="allow"):
    id: str
    state: str | None = None
    items_page: ItemsPage


class ItemsByBoardResponse(BaseModel, extra="allow"):
    boards: list[BoardItems] | None = Field(default_factory=lambda: [])


class ItemsByBoardPageResponse(BaseModel, extra="allow"):
    next_items_page: ItemsPage


class ItemsByIdResponse(BaseModel, extra="allow"):
    items: list[Item] | None = Field(default_factory=lambda: [])


class Team(BaseDocument, extra="allow"):
    pass


class TeamsResponse(BaseModel, extra="allow"):
    teams: list[Team]


class User(BaseDocument, extra="allow"):
    id: str
    pass


class UsersResponse(BaseModel, extra="allow"):
    users: list[User]


class TagsResponse(BaseModel, extra="allow"):
    tags: list[Tag]


FullRefreshResourceFetchFn = Callable[
    [HTTPSession, Logger], AsyncGenerator[BaseDocument, None]
]

IncrementalResourceFetchChangesFn = Callable[
    [HTTPSession, Logger, LogCursor],
    AsyncGenerator[BaseDocument | LogCursor, None],
]

IncrementalResourceFetchPageFn = Callable[
    [HTTPSession, Logger, PageCursor, LogCursor],
    AsyncGenerator[BaseDocument | PageCursor, None],
]
