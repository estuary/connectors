from logging import Logger
from datetime import datetime, UTC
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    AsyncGenerator,
    Callable,
    Generic,
    Literal,
    TypeVar,
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

    class Advanced(BaseModel, extra="forbid"):
        limit: Annotated[
            int,
            Field(
                description="Limit used in queries for incremental streams. This should be left as the default value unless connector errors indicate a smaller limit is required.",
                title="Limit",
                default=5,
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


ResponseObject = TypeVar("ResponseObject", bound=BaseModel)
State = Literal["active", "all", "archived", "deleted"]


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class IncrementalResource(BaseDocument, extra="allow"):
    id: str
    updated_at: AwareDatetime


class GraphQLErrorLocation(BaseModel, extra="forbid"):
    line: int
    column: int


class GraphQLError(BaseModel, extra="forbid"):
    message: str
    locations: list[GraphQLErrorLocation] | None = None
    path: list[Any] | None = None
    extensions: dict[str, Any] | None = None


class GraphQLResponse(BaseModel, Generic[ResponseObject], extra="forbid"):
    data: ResponseObject | None = None
    errors: list[GraphQLError] | None = None


class ActivityLog(BaseModel, extra="forbid"):
    id: str
    entity: Literal["board", "pulse"]
    event: str
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: AwareDatetime

    @field_validator("data", mode="before")
    @classmethod
    def parse_json_data(cls, v: Any) -> dict[str, Any]:
        if isinstance(v, str):
            return json.loads(v)
        return v

    @field_validator("created_at", mode="before")
    @classmethod
    def parse_created_at(cls, v: Any) -> datetime:
        if isinstance(v, str):
            unix_millis = int(v) / 10_000
            return datetime.fromtimestamp(unix_millis / 1_000, tz=UTC)
        raise ValueError(f"Invalid created_at value: {v}")


class BoardActivityLogs(BaseModel, extra="forbid"):
    id: str
    activity_logs: list[ActivityLog]


class ActivityLogsResponse(BaseModel, extra="forbid"):
    boards: list[BoardActivityLogs]


# A simple reference model for entities with just an id.
class EntityRef(BaseModel, extra="forbid"):
    id: str


class Column(BaseModel, extra="forbid"):
    archived: bool
    description: str | None = None
    id: str
    settings_str: str | None = None
    title: str
    type: str
    width: int | None = None


class Group(BaseModel, extra="forbid"):
    archived: bool
    color: str | None = None
    deleted: bool
    id: str
    position: int | None = None
    title: str


class Tag(BaseDocument, extra="forbid"):
    id: str
    color: str
    name: str


class View(BaseModel, extra="forbid"):
    id: str
    name: str
    settings_str: str | None = None
    type: str
    view_specific_data_str: str | None = None


class Workspace(BaseModel, extra="forbid"):
    id: str
    name: str
    kind: str
    description: str | None = None


class Board(BaseDocument, extra="forbid"):
    id: str
    name: str
    board_kind: str
    type: str
    columns: list[Column]
    communication: str | None = None
    description: str | None = None
    groups: list[Group]
    owners: list[EntityRef]
    creator: EntityRef
    permissions: str | None = None
    state: str
    subscribers: list[EntityRef]
    tags: list[Tag]
    top_group: EntityRef
    updated_at: AwareDatetime
    updates: list[EntityRef]
    views: list[View]
    workspace: Workspace


class BoardsResponse(BaseModel, extra="forbid"):
    boards: list[Board]


class BoardRef(BaseModel, extra="forbid"):
    id: str
    name: str


class Asset(BaseModel, extra="forbid"):
    created_at: AwareDatetime
    file_extension: str
    file_size: int
    id: str
    name: str
    original_geometry: str | None = None
    public_url: str
    uploaded_by: EntityRef
    url: str
    url_thumbnail: str


class ColumnValue(BaseModel, extra="forbid"):
    id: str
    text: str | None = None
    type: str
    value: str | None = None


class GroupRef(BaseModel, extra="forbid"):
    id: str


class ParentItemRef(BaseModel, extra="forbid"):
    id: str


class Update(BaseModel, extra="forbid"):
    id: str


class Item(BaseDocument, extra="forbid"):
    id: str
    name: str
    assets: list[Asset]
    board: BoardRef
    column_values: list[ColumnValue]
    created_at: AwareDatetime
    creator_id: str
    group: GroupRef
    parent_item: ParentItemRef | None = None
    state: str
    subscribers: list[EntityRef]
    updated_at: AwareDatetime
    updates: list[Update]
    subitems: list["Item"] | None = None


Item.model_rebuild()


class ItemsPage(BaseModel, extra="forbid"):
    cursor: str | None = None
    items: list[Item]


class BoardItems(BaseModel, extra="forbid"):
    id: str
    items_page: ItemsPage


class ItemsByBoardResponse(BaseModel, extra="forbid"):
    boards: list[BoardItems] | None = Field(default_factory=list)


class ItemsByBoardPageResponse(BaseModel, extra="forbid"):
    next_items_page: ItemsPage


class ItemsByIdResponse(BaseModel, extra="forbid"):
    items: list[Item] | None = Field(default_factory=list)


class Team(BaseDocument, extra="forbid"):
    id: str
    name: str
    picture_url: str | None = None
    users: list[EntityRef]
    owners: list[EntityRef]


class TeamsResponse(BaseModel, extra="forbid"):
    teams: list[Team]


class User(BaseDocument, extra="forbid"):
    birthday: str | None = None
    country_code: str | None = None
    created_at: AwareDatetime | None = None
    join_date: AwareDatetime | None = None
    email: str | None = None
    enabled: bool
    id: str
    is_admin: bool
    is_guest: bool
    is_pending: bool
    is_view_only: bool
    is_verified: bool
    location: str | None = None
    mobile_phone: str | None = None
    name: str
    phone: str | None = None
    photo_original: str | None = None
    photo_small: str | None = None
    photo_thumb: str | None = None
    photo_thumb_small: str | None = None
    photo_tiny: str | None = None
    time_zone_identifier: str | None = None
    title: str | None = None
    url: str | None = None
    utc_hours_diff: float | None = None


class UsersResponse(BaseModel, extra="forbid"):
    users: list[User]


class TagsResponse(BaseModel, extra="forbid"):
    tags: list[Tag]


FullRefreshResourceFetchFn = Callable[
    [HTTPSession, int, Logger], AsyncGenerator[BaseDocument, None]
]

IncrementalResourceFetchChangesFn = Callable[
    [HTTPSession, int, Logger, LogCursor],
    AsyncGenerator[BaseDocument | LogCursor, None],
]

IncrementalResourceFetchPageFn = Callable[
    [HTTPSession, int, Logger, PageCursor, LogCursor],
    AsyncGenerator[BaseDocument | PageCursor, None],
]
