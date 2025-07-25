import json
from logging import Logger
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Generic,
    Literal,
    TypeVar,
)

from dataclasses import dataclass

from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    LogCursor,
    PageCursor,
    ResourceState,
    make_cursor_dict,
)
from estuary_cdk.flow import (
    AccessToken,
    AuthorizationCodeFlowOAuth2Credentials,
    OAuth2Spec,
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


class GraphQLErrorLocation(BaseModel, extra="forbid"):
    line: int
    column: int


class GraphQLError(BaseModel, extra="allow"):
    class Extensions(BaseModel, extra="allow"):
        code: str = Field(
            default="INTERNAL_SERVER_ERROR"
        )  # Default code for errors if the API does not specify one.
        status_code: int | None = None
        complexity: int | None = None
        maxComplexity: int | None = None

    message: str
    locations: list[GraphQLErrorLocation] | None = None
    path: list[Any] | None = None
    extensions: Extensions | None = None


class ComplexityInfo(BaseModel, extra="allow"):
    query: int
    after: int
    reset_in_x_seconds: int


class GraphQLResponseData(BaseModel, extra="allow"):
    complexity: ComplexityInfo | None = None


TGraphQLResponseData = TypeVar("TGraphQLResponseData", bound=GraphQLResponseData)


class GraphQLResponseRemainder(BaseModel, Generic[TGraphQLResponseData], extra="allow"):
    data: TGraphQLResponseData | None = None
    errors: list[GraphQLError] | None = None

    def has_errors(self) -> bool:
        return self.errors is not None and len(self.errors) > 0

    def get_errors(self) -> list[GraphQLError]:
        return self.errors or []


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


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class IncrementalResource(BaseDocument, extra="allow"):
    id: str
    updated_at: AwareDatetime


class Board(IncrementalResource):
    class Workspace(BaseModel, extra="allow"):
        kind: str | None = None

    state: Literal["all", "active", "archived", "deleted"]
    workspace: Workspace | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )

    # Fields that can be null due to authorization restrictions
    activity_logs: list[ActivityLog] | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )
    columns: list[Any] | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )
    groups: list[Any] | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )
    owners: list[Any] | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )
    subscribers: list[Any] | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )
    views: list[Any] | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )
    tags: list[Any] | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )
    updates: list[Any] | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )
    items_page: Any | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )


class Item(IncrementalResource):
    class Board(BaseModel, extra="allow"):
        id: str

    state: Literal["all", "active", "archived", "deleted"]
    board: Board | None = Field(
        default=None,
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )


@dataclass
class ItemsBackfillCursor:
    """
    Cursor structure for items backfill operations.

    Tracks which boards need to be processed during backfill:
    - Keys: Board IDs (strings) that need processing
    - Values: Always False

    When boards are completed, they're removed via JSON merge patch:
    {"completed_board_id": null}
    """

    boards: dict[str, bool]

    @classmethod
    def from_cursor_dict(
        cls, cursor_dict: dict[str, Literal[False] | None]
    ) -> "ItemsBackfillCursor":
        boards = {k: v for k, v in cursor_dict.items() if v is not None}
        return cls(boards=boards)

    @classmethod
    def from_board_ids(cls, board_ids: list[str]) -> "ItemsBackfillCursor":
        if not board_ids:
            return cls(boards={})

        boards = {board_id: False for board_id in board_ids}
        return cls(boards=boards)

    def get_next_boards(self, limit: int) -> list[str]:
        return list(self.boards.keys())[:limit]

    def create_initial_cursor(self) -> dict[str, Literal[False]]:
        return make_cursor_dict(self.boards)

    def create_completion_patch(
        self, completed_board_ids: list[str]
    ) -> dict[str, None]:
        return make_cursor_dict({board_id: None for board_id in completed_board_ids})


IncrementalResourceFetchChangesFn = Callable[
    [HTTPSession, Logger, LogCursor],
    AsyncGenerator[BaseDocument | LogCursor, None],
]

IncrementalResourceFetchPageFn = Callable[
    [HTTPSession, Logger, PageCursor, LogCursor],
    AsyncGenerator[BaseDocument | PageCursor, None],
]

FullRefreshResourceFetchSnapshotFn = Callable[
    [HTTPSession, str, str, Logger],
    AsyncGenerator[BaseDocument, None],
]
