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
from enum import StrEnum, auto

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
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    field_validator,
    ValidationError,
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


class ActivityLogProcessingError(RuntimeError):
    """Exception raised for errors when processing new activity logs."""

    def __init__(
        self,
        message: str,
        query: str | None = None,
        variables: dict[str, Any] | None = None,
    ):
        self.message = message
        self.query = query
        self.variables = variables

        self.details: dict[str, Any] = {
            "message": message,
        }

        if self.query:
            self.details["query"] = query
        if self.variables:
            self.details["variables"] = variables

        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"ActivityLogProcessingError(message={self.message!r}, "
            f"query={self.query!r}, "
            f"variables={self.variables!r})"
        )

    def __repr__(self) -> str:
        return (
            f"ActivityLogProcessingError(message={self.message!r}, "
            f"query={self.query!r}, "
            f"variables={self.variables!r})"
        )


class ActivityLogEvents(StrEnum):
    """
    List of activity log events that can be used to filter or process activity logs.

    None of these are documented by Monday.com, but they are do exist from the
    activity logs returned by the API.
    """

    ADD_OWNER = "add_owner"
    ARCHIVE_GROUP_PULSE = "archive_group_pulse"
    ARCHIVE_GROUP = "archive_group"
    ARCHIVE_PULSE = "archive_pulse"
    BATCH_CHANGE_PULSES_COLUMN_VALUE = "batch_change_pulses_column_value"
    BOARD_DELETED = "board_deleted"
    BOARD_DISCONNECT = "board_disconnect"
    BATCH_CREATE_PULSES = "batch_create_pulses"
    BATCH_DELETE_PULSES = "batch_delete_pulses"
    BATCH_DUPLICATE_PULSES = "batch_duplicate_pulses"
    BATCH_MOVE_PULSES_FROM_BOARD = "batch_move_pulses_from_board"
    BATCH_MOVE_PULSES_FROM_GROUP = "batch_move_pulses_from_group"
    BATCH_MOVE_PULSES_INTO_BOARD = "batch_move_pulses_into_board"
    BATCH_MOVE_PULSES_INTO_GROUP = "batch_move_pulses_into_group"
    BOARD_STATE_CHANGED = "board_state_changed"
    BOARD_VIEW_ADDED = "board_view_added"
    BOARD_VIEW_CHANGED = "board_view_changed"
    BOARD_VIEW_ENABLED = "board_view_enabled"
    BOARD_VIEW_DELETED = "board_view_deleted"
    BOARD_WORKSPACE_ID_CHANGED = "board_workspace_id_changed"
    CHANGE_COLUMN_SETTINGS = "change_column_settings"
    CREATE_COLUMN = "create_column"
    CREATE_GROUP = "create_group"
    CREATE_PULSE = "create_pulse"
    DELETE_COLUMN = "delete_column"
    DELETE_GROUP = "delete_group"
    DELETE_GROUP_PULSE = "delete_group_pulse"
    DELETE_PULSE = "delete_pulse"
    MOVE_PULSE_FROM_BOARD = "move_pulse_from_board"
    MOVE_PULSE_FROM_GROUP = "move_pulse_from_group"
    MOVE_PULSE_INTO_BOARD = "move_pulse_into_board"
    MOVE_PULSE_INTO_GROUP = "move_pulse_into_group"
    MOVE_SUBITEM = "move_subitem"
    REMOVE_OWNER = "remove_owner"
    RESTORE_COLUMN = "restore_column"
    RESTORE_GROUP = "restore_group"
    RESTORE_PULSE = "restore_pulse"
    SUBSCRIBE = "subscribe"
    UPDATE_BOARD_NAME = "update_board_name"
    UPDATE_BOARD_NICKNAME = "update_board_nickname"
    UPDATE_COLUMN_NAME = "update_column_name"
    UPDATE_COLUMN_VALUE = "update_column_value"
    UPDATE_GROUP_NAME = "update_group_name"
    UPDATE_NAME = "update_name"
    UNSUBSCRIBE = "unsubscribe"


class ActivityLogEventType(StrEnum):
    BOARD_CHANGED = auto()
    BOARD_DELETED = auto()
    ITEM_CHANGED = auto()
    ITEM_DELETED = auto()
    FULL_BOARD_ITEM_REFRESH = auto()


class ActivityLogEventData(BaseModel, extra="allow"):
    board_id: int | None = None
    item_id: int | None = None
    pulse_id: int | None = None
    pulse_ids: list[int] | None = None
    duplicated_pulse_ids: list[int] | None = None
    subitem: int | None = None


class ActivityLog(BaseModel, extra="allow"):
    entity: Literal["board", "pulse"]
    event: str
    data: ActivityLogEventData
    created_at: str
    # Set by the connector for logging purposes. These are only None
    # when there are user unauthorized errors causing the response to be null.
    query: str | None = None
    query_variables: dict[str, Any] | None = None

    @field_validator("data", mode="before")
    @classmethod
    def parse_json_data(cls, data: Any) -> ActivityLogEventData:
        if isinstance(data, dict):
            try:
                return ActivityLogEventData.model_validate(data)
            except ValidationError as e:
                raise ValueError(f"Invalid data for ActivityLog: {e}") from e
        elif isinstance(data, str):
            try:
                return ActivityLogEventData.model_validate_json(data)
            except ValidationError as e:
                raise ValueError(f"Invalid data for ActivityLog: {e}") from e
        else:
            raise ValueError(
                f"Invalid data type for ActivityLog: {type(data).__name__}. Expected dict or str."
            )

    def get_event_result(
        self,
        log: Logger,
        stream: Literal["boards", "items"],
    ) -> tuple[ActivityLogEventType, list[str]]:
        """
        Determine the action to take based on the event and return the affected IDs.
        """
        log.debug(f"Processing activity log event: {self.event}", {"data": self.data})
        match self.event:
            case (
                ActivityLogEvents.ARCHIVE_GROUP
                | ActivityLogEvents.ADD_OWNER
                | ActivityLogEvents.REMOVE_OWNER
                | ActivityLogEvents.BOARD_DISCONNECT
                | ActivityLogEvents.BOARD_VIEW_ADDED
                | ActivityLogEvents.BOARD_VIEW_CHANGED
                | ActivityLogEvents.BOARD_VIEW_ENABLED
                | ActivityLogEvents.BOARD_VIEW_DELETED
                | ActivityLogEvents.BOARD_WORKSPACE_ID_CHANGED
                | ActivityLogEvents.CHANGE_COLUMN_SETTINGS
                | ActivityLogEvents.UPDATE_BOARD_NICKNAME
                | ActivityLogEvents.RESTORE_GROUP
            ):
                log.debug(f"Board change event: {self.event}")
                ids = []
                if self.data.board_id:
                    ids.append(str(self.data.board_id))

                if not ids:
                    raise ActivityLogProcessingError(
                        f"Activity log event {self.event} has no identifiable board IDs.",
                        query=self.query,
                        variables=self.query_variables,
                    )

                return ActivityLogEventType.BOARD_CHANGED, ids
            case ActivityLogEvents.BOARD_DELETED:
                log.debug(f"Board deleted event: {self.event}")
                ids = []
                if self.data.board_id:
                    ids.append(str(self.data.board_id))

                if not ids:
                    raise ActivityLogProcessingError(
                        f"Activity log event {self.event} has no identifiable board IDs.",
                        query=self.query,
                        variables=self.query_variables,
                    )

                return ActivityLogEventType.BOARD_DELETED, ids
            case (
                ActivityLogEvents.CREATE_PULSE
                | ActivityLogEvents.ARCHIVE_PULSE
                | ActivityLogEvents.ARCHIVE_GROUP_PULSE
                | ActivityLogEvents.MOVE_PULSE_FROM_BOARD
                | ActivityLogEvents.MOVE_PULSE_FROM_GROUP
                | ActivityLogEvents.MOVE_PULSE_INTO_BOARD
                | ActivityLogEvents.MOVE_PULSE_INTO_GROUP
                | ActivityLogEvents.UPDATE_NAME
                | ActivityLogEvents.UPDATE_COLUMN_VALUE
                | ActivityLogEvents.BATCH_CHANGE_PULSES_COLUMN_VALUE
                | ActivityLogEvents.BATCH_CREATE_PULSES
                | ActivityLogEvents.BATCH_MOVE_PULSES_FROM_BOARD
                | ActivityLogEvents.BATCH_MOVE_PULSES_FROM_GROUP
                | ActivityLogEvents.BATCH_MOVE_PULSES_INTO_BOARD
                | ActivityLogEvents.BATCH_MOVE_PULSES_INTO_GROUP
                | ActivityLogEvents.BATCH_DUPLICATE_PULSES
                | ActivityLogEvents.MOVE_SUBITEM
                | ActivityLogEvents.RESTORE_PULSE
                | ActivityLogEvents.DELETE_GROUP_PULSE
            ):
                log.debug(f"Item change event: {self.event}")
                ids = []
                if self.data.pulse_id:
                    ids.append(str(self.data.pulse_id))
                if self.data.item_id:
                    ids.append(str(self.data.item_id))
                if self.data.pulse_ids:
                    ids.extend(str(pulse_id) for pulse_id in self.data.pulse_ids)
                if self.data.duplicated_pulse_ids:
                    ids.extend(
                        str(duplicated_pulse_id)
                        for duplicated_pulse_id in self.data.duplicated_pulse_ids
                    )
                if self.data.subitem:
                    ids.append(str(self.data.subitem))

                if not ids:
                    raise ActivityLogProcessingError(
                        f"Activity log event {self.event} has no identifiable item IDs.",
                        query=self.query,
                        variables=self.query_variables,
                    )

                return (ActivityLogEventType.ITEM_CHANGED, [str(self.data.pulse_id)])
            case ActivityLogEvents.DELETE_PULSE | ActivityLogEvents.BATCH_DELETE_PULSES:
                log.debug(f"Item deleted event: {self.event}")
                ids = []
                if self.data.pulse_id:
                    ids.append(str(self.data.pulse_id))
                if self.data.item_id:
                    ids.append(str(self.data.item_id))
                if self.data.pulse_ids:
                    ids.extend(str(pulse_id) for pulse_id in self.data.pulse_ids)

                if not ids:
                    raise ActivityLogProcessingError(
                        f"Activity log event {self.event} has no identifiable item IDs.",
                        query=self.query,
                        variables=self.query_variables,
                    )

                return ActivityLogEventType.ITEM_DELETED, ids
            case (
                ActivityLogEvents.BOARD_STATE_CHANGED
                | ActivityLogEvents.CREATE_COLUMN
                | ActivityLogEvents.DELETE_COLUMN
                | ActivityLogEvents.CREATE_GROUP
                | ActivityLogEvents.DELETE_GROUP
                | ActivityLogEvents.RESTORE_COLUMN
                | ActivityLogEvents.UPDATE_GROUP_NAME
                | ActivityLogEvents.UPDATE_COLUMN_NAME
                | ActivityLogEvents.UPDATE_BOARD_NAME
            ):
                log.debug(f"Board or item change event: {self.event}")
                ids = []
                if self.data.board_id:
                    ids.append(str(self.data.board_id))

                if not ids:
                    raise ActivityLogProcessingError(
                        f"Activity log event {self.event} has no identifiable IDs.",
                        query=self.query,
                        variables=self.query_variables,
                    )

                event_type = (
                    ActivityLogEventType.FULL_BOARD_ITEM_REFRESH
                    if stream == "items"
                    else ActivityLogEventType.BOARD_CHANGED
                )

                return event_type, ids
            case (
                ActivityLogEvents.SUBSCRIBE
                | ActivityLogEvents.UNSUBSCRIBE
            ):
                log.debug(f"Subscription event: {self.event}")
                board_ids = []
                item_ids = []
                if self.data.board_id:
                    board_ids.append(str(self.data.board_id))
                if self.data.pulse_id:
                    item_ids.append(str(self.data.pulse_id))
                if self.data.item_id:
                    item_ids.append(str(self.data.item_id))

                if stream == "boards":
                    if not board_ids:
                        raise ActivityLogProcessingError(
                            f"Activity log event {self.event} has no identifiable board IDs.",
                            query=self.query,
                            variables=self.query_variables,
                        )

                    return ActivityLogEventType.BOARD_CHANGED, board_ids
                if stream == "items":
                    if not item_ids:
                        raise ActivityLogProcessingError(
                            f"Activity log event {self.event} has no identifiable item IDs.",
                            query=self.query,
                            variables=self.query_variables,
                        )

                    return ActivityLogEventType.ITEM_CHANGED, item_ids
            case _:
                raise ActivityLogProcessingError(
                    f"Unanticipated activity log event: {self.event}. Please reach out to Estuary support for help resolving this issue.",
                    query=self.query,
                    variables=self.query_variables,
                )


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
