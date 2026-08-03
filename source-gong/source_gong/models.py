from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, ClassVar, Literal

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import BasicAuth
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    TypeAdapter,
    ValidationInfo,
    model_validator,
)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def default_start_date() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=30)


class GongCredentials(BasicAuth):
    credentials_title: Literal["API Key"] = Field(  # type: ignore[assignment]
        default="API Key",
        json_schema_extra={"type": "string"},
    )
    username: str = Field(
        title="Access Key",
        description="The Gong API Access Key.",
        json_schema_extra={"secret": True},
        alias="access_key",
    )
    password: str = Field(
        title="Access Key Secret",
        description="The Gong API Access Key Secret.",
        json_schema_extra={"secret": True},
        alias="access_key_secret",
    )


class EndpointConfig(BaseModel):
    credentials: GongCredentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    region: str = Field(
        default="us-55616",
        description="The API region for your Gong account. Check your Gong API settings for the correct region.",
        title="API Region",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data before this date will not be replicated. If left blank, defaults to 30 days before current time.",
        title="Start Date",
        default_factory=default_start_date,
        le=datetime.now(tz=UTC),
        ge=EPOCH,
    )
    calls_lookback_window: int = Field(
        default=7,
        ge=1,
        le=30,
        title="Calls Lookback Window (Days)",
        description="Number of days to look back when fetching calls to capture post-call enrichment. Applied once per day.",
    )

    @property
    def base_url(self) -> str:
        return f"https://{self.region}.api.gong.io"


ConnectorState = GenericConnectorState[ResourceState]


class HttpMethod(StrEnum):
    GET = "GET"
    POST = "POST"


class DateFormat(StrEnum):
    ISO = "iso"
    DATE = "date"


@dataclass(frozen=True, slots=True)
class ResponseContext:
    """Typed validation context for GongResponseEnvelope.model_validate_json."""

    item_cls: type
    items_key: str


class RecordsMeta(BaseModel, extra="ignore"):
    """Pagination metadata from Gong API responses."""

    cursor: str | None = None
    totalRecords: int | None = None
    currentPageSize: int | None = None
    currentPageNumber: int | None = None


class GongResource(BaseDocument, extra="allow"):
    NAME: ClassVar[str]
    URL_PATH: ClassVar[str]
    ITEMS_KEY: ClassVar[str]
    KEY: ClassVar[list[str]]


class GongResponseEnvelope[T: GongResource](BaseModel, extra="allow"):
    """
    Gong API response envelope — single-pass JSON parsing.

    Call via model_validate_json(response, context=ResponseContext(item_cls=Call, items_key="calls")).
    The wrap validator extracts items from the dynamic key and validates them via TypeAdapter,
    while Pydantic handles the records/pagination fields normally.
    """

    records: RecordsMeta | None = None
    items: list[T] = Field(default_factory=list)

    @model_validator(mode="wrap")
    @classmethod
    def extract_items(
        cls, data: Any, handler: Any, info: ValidationInfo
    ) -> "GongResponseEnvelope":
        if isinstance(data, dict) and isinstance(info.context, ResponseContext):
            ctx = info.context
            if ctx.items_key in data:
                raw_items = data.pop(ctx.items_key)
                data["items"] = TypeAdapter(list[ctx.item_cls]).validate_python(  # type: ignore[name-defined]
                    raw_items
                )
        return handler(data)

    @property
    def next_cursor(self) -> str | None:
        return self.records.cursor if self.records else None


class IncrementalGongResource(GongResource):
    CURSOR_FIELD: ClassVar[str]
    ID_FIELD: ClassVar[str] = "id"
    METHOD: ClassVar[HttpMethod] = HttpMethod.GET
    DATE_FORMAT: ClassVar[DateFormat] = DateFormat.ISO
    FROM_PARAM: ClassVar[str]
    TO_PARAM: ClassVar[str]
    FILTER_WRAPPER: ClassVar[bool] = False

    # Synthetic field populated by the `extract_cursor` before-validator from
    # the resource's CURSOR_FIELD. Excluded from serialized output (exclude=True)
    # so it does not appear in captured documents.
    cursor_value: int = Field(exclude=True)

    @classmethod
    def get_key_json_path(cls) -> str:
        return f"/{cls.ID_FIELD}"

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for attr in ("CURSOR_FIELD", "URL_PATH", "ITEMS_KEY", "FROM_PARAM", "TO_PARAM"):
            if not hasattr(cls, attr) or getattr(cls, attr) == "":
                raise TypeError(
                    f"Class {cls.__name__} must define class attribute '{attr}'"
                )

    @model_validator(mode="before")
    @classmethod
    def extract_cursor(cls, data: dict[str, object]) -> dict[str, object]:
        if isinstance(data, dict):
            raw = data.get(cls.CURSOR_FIELD)
            if isinstance(raw, str):
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                data["cursor_value"] = int(dt.timestamp())
            elif isinstance(raw, int):
                data["cursor_value"] = raw
            else:
                data["cursor_value"] = 0
        return data


class Call(IncrementalGongResource):
    """
    Gong Call resource.

    Incremental by started timestamp with a lookback window to capture
    post-call enrichment.
    """

    NAME = "calls"
    KEY = ["/id"]
    CURSOR_FIELD = "started"
    ID_FIELD = "id"
    URL_PATH = "/v2/calls"
    ITEMS_KEY = "calls"
    FROM_PARAM = "fromDateTime"
    TO_PARAM = "toDateTime"

    id: int


class User(IncrementalGongResource):
    """
    Gong User resource.

    Incremental by created timestamp via the /v2/users/extensive endpoint,
    with scheduled backfill to capture updates.
    """

    NAME = "users"
    KEY = ["/id"]
    CURSOR_FIELD = "created"
    ID_FIELD = "id"
    URL_PATH = "/v2/users/extensive"
    METHOD = HttpMethod.POST
    ITEMS_KEY = "users"
    FROM_PARAM = "createdFromDateTime"
    TO_PARAM = "createdToDateTime"
    FILTER_WRAPPER = True

    id: int


class Scorecard(IncrementalGongResource):
    """Gong Scorecard resource - incremental by reviewTime timestamp."""

    NAME = "scorecards"
    KEY = ["/scorecardId"]
    CURSOR_FIELD = "reviewTime"
    ID_FIELD = "scorecardId"
    URL_PATH = "/v2/stats/activity/scorecards"
    METHOD = HttpMethod.POST
    ITEMS_KEY = "scorecards"
    DATE_FORMAT = DateFormat.DATE
    FROM_PARAM = "reviewFromDate"
    TO_PARAM = "reviewToDate"
    FILTER_WRAPPER = True

    scorecardId: int


class ScorecardDefinition(GongResource):
    """Gong Scorecard Definition resource - full refresh."""

    NAME = "scorecard_definitions"
    URL_PATH = "/v2/settings/scorecards"
    ITEMS_KEY = "scorecards"
    KEY = ["/_meta/row_id"]
