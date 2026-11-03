from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Literal
from urllib.parse import quote

from estuary_cdk.capture.common import ConnectorState as GenericConnectorState
from estuary_cdk.capture.common import ResourceState
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import AccessToken
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    JsonValue,
    ValidationInfo,
    model_validator,
)


def default_start_date() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=30)


class ApiKey(AccessToken):
    credentials_title: Literal["API Key"] = Field(  # type: ignore[assignment]
        default="API Key", json_schema_extra={"type": "string", "order": 0}
    )
    access_token: str = Field(
        title="API Key",
        description="Your Pylon API token. Admins can create one under Settings > Integrations & dev > API tokens.",
        json_schema_extra={"secret": True, "order": 1},
    )


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Issues updated before this date will not be replicated; other streams are always captured in full. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
        description="Your Pylon API credentials.",
    )
    region: Literal["US", "EU"] = Field(
        default="US",
        title="Region",
        description="The region where your Pylon organization is hosted. EU organizations use the api.eu.usepylon.com API.",
    )


ConnectorState = GenericConnectorState[ResourceState]

type HTTPMethod = Literal["GET", "POST"]


class PylonDocument(BaseDocument, extra="allow"):
    name: ClassVar[str]
    path: ClassVar[str]
    method: ClassVar[HTTPMethod] = "GET"
    paginated: ClassVar[bool] = False
    page_size: ClassVar[int] = 100
    request_params: ClassVar[dict[str, JsonValue]] = {}
    # Each partition is read with its own params; most resources have one.
    partitions: ClassVar[tuple[dict[str, JsonValue], ...]] = ({},)


class Accounts(PylonDocument):
    name: ClassVar[str] = "accounts"
    path: ClassVar[str] = "/accounts"
    paginated: ClassVar[bool] = True


class Contacts(PylonDocument):
    name: ClassVar[str] = "contacts"
    path: ClassVar[str] = "/contacts"
    paginated: ClassVar[bool] = True


class Users(PylonDocument):
    name: ClassVar[str] = "users"
    path: ClassVar[str] = "/users"
    request_params: ClassVar[dict[str, JsonValue]] = {"include_deactivated": "true"}


class Teams(PylonDocument):
    name: ClassVar[str] = "teams"
    path: ClassVar[str] = "/teams"


class Tags(PylonDocument):
    name: ClassVar[str] = "tags"
    path: ClassVar[str] = "/tags"


class CustomFields(PylonDocument):
    name: ClassVar[str] = "custom_fields"
    path: ClassVar[str] = "/custom-fields"
    partitions: ClassVar[tuple[dict[str, JsonValue], ...]] = tuple(
        {"object_type": kind}
        for kind in (
            "account",
            "issue",
            "contact",
            "task",
            "project",
            "meeting",
            "opportunity",
        )
    )


class Issue(PylonDocument):
    name: ClassVar[str] = "issues"
    path: ClassVar[str] = "/issues/search"
    method: ClassVar[HTTPMethod] = "POST"  # Read-only search.
    paginated: ClassVar[bool] = True
    cursor_field: ClassVar[str] = "updated_at"
    id: str = Field(min_length=1)
    updated_at: AwareDatetime

    def get_cursor(self) -> datetime:
        return self.updated_at

    @classmethod
    def window_filter(cls, lower: datetime, upper: datetime) -> dict[str, JsonValue]:
        return {
            "field": cls.cursor_field,
            "operator": "time_range",
            "values": [lower.isoformat(), upper.isoformat()],
        }


@dataclass(frozen=True)
class IssueContext:
    issue_id: str


class IssueMessage(PylonDocument):
    name: ClassVar[str] = "issue_messages"
    path: ClassVar[str] = "/issues/{issue_id}/messages"
    paginated: ClassVar[bool] = True
    page_size: ClassVar[int] = 1000
    issue_id: str = Field(min_length=1)

    @classmethod
    def path_for(cls, issue_id: str) -> str:
        return cls.path.format(issue_id=quote(issue_id, safe=""))

    @model_validator(mode="before")
    @classmethod
    def _stamp_issue_id(cls, data: Any, info: ValidationInfo) -> Any:
        # Message responses don't include their issue's ID; it comes from the
        # request path.
        if isinstance(data, dict) and isinstance(info.context, IssueContext):
            data["issue_id"] = info.context.issue_id
        return data


class Pagination(BaseModel):
    cursor: str | None = None
    has_next_page: bool


class ListResponse(BaseModel):
    # Pylon omits `data` from successful empty responses, returning only
    # `request_id`.
    data: list[dict[str, JsonValue]] = Field(default_factory=list)
    pagination: Pagination | None = None


SNAPSHOT_STREAMS = (Accounts, Contacts, Users, Teams, Tags, CustomFields)
