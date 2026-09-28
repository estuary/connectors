from datetime import UTC, datetime, timedelta
from typing import ClassVar, Literal

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.capture.common import (
    ResourceConfigWithSchedule as ResourceConfigWithSchedule,
)
from estuary_cdk.capture.common import (
    ResourceState,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import BasicAuth
from pydantic import AwareDatetime, BaseModel, Field, model_validator

from .shared import API, API_V2


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class ApiToken(BasicAuth):
    credentials_title: Literal["API Token"] = Field(  # type: ignore[assignment]
        default="API Token",
        json_schema_extra={"type": "string"},
    )
    username: str = Field(
        title="API ID",
        description="The Aircall API ID.",
        json_schema_extra={"secret": True},
        alias="api_id",
    )
    password: str = Field(
        title="API Token",
        description="The Aircall API token.",
        json_schema_extra={"secret": True},
        alias="api_token",
    )


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Calls started before this date will not be replicated. Aircall only serves the last six months of calls, so earlier dates are clamped to that horizon. Other streams capture all of their data regardless of this date. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: ApiToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class AircallMeta(BaseModel, extra="allow"):
    count: int
    total: int
    current_page: int
    per_page: int
    # /v1/webhooks never sends the page links, so completion is driven by
    # `total` instead.
    next_page_link: str | None = None


class AircallListRemainder(BaseModel, extra="allow"):
    meta: AircallMeta


class AircallReferenceEntity(BaseDocument, extra="allow"):
    name: ClassVar[str]
    base_url: ClassVar[str] = API
    path: ClassVar[str]
    response_key: ClassVar[str]


# Snapshot streams
class Users(AircallReferenceEntity):
    name: ClassVar[str] = "users"
    # GET /v1/users is sunset on 2026-09-30.
    base_url: ClassVar[str] = API_V2
    path: ClassVar[str] = "users"
    response_key: ClassVar[str] = "users"


class UserAvailability(AircallReferenceEntity):
    name: ClassVar[str] = "user_availability"
    path: ClassVar[str] = "users/availabilities"
    response_key: ClassVar[str] = "users"


class Teams(AircallReferenceEntity):
    name: ClassVar[str] = "teams"
    path: ClassVar[str] = "teams"
    response_key: ClassVar[str] = "teams"


class Numbers(AircallReferenceEntity):
    name: ClassVar[str] = "numbers"
    path: ClassVar[str] = "numbers"
    response_key: ClassVar[str] = "numbers"


class Tags(AircallReferenceEntity):
    name: ClassVar[str] = "tags"
    path: ClassVar[str] = "tags"
    response_key: ClassVar[str] = "tags"


class Webhooks(AircallReferenceEntity):
    name: ClassVar[str] = "webhooks"
    path: ClassVar[str] = "webhooks"
    response_key: ClassVar[str] = "webhooks"

    # `token` is the webhook's signing secret, which could be used to forge
    # Aircall events to the customer's receiver. It is never captured.
    @model_validator(mode="before")
    @classmethod
    def _strip_token(cls, data: object) -> object:
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if k != "token"}  # pyright: ignore[reportUnknownVariableType]
        return data


class Company(AircallReferenceEntity):
    name: ClassVar[str] = "company"
    path: ClassVar[str] = "company"
    response_key: ClassVar[str] = "company"


class CompanyEnvelope(BaseModel, extra="allow"):
    company: Company


# Incremental streams
class Call(BaseDocument, extra="allow"):
    name: ClassVar[str] = "calls"
    path: ClassVar[str] = "calls"

    id: int


class CallsResponse(BaseModel, extra="allow"):
    calls: list[Call]
    meta: AircallMeta


class Contact(BaseDocument, extra="allow"):
    name: ClassVar[str] = "contacts"
    path: ClassVar[str] = "contacts"

    id: int
    created_at: int
    updated_at: int

    def get_cursor(self) -> datetime:
        return datetime.fromtimestamp(self.updated_at, tz=UTC)


class ContactsResponse(BaseModel, extra="allow"):
    contacts: list[Contact]
    meta: AircallMeta


SNAPSHOT_LIST_STREAMS: list[type[AircallReferenceEntity]] = [
    Users,
    UserAvailability,
    Teams,
    Numbers,
    Tags,
    Webhooks,
]
SNAPSHOT_OBJECT_STREAMS: list[type[AircallReferenceEntity]] = [Company]
