from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Generic, Literal, TypeVar

from estuary_cdk.capture.common import ConnectorState as GenericConnectorState
from estuary_cdk.capture.common import ResourceState
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import BasicAuth
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    JsonValue,
    ValidationInfo,
    model_validator,
)

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
    total: int
    current_page: int


_Item = TypeVar("_Item", bound=BaseDocument)
_Doc = TypeVar("_Doc", bound=BaseDocument, covariant=True)


class ListPage(BaseModel, Generic[_Item]):
    items: list[_Item]
    meta: AircallMeta

    # Aircall nests each list under a resource-specific key, such as `calls`,
    # which is passed in the validation context.
    @model_validator(mode="before")
    @classmethod
    def _items_from_response_key(
        cls, data: dict[str, JsonValue], info: ValidationInfo
    ) -> dict[str, JsonValue]:
        assert info.context is not None
        return {"items": data[info.context["response_key"]], "meta": data["meta"]}


class CompanyResponse(BaseModel):
    company: BaseDocument


class Call(BaseDocument, extra="allow"):
    id: int


class Contact(BaseDocument, extra="allow"):
    id: int
    created_at: int
    updated_at: int


class Webhook(BaseDocument, extra="allow"):
    # `token` is the webhook's signing secret, which could be used to forge
    # Aircall events to the customer's receiver. It is never captured.
    @model_validator(mode="before")
    @classmethod
    def _strip_token(cls, data: dict[str, JsonValue]) -> dict[str, JsonValue]:
        return {k: v for k, v in data.items() if k != "token"}


@dataclass(frozen=True)
class Endpoint(Generic[_Doc]):
    url: str
    response_key: str
    model: type[_Doc]


CALLS = Endpoint(f"{API}/calls", "calls", Call)
CONTACTS = Endpoint(f"{API}/contacts", "contacts", Contact)
# GET /v1/users is sunset on 2026-09-30.
USERS = Endpoint(f"{API_V2}/users", "users", BaseDocument)
USER_AVAILABILITY = Endpoint(f"{API}/users/availabilities", "users", BaseDocument)
TEAMS = Endpoint(f"{API}/teams", "teams", BaseDocument)
NUMBERS = Endpoint(f"{API}/numbers", "numbers", BaseDocument)
TAGS = Endpoint(f"{API}/tags", "tags", BaseDocument)
WEBHOOKS = Endpoint(f"{API}/webhooks", "webhooks", Webhook)
