from __future__ import annotations

from abc import ABCMeta, abstractmethod
from datetime import UTC, datetime, timedelta
from typing import ClassVar, override

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken
from pydantic import AwareDatetime, BaseModel, Field

API_BASE_URL = "https://api.calendly.com"


def default_start_date() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=30)


class EndpointConfig(BaseModel):
    credentials: AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
        json_schema_extra={"order": 0},
    )
    start_date: AwareDatetime = Field(
        title="Start Date",
        description=(
            "UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. "
            "Any data generated before this date will not be replicated. "
            "If left blank, the start date will be set to 30 days before the present."
        ),
        default_factory=default_start_date,
        json_schema_extra={"order": 1},
    )


ConnectorState = GenericConnectorState[ResourceState]


class PaginationInfo(BaseModel, extra="allow"):
    next_page_token: str | None = None


class CalendlyResponse(BaseModel, extra="allow"):
    pagination: PaginationInfo = Field(default_factory=PaginationInfo)


class UserInfo(BaseModel, extra="allow"):
    current_organization: str


class UserInfoResponse(BaseModel, extra="allow"):
    resource: UserInfo


class CalendlyEntity(BaseDocument, extra="allow"):
    name: ClassVar[str]
    path: ClassVar[str]

    uri: str
    updated_at: AwareDatetime


class EventType(CalendlyEntity):
    name: ClassVar[str] = "event_types"
    path: ClassVar[str] = "/event_types"


class ScheduledEvent(CalendlyEntity):
    name: ClassVar[str] = "scheduled_events"
    path: ClassVar[str] = "/scheduled_events"


class OrganizationMembership(CalendlyEntity):
    name: ClassVar[str] = "organization_memberships"
    path: ClassVar[str] = "/organization_memberships"


class RoutingForm(CalendlyEntity):
    name: ClassVar[str] = "routing_forms"
    path: ClassVar[str] = "/routing_forms"


class Group(CalendlyEntity):
    name: ClassVar[str] = "groups"
    path: ClassVar[str] = "/groups"


class CalendlyChildEntity(CalendlyEntity, metaclass=ABCMeta):
    """Entity fetched per-parent via a child endpoint."""

    parent_cls: ClassVar[type[CalendlyEntity]]

    @classmethod
    @abstractmethod
    def get_child_url(cls, parent_uri: str) -> str: ...

    @classmethod
    @abstractmethod
    def get_child_params(cls, parent_uri: str) -> dict[str, str | int]: ...


class EventInvitee(CalendlyChildEntity):
    name: ClassVar[str] = "event_invitees"
    path: ClassVar[str] = "/invitees"

    parent_cls: ClassVar[type[CalendlyEntity]] = ScheduledEvent

    @override
    @classmethod
    def get_child_url(cls, parent_uri: str) -> str:
        return parent_uri + cls.path

    @override
    @classmethod
    def get_child_params(cls, parent_uri: str) -> dict[str, str | int]:
        return {}


class RoutingFormSubmission(CalendlyChildEntity):
    name: ClassVar[str] = "routing_form_submissions"
    path: ClassVar[str] = "/routing_form_submissions"

    parent_cls: ClassVar[type[CalendlyEntity]] = RoutingForm

    @override
    @classmethod
    def get_child_url(cls, parent_uri: str) -> str:
        return API_BASE_URL + cls.path

    @override
    @classmethod
    def get_child_params(cls, parent_uri: str) -> dict[str, str | int]:
        return {"form": parent_uri}


INCREMENTAL_STREAMS: list[type[CalendlyEntity]] = [
    EventType,
]

SNAPSHOT_STREAMS: list[type[CalendlyEntity]] = [
    ScheduledEvent,
    OrganizationMembership,
    RoutingForm,
    Group,
    EventInvitee,
    RoutingFormSubmission,
]
