from datetime import timedelta
from typing import Any, ClassVar, Literal

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken
from pydantic import BaseModel, Field, ValidationInfo, model_validator


class ApiKey(AccessToken):
    """A Luma API key, sent on every request in the `x-luma-api-key` header.

    Keys are minted per calendar (luma.com/calendar/manage/api-keys) and
    grant access only to the calendar they were created on.
    """

    credentials_title: Literal["API Key"] = Field(  # type: ignore[assignment]
        default="API Key", json_schema_extra={"type": "string"}
    )
    access_token: str = Field(
        title="API Key",
        description="Luma calendar API key. Requires a Luma Plus subscription on the calendar.",
        json_schema_extra={"secret": True},
    )


class EndpointConfig(BaseModel):
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class PageMeta(BaseModel, extra="allow"):
    """The non-`entries` remainder of a Luma list response."""

    has_more: bool
    next_cursor: str | None = None


class LumaResource(BaseDocument, extra="allow"):
    """Base for every Luma document. Each stream fixes its endpoint contract here.

    Every list endpoint shares one shape — `GET {PATH}` with
    `pagination_limit`/`pagination_cursor` and an `{entries, has_more,
    next_cursor}` envelope — so a stream is fully described by its path, its
    fixed request params, and how often to re-snapshot it.
    """

    NAME: ClassVar[str]
    PATH: ClassVar[str]
    # Fixed query params sent on every page; a key may map to several values
    # (Luma takes repeated query params for array filters).
    REQUEST_PARAMS: ClassVar[dict[str, str | list[str]]] = {}
    INTERVAL: ClassVar[timedelta]


class Calendar(LumaResource):
    """The single calendar the API key is scoped to. `/v1/calendars/get`
    returns one bare object rather than a list envelope."""

    NAME = "calendars"
    PATH = "/v1/calendars/get"
    INTERVAL = timedelta(hours=1)

    id: str


class Event(LumaResource):
    NAME = "events"
    PATH = "/v1/calendars/events/list"
    # Escape the listing's defaults (`status=approved`, `platforms=luma`),
    # which silently drop pending submissions and external events. `access`
    # stays at its `manage` default: view-only events are managed by another
    # calendar, come back obfuscated, and their guests can't be listed.
    REQUEST_PARAMS = {"platforms": ["luma", "external"]}
    # `status` is a single-valued filter, so the stream sweeps each value.
    STATUS_SWEEPS: ClassVar[tuple[str, ...]] = ("approved", "pending")
    INTERVAL = timedelta(minutes=5)

    id: str
    # "luma" for events hosted on Luma, "external" for listings of events
    # hosted elsewhere. Only Luma-hosted events have a guest list.
    platform: str


class EventContext:
    """Validation context carrying the parent event id for guest documents."""

    def __init__(self, event_id: str):
        self.event_id = event_id


class Guest(LumaResource):
    NAME = "guests"
    PATH = "/v1/events/guests/list"
    # A deterministic walk order; the endpoint has no default sort documented.
    REQUEST_PARAMS = {"sort_column": "created_at", "sort_direction": "asc"}
    INTERVAL = timedelta(minutes=15)

    id: str
    # Not in the response body: guests are listed per event and the event id
    # exists only in the request, so it is stamped on during validation.
    event_id: str

    @model_validator(mode="before")
    @classmethod
    def _stamp_event_id(cls, values: dict[str, Any], info: ValidationInfo):
        if not isinstance(info.context, EventContext):
            raise RuntimeError(
                f"Guest validation requires an EventContext, got {info.context!r}"
            )
        values["event_id"] = info.context.event_id
        return values


class MembershipTier(LumaResource):
    NAME = "membership_tiers"
    PATH = "/v1/memberships/tiers/list"
    INTERVAL = timedelta(hours=1)

    id: str
