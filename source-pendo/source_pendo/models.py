from typing import ClassVar, Generic, TypeVar
import pendulum
from pydantic import AwareDatetime, BaseModel, Field

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken


def default_start_date():
    return pendulum.now().subtract(hours=1).in_timezone("UTC").format("YYYY-MM-DDTHH:mm:ssZ")


class EndpointConfig(BaseModel):
    startDate: str = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any event data generated before this date will not be replicated. If left blank, the start date will be set to one hour before the present date.",
        title="Start Date",
        examples= ["2024-08-27T00:00:00Z"],
        pattern= "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
        format='date-time', # type: ignore - format is required for the UI to know this is a date-time field & give a nice popup.
        default_factory=default_start_date,
    )
    credentials: AccessToken = Field(
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class Metadata(BaseDocument, extra="allow"):
    path: ClassVar[str]
    resource_name: ClassVar[str]


class AccountMetadata(Metadata):
    path: ClassVar[str] = "account"
    resource_name: ClassVar[str] = "AccountMetadata"


class VisitorMetadata(Metadata):
    path: ClassVar[str] = "visitor"
    resource_name: ClassVar[str] = "VisitorMetadata"


class FullRefreshResource(BaseDocument, extra="allow"):
    entity_name: ClassVar[str]
    resource_name: ClassVar[str]


class Feature(FullRefreshResource):
    entity_name: ClassVar[str] = "feature"
    resource_name: ClassVar[str] = "Feature"


class Guide(FullRefreshResource):
    entity_name: ClassVar[str] = "guide"
    resource_name: ClassVar[str] = "Guide"


class Page(FullRefreshResource):
    entity_name: ClassVar[str] = "page"
    resource_name: ClassVar[str] = "Page"


class Report(FullRefreshResource):
    entity_name: ClassVar[str] = "report"
    resource_name: ClassVar[str] = "Report"


class Segment(FullRefreshResource):
    entity_name: ClassVar[str] = "segment"
    resource_name: ClassVar[str] = "Segment"


class IncrementalResource(FullRefreshResource):
    primary_keys: ClassVar[list[str]]
    cursor_field: ClassVar[str]
    identifying_field: ClassVar[str]


class Account(IncrementalResource):
    entity_name: ClassVar[str] = "accounts"
    resource_name: ClassVar[str] = "Account"
    identifying_field: ClassVar[str] = "accountId"
    primary_keys: ClassVar[list[str]] = ["accountId"]
    cursor_field: ClassVar[str] = "metadata.auto.lastupdated"

    accountId: str


class Visitor(IncrementalResource):
    entity_name: ClassVar[str] = "visitors"
    resource_name: ClassVar[str] = "Visitor"
    identifying_field: ClassVar[str] = "visitorId"
    primary_keys: ClassVar[list[str]] = ["visitorId"]
    cursor_field: ClassVar[str] = "metadata.auto.lastupdated"

    visitorId: str


class TrackType(IncrementalResource):
    entity_name: ClassVar[str] = "trackTypes"
    resource_name: ClassVar[str] = "TrackType"
    identifying_field: ClassVar[str] = "id"
    primary_keys: ClassVar[list[str]] = ["id"]
    cursor_field: ClassVar[str] = "lastUpdatedAt"


    id: str


class BaseEvent(FullRefreshResource):
    identifying_field: ClassVar[str]
    primary_keys: ClassVar[list[str]]

    accountId: str
    appId: int
    remoteIp: str
    visitorId: str
    userAgent: str


class EventAggregate(BaseEvent):
    server: str
    firstTime: int
    lastTime: AwareDatetime


class PageEvent(EventAggregate):
    entity_name: ClassVar[str] = "pageEvents"
    resource_name: ClassVar[str] = "PageEvents"
    identifying_field: ClassVar[str] = "pageId"
    primary_keys: ClassVar[list[str]] = ["pageId"]

    pageId: str


class FeatureEvent(EventAggregate):
    entity_name: ClassVar[str] = "featureEvents"
    resource_name: ClassVar[str] = "FeatureEvents"
    identifying_field: ClassVar[str] = "featureId"
    primary_keys: ClassVar[list[str]] = ["featureId"]

    featureId: str


class TrackEvent(EventAggregate):
    entity_name: ClassVar[str] = "trackEvents"
    resource_name: ClassVar[str] = "TrackEvents"
    identifying_field: ClassVar[str] = "trackTypeId"
    primary_keys: ClassVar[list[str]] = ["trackTypeId"]

    trackTypeId: str


class Event(BaseEvent):
    guideTimestamp: AwareDatetime
    serverName: str


class GuideEvent(Event):
    entity_name: ClassVar[str] = "guideEvents"
    resource_name: ClassVar[str] = "GuideEvents"
    identifying_field: ClassVar[str] = "guideId"
    primary_keys: ClassVar[list[str]] = ["guideId"]

    guideId: str


class PollEvent(Event):
    entity_name: ClassVar[str] = "pollEvents"
    resource_name: ClassVar[str] = "PollEvents"
    identifying_field: ClassVar[str] = "pollId"
    primary_keys: ClassVar[list[str]] = ["guideId", "pollId"]

    guideId: str
    pollId: str


# Supported snapshot resource types.
# Most, if not all, of these snapshot resources could be moved over
# to be incremental resources listed in INCREMENTAL_RESOURCE_TYPES.
# Doing so would change the primary key for already created collections,
# so I'm holding off doing that until later. We'll likely want to increment
# the connector version to let existing users migrate to the newer version
# at their convenience.
FULL_REFRESH_RESOURCE_TYPES: list[type[FullRefreshResource]] = [
    Feature,
    Guide,
    Page,
    Report,
    Segment,
]


# Supported metadata types and their corresponding name.
METADATA_TYPES: list[type[Metadata]] = [
    AccountMetadata,
    VisitorMetadata
]


# Supported event types, their corresponding name, their keys, and their model.
EVENT_TYPES: list[type[Event]] = [
    GuideEvent,
    PollEvent,
]


# Supported aggregated event types, their corresponding resource name, their keys, and their model.
AGGREGATED_EVENT_TYPES: list[type[EventAggregate]] = [
    PageEvent,
    FeatureEvent,
    TrackEvent,
]


# Supported incremental resource types, their corresponding resource name, their key, their updated_at field, and their model.
INCREMENTAL_RESOURCE_TYPES: list[type[IncrementalResource]] = [
    Account,
    Visitor,
    TrackType,
]
