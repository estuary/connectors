from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
import pendulum
from pydantic import AwareDatetime, BaseModel, Field


def default_start_date():
    return pendulum.now().subtract(days=1).in_timezone("UTC").format("YYYY-MM-DDTHH:mm:ssZ")


class EndpointConfig(BaseModel):
    startDate: str = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any event data generated before this date will not be replicated. If left blank, the start date will be set to one hour before the present date.",
        title="Start Date",
        examples= ["2024-08-27T00:00:00Z"],
        pattern= "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
        default_factory=default_start_date,
    )
    apiToken: str = Field(
        description="API Token to use for Authentication",
        title="API Token",
        json_schema_extra={"secret": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class Resource(BaseDocument, extra="allow"):
    pass


class Metadata(BaseDocument, extra="allow"):
    pass


# Event streams have slightly different required fields, so we have models for each event type.
class PageEvent(BaseDocument, extra="allow"):
    appId: int
    hour: int
    remoteIp: str
    lastTime: int
    pageId: str


class FeatureEvent(BaseDocument, extra="allow"):
    appId: int
    hour: int
    remoteIp: str
    lastTime: int
    featureId: str


class TrackEvent(BaseDocument, extra="allow"):
    appId: int
    hour: int
    remoteIp: str
    lastTime: int
    trackTypeId: str


class GuideEvent(BaseDocument, extra="allow"):
    appId: int
    remoteIp: str
    guideTimestamp: int
    guideId: str


class PollEvent(BaseDocument, extra="allow"):
    appId: int
    remoteIp: str
    guideTimestamp: int
    pollId: str


class EventResponse(BaseDocument, extra="forbid"):
    startTime: int
    results: list[GuideEvent | PollEvent]


class AggregatedEventResponse(BaseDocument, extra="forbid"):
    startTime: int
    results: list[PageEvent | FeatureEvent | TrackEvent]

# Supported resource types and their corresponding name.
RESOURCE_TYPES: list[tuple[str, str]] = [
    ("feature", "Feature"),
    ("guide", "Guide"),
    ("page", "Page"),
    ("report", "Report"),
]

# Supported metadata types and their corresponding name.
METADATA_TYPES: list[tuple[str, str]] = [
    ("account", "AccountMetadata"),
    ("visitor", "VisitorMetadata"),
]

# Supported event types, their corresponding name, their keys, and their model.
EVENT_TYPES: list[tuple[str, str, str, type[BaseDocument]]] = [
    ("guideEvents", "GuideEvents", "/guideId", GuideEvent),
    ("pollEvents", "PollEvents", "/pollId", PollEvent),
]

# Supported aggregated event types, their corresponding resource name, their keys, and their model.
AGGREGATED_EVENT_TYPES: list[tuple[str, str, str, type[BaseDocument]]] = [
    ("pageEvents", "PageEvents", "/pageId", PageEvent),
    ("featureEvents", "FeatureEvents", "/featureId", FeatureEvent),
    ("trackEvents", "TrackEvents", "/trackTypeId", TrackEvent),
]
