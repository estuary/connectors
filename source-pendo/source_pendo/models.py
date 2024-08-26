from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from pydantic import AwareDatetime, BaseModel, Field

# Supported resource types for the Pendo API that have an id and their corresponding resource name.
RESOURCE_TYPES: list[tuple[str, str]] = [
    ("feature", "Feature"),
    ("guide", "Guide"),
    ("page", "Page"),
    ("report", "Report"),
]

# Supported metadata types for the Pendo API and their corresponding resource name.
METADATA_TYPES: list[tuple[str, str]] = [
    ("account", "AccountMetadata"),
    ("visitor", "VisitorMetadata")
]

# Supported event types for the Pendo Aggregation API, their corresponding resource name, their associated identifying field, and their "last updated" timestamp field.
EVENT_TYPES: list[tuple[str, str, str]] = [
    ("pageEvents", "PageEvents", "/pageId", "lastTime"),
    ("featureEvents", "FeatureEvents", "/featureId", "lastTime"),
    ("trackEvents", "TrackEvents", "/trackTypeId", "lastTime"),
    ("guideEvents", "GuideEvents", "/guideId", "guideTimestamp"),
    ("pollEvents", "PollEvents", "/pollId", "guideTimestamp")
]


class EndpointConfig(BaseModel):
    startDate: str = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any event data generated before this date will not be replicated.",
        title="Start Date",
        examples= ["2024-08-27T00:00:00Z"],
        pattern= "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
        format= "date-time",
    )
    apiToken: str = Field(
        description="API Token to use for Authentication",
        title="API Token",
        json_schema_extra={"secret": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class Resource(BaseDocument, extra="allow"):
    id: str = ""


class Metadata(BaseDocument, extra="allow"):
    pass


class Event(BaseDocument, extra="allow"):
    appId: int = -1
    hour: int = -1
    remoteIp: str = ""
    lastTime: int | None = None
    guideTimestamp: int | None = None
    pageId: str | None = None
    featureId: str | None = None
    trackTypeId: str | None = None
    guideId: str | None = None
    pollId: str | None = None


class EventResponse(BaseDocument, extra="forbid"):
    startTime: int
    results: list[Event]
