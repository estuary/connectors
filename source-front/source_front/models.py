from datetime import datetime, UTC
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


# Front was founded in 2013, so having a start date before then should capture all available data.
def default_start_date():
    dt = datetime(year=2000, month=1, day=1, tzinfo=UTC)
    return dt


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any event data generated before this date will not be replicated. If left blank, all available data will be captured.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: AccessToken = Field(
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class FrontResource(BaseDocument, extra="allow"):
    pass


class Contact(FrontResource):
    id: str
    updated_at: float


class Event(FrontResource):
    id: str
    emitted_at: float


class Conversation(FrontResource):
    id: str


class Pagination(BaseModel, extra="forbid"):
    next: str | None


class Links(BaseModel, extra="forbid"):
    self: str


class Response(BaseModel, extra="forbid"):
    # The top level fields in each response have leading underscores. Pydantic treats
    # attributes with leading underscores as private & doesn't include them in the model.
    # We have to use aliases in the Pydantic model so these fields are included in
    # model without underscores & we can access them.
    links: Links = Field(..., alias='_links')
    results: list[FrontResource] = Field(..., alias='_results')
    pagination: Pagination = Field(..., alias='_pagination')
    total: int | None = Field(None, alias='_total')


# Resources that only support full refresh & do not support pagination.
FULL_REFRESH_RESOURCES: list[str] = [
    'channels',
    'inboxes',
    "tags",
    "teammates",
    "teams",
]

# Resources that contain cursor fields in their records. Each tuple contains:
# - resource name
# - q filter query param
# - sort_by query param
# - document model
# - cursor field within each record
INCREMENTAL_RESOURCES_WITH_CURSOR_FIELDS: list[tuple[str, str, str, type[FrontResource], str]] = [
    ("events", "after", "created_at", Event, "emitted_at"),
    ("contacts", "updated_after", "updated_at", Contact, "updated_at"),
]
