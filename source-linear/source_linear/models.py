from datetime import datetime, timedelta, UTC
from typing import ClassVar, Literal

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
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class ApiKey(AccessToken):
    # Narrowing the base's Literal is what makes the discriminated union work; mypy reads
    # the narrowed override as an incompatible assignment.
    credentials_title: Literal["API Key"] = Field(  # type: ignore[assignment]
        default="API Key",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="API Key",
        description="Linear personal API key, created under Settings > Security & access > Personal API keys.",
        json_schema_extra={"secret": True, "order": 1},
    )


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


# Selection sets are explicit because GraphQL returns nothing that was not asked for.
# Relations are selected as `{ id }` and nested connections are excluded, which keeps a
# 250-row page well inside the 10,000-point per-query cap. Fields gated behind paid add-ons
# are excluded too: an un-entitled field errors on every page rather than arriving absent.

ISSUE_SELECTION = """
    id createdAt updatedAt archivedAt
    number identifier title description
    priority priorityLabel estimate
    sortOrder prioritySortOrder subIssueSortOrder
    startedAt startedTriageAt triagedAt completedAt canceledAt
    autoClosedAt autoArchivedAt
    addedToProjectAt addedToCycleAt addedToTeamAt snoozedUntilAt
    dueDate slaStartedAt slaBreachesAt slaType
    customerTicketCount branchName url trashed
    team { id }
    state { id }
    assignee { id }
    creator { id }
    delegate { id }
    project { id }
    projectMilestone { id }
    cycle { id }
    parent { id }
    snoozedBy { id }
    lastAppliedTemplate { id }
"""

# `status` is a ProjectStatus object here but a bare enum on Initiative, so the two cannot
# share a template. `progressHistory`/`currentProgress` are omitted as high-churn aggregates.
PROJECT_SELECTION = """
    id createdAt updatedAt archivedAt
    name description slugId icon color health
    priority priorityLabel sortOrder prioritySortOrder
    startDate targetDate startedAt completedAt canceledAt
    autoArchivedAt healthUpdatedAt trashed
    progress scope url slackChannelId content
    status { id }
    creator { id }
    lead { id }
    leadTeam { id }
    lastAppliedTemplate { id }
    convertedFromIssue { id }
"""

# `Initiative` has no `priorityLabel`, unlike Issue and Project; selecting it is an error.
INITIATIVE_SELECTION = """
    id createdAt updatedAt archivedAt
    name description slugId icon color status
    priority sortOrder prioritySortOrder
    targetDate startedAt completedAt canceledAt
    healthUpdatedAt trashed url content
    creator { id }
    owner { id }
    leadTeam { id }
    parentInitiative { id }
    organization { id }
"""

LABEL_SELECTION = """
    id createdAt updatedAt archivedAt
    name description color isGroup
    lastAppliedAt retiredAt
    team { id }
    parent { id }
    creator { id }
    inheritedFrom { id }
"""


class LinearResource(BaseDocument, extra="allow"):
    """One Relay connection on Linear's single GraphQL endpoint."""

    name: ClassVar[str]
    root_field: ClassVar[str]
    selection: ClassVar[str]
    CURSOR_FIELD: ClassVar[str] = "updatedAt"
    # Second clock, set only where archival is separately detectable: `IssueFilter` is the
    # one filter type exposing an `archivedAt` comparator.
    ARCHIVAL_CURSOR_FIELD: ClassVar[str | None] = None
    # False means `orderBy` only, which is descending-only. `issueLabels` rejects `sort`.
    supports_sort: ClassVar[bool]

    # Required rather than defensively read: the cursor logic depends on these, so a
    # provider shape change should fail at validation rather than silently emit nothing.
    id: str
    createdAt: AwareDatetime
    updatedAt: AwareDatetime
    # The tombstone. Archiving does not advance `updatedAt`, so this field — not cursor
    # movement — is the only archival signal.
    archivedAt: AwareDatetime | None = None

    def get_cursor(self) -> AwareDatetime:
        """The document's position along its stream's incremental clock."""
        return self.updatedAt


class Issue(LinearResource):
    name: ClassVar[str] = "issues"
    root_field: ClassVar[str] = "issues"
    selection: ClassVar[str] = ISSUE_SELECTION
    supports_sort: ClassVar[bool] = True
    ARCHIVAL_CURSOR_FIELD: ClassVar[str | None] = "archivedAt"


class Project(LinearResource):
    name: ClassVar[str] = "projects"
    root_field: ClassVar[str] = "projects"
    selection: ClassVar[str] = PROJECT_SELECTION
    supports_sort: ClassVar[bool] = True


class Initiative(LinearResource):
    name: ClassVar[str] = "initiatives"
    root_field: ClassVar[str] = "initiatives"
    selection: ClassVar[str] = INITIATIVE_SELECTION
    supports_sort: ClassVar[bool] = True


class IssueLabel(LinearResource):
    name: ClassVar[str] = "labels"
    root_field: ClassVar[str] = "issueLabels"
    selection: ClassVar[str] = LABEL_SELECTION
    supports_sort: ClassVar[bool] = False


ALL_RESOURCES: list[type[LinearResource]] = [Issue, Project, Initiative, IssueLabel]


class PageInfo(BaseModel, extra="allow"):
    hasNextPage: bool = False
    endCursor: str | None = None


class Connection(BaseModel, extra="allow"):
    pageInfo: PageInfo = Field(default_factory=PageInfo)


class GraphQLError(BaseModel, extra="allow"):
    class Extensions(BaseModel, extra="allow"):
        # Linear omits `code` on some errors; default to the retryable class.
        code: str = "INTERNAL_SERVER_ERROR"

    message: str
    extensions: Extensions | None = None


class LinearGraphQLRemainder(BaseModel, extra="allow"):
    """Everything outside the streamed `data.<root>.nodes` array.

    `data` is keyed by the stream's root field, which varies per stream.
    """

    data: dict[str, Connection] | None = None
    errors: list[GraphQLError] | None = None

    def page_info(self, root_field: str) -> PageInfo:
        connection = (self.data or {}).get(root_field)
        return connection.pageInfo if connection else PageInfo()
