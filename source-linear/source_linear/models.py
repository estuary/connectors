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


# GraphQL selection sets are explicit and per-stream: GraphQL returns nothing that was not
# asked for, so these cannot be inferred from the document models the way REST fields can.
#
# Three rules, applied uniformly (each verified against the live schema by the `x0 - … Bare`
# requests in bruno/):
#   1. Scalars are effectively free (~0.1 complexity points each).
#   2. Relations are selected as `{ id }` only. A nested object costs 1 point, so ten
#      relations cost as much as a hundred scalars.
#   3. NO nested connections. A connection multiplies its children by its page size — a
#      single `labels(first: 20)` on Issues adds ~22 pts/node, which would push a 250-row
#      page from ~4,000 to ~9,500 against the hard 10,000 per-query cap. Issue<->label
#      membership belongs in its own child stream, not widened into this selection set.
#
# Only universally-available fields may appear here. In GraphQL an un-entitled field does
# not come back absent — it emits an error on *every* page for workspaces lacking the
# add-on. `identifier` is therefore deliberately absent from Projects and Initiatives
# (gated behind the paid 'Project IDs' / 'Initiative IDs' features); `id`, `slugId` and
# `url` identify those records instead. `Issue.identifier` is core and is safe to select.

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

# `progressHistory` / `currentProgress` are omitted: large, high-churn derived aggregates.
# `status` is a ProjectStatus OBJECT here, unlike Initiative.status which is a bare enum —
# the two streams look symmetrical but are not, so they cannot share a selection template.
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

# `Initiative` has no `priorityLabel` (unlike Issue and Project) — selecting it is a live
# HTTP 400. `status` is a bare enum here, not an object.
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

    # Per-stream identity as ClassVars rather than parallel registries, so a stream's
    # name, root field, selection set and ordering capability travel with its model.
    name: ClassVar[str]
    root_field: ClassVar[str]
    selection: ClassVar[str]
    # Name of the filter/sort field the incremental window is expressed over.
    CURSOR_FIELD: ClassVar[str] = "updatedAt"
    # Second clock for streams whose archival is separately detectable. Only `IssueFilter`
    # exposes an `archivedAt` comparator, so Issues is the sole stream that sets it.
    ARCHIVAL_CURSOR_FIELD: ClassVar[str | None] = None
    # True  -> accepts `sort: [{updatedAt: {order: ...}}]`, so the walk direction is ours to
    #          choose and a backfill can resume forwards from a value watermark.
    # False -> `orderBy` only, which is DESCENDING-only. `issueLabels` is the one root field
    #          that rejects `sort`.
    supports_sort: ClassVar[bool]

    # Declared required rather than read defensively: the fetch and cursor logic depends on
    # all of these, so a provider shape change must fail loudly at validation instead of
    # silently yielding no documents. All three are NON_NULL in Linear's schema.
    id: str
    createdAt: AwareDatetime
    updatedAt: AwareDatetime
    # The tombstone. Nullable on all four types. NOTE: archiving does NOT advance
    # `updatedAt`, so this field — not cursor movement — is the only archival signal.
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
    """The envelope around `nodes`, carrying pagination state."""

    pageInfo: PageInfo = Field(default_factory=PageInfo)


class GraphQLError(BaseModel, extra="allow"):
    class Extensions(BaseModel, extra="allow"):
        # Linear omits `code` on some errors; default to the retryable class so an
        # unlabelled server-side failure is retried rather than treated as fatal.
        code: str = "INTERNAL_SERVER_ERROR"

    message: str
    extensions: Extensions | None = None


class LinearGraphQLRemainder(BaseModel, extra="allow"):
    """Everything outside the streamed `data.<root>.nodes` array.

    `data` is keyed by the stream's root field, which varies per stream, so it is typed as
    a mapping to `Connection` rather than one model per root.
    """

    data: dict[str, Connection] | None = None
    errors: list[GraphQLError] | None = None

    def page_info(self, root_field: str) -> PageInfo:
        connection = (self.data or {}).get(root_field)
        return connection.pageInfo if connection else PageInfo()
