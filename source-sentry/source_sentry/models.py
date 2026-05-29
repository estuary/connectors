from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, ClassVar

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken
from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    Field,
    field_validator,
    model_validator,
)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class Dataset(StrEnum):
    SPANS = "spans"
    ERRORS = "errors"
    TRANSACTIONS = "transactions"


# Primary key per dataset. `trace` and `id` are flat top-level fields in
# Explore rows, so these JSON pointers are direct. Span IDs are unique only
# within a trace, so spans key on (trace, id); error/transaction event IDs are
# globally unique within the org.
_DATASET_PK: dict[Dataset, list[str]] = {
    Dataset.SPANS: ["/trace", "/id"],
    Dataset.ERRORS: ["/id"],
    Dataset.TRANSACTIONS: ["/id"],
}

# Fields the connector requires regardless of the user's field list: the PK
# components and the `timestamp` cursor. Injected at validate-time if omitted.
_REQUIRED_FIELDS: dict[Dataset, list[str]] = {
    Dataset.SPANS: ["id", "trace", "timestamp"],
    Dataset.ERRORS: ["id", "timestamp"],
    Dataset.TRANSACTIONS: ["id", "timestamp"],
}


def dataset_pk(dataset: Dataset) -> list[str]:
    return _DATASET_PK[dataset]


def required_fields(dataset: Dataset) -> list[str]:
    return _REQUIRED_FIELDS[dataset]


# Explore query stream names are prefixed so they can never collide with a
# built-in stream no matter what the user names them.
CUSTOM_EXPLORE_NAME_PREFIX = "custom_explore_"


def explore_query_stream_name(name: str) -> str:
    """Resolve an explore query's configured name to its stream/binding name.
    Idempotent so a user-typed prefix isn't doubled."""
    if name.startswith(CUSTOM_EXPLORE_NAME_PREFIX):
        return name
    return CUSTOM_EXPLORE_NAME_PREFIX + name


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


class ExploreQuery(BaseModel):
    name: str = Field(
        title="Name",
        description=f"Name for this Explore query stream. The connector prefixes it with '{CUSTOM_EXPLORE_NAME_PREFIX}' to form the stream name.",
        min_length=1,
    )
    dataset: Dataset = Field(
        title="Dataset",
        description="Dataset to query. Can be one of 'spans', 'errors', or 'transactions'.",
    )
    fields: str = Field(
        title="Fields",
        description="Comma-separated field list to return, e.g. 'span.description, transaction, project'. The dataset's primary key fields and 'timestamp' are added automatically.",
    )
    query: str = Field(
        default="",
        title="Query",
        description="Optional Sentry search query to filter rows. The connector manages the time window itself, so a 'timestamp:' clause is not allowed.",
    )
    projects: str = Field(
        default="",
        title="Projects",
        description="Comma-separated project IDs to include. Leave empty to query all projects.",
    )

    @field_validator("query")
    @classmethod
    def _reject_timestamp_clause(cls, v: str) -> str:
        if "timestamp:" in v:
            raise ValueError(
                "The 'timestamp' field is reserved: the connector manages the capture "
                "window itself. Remove any 'timestamp:' clause from the explore query."
            )
        return v

    @field_validator("projects")
    @classmethod
    def _validate_projects(cls, v: str) -> str:
        for token in _split_csv(v):
            if not token.isdigit():
                raise ValueError(
                    f"Project '{token}' is not a valid project ID; provide comma-separated numeric IDs."
                )
        return v

    @property
    def field_list(self) -> list[str]:
        """User-specified fields followed by the required PK/cursor fields the
        connector always queries."""
        return list(dict.fromkeys(_split_csv(self.fields) + required_fields(self.dataset)))

    @property
    def project_ids(self) -> list[int]:
        return [int(token) for token in _split_csv(self.projects)]


class ExploreRow(BaseDocument, extra="allow"):
    # Typed fields are the dataset's primary key components and the cursor, so
    # Pydantic validates their presence on every row; the remaining fields are
    # driven by the user's `fields` list (extra="allow" + schema inference).
    # errors and transactions key on `id` alone and use this base directly.
    id: str
    timestamp: AwareDatetime


class SpanExploreRow(ExploreRow):
    # spans key on (trace, id); `trace` must be present, so validate it here.
    trace: str


# Mirrors _DATASET_PK: every JSON pointer in the PK must be a typed (required)
# field on the corresponding row model, so a row missing a key component fails
# validation instead of producing a document with a null key.
_DATASET_ROW_MODEL: dict[Dataset, type[ExploreRow]] = {
    Dataset.SPANS: SpanExploreRow,
    Dataset.ERRORS: ExploreRow,
    Dataset.TRANSACTIONS: ExploreRow,
}


def dataset_row_model(dataset: Dataset) -> type[ExploreRow]:
    return _DATASET_ROW_MODEL[dataset]


class ExploreMeta(BaseModel, extra="allow"):
    # dataScanned indicated whether Sentry returned full or partial data.
    dataScanned: str | None = None


class ExploreResponse(BaseModel, extra="allow"):
    data: list[ExploreRow]
    meta: ExploreMeta


class EndpointConfig(BaseModel):
    organization: str = Field(
        description="Organization to Request Data From",
        title="Organization",
        json_schema_extra={"order": 0},
    )
    credentials: AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
        json_schema_extra={"order": 1},
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
        json_schema_extra={"order": 2},
    )

    class Advanced(BaseModel):
        window_size: Annotated[
            timedelta,
            Field(
                description="Date window size for the events backfill in ISO 8601 format. ex: P30D means 30 days, PT6H means 6 hours.",
                title="Window size",
                default=timedelta(days=7),
                ge=timedelta(seconds=30),
                le=timedelta(days=365),
            ),
        ]
    explore_queries: list[ExploreQuery] = Field(
        default_factory=list,
        title="Explore Queries",
        description="User-defined explore query streams. Each entry becomes its own incremental stream backed by Sentry's Explore events endpoint.",
        json_schema_extra={"advanced": True, "order": 3},
    )

    @model_validator(mode="after")
    def _validate_explore_query_names(self) -> "EndpointConfig":
        # Names are prefixed (explore_query_stream_name) so they can't collide
        # with a built-in stream; we only need to enforce uniqueness among
        # explore queries, checked on the resolved stream names.
        seen: set[str] = set()
        for explore_query in self.explore_queries:
            stream_name = explore_query_stream_name(explore_query.name)
            if stream_name in seen:
                raise ValueError(
                    f"Duplicate explore query stream name '{stream_name}'; names must be "
                    "unique among explore queries."
                )
            seen.add(stream_name)
        return self

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True, "order": 4},
    )


ConnectorState = GenericConnectorState[ResourceState]


class SentryEntity(BaseDocument, extra="allow"):
    resource_name: ClassVar[str]
    resource_path: ClassVar[str]
    query_params: ClassVar[dict[str, Any]] = {}


class FullRefreshResource(SentryEntity):
    pass


class Environment(SentryEntity):
    resource_name: ClassVar[str] = "Environments"
    resource_path: ClassVar[str] = "environments"
    query_params: ClassVar[dict[str, str]] = {"visibility": "all"}


class Project(SentryEntity):
    resource_name: ClassVar[str] = "Projects"
    resource_path: ClassVar[str] = "projects"

    # List ordering is not consistent so we need to sort it on our end
    access: Annotated[list[str], AfterValidator(sorted)]
    features: Annotated[list[str], AfterValidator(sorted)]


class Team(SentryEntity):
    resource_name: ClassVar[str] = "Teams"
    resource_path: ClassVar[str] = "teams"
    query_params: ClassVar[dict[str, str]] = {"detailed": "0"}

    # List ordering is not consistent so we need to sort it on our end
    access: Annotated[list[str], AfterValidator(sorted)]


class Release(SentryEntity, extra="allow"):
    resource_name: ClassVar[str] = "Releases"
    resource_path: ClassVar[str] = "releases"


FULL_REFRESH_RESOURCES = [Environment, Project, Team, Release]


class Issue(SentryEntity):
    resource_name: ClassVar[str] = "Issues"
    resource_path: ClassVar[str] = "issues"
    query_params: ClassVar[dict[str, str | int | list[str]]] = {
        "project": -1,  # The project IDs to affect. -1 means all available projects.
        "query": "",  # Override the default query of 'is:unresolved issue.priority:[high,medium]' to return all items.
        "sort": "date",  # The sort order of the view, newest to oldest. Options include 'Last Seen' (date), 'First Seen' (new), 'Trends' (trends), 'Events' (freq), 'Users' (user), and 'Date Added' (inbox).
        "limit": 100,  # The maximum number of issues to affect. The maximum is 100 per request.
        "expand": [  # Additional data to include in the response.
            "inbox",
            "integrationIssues",
            "latestEventHasAttachments",
            "owners",
            "pluginActions",
            "pluginIssues",
            "sentryAppIssues",
            # "sessions",  # Though documented as a valid option, including it will lead to HTTP 500 errors
        ],
    }

    id: str
    lastSeen: AwareDatetime
