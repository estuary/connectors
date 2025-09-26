from datetime import UTC, datetime, timedelta
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
from pydantic import AfterValidator, AwareDatetime, BaseModel, Field

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


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

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True, "order": 3},
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
