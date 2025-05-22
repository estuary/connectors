from datetime import datetime, UTC, timedelta
from typing import AsyncGenerator, Callable, Literal, Optional

from estuary_cdk.capture.common import (
    BasicAuth,
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    LogCursor,
    Logger,
)
from estuary_cdk.http import HTTPSession
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Allow domains of any length, with an optional port.
DOMAIN_REGEX = r"^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}(:\d{1,5})?$"
# Allow an empty string, OR one or more project ids/keys separated by commas.
PROJECTS_REGEX =  r"^$|^([a-zA-Z0-9]+)(,[a-zA-Z0-9]+)*$"

class ApiToken(BasicAuth):
    credentials_title: Literal["Email & API Token"] = Field(
        default="Email & API Token",
        json_schema_extra={"type": "string", "order": 0},
    )
    username: str = Field(
        title="Email",
        description="Email",
        json_schema_extra={"order": 1}
    )
    password: str = Field(
        title="API Token",
        description="API Token",
        json_schema_extra={"secret": True, "order": 2},
    )


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    domain: str = Field(
        description="This is the domain for your Jira account. Ex: estuary.atlassian.net, estuary.jira.com, jira.your-domain.com.",
        title="Domain",
        pattern=DOMAIN_REGEX,
        json_schema_extra={"order": 0}
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
        json_schema_extra={"order": 1}
    )
    credentials: ApiToken = Field(
        discriminator="credentials_title",
        title="Authentication",
        json_schema_extra={"order": 2}
    )

    class Advanced(BaseModel):
        projects: str = Field(
            description="Comma-separated list of project IDs from which to replicate issues. If left blank, issues from all projects wil be replicated.",
            title="Projects",
            default="",
            pattern=PROJECTS_REGEX,
        )

    advanced: Advanced = Field(
        default_factory=Advanced, #type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True, "order": 3},
    )


ConnectorState = GenericConnectorState[ResourceState]


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class JiraResource(FullRefreshResource):
    id: int


class Issue(JiraResource):
    class Fields(BaseModel, extra="allow"):
        updated: AwareDatetime

    fields: Fields


class PaginatedResponse(BaseModel, extra="allow"):
    maxResults: int
    startAt: int
    total: int
    isLast: bool
    values: list[FullRefreshResource]


class IssuesResponse(BaseModel, extra="allow"):
    issues: list[Issue]
    # nextPageToken is absent on the last page of results.
    nextPageToken: str | None = None


class MyselfResponse(BaseModel, extra="allow"):
    accountId: str
    timeZone: str


# Full refresh resources whose API response is an array containing all results.
# Tuples contain the resource name, path, and any additional query params.
# issue_types
FULL_REFRESH_ARRAYED_RESOURCES: list[tuple[str, str, Optional[dict[str, str]]]] = [
    ("issue_types", "issuetype", None)
]


# Full refresh resources whose API response is an array containing a subset of results.
# The response does not contain any pagination information but results _are_ paginated
# with a `startAt` parameter.
# Tuples contain the resource name, path, and any additional query params.
FULL_REFRESH_PAGINATED_ARRAYED_RESOURCES: list[tuple[str, str, Optional[dict[str, str]]]] = [
    ("users", "users/search", None)
]


# Full refresh resources whose API response contains pagination information.
# Tuples contain the resource name, path, and any additional query params.
FULL_REFRESH_PAGINATED_RESOURCES: list[tuple[str, str, Optional[dict[str, str]]]] = [
    ("projects", "project/search", {"status": "live,archived,deleted", "expand": "description,lead,projectKeys,url,issueTypes"})
]


FullRefreshFn = Callable[
    [HTTPSession, str, str, dict[str, str] | None, Logger],
    AsyncGenerator[FullRefreshResource, None],
]
