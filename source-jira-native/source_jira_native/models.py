from datetime import datetime, UTC, timedelta
from typing import AsyncGenerator, Callable, ClassVar, Literal, Optional

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
from pydantic import AwareDatetime, BaseModel, Field


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
    isLast: bool | None = None
    values: list[FullRefreshResource]


class IssuesResponse(BaseModel, extra="allow"):
    issues: list[Issue]
    # nextPageToken is absent on the last page of results.
    nextPageToken: str | None = None


class MyselfResponse(BaseModel, extra="allow"):
    accountId: str
    timeZone: str


class FullRefreshStream():
    name: ClassVar[str]
    path: ClassVar[str]
    extra_params: ClassVar[Optional[dict[str, str]]] = None


# Full refresh resources whose API response is an array containing all results.
class FullRefreshArrayedStream(FullRefreshStream):
    pass


class ApplicationRoles(FullRefreshArrayedStream):
    name: ClassVar[str] = "application_roles"
    path: ClassVar[str] = "applicationrole"


class IssueFields(FullRefreshArrayedStream):
    name: ClassVar[str] = "issue_fields"
    path: ClassVar[str] = "field"


class IssueNavigatorSettings(FullRefreshArrayedStream):
    name: ClassVar[str] = "issue_navigator_settings"
    path: ClassVar[str] = "settings/columns"


class IssuePriorities(FullRefreshArrayedStream):
    name: ClassVar[str] = "issue_priorities"
    path: ClassVar[str] = "priority"


class IssueTypes(FullRefreshArrayedStream):
    name: ClassVar[str] = "issue_types"
    path: ClassVar[str] = "issuetype"


class ProjectCategories(FullRefreshArrayedStream):
    name: ClassVar[str] = "project_categories"
    path: ClassVar[str] = "projectCategory"


class ProjectTypes(FullRefreshArrayedStream):
    name: ClassVar[str] = "project_types"
    path: ClassVar[str] = "project/type"


class ProjectRoles(FullRefreshArrayedStream):
    name: ClassVar[str] = "project_roles"
    path: ClassVar[str] = "role"


class WorkflowStatusCategories(FullRefreshArrayedStream):
    name: ClassVar[str] = "workflow_status_categories"
    path: ClassVar[str] = "statuscategory"


class WorkflowStatuses(FullRefreshArrayedStream):
    name: ClassVar[str] = "workflow_statuses"
    path: ClassVar[str] = "status"


# Full refresh resources whose API response is an array containing a subset of results,
# and are paginated with a `startAt` parameter but lack pagination metadata.
class FullRefreshPaginatedArrayedStream(FullRefreshStream):
    pass


class Users(FullRefreshPaginatedArrayedStream):
    name: ClassVar[str] = "users"
    path: ClassVar[str] = "users/search"


# Full refresh resources whose API response contains pagination information.
class FullRefreshPaginatedStream(FullRefreshStream):
    response_model: ClassVar[type[PaginatedResponse]] = PaginatedResponse


class DashboardsResponse(PaginatedResponse):
    values: list[FullRefreshResource] = Field(alias="dashboards")


class Dashboards(FullRefreshPaginatedStream):
    name: ClassVar[str] = "dashboards"
    path: ClassVar[str] = "dashboard"
    response_model: ClassVar[type[PaginatedResponse]] = DashboardsResponse


class IssueFieldConfigurations(FullRefreshPaginatedStream):
    name: ClassVar[str] = "issue_field_configurations"
    path: ClassVar[str] = "fieldconfiguration"


class IssueResolutions(FullRefreshPaginatedStream):
    name: ClassVar[str] = "issue_resolutions"
    path: ClassVar[str] = "resolution/search"


class IssueTypeScreenSchemes(FullRefreshPaginatedStream):
    name: ClassVar[str] = "issue_type_screen_schemes"
    path: ClassVar[str] = "issuetypescreenscheme"


class IssueTypeSchemes(FullRefreshPaginatedStream):
    name: ClassVar[str] = "issue_type_schemes"
    path: ClassVar[str] = "issuetypescheme"


class Filters(FullRefreshPaginatedStream):
    name: ClassVar[str] = "filters"
    path: ClassVar[str] = "filter/search"


class Screens(FullRefreshPaginatedStream):
    name: ClassVar[str] = "screens"
    path: ClassVar[str] = "screens"


class ScreenTabs(FullRefreshPaginatedStream):
    name: ClassVar[str] = "screen_tabs"
    path: ClassVar[str] = "screens/tabs"


class ScreenSchemes(FullRefreshPaginatedStream):
    name: ClassVar[str] = "screen_schemes"
    path: ClassVar[str] = "screenscheme"


class Groups(FullRefreshPaginatedStream):
    name: ClassVar[str] = "groups"
    path: ClassVar[str] = "group/bulk"


class WorkflowSchemes(FullRefreshPaginatedStream):
    name: ClassVar[str] = "workflow_schemes"
    path: ClassVar[str] = "workflowscheme"


class Workflows(FullRefreshPaginatedStream):
    name: ClassVar[str] = "workflows"
    path: ClassVar[str] = "workflows/search"
    extra_params: ClassVar[dict[str, str]] = {"orderBy": "created"}


class Projects(FullRefreshPaginatedStream):
    name: ClassVar[str] = "projects"
    path: ClassVar[str] = "project/search"
    extra_params: ClassVar[dict[str, str]] = {
        "status": "live,archived,deleted",
        "expand": "description,lead,projectKeys,url,issueTypes"
    }


# Full refresh resources whose API response contains an array of results at some top level field.
class FullRefreshNestedArrayStream(FullRefreshStream):
    response_field: ClassVar[str]


class IssueSecuritySchemes(FullRefreshNestedArrayStream):
    name: ClassVar[str] = "issue_security_schemes"
    path: ClassVar[str] = "issuesecurityschemes"
    response_field: ClassVar[str] = "issueSecuritySchemes"


class IssueLinkTypes(FullRefreshNestedArrayStream):
    name: ClassVar[str] = "issue_link_types"
    path: ClassVar[str] = "issueLinkType"
    response_field: ClassVar[str] = "issueLinkTypes"


class PermissionSchemes(FullRefreshNestedArrayStream):
    name: ClassVar[str] = "permission_schemes"
    path: ClassVar[str] = "permissionscheme"
    response_field: ClassVar[str] = "permissionSchemes"


# One of a kind full refresh streams whose request and/or response don't align
# with other endpoints & require special handling.
class SystemAvatars(FullRefreshStream):
    name: ClassVar[str] = "system_avatars"
    path: ClassVar[str] = "avatar/TYPE/system"


class SystemAvatarsResponse(BaseModel, extra="allow"):
    system: list[FullRefreshResource]


class Permissions(FullRefreshStream):
    name: ClassVar[str] = "permissions"
    path: ClassVar[str] = "permissions"


class PermissionsResponse(BaseModel, extra="allow"):
    permissions: dict[str, dict[str, str]]


class Labels(FullRefreshStream):
    name: ClassVar[str] = "labels"
    path: ClassVar[str] = "label"


class LabelsResponse(PaginatedResponse):
    values: list[str]


# Child streams
class FilterSharing(FullRefreshStream):
    name: ClassVar[str] = "filter_sharing"
    path: ClassVar[str] = "permission"


class IssueCustomFieldContexts(FullRefreshStream):
    name: ClassVar[str] = "issue_custom_field_contexts"
    path: ClassVar[str] = "context"


class IssueCustomFieldOptions(FullRefreshStream):
    name: ClassVar[str] = "issue_custom_field_options"
    path: ClassVar[str] = "option"


class ScreenTabFields(FullRefreshStream):
    name: ClassVar[str] = "screen_tab_fields"
    path: ClassVar[str] = "fields"


class ProjectChildStream(FullRefreshStream):
    add_parent_id_to_documents: ClassVar[bool] = True


class ProjectAvatars(ProjectChildStream):
    name: ClassVar[str] = "project_avatars"
    path: ClassVar[str] = "avatars"


class ProjectAvatarsResponse(BaseModel):
    custom: list[FullRefreshResource]
    system: list[FullRefreshResource]


class ProjectComponents(ProjectChildStream):
    name: ClassVar[str] = "project_components"
    path: ClassVar[str] = "component"
    add_parent_id_to_documents: ClassVar[bool] = False


class ProjectEmails(ProjectChildStream):
    name: ClassVar[str] = "project_emails"
    path: ClassVar[str] = "email"


class ProjectVersions(ProjectChildStream):
    name: ClassVar[str] = "project_versions"
    path: ClassVar[str] = "version"
    add_parent_id_to_documents: ClassVar[bool] = False


FULL_REFRESH_STREAMS: list[type[FullRefreshStream]] = [
    ApplicationRoles,
    Dashboards,
    Filters,
    Groups,
    Labels,
    Permissions,
    SystemAvatars,
    IssueSecuritySchemes,
    IssueLinkTypes,
    IssueFieldConfigurations,
    IssueFields,
    IssueNavigatorSettings,
    IssueResolutions,
    IssueTypeSchemes,
    IssueTypeScreenSchemes,
    IssueTypes,
    PermissionSchemes,
    ProjectCategories,
    ProjectRoles,
    ProjectTypes,
    Projects,
    Screens,
    ScreenSchemes,
    ScreenTabs,
    Users,
    WorkflowSchemes,
    WorkflowStatusCategories,
    WorkflowStatuses,
    Workflows,
    IssuePriorities,
    FilterSharing,
    IssueCustomFieldContexts,
    IssueCustomFieldOptions,
    ScreenTabFields,
    ProjectAvatars,
    ProjectComponents,
    ProjectEmails,
    ProjectVersions,
]
