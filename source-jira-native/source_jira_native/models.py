from datetime import datetime, UTC, timedelta
from enum import auto, StrEnum
from typing import Any, ClassVar, Literal, Optional

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

# APIRecord is a convenience type.
APIRecord = dict[str, Any]


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class JiraResource(FullRefreshResource):
    id: int


class Issue(JiraResource):
    class Fields(BaseModel, extra="allow"):
        class NestedResources(BaseModel, extra="allow"):
            maxResults: int
            total: int

        class Comment(NestedResources):
            comments: list[APIRecord]

            @property
            def resources(self) -> list[APIRecord]:
                return self.comments

        class Worklog(NestedResources):
            worklogs: list[APIRecord]

            @property
            def resources(self) -> list[APIRecord]:
                return self.worklogs

        updated: AwareDatetime
        # The following fields only default to None when fetching
        # child resources of a specific issue. These fields are
        # always present when an issue is yielded.
        comment: Comment | None = None
        worklog: Worklog | None = None

    class ChangeLog(Fields.NestedResources):
        histories: list[APIRecord]

    fields: Fields
    # The following fields only default to None when fetching
    # child resources of a specific issue. These fields are
    # always present when an issue is yielded.
    transitions: list[APIRecord] | None = None
    changelog: ChangeLog | None = None


class PaginatedResponse(BaseModel, extra="allow"):
    maxResults: int
    startAt: int
    total: int | None =  None
    isLast: bool | None = None
    values: list[APIRecord]


class IssuesResponse(BaseModel, extra="allow"):
    issues: list[Issue]
    # nextPageToken is absent on the last page of results.
    nextPageToken: str | None = None


class MyselfResponse(BaseModel, extra="allow"):
    accountId: str
    timeZone: str


# The different Jira APIs.
class JiraAPI(StrEnum):
    # Platform API docs - https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/
    PLATFORM = auto()
    # Service Management API docs - https://developer.atlassian.com/cloud/jira/service-desk/rest/intro/
    SERVICE_MANAGEMENT = auto()
    # Software API docs - https://developer.atlassian.com/cloud/jira/software/rest/intro/
    SOFTWARE = auto()


class FullRefreshStream():
    name: ClassVar[str]
    path: ClassVar[str]
    extra_params: ClassVar[Optional[dict[str, str]]] = None
    extra_headers: ClassVar[Optional[dict[str, str]]] = None
    # Unless overwritten by a subclass, FullRefreshStreams use the Platform API.
    api: ClassVar[JiraAPI] = JiraAPI.PLATFORM


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


# The various Jira APIs use different parameter names for the
# exact same purpose of paginating through results. And also
# for specifying how many results to return in a single response.
class PaginationParameter(StrEnum):
    # startAt is used by the Platform and Software APIs.
    START_AT = "startAt"
    # start is used by the Service Management API.
    START = "start"


class ResponseSizeParameter(StrEnum):
    # maxResults is used by the Platform and Software APIs.
    MAX_RESULTS = "maxResults"
    # limit is used by the Service Management API.
    LIMIT = "limit"


# Full refresh resources whose API response contains pagination information.
class FullRefreshPaginatedStream(FullRefreshStream):
    response_model: ClassVar[type[PaginatedResponse]] = PaginatedResponse
    pagination_param: ClassVar[PaginationParameter] = PaginationParameter.START_AT
    response_size_param: ClassVar[ResponseSizeParameter] = ResponseSizeParameter.MAX_RESULTS


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


# Platform API child streams
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


class IssueChildResource(JiraResource):
    # The connector adds issueId into each document.
    issueId: int


class IssueCommentsResponse(PaginatedResponse):
    values: list[APIRecord] = Field(alias="comments")


class IssueWorklogsResponse(PaginatedResponse):
    values: list[APIRecord] = Field(alias="worklogs")


class IssueChildStream():
    name: ClassVar[str]
    path: ClassVar[str]
    api: ClassVar[JiraAPI] = JiraAPI.PLATFORM
    fields: ClassVar[str] = ""
    expand: ClassVar[str] = ""
    response_model: ClassVar[type[PaginatedResponse] | None] = None


class IssueComments(IssueChildStream):
    name: ClassVar[str] = "issue_comments"
    path: ClassVar[str] = "comment"
    fields: ClassVar[str] = "comment"
    response_model: ClassVar[type[IssueCommentsResponse]] = IssueCommentsResponse


class IssueChangelogs(IssueChildStream):
    name: ClassVar[str] = "issue_changelogs"
    path: ClassVar[str] = "changelog"
    expand: ClassVar[str] = "changelog"


class IssueTransitions(IssueChildStream):
    name: ClassVar[str] = "issue_transitions"
    path: ClassVar[str] = "transitions"
    expand: ClassVar[str] = "transitions"


class IssueWorklogs(IssueChildStream):
    name: ClassVar[str] = "issue_worklogs"
    path: ClassVar[str] = "worklog"
    fields: ClassVar[str] = "worklog"
    response_model: ClassVar[type[IssueWorklogsResponse]] = IssueWorklogsResponse


# Software API streams
class Boards(FullRefreshPaginatedStream):
    name: ClassVar[str] = "boards"
    path: ClassVar[str] = "board"
    extra_params: ClassVar[dict[str, str]] = {
        "includePrivate": "true",
        "orderBy": "name"
    }
    api: ClassVar[JiraAPI] = JiraAPI.SOFTWARE


# Software API child streams
class BoardChildStream(FullRefreshStream):
    add_parent_id_to_documents: ClassVar[bool] = True
    api: ClassVar[JiraAPI] = JiraAPI.SOFTWARE


class Epics(BoardChildStream):
    name: ClassVar[str] = "epics"
    path: ClassVar[str] = "epic"


class Sprints(BoardChildStream):
    name: ClassVar[str] = "sprints"
    path: ClassVar[str] = "sprint"
    add_parent_id_to_documents: ClassVar[bool] = False


# Service Managmement API Streams
class ServiceManagementPaginatedResponse(PaginatedResponse):
    maxResults: int = Field(alias="limit")
    startAt: int = Field(alias="start")
    isLast: bool | None = Field(default=None, alias="isLastPage")


class ServiceManagementFullRefreshPaginatedStream(FullRefreshPaginatedStream):
    api: ClassVar[JiraAPI] = JiraAPI.SERVICE_MANAGEMENT
    pagination_param: ClassVar[PaginationParameter] = PaginationParameter.START
    response_size_param: ClassVar[ResponseSizeParameter] = ResponseSizeParameter.LIMIT
    response_model: ClassVar[type[PaginatedResponse]] = ServiceManagementPaginatedResponse


class ServiceDesks(ServiceManagementFullRefreshPaginatedStream):
    name: ClassVar[str] = "service_desks"
    path: ClassVar[str] = "servicedesk"


class RequestTypes(ServiceManagementFullRefreshPaginatedStream):
    name: ClassVar[str] = "request_types"
    path: ClassVar[str] = "requesttype"
    # Jira classifies this endpoint as experimental & requires us to "opt-in"
    # to use it with the X-ExperimentalApi header.
    extra_headers: ClassVar[dict[str, str]] = {
        "X-ExperimentalApi": "opt-in"
    }


FULL_REFRESH_STREAMS: list[type[FullRefreshStream]] = [
    ApplicationRoles,
    Boards,
    Dashboards,
    Epics,
    FilterSharing,
    Filters,
    Groups,
    IssueCustomFieldContexts,
    IssueCustomFieldOptions,
    IssueFieldConfigurations,
    IssueFields,
    IssueLinkTypes,
    IssueNavigatorSettings,
    IssuePriorities,
    IssueResolutions,
    IssueSecuritySchemes,
    IssueTypeSchemes,
    IssueTypeScreenSchemes,
    IssueTypes,
    Labels,
    PermissionSchemes,
    Permissions,
    ProjectAvatars,
    ProjectCategories,
    ProjectComponents,
    ProjectEmails,
    ProjectRoles,
    ProjectTypes,
    ProjectVersions,
    Projects,
    RequestTypes,
    ScreenSchemes,
    ScreenTabFields,
    ScreenTabs,
    Screens,
    ServiceDesks,
    Sprints,
    SystemAvatars,
    Users,
    WorkflowSchemes,
    WorkflowStatusCategories,
    WorkflowStatuses,
    Workflows,
]


ISSUE_CHILD_STREAMS: list[type[IssueChildStream]] = [
    IssueComments,
    IssueChangelogs,
    IssueTransitions,
    IssueWorklogs,
]