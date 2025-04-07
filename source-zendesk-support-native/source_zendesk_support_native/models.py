from datetime import datetime, UTC, timedelta
from enum import StrEnum
import json
from typing import Annotated, Any, Literal, TYPE_CHECKING

from estuary_cdk.capture.common import (
    BasicAuth,
    BaseDocument,
    LongLivedClientCredentialsOAuth2Credentials,
    OAuth2Spec,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    LogCursor,
    Logger,
)
from estuary_cdk.http import HTTPSession

from pydantic import AfterValidator, AwareDatetime, BaseModel, Field


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MAX_INCREMENTAL_EXPORT_PAGE_SIZE = 1000


def urlencode_field(field: str):
    return "{{#urlencode}}{{{ " + field + " }}}{{/urlencode}}"

accessTokenBody = {
    "grant_type": "authorization_code",
    "code": "{{{ code }}}",
    "client_id": "{{{ client_id }}}",
    "client_secret": "{{{ client_secret }}}",
    "redirect_uri": "{{{ redirect_uri }}}",
    "scope": "read"
}


OAUTH2_SPEC = OAuth2Spec(
    provider="zendesk",
    accessTokenBody=json.dumps(accessTokenBody),
    authUrlTemplate=(
        "https://{{{ config.subdomain }}}.zendesk.com/oauth/authorizations/new?"
        f"response_type=code&"
        f"client_id={urlencode_field('client_id')}&"
        f"redirect_uri={urlencode_field('redirect_uri')}&"
        f"scope=read&"
        f"state={urlencode_field('state')}"
    ),
    accessTokenUrlTemplate=("https://{{{ config.subdomain }}}.zendesk.com/oauth/tokens"),
    accessTokenResponseMap={
        "access_token": "/access_token",
    },
    accessTokenHeaders={
        "Content-Type": "application/json",
    },
)


if TYPE_CHECKING:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


class ApiToken(BasicAuth):
    credentials_title: Literal["Email & API Token"] = Field(
        default="Email & API Token",
        json_schema_extra={"type": "string"}
    )
    username: Annotated[str, AfterValidator(lambda s: s + '/token')] = Field(
        title="Email"
    )
    password: str = Field(
        title="API Token",
        json_schema_extra={"secret": True},
    )


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    subdomain: str = Field(
        description="This is your Zendesk subdomain that can be found in your account URL. For example, in https://{MY_SUBDOMAIN}.zendesk.com/, where MY_SUBDOMAIN is the value of your subdomain.",
        title="Subdomain",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
    )
    credentials: OAuth2Credentials | ApiToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    class Advanced(BaseModel):
        incremental_export_page_size: Annotated[int, Field(
            description="Page size for incremental export streams. Typically left as the default unless Estuary Support or the connector logs indicate otherwise.",
            title="Incremental Export Streams' Page Size",
            default=MAX_INCREMENTAL_EXPORT_PAGE_SIZE,
            le=MAX_INCREMENTAL_EXPORT_PAGE_SIZE,
            gt=0,
        )]

    advanced: Advanced = Field(
        default_factory=Advanced, #type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class ZendeskResource(FullRefreshResource):
    id: int


class TimestampedResource(ZendeskResource):
    updated_at: AwareDatetime


class IncrementalTimeExportResponse(BaseModel, extra="allow"):
    next_page: str | None
    count: int
    end_of_stream: bool
    end_time: int | None
    resources: list[TimestampedResource]


class OrganizationsResponse(IncrementalTimeExportResponse):
    resources: list[TimestampedResource] = Field(alias="organizations")

# Incremental time based export resources.
# Tuples contain the name, path, and response model for each resource.
INCREMENTAL_TIME_EXPORT_RESOURCES: list[tuple[str, str, type[IncrementalTimeExportResponse]]] = [
    ("organizations", "organizations", OrganizationsResponse),
]


class IncrementalCursorExportResponse(BaseModel, extra="allow"):
    after_cursor: str | None
    end_of_stream: bool
    resources: list[TimestampedResource]


class TicketsResponse(IncrementalCursorExportResponse):
    resources: list[TimestampedResource] = Field(alias="tickets")


class UsersResponse(IncrementalCursorExportResponse):
    resources: list[TimestampedResource] = Field(alias="users")


INCREMENTAL_CURSOR_EXPORT_TYPES = Literal["tickets", "users"]

INCREMENTAL_CURSOR_EXPORT_RESOURCES: list[INCREMENTAL_CURSOR_EXPORT_TYPES] = [
    "tickets",
    "users",
]


class FullRefreshResponse(BaseModel, extra="allow"):
    resources: list[FullRefreshResource]


class SchedulesResponse(FullRefreshResponse):
    resources: list[FullRefreshResource] = Field(alias="schedules")


# Full refresh resources with no pagination.
# Tuples contain the name, path, and response model for each resource.
FULL_REFRESH_RESOURCES: list[tuple[str, str, type[FullRefreshResponse]]] = [
    ("schedules", "business_hours/schedules", SchedulesResponse),
]


class FullRefreshOffsetPaginatedResponse(FullRefreshResponse):
    next_page: str | None


class AccountAttributesResponse(FullRefreshOffsetPaginatedResponse):
    resources: list[FullRefreshResource] = Field(alias="attributes")


class SlaPoliciesResponse(FullRefreshOffsetPaginatedResponse):
    resources: list[FullRefreshResource] = Field(alias="sla_policies")


# Full refresh resources that paginted with page offsets.
# Tuples contain the name, path, and response model for each resource.
FULL_REFRESH_OFFSET_PAGINATED_RESOURCES: list[tuple[str, str, type[FullRefreshOffsetPaginatedResponse]]] = [
        ("account_attributes", "routing/attributes", AccountAttributesResponse),
        ("sla_policies", "slas/policies", SlaPoliciesResponse),
]


class FullRefreshCursorPaginatedResponse(FullRefreshResponse):
    class Meta(BaseModel, extra="forbid"):
        has_more: bool
        # after_cursor and before_cursor are not present in the response if there are no results.
        after_cursor: str | None = None
        before_cursor: str | None = None

    meta: Meta
    resources: list[FullRefreshResource]


class TagsResponse(FullRefreshCursorPaginatedResponse):
    resources: list[FullRefreshResource] = Field(alias="tags")

# Full refresh resources that are paginated through with a cursor.
# Tuples contain the name, path, and response model for each resource.
FULL_REFRESH_CURSOR_PAGINATED_RESOURCES: list[tuple[str, str, type[FullRefreshCursorPaginatedResponse]]] = [
    ("tags", "tags", TagsResponse),
]


class IncrementalCursorPaginatedResponse(FullRefreshCursorPaginatedResponse):
    resources: list[ZendeskResource]


class SatisfactionRatingsResponse(IncrementalCursorPaginatedResponse):
    resources: list[ZendeskResource] = Field(alias="satisfaction_ratings")


class TicketSkipsResponse(IncrementalCursorPaginatedResponse):
    resources: list[ZendeskResource] = Field(alias="skips")


class TicketMetricEventsResponse(IncrementalCursorPaginatedResponse):
    resources: list[ZendeskResource] = Field(alias="ticket_metric_events")


class TicketActivitiesResponse(IncrementalCursorPaginatedResponse):
    resources: list[ZendeskResource] = Field(alias="activities")


class FilterParam(StrEnum):
    START_TIME = "start_time"
    SINCE = "since"


# Incremental resources that can be filtered by a start_time query param.
# Tuples contain the name, path, filter param name, cursor field, and response model for each resource. 
INCREMENTAL_CURSOR_PAGINATED_RESOURCES: list[tuple[str, str, FilterParam, str, type[IncrementalCursorPaginatedResponse]]] = [
    ("ticket_skips", "skips", FilterParam.START_TIME, "updated_at", TicketSkipsResponse),
    ("ticket_metric_events", "incremental/ticket_metric_events", FilterParam.START_TIME, "time", TicketMetricEventsResponse),
    ("ticket_activities", "activities", FilterParam.START_TIME, "updated_at", TicketActivitiesResponse),
]


class TicketAuditsResponse(IncrementalCursorPaginatedResponse):
    resources: list[ZendeskResource] = Field(alias="audits")


class TicketCommentsResponse(IncrementalCursorPaginatedResponse):
    resources: list[ZendeskResource] = Field(alias="comments")


class AbbreviatedTicket(BaseModel):
    id: int
    status: str
    updated_at: AwareDatetime


# Resources that are fetched by following the tickets stream & fetching resources for updated tickets in a separate request.
# Tuples contain the name, path, and response model for each resource.
TICKET_CHILD_RESOURCES: list[tuple[str, str, type[IncrementalCursorPaginatedResponse]]] = [
    ("ticket_audits", "audits", TicketAuditsResponse),
    ("ticket_comments", "comments", TicketCommentsResponse),
]


class ClientSideIncrementalOffsetPaginatedResponse(FullRefreshOffsetPaginatedResponse):
    resources: list[TimestampedResource]


class CustomRolesResponse(ClientSideIncrementalOffsetPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="custom_roles")


class TicketFormsResponse(ClientSideIncrementalOffsetPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="ticket_forms")


# Incremental client side resources that are paginated with page offsets.
# Tuples contain the name, path, and response model for each resource.
CLIENT_SIDE_FILTERED_OFFSET_PAGINATED_RESOURCES: list[tuple[str, str, type[ClientSideIncrementalOffsetPaginatedResponse]]] = [
    ("custom_roles", "custom_roles", CustomRolesResponse),
    ("ticket_forms", "ticket_forms", TicketFormsResponse),
]


class ClientSideIncrementalCursorPaginatedResponse(FullRefreshCursorPaginatedResponse):
    resources: list[TimestampedResource]


class AutomationsResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="automations")


class BrandsResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="brands")


class TicketFieldsResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="ticket_fields")


class GroupsResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="groups")


class GroupMembershipsResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="group_memberships")


class MacrosResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="macros")


class OrganizationMembershipsResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="organization_memberships")


class Post(TimestampedResource):
    comment_count: int
    vote_count: int


class PostsResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[Post] = Field(alias="posts")


class TopicsResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="topics")


# Incremental client side resources that are paginated with a cursor.
# Tuples contain the name, path, any additional request query params, and response model for each resource.
CLIENT_SIDE_FILTERED_CURSOR_PAGINATED_RESOURCES: list[tuple[str, str, dict[str, str | int] | None, type[ClientSideIncrementalCursorPaginatedResponse]]] = [
    ("automations", "automations", None, AutomationsResponse),
    ("brands", "brands", None, BrandsResponse),
    ("groups", "groups", {"exclude_deleted": "false"}, GroupsResponse),
    ("group_memberships", "group_memberships", None, GroupMembershipsResponse),
    ("macros", "macros", None, MacrosResponse),
    ("organization_memberships", "organization_memberships", None, OrganizationMembershipsResponse),
    ("posts", "community/posts", None, PostsResponse),
    ("ticket_fields", "ticket_fields", None, TicketFieldsResponse),
    ("topics", "community/topics", None, TopicsResponse),
]


class PostVotesResponse(IncrementalCursorPaginatedResponse):
    resources: list[ZendeskResource] = Field(alias="votes")


class PostComment(ZendeskResource):
    post_id: int
    vote_count: int


class PostCommentsResponse(IncrementalCursorPaginatedResponse):
    resources: list[PostComment] = Field(alias="comments")


# Resources that are fetched by following the posts stream & fetching resources for updated posts in a separate request.
# Tuples contain the name, path segment, and response model for each resource.
POST_CHILD_RESOURCES: list[tuple[str, str, type[IncrementalCursorPaginatedResponse]]] = [
    ("post_votes", "votes", PostVotesResponse),
    ("post_comments", "comments", PostCommentsResponse),
]


class AuditLog(ZendeskResource):
    created_at: AwareDatetime


class AuditLogsResponse(FullRefreshCursorPaginatedResponse):
    resources: list[AuditLog] = Field(alias="audit_logs")


class PostCommentVotesResponse(PostVotesResponse):
    pass
