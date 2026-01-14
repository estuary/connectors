from datetime import datetime, UTC, timedelta
from enum import StrEnum
import json
from typing import Annotated, Any, Literal, TYPE_CHECKING

from estuary_cdk.capture.common import (
    BasicAuth,
    BaseDocument,
    LongLivedClientCredentialsOAuth2Credentials,
    RotatingOAuth2Credentials,
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

from pydantic import AfterValidator, AwareDatetime, BaseModel, Field, ValidationInfo, model_validator


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Valid subdomains are between 3-63 characters long, start with a
# lowercase letter, and only contain lowercase letters, digits, or dashes.
SUBDOMAIN_REGEX = r"^[a-z][a-z0-9-]{2,62}$"

# Zendesk supports variable access token durations, between 5 minutes to 2 days.
# To reduce how frequently we need to perform token rotations, we use the
# largest possible access token duration.
MAX_VALID_ACCESS_TOKEN_DURATION = int(timedelta(days=2).total_seconds())

def urlencode_field(field: str):
    return "{{#urlencode}}{{{ " + field + " }}}{{/urlencode}}"

accessTokenBody = {
    "grant_type": "authorization_code",
    "code": "{{{ code }}}",
    "client_id": "{{{ client_id }}}",
    "client_secret": "{{{ client_secret }}}",
    "redirect_uri": "{{{ redirect_uri }}}",
    "scope": "read",
    "expires_in": MAX_VALID_ACCESS_TOKEN_DURATION,
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
        "refresh_token": "/refresh_token",
        "access_token_expires_at": r"{{#now_plus}}{{ expires_in }}{{/now_plus}}",
    },
    accessTokenHeaders={
        "Content-Type": "application/json",
    },
    additionalTokenExchangeBody={
        "expires_in": MAX_VALID_ACCESS_TOKEN_DURATION,
    },
)

# Mustache templates in accessTokenUrlTemplate are not interpolated within the connector, so update_oauth_spec reassigns it for use within the connector.
def update_oauth_spec(subdomain: str):
    OAUTH2_SPEC.accessTokenUrlTemplate = f"https://{subdomain}.zendesk.com/oauth/tokens"


if TYPE_CHECKING:
    OAuth2Credentials = RotatingOAuth2Credentials
    DeprecatedOAuthCredentials = LongLivedClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = RotatingOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)

    class DeprecatedOAuthCredentials(
        LongLivedClientCredentialsOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)  # type: ignore
    ):
        credentials_title: Literal["Deprecated OAuth Credentials"] = Field(
            default="Deprecated OAuth Credentials",
            json_schema_extra={"type": "string"},
        )


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
        description="This is your Zendesk subdomain that can be found in your account URL. For example, in https://{MY_SUBDOMAIN}.zendesk.com, MY_SUBDOMAIN is the value of your subdomain.",
        title="Subdomain",
        pattern=SUBDOMAIN_REGEX,
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
    )
    credentials: OAuth2Credentials | ApiToken | DeprecatedOAuthCredentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

    # Zendesk is updating their OAuth to expire access tokens, meaning this connector
    # will need to use RotatingOAuth2Credentials instead of DeprecatedOAuthCredentials.
    # During the migration period where both types of OAuth credentials are valid, we'll
    # show the RotatingOAuth2Credentials option as the _only_ OAuth option in the UI &
    # we'll hide the DeprecatedOAuthCredentials option. This lets existing
    # captures continue working until users re-authenticate their tasks with the
    # RotatingOAuth2Credentials option.
    #
    # Patching model_json_schema to remove DeprecatedOAuthCredentials makes
    # RotatingOAuth2Credentials the only OAuth authentication option in the UI, and effectively
    # "hides" the deprecated OAuth option. This is a bit gross, but it should only be needed for
    # a little bit until all users that authenticated with OAuth have re-authenticated.
    @classmethod
    def model_json_schema(cls, *args, **kwargs) -> dict:
        schema = super().model_json_schema(*args, **kwargs)

        creds = schema.get("properties", {}).get("credentials", {})
        one_of = creds.get("oneOf", [])
        filtered_one_of = []

        for option in one_of:
            if "$ref" in option:
                ref = option["$ref"]
                def_name = ref.split("/")[-1]
                # Do not include DeprecatedOAuthCredentials as an authentication option.
                if "DeprecatedOAuthCredentials" in def_name:
                    continue

            filtered_one_of.append(option)

        creds["oneOf"] = filtered_one_of
        return schema

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


class CustomTicketStatusesResponse(FullRefreshCursorPaginatedResponse):
    resources: list[FullRefreshResource] = Field(alias="custom_statuses")


# Full refresh resources that are paginated through with a cursor.
# Tuples contain the name, path, and response model for each resource.
FULL_REFRESH_CURSOR_PAGINATED_RESOURCES: list[tuple[str, str, type[FullRefreshCursorPaginatedResponse]]] = [
    ("custom_ticket_statuses", "custom_statuses", CustomTicketStatusesResponse),
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


class TicketChildResourceValidationContext:
    def __init__(self, ticket_id: int):
        self.ticket_id = ticket_id


class TicketChildResource(ZendeskResource):
    @model_validator(mode="before")
    @classmethod
    def _add_ticket_id(cls, values: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:
        if info.context and isinstance(info.context, TicketChildResourceValidationContext):
            if 'ticket_id' not in values:
                values['ticket_id'] = info.context.ticket_id
        else:
            raise RuntimeError("TicketChildResource requires either a TicketChildResourceValidationContext containing the ticket_id.")
        return values


class TicketAuditsResponse(IncrementalCursorPaginatedResponse):
    resources: list[TicketChildResource] = Field(alias="audits")


class TicketCommentsResponse(IncrementalCursorPaginatedResponse):
    resources: list[TicketChildResource] = Field(alias="comments")


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


class ArticlesResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="articles")


class CategoriesResponse(ClientSideIncrementalCursorPaginatedResponse):
    resources: list[TimestampedResource] = Field(alias="categories")


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
    ("macros", "macros", {"include": "permissions"}, MacrosResponse),
    ("organization_memberships", "organization_memberships", None, OrganizationMembershipsResponse),
    ("posts", "community/posts", None, PostsResponse),
    ("ticket_fields", "ticket_fields", None, TicketFieldsResponse),
    ("topics", "community/topics", None, TopicsResponse),
    ("articles", "help_center/articles", None, ArticlesResponse),
    ("categories", "help_center/categories", None, CategoriesResponse),
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
