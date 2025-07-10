from datetime import datetime, timezone, timedelta
from enum import StrEnum
from pydantic import AwareDatetime, BaseModel, Field
from typing import ClassVar, Literal, Optional


from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    ClientCredentialsOAuth2Credentials,
    OAuth2TokenFlowSpec,
    ResourceConfig,
    ResourceState,
)


OAUTH2_SPEC = OAuth2TokenFlowSpec(
    # The access token URL requires a cloud region dependent domain. REGION_DEPENDENT_DOMAIN is replaced
    # at runtime with the genesys_cloud_domain configured by the user.
    accessTokenUrlTemplate=f"https://login.REGION_DEPENDENT_DOMAIN/oauth/token",
    accessTokenResponseMap={
        "access_token": "/access_token"
    }
)

# The class name appears in the UI's Authentication section, so we wrap the non-user friendly name in a slighly better name.
# TODO(alex): figure out why the class name is appearing in the UI & determine if there's some property to set that overrides it.
class OAuth(ClientCredentialsOAuth2Credentials):
    pass


def default_start_date():
    dt = datetime.now(timezone.utc) - timedelta(days=30)
    return dt

class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any event data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date.",
        title="Start Date",
        default_factory=default_start_date,
    )
    # Genesis Cloud URLs are listed at https://help.mypurecloud.com/articles/aws-regions-for-genesys-cloud-deployment/.
    genesys_cloud_domain: Literal[
        'aps1.pure.cloud',
        'apne3.pure.cloud',
        'apne2.pure.cloud',
        'mypurecloud.com.au',
        'mypurecloud.jp',
        'cac1.pure.cloud',
        'mypurecloud.de',
        'mypurecloud.ie',
        'euw2.pure.cloud',
        'euc2.pure.cloud',
        'mec1.pure.cloud',
        'sae1.pure.cloud',
        'mypurecloud.com',
        'use2.us-gov-pure.cloud',
        'usw2.pure.cloud',
    ] = Field(
        title="Genesys Cloud Domain"
    )
    credentials: OAuth = Field(
        title="Authentication",
        discriminator="credentials_title"
    )


ConnectorState = GenericConnectorState[ResourceState]


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class GenesysResource(FullRefreshResource):
    id: str


class SnapshotListResponse(BaseModel, extra="allow"):
    entities: list[GenesysResource]
    pageSize: int
    pageNumber: int


class SnapshotSearchResponse(BaseModel, extra="allow"):
    results: list[GenesysResource] | None = None
    pageSize: int
    pageNumber: int
    total: int
    pageCount: int



class Conversation(BaseDocument, extra="allow"):
    conversationId: str
    conversationStart: AwareDatetime
    # Conversations that have not ended do not have a conversationEnd field.
    conversationEnd: AwareDatetime = Field(
        default=None,
        # Don't schematize the default value.
        json_schema_extra=lambda x: x.pop('default') # type: ignore
    )


class CreateJobResponse(BaseModel, extra="forbid"):
    jobId: str


class CheckJobStatusResponse(BaseModel, extra="forbid"):
    state: Literal[
        "QUEUED",
        "PENDING",
        "FAILED",
        "CANCELLED",
        "FULFILLED",
        "EXPIRED",
    ]
    submissionDate: AwareDatetime
    completionDate: AwareDatetime | None = None # Only included if the job completed.
    expirationDate: AwareDatetime | None = None # Only included if the job has expired.
    errorMessage: str | None = None # Only included if the job status is FAILED.


class JobResultsResponse(BaseModel, extra="forbid"):
    conversations: list[Conversation]
    dataAvailabilityDate: AwareDatetime
    cursor: str | None = None # Included if response does not contain the last page of results.


class SortOrderParam(StrEnum):
    ASC = "ASC"
    ASCENDING = "ascending"
    DESC = "DESC"
    DESCENDING = "descending"

class EndpointType(StrEnum):
    LIST = "list"
    SEARCH = "search"


class GenesysStream():
    name: ClassVar[str]
    path: ClassVar[str]
    sort_order_param: ClassVar[SortOrderParam] = SortOrderParam.ASC
    extra_params: ClassVar[Optional[dict[str, str]]] = None
    endpoint_type: ClassVar[EndpointType] = EndpointType.LIST


class Users(GenesysStream):
    name: ClassVar[str] = "users"
    path: ClassVar[str] = "users"
    extra_params: ClassVar[Optional[dict[str, str]]] = {
        "expand": "team",
    }


class Teams(GenesysStream):
    name: ClassVar[str] = "teams"
    path: ClassVar[str] = "teams/search"
    endpoint_type: ClassVar[EndpointType] = EndpointType.SEARCH


class Queues(GenesysStream):
    name: ClassVar[str] = "queues"
    path: ClassVar[str] = "routing/queues"


class Campaigns(GenesysStream):
    name: ClassVar[str] = "campaigns"
    path: ClassVar[str] = "outbound/campaigns/all"
    sort_order_param: ClassVar[SortOrderParam] = SortOrderParam.ASCENDING


class MessagingCampaigns(GenesysStream):
    name: ClassVar[str] = "messaging_campaigns"
    path: ClassVar[str] = "outbound/messagingcampaigns"
    sort_order_param: ClassVar[SortOrderParam] = SortOrderParam.ASCENDING


class GenesysChildStream(GenesysStream):
    parent_stream: ClassVar[type[GenesysStream]]
    parent_id_field: ClassVar[str]


class QueueMembers(GenesysChildStream):
    name: ClassVar[str] = "queue_members"
    path: ClassVar[str] = "members"
    parent_stream: ClassVar[type[GenesysStream]] = Queues
    parent_id_field: ClassVar[str] = "queueId"


FULL_REFRESH_STREAMS: list[type[GenesysStream]] = [
    Campaigns,
    MessagingCampaigns,
    QueueMembers,
    Queues,
    Teams,
    Users,
]
