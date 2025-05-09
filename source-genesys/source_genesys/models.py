from datetime import datetime, timezone, timedelta
from pydantic import AwareDatetime, BaseModel, Field
from typing import Literal


from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    ClientCredentialsOAuth2Credentials,
    ClientCredentialsOAuth2Spec,
    ResourceConfig,
    ResourceState,
)


OAUTH2_SPEC = ClientCredentialsOAuth2Spec(
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


class User(BaseDocument, extra="allow"):
    pass


class UserResponse(BaseModel, extra="forbid"):
    entities: list[User]
    pageSize: int
    pageNumber: int
    total: int
    pageCount: int
    lastUri: str
    firstUri: str
    selfUri: str

class Team(BaseDocument, extra="allow"):
    id: str
    name: str
    dateModified: str

class TeamResponse(BaseModel, extra="forbid"):
    entities: list[Team]
    nextUri: str
    selfUri: str
    previousUri: str


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
