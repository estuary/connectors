from typing import Any, List, Mapping, Optional, Tuple, TYPE_CHECKING
import requests, json
import subprocess
import tempfile

from estuary_cdk.capture.common import (
    AccessToken,
    BaseDocument,
    BaseOAuth2Credentials,
    OAuth2Spec,
    ResourceConfig,
    ResourceState,
)

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)

from pydantic import AwareDatetime, BaseModel, Field, model_validator

OAUTH2_SPEC = OAuth2Spec(
    provider="salesforce",
    authUrlTemplate=(
        r"https://login.salesforce.com/services/oauth2/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&response_type=code&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate=r"https://login.salesforce.com/services/oauth2/token",
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        "grant_type=authorization_code"
        r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
      ),
    accessTokenResponseMap={"refresh_token": "/refresh_token"},
)


if TYPE_CHECKING:
    OAuth2Credentials = BaseOAuth2Credentials
else:
    OAuth2Credentials = BaseOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)

class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials | AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

ConnectorState = GenericConnectorState[ResourceState]

class RestResponse(BaseModel):
    totalSize: int
    done: bool
    records: List
    nextRecordsUrl: str | None = None

class RestStream(BaseDocument, extra="allow"):
    Id: str

class BulkJobResponse(BaseModel, extra="allow"):
    id: str
    operation: str
    object: str
    createdById: str
    createdDate: AwareDatetime
    systemModstamp: AwareDatetime
    state: str
    concurrencyMode: str
    contentType: str
    apiVersion: float
    lineEnding: str
    columnDelimiter: str

class BulkDataResponse(BaseDocument, extra="allow"):
    Id: str

