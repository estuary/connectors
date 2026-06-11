import re
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, Union

from pydantic import AwareDatetime, BaseModel, Field, model_validator

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import BasicAuth, ClientCredentialsOAuth2Credentials

_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class BasicCredentials(BasicAuth):
    # Subclasses the CDK type to reuse its username/password fields.
    # auth_type is added for the discriminated union.
    auth_type: Literal["basic"] = "basic"


class OAuthCredentials(ClientCredentialsOAuth2Credentials):
    # Subclasses the CDK type so TokenSource's isinstance checks pass,
    # while adding auth_type for the discriminated union.
    auth_type: Literal["client_credentials"] = "client_credentials"


Credentials = Annotated[
    Union[BasicCredentials, OAuthCredentials],
    Field(discriminator="auth_type", title="Authentication"),
]


class EndpointConfig(BaseModel, title="Source Zuora Configuration"):
    credentials: Credentials
    base_url: str = Field(
        title="API Base URL",
        description=(
            "Zuora REST API base URL. Use https://rest.zuora.com for US production, "
            "https://rest.apisandbox.zuora.com for US sandbox, "
            "or https://rest.eu.zuora.com for EU production."
        ),
        default="https://rest.zuora.com",
        examples=[
            "https://rest.zuora.com",
            "https://rest.apisandbox.zuora.com",
            "https://rest.eu.zuora.com",
            "https://rest.sandbox.eu.zuora.com",
        ],
    )
    start_date: AwareDatetime = Field(
        title="Start Date",
        description=(
            "UTC date and time from which to start replicating data. "
            "Defaults to January 1, 2000, which captures all available history."
        ),
        default_factory=lambda: datetime(2000, 1, 1, tzinfo=UTC),
    )


ConnectorState = GenericConnectorState[ResourceState]


class ZuoraDocument(BaseDocument, extra="allow"):
    Id: str = ""
    UpdatedDate: AwareDatetime | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_dates(cls, data: Any) -> Any:
        # Zuora returns some date fields as date-only strings ('YYYY-MM-DD').
        # Append a UTC midnight time so Pydantic accepts them as AwareDatetime.
        if not isinstance(data, dict):
            return data
        return {
            k: (v + "T00:00:00+00:00" if isinstance(v, str) and _DATE_ONLY_RE.match(v) else v)
            for k, v in data.items()
        }
