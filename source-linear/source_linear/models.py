from datetime import datetime, timedelta, UTC
from typing import Literal

from pydantic import AwareDatetime, BaseModel, Field

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class ApiKey(AccessToken):
    credentials_title: Literal["API Key"] = Field(
        default="API Key",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="API Key",
        description="Linear personal API key, created under Settings > Security & access > Personal API keys.",
        json_schema_extra={"secret": True, "order": 1},
    )


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]

# Stream document models and resource registries are added by `add-stream`.
