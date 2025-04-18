import abc
from datetime import datetime, UTC, timedelta
from typing import Literal

from estuary_cdk.capture.common import (
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


class FakeOAuth2Credentials(abc.ABC, BaseModel):
    credentials_title: Literal["Fake OAuth Credentials"] = Field(
        default="Fake OAuth Credentials",
        json_schema_extra={"type": "string"}
    )
    client_id: str = Field(
        title="Client Id",
        json_schema_extra={"secret": True},
    )
    client_secret: str = Field(
        title="Client Secret",
        json_schema_extra={"secret": True},
    )
    refresh_token: str = Field(
        title="Refresh Token",
        json_schema_extra={"secret": True},
    )


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )

    credentials: FakeOAuth2Credentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    class Advanced(BaseModel):
        should_encrypt: bool = Field(
            description="Whether or not to encrypt the config in the configUpdate event. Defaults to always encrypting.",
            title="Encrypt config in configUpdate events",
            default=True,
        )

    advanced: Advanced = Field(
        default_factory=Advanced, #type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

ConnectorState = GenericConnectorState[ResourceState]


class Resource(BaseDocument, extra="allow"):
    id: int
