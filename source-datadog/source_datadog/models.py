from typing import Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel, Field, AwareDatetime

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
    ConnectorState as GenericConnectorState,
)

class EndpointConfig(BaseModel):
    """Configuration for the Datadog API endpoint."""
    api_key: str = Field(
        description="Datadog API Key",
        title="API Key",
        json_schema_extra={"secret": True},
    )
    app_key: str = Field(
        description="Datadog Application Key",
        title="Application Key",
        json_schema_extra={"secret": True},
    )
    site: str = Field(
        default="datadoghq.com",
        description="Datadog site (e.g., datadoghq.com, datadoghq.eu)",
        title="Site",
    )
    start_time: Optional[datetime] = Field(
        default=None,
        description="When to start collecting data from. If not specified, defaults to 1 hour ago. Use ISO format (e.g., '2024-03-01T00:00:00Z').",
        title="Start Time",
    )

    class Advanced(BaseModel):
        base_url: Optional[str] = Field(
            None,
            description="Base URL to use for connecting to Datadog API. Only override if using a custom endpoint.",
            title="Base URL",
        )

    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

class RUMEvent(BaseDocument, extra="allow"):
    """Represents a RUM event from Datadog."""
    id: str
    type: str
    timestamp: AwareDatetime
    attributes: dict
    relationships: Optional[dict] = None

    class Meta(BaseDocument.Meta):
        op: str = "c"  # All events are creates

ConnectorState = GenericConnectorState[ResourceState]

# Define the RUM events stream
RUM_EVENTS_STREAM = "RUMEvents" 