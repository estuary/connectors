from typing import Optional

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from pydantic import AwareDatetime, BaseModel, Field

# Supported entity types for the Gladly Events API and the corresponding resource name.
EVENTS_ENTITY_TYPES: list[tuple[str, str]] = [
    ("AGENT_AVAILABILITY", "AgentAvailabilityEvents"),
    ("AGENT_STATUS", "AgentStatusEvents"),
    ("CONTACT", "ContactEvents"),
    ("CONVERSATION", "ConversationEvents"),
    ("CUSTOMER", "CustomerEvents"),
    ("PAYMENT_REQUEST", "PaymentRequestEvents"),
    ("TASK", "TaskEvents"),
]


class EndpointConfig(BaseModel):
    organization: str = Field(
        description="Organization to Request Data From",
        title="Organization",
    )
    agentEmail: str = Field(
        description="Agent Email Address to use for Authentication",
        title="Agent Email",
    )
    apiToken: str = Field(
        description="API Token to use for Authentication",
        title="API Token",
        json_schema_extra={"secret": True},
    )

    class Advanced(BaseModel):
        base_url: Optional[str] = Field(
            None,
            description="Base URL to use for connecting to Gladly. May be useful if you are connecting to a sandbox account that does not use a URL in the form of 'https://{organization}.gladly.com'",
            title="Base URL",
        )

    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


# Event represents one of the many data types returned from the Events API. The "content" field
# contains the entity-specific data, and its schema allows additional properties to be inferred
# through schema inference.
class Event(BaseDocument, extra="forbid"):

    class Initiator(BaseModel, extra="forbid"):
        id: str
        type: str

    class Content(BaseModel, extra="allow"):
        pass

    id: str
    type: str
    timestamp: AwareDatetime
    initiator: Initiator
    content: Content
