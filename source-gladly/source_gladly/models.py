from enum import StrEnum
from pydantic import BaseModel, Field
from typing import TYPE_CHECKING

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    BaseDocument,
    ResourceConfig,
    ResourceState,
)

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
        secret=True,
    )


ConnectorState = GenericConnectorState[ResourceState]


# Event represents one of the many data types returned from the Events API. The "content" field
# contains the entity-specific data, and its schema allows additional properties to be inferred
# through schema inference.
class Event(BaseDocument, extra="forbid"):

    class Initiator(BaseDocument, extra="forbid"):
        id: str
        type: str

    class Content(BaseDocument, extra="allow"):
        class Context(BaseDocument, extra="allow"):
            pass

        context: Context

    id: str
    type: str
    timestamp: str
    initiator: Initiator
    content: Content
