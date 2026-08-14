"""
Config and document models for the Azure Service Bus capture connector.

Replaces the Azure Function + HTTP-ingest bridge with a native pull connector:
Estuary runs it on a schedule, it drains the queue/subscription via a Service Bus
receiver, and settles (completes) each message only after Estuary checkpoints it.
"""

from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)


class EndpointConfig(BaseModel):
    connection_string: str = Field(
        title="Service Bus Connection String",
        description="Namespace-level connection string (NO ';EntityPath=' suffix).",
        json_schema_extra={"secret": True},
    )
    queue_name: Optional[str] = Field(
        default=None,
        title="Queue Name",
        description="Queue to read from. Set this OR topic_name + subscription_name.",
    )
    topic_name: Optional[str] = Field(
        default=None,
        title="Topic Name",
        description="Topic to read from (requires subscription_name).",
    )
    subscription_name: Optional[str] = Field(
        default=None,
        title="Subscription Name",
        description="Subscription on the topic to read from.",
    )

    class Advanced(BaseModel):
        max_messages_per_sweep: int = Field(
            default=5000,
            title="Max Messages Per Sweep",
            description="Upper bound on messages drained in a single invocation.",
        )
        max_wait_seconds: int = Field(
            default=5,
            title="Max Wait Seconds",
            description="Seconds to wait for a batch before treating the entity as drained.",
        )
        prefetch_count: int = Field(
            default=250,
            title="Prefetch / Batch Size",
            description="Messages requested per receive call.",
        )

    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        json_schema_extra={"advanced": True},
    )

    @model_validator(mode="after")
    def _validate_entity(self) -> "EndpointConfig":
        has_queue = bool(self.queue_name)
        has_sub = bool(self.topic_name and self.subscription_name)
        if has_queue == has_sub:
            raise ValueError(
                "Configure EITHER queue_name OR (topic_name AND subscription_name)."
            )
        return self

    def entity_name(self) -> str:
        """Stable resource/binding name for the configured entity."""
        if self.queue_name:
            return self.queue_name
        return f"{self.topic_name}.{self.subscription_name}"


ConnectorState = GenericConnectorState[ResourceState]


class ServiceBusLog(BaseDocument, extra="allow"):
    """One document per Service Bus message. Keyed on sequence_number (unique &
    monotonic within an entity), so a redelivery upserts instead of duplicating."""

    sequence_number: int
    message_id: Optional[str] = None
    enqueued_time: Optional[str] = None
    subject: Optional[str] = None
    content_type: Optional[str] = None
    correlation_id: Optional[str] = None
    application_properties: dict[str, Any] = {}
    body: Any = None
