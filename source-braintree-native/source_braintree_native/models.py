from datetime import datetime, timezone, timedelta
from pydantic import AwareDatetime, BaseModel, Field
from typing import Literal


from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    ResourceConfig,
    ResourceState,
)

def default_start_date():
    dt = datetime.now(timezone.utc) - timedelta(days=30)
    return dt

class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any Braintree data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date.",
        title="Start Date",
        default_factory=default_start_date,
    )
    merchant_id: str = Field(
        description="Braintree Merchant ID associated with your account.",
        title="Merchant ID",
    )

    class ApiKey(BaseModel):
        credentials_title: Literal["API Key"] = Field(
            default="API Key",
            json_schema_extra={"type": "string"}
        )
        public_key: str = Field(
            title="Public Key",
            json_schema_extra={"secret": True},
        )
        private_key: str = Field(
            title="Private Key",
            json_schema_extra={"secret": True},
        )

    credentials: ApiKey = Field(
        title="Authentication",
        discriminator="credentials_title"
    )

    class Advanced(BaseModel):
        is_sandbox: bool = Field(
            description="Check if you are using a Braintree Sandbox environment.",
            title="Is a Sandbox Environment",
            default=False,
        )

    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

ConnectorState = GenericConnectorState[ResourceState]


class FullRefreshResource(BaseDocument, extra="allow"):
    id: str | None


# Supported full refresh resources and their corresponding name, gateway property, and gateway response property.
FULL_REFRESH_RESOURCES: list[tuple[str, str, str | None]] = [
    ("merchant_accounts", "merchant_account", "merchant_accounts"),
    ("discounts", "discount", None),
    ("add_ons", "add_on", None),
    ("plans", "plan", None),
]


class Customer(BaseDocument, extra="allow"):
    id: str
    created_at: AwareDatetime


class Dispute(BaseDocument, extra="allow"):
    id: str
    created_at: AwareDatetime


class Subscription(BaseDocument, extra="allow"):
    id: str
    created_at: AwareDatetime


class Transaction(BaseDocument, extra="allow"):
    id: str
    created_at: AwareDatetime


class CreditCardVerification(BaseDocument, extra="allow"):
    id: str
    created_at: AwareDatetime
