from braintree import BraintreeGateway
from datetime import datetime, timezone, timedelta
from logging import Logger
from pydantic import AwareDatetime, BaseModel, Field
from typing import Annotated, AsyncGenerator, Callable, Literal

from estuary_cdk.capture.common import (
    BaseDocument,
    BasicAuth,
    ConnectorState as GenericConnectorState,
    LogCursor,
    PageCursor,
    ResourceConfigWithSchedule,
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

    class ApiKey(BasicAuth):
        credentials_title: Literal["API Key"] = Field(
            default="API Key",
            json_schema_extra={"type": "string"}
        )
        username: str = Field(
            title="Public Key",
            json_schema_extra={"secret": True},
        )
        password: str = Field(
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
        window_size: Annotated[int, Field(
            description="Window size in hours for incremental streams. This should be left as the default value unless connector errors indicate a smaller window size is required.",
            title="Window Size",
            default=24,
            gt=0,
        )]

    advanced: Advanced = Field(
        default_factory=Advanced, #type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

ConnectorState = GenericConnectorState[ResourceState]


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class IncrementalResource(BaseDocument, extra="allow"):
    id: str
    created_at: AwareDatetime


class Transaction(IncrementalResource):
    updated_at: AwareDatetime


IncrementalResourceFetchChangesFn = Callable[
    [BraintreeGateway, int, Logger, LogCursor],
    AsyncGenerator[IncrementalResource | LogCursor, None],
]

IncrementalResourceFetchPageFn = Callable[
    [BraintreeGateway, int, Logger, PageCursor, LogCursor],
    AsyncGenerator[IncrementalResource | PageCursor, None],
]
