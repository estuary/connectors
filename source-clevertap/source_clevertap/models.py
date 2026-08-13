"""
Config and document models for the CleverTap native capture connector.

The two document models (`ProfileData`, `EventsData`) reproduce the exact
columns/keys of the Fivetran `clevertap_csp_api` connector so the Snowflake
tables (`PROFILE_DATA`, `EVENTS_DATA`) stay identical.
"""

from typing import Optional

from pydantic import BaseModel, Field

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)

# Resource (binding) names. `events_data` is the *driver*: its fetch performs the
# single CleverTap scan and emits BOTH event docs and (as AssociatedDocuments)
# profile docs into `profile_data`. See resources.py / api.py.
EVENTS = "events_data"
PROFILE = "profile_data"


class EndpointConfig(BaseModel):
    account_id: str = Field(
        title="CleverTap Account ID",
        description="Value sent as the X-CleverTap-Account-Id header.",
    )
    secret: str = Field(
        title="CleverTap Passcode",
        description="Value sent as the X-CleverTap-Passcode header.",
        json_schema_extra={"secret": True},
    )
    event_names: list[str] = Field(
        title="Event Names",
        description="CleverTap event names to pull (the connector fetches each one).",
    )
    api_base_url: str = Field(
        default="https://eu1.api.clevertap.com/1/events.json",
        title="API Base URL",
        description="Region-specific CleverTap events.json endpoint (eu1, us1, in1, ...).",
    )
    timezone: str = Field(
        default="Asia/Kolkata",
        title="Timezone",
        description="Timezone used to derive the CleverTap from/to date range.",
    )

    class Advanced(BaseModel):
        start_date: Optional[str] = Field(
            default=None,
            title="Start Date",
            description="YYYY-MM-DD to begin the first sync from. Defaults to 2 days ago.",
        )
        end_date: Optional[str] = Field(
            default=None,
            title="End Date",
            description="YYYY-MM-DD (inclusive) to stop pulling at. Leave empty to keep "
            "pulling up to the current day. Set it only for a bounded historical backfill.",
        )
        lookback_days: int = Field(
            default=1,
            title="Lookback Days",
            description="Re-pull this many days before the cursor each run to catch "
            "late-arriving data. Duplicates are deduped by event_id.",
        )
        skip_invalid_events: bool = Field(
            default=False,
            title="Skip Invalid Events",
            description="If CleverTap rejects an event name as invalid, log a warning "
            "and continue with the next event instead of failing the whole capture.",
        )

    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class ProfileData(BaseDocument, extra="ignore"):
    """Matches Fivetran PROFILE_DATA — key /clevertapId, all STRING columns."""

    clevertapId: str
    name: Optional[str] = None
    identity: Optional[str] = None
    platform: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[str] = None
    os: Optional[str] = None
    device: Optional[str] = None
    make: Optional[str] = None
    brand: Optional[str] = None
    sdklevel: Optional[str] = None
    accountid: Optional[str] = None
    appversioncode: Optional[str] = None
    language: Optional[str] = None
    appversion: Optional[str] = None
    cspid: Optional[str] = None
    model: Optional[str] = None
    osversion: Optional[str] = None


class EventsData(BaseDocument, extra="ignore"):
    """Matches Fivetran EVENTS_DATA — key /event_id (SHA-256), all STRING columns."""

    event_id: str
    clevertapId: Optional[str] = None
    timestamp: Optional[str] = None
    event_name: Optional[str] = None
    properties: Optional[str] = None
