"""Pydantic models for the source-claude-api (Claude Admin API) connector."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from pydantic import AwareDatetime, BaseModel, Field, computed_field

ConnectorState = GenericConnectorState[ResourceState]

# The Claude Admin API requires this static version header on every request.
# It is not part of the x-api-key auth handled by TokenSource, so it must be
# passed explicitly via headers= on every http.request* call.
ANTHROPIC_VERSION_HEADERS = {"anthropic-version": "2023-06-01"}

DEFAULT_BASE_URL = "https://api.anthropic.com"


def default_start_date() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=7)


class EndpointConfig(BaseModel):
    admin_api_key: Annotated[
        str,
        Field(
            title="Admin API Key",
            description="Anthropic Admin API key (starts with `sk-ant-admin...`). "
            "Used as the x-api-key header for all Admin API requests.",
            json_schema_extra={"secret": True},
        ),
    ]

    class Advanced(BaseModel):
        base_url: Annotated[
            str,
            Field(
                default=DEFAULT_BASE_URL,
                title="Base URL",
                description="Base URL for the Anthropic API.",
            ),
        ]
        start_date: Annotated[
            AwareDatetime,
            Field(
                default_factory=default_start_date,
                title="Start Date",
                description="UTC date from which to begin capturing the Claude Code "
                "usage report. Defaults to 7 days ago. Only used by the "
                "ClaudeCodeUsageReport stream.",
            ),
        ]

    advanced: Annotated[
        Advanced,
        Field(
            default_factory=Advanced,
            title="Advanced",
            json_schema_extra={"advanced": True},
        ),
    ]


class Organization(BaseDocument, extra="allow"):
    id: str


class User(BaseDocument, extra="allow"):
    id: str
    # The docs mark added_at as always-present, but that's [INFERRED] (no published
    # JSON Schema). Keep it optional so a single member missing it can't fail the
    # whole snapshot page; it isn't a key field.
    added_at: AwareDatetime | None = None


class UserList(BaseModel, extra="allow"):
    data: list[User]
    has_more: bool
    last_id: str | None = None


class ClaudeCodeUsageRecord(BaseDocument, extra="allow"):
    date: AwareDatetime
    organization_id: str
    # The API nests the actor identity under `actor`, a discriminated union of
    # user_actor (email_address) / api_actor (api_key_name). The composite key
    # needs a single non-null scalar, so derive a flat actor_id here.
    actor: dict[str, Any]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def actor_id(self) -> str:
        actor = self.actor or {}
        identity = actor.get("email_address") or actor.get("api_key_name")
        if identity:
            return identity
        # Unknown/empty actor shape (this stream is live-unvalidated). Derive a stable
        # id from the raw actor so distinct actors never collapse onto a shared key and
        # silently overwrite each other; a new actor shape surfaces as a new key, not
        # as lost rows.
        if actor:
            digest = hashlib.sha1(
                json.dumps(actor, sort_keys=True, default=str).encode()
            ).hexdigest()[:16]
            return f"{actor.get('type', 'unknown')}:{digest}"
        return "unknown"


class ClaudeCodeUsageReport(BaseModel, extra="allow"):
    # Defaults make a missing/short final page terminate the day-drain loop cleanly
    # instead of raising mid-stream (this envelope is live-unvalidated).
    data: list[ClaudeCodeUsageRecord] = []
    has_more: bool = False
    next_page: str | None = None
