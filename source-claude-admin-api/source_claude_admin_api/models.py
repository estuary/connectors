"""Pydantic models for the source-claude-admin-api (Claude Admin API) connector.

Each document model is aware of its own API endpoint via the `api_path` ClassVar
on `BaseClaudeEntity`; the fetch functions in api.py read that instead of
hardcoding URLs, and delegate to the fetch shape that fits the resource.
"""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, ClassVar, Generic, TypeVar

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

# The Claude Admin API requires this static version header on every request. It is
# not part of the x-api-key auth handled by TokenSource, so it must be passed
# explicitly via headers= on every http.request* call.
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


# --- Endpoint-aware base + document models ---
# api_path encapsulates the endpoint on the model; fetchers read it. Keep models
# minimal (extra="allow" passes everything else through to schema inference).


class BaseClaudeEntity(BaseDocument, extra="allow"):
    resource_name: ClassVar[str]
    api_path: ClassVar[str]
    page_limit: ClassVar[int] = 1000


class Organization(BaseClaudeEntity):
    resource_name: ClassVar[str] = "Organization"
    api_path: ClassVar[str] = "v1/organizations/me"

    id: str


class User(BaseClaudeEntity):
    resource_name: ClassVar[str] = "Users"
    api_path: ClassVar[str] = "v1/organizations/users"

    id: str
    added_at: AwareDatetime | None = None


class ClaudeCodeUsageRecord(BaseClaudeEntity):
    resource_name: ClassVar[str] = "ClaudeCodeUsageReport"
    api_path: ClassVar[str] = "v1/organizations/usage_report/claude_code"

    date: AwareDatetime
    organization_id: str
    # actor is a discriminated union (user_actor: email_address / api_actor:
    # api_key_name). The composite key needs a single non-null scalar, so derive
    # a flat actor_id here.
    actor: dict[str, Any]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def actor_id(self) -> str:
        actor = self.actor or {}
        identity = actor.get("email_address") or actor.get("api_key_name")
        if identity:
            return identity
        # Unknown/empty actor shape (this stream is live-unvalidated). Derive a stable
        # id from the raw actor so distinct actors never collapse onto a shared key.
        if actor:
            digest = hashlib.sha1(
                json.dumps(actor, sort_keys=True, default=str).encode()
            ).hexdigest()[:16]
            return f"{actor.get('type', 'unknown')}:{digest}"
        return "unknown"


# --- Generic paginated response envelopes ---
# One generic per pagination contract, so no per-resource envelope class is needed.

T = TypeVar("T")


class CursorPage(BaseModel, Generic[T], extra="allow"):
    """ID-cursor list envelope: page forward with after_id = last_id while has_more."""

    data: list[T] = []
    has_more: bool = False
    last_id: str | None = None


class OpaquePage(BaseModel, Generic[T], extra="allow"):
    """Opaque-cursor report envelope: page forward with page = next_page while has_more."""

    data: list[T] = []
    has_more: bool = False
    next_page: str | None = None
