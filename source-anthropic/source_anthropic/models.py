from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, ClassVar, Literal
from urllib.parse import quote

from pydantic import AwareDatetime, BaseModel, Field, model_validator

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken


DEFAULT_START_DATE_DAYS_AGO = 30
DEFAULT_LOOKBACK_HOURS = 48
# Anthropic documents data landing up to 24 hours late, so a shorter lookback
# could advance past rows that haven't arrived yet.
MIN_LOOKBACK_HOURS = 24
MAX_LOOKBACK_HOURS = 168


def utc_now() -> datetime:
    """The current time. Tests patch it to pin "now"."""
    return datetime.now(tz=UTC)


def default_start_date() -> datetime:
    return utc_now() - timedelta(days=DEFAULT_START_DATE_DAYS_AGO)


# An Anthropic Admin API key (Claude Console orgs, `sk-ant-admin01-...`) or an
# Analytics API key with the `read:analytics` scope (Claude Enterprise orgs),
# sent raw in the `x-api-key` header. A comment, not a docstring: pydantic would
# publish a docstring as this schema's description.
class ApiKey(AccessToken):

    credentials_title: Literal["API Key"] = Field(
        default="API Key",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="API Key",
        description="An Admin API key (Claude Console organizations) or an Analytics API key with the read:analytics scope (Claude Enterprise organizations).",
        json_schema_extra={"secret": True, "order": 1},
    )


OrganizationType = Literal["Claude Console", "Claude Enterprise"]

# The dimensions each organization type groups by: the API allows five, and
# dimensions that don't apply to an organization type are always null. For
# Claude Enterprise, `api_key_id` and `workspace_id` are always null, so those
# slots go to the dimensions that explain price differences.
GROUP_BY: dict[str, list[str]] = {
    "Claude Console": ["actor", "model", "product", "api_key_id", "workspace_id"],
    "Claude Enterprise": ["actor", "model", "product", "speed", "context_window"],
}


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present. Anthropic serves no data before 2026-01-01 or older than 365 days.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    organization_type: OrganizationType = Field(
        title="Organization Type",
        description="Claude Console organizations are grouped by actor, model, product, API key and workspace. Claude Enterprise organizations are grouped by actor, model, product, speed and context window, since API keys and workspaces are always empty for them. Changing this changes what each row means, so reset and backfill the collections afterwards.",
    )

    class Advanced(BaseModel):
        lookback_hours: int = Field(
            default=DEFAULT_LOOKBACK_HOURS,
            title="Lookback Window (Hours)",
            description="Hours of usage and cost data to re-fetch on each sync, to pick up late-arriving and revised data. Minutely streams re-fetch at least the last 24 hours and don't backfill history. Once a month, the first sync after 00:00 UTC on the 9th also re-fetches the previous month, which Anthropic may revise until about seven days after it ends.",
            ge=MIN_LOOKBACK_HOURS,
            le=MAX_LOOKBACK_HOURS,
        )

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore[arg-type]
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


@dataclass(frozen=True)
class Granularity:
    """A `time_bucket_width` and the request limits that go with it."""

    name: str
    bucket_width: str
    tick: timedelta
    # The widest `starting_at`..`ending_at` span the API accepts.
    max_span: timedelta
    # Set for granularities that don't backfill history: each sweep re-reads
    # at least this much (or the lookback, if longer) back from now.
    rolling_window: timedelta | None = None


MINUTELY = Granularity(
    name="minutely",
    bucket_width="1m",
    tick=timedelta(minutes=1),
    max_span=timedelta(hours=24),
    rolling_window=timedelta(hours=24),
)
HOURLY = Granularity(
    name="hourly", bucket_width="1h", tick=timedelta(hours=1), max_span=timedelta(days=31)
)
DAILY = Granularity(
    name="daily", bucket_width="1d", tick=timedelta(days=1), max_span=timedelta(days=31)
)
GRANULARITIES = [MINUTELY, HOURLY, DAILY]


# Actors are discriminated by `type`. A new actor type fails validation
# loudly rather than being keyed ambiguously.
class UserActor(BaseModel, extra="allow"):
    type: Literal["user_actor"]
    user_id: str


class ServiceAccountActor(BaseModel, extra="allow"):
    type: Literal["service_account_actor"]
    service_account_id: str


class ApiActor(BaseModel, extra="allow"):
    type: Literal["api_actor"]
    api_key_id: str


class FederatedProvider(BaseModel, extra="allow"):
    type: str
    account_id: str | None = None
    subscription_id: str | None = None
    project_number: str | None = None
    issuer: str | None = None
    # AWS only: the role that signed the request. With a null subject it is
    # what tells two principals in one account apart.
    signed_principal: str | None = None


class FederatedActor(BaseModel, extra="allow"):
    type: Literal["federated_actor"]
    provider: FederatedProvider
    subject: str | None


Actor = Annotated[
    UserActor | ServiceAccountActor | ApiActor | FederatedActor,
    Field(discriminator="type"),
]


def _escape(value: str | None) -> str:
    """Percent-encode every character but letters, digits and `-._~`, so a
    value can never contain the key's `:`, `|` or `=` separators."""
    return quote(value or "", safe="")


def actor_key(actor: Actor | None) -> str:
    match actor:
        case None:
            return ""
        case UserActor():
            return f"user:{_escape(actor.user_id)}"
        case ServiceAccountActor():
            return f"service_account:{_escape(actor.service_account_id)}"
        case ApiActor():
            return f"api:{_escape(actor.api_key_id)}"
        case FederatedActor():
            p = actor.provider
            provider_id = p.account_id or p.subscription_id or p.project_number or p.issuer
            return "federated:" + ":".join(
                _escape(v)
                for v in (p.type, provider_id, p.signed_principal, actor.subject)
            )


class UsageGroup(BaseModel, extra="allow"):
    actor: Actor | None
    model: str | None
    product: str | None
    api_key_id: str | None
    workspace_id: str | None
    speed: str | None
    context_window: str | None

    def key_parts(self) -> list[tuple[str, str]]:
        """Each dimension's name and its escaped value. actor_key escapes its
        own parts, so its `:` separators stay readable."""
        return [
            ("actor", actor_key(self.actor)),
            ("model", _escape(self.model)),
            ("product", _escape(self.product)),
            ("api_key", _escape(self.api_key_id)),
            ("workspace", _escape(self.workspace_id)),
            ("speed", _escape(self.speed)),
            ("context_window", _escape(self.context_window)),
        ]


class CostGroup(UsageGroup):
    cost_type: str | None
    token_type: str | None

    def key_parts(self) -> list[tuple[str, str]]:
        return super().key_parts() + [
            ("cost_type", _escape(self.cost_type)),
            ("token_type", _escape(self.token_type)),
        ]


class ReportRow(BaseDocument, extra="allow"):
    """One bucket of one group combination, keyed by its start and a
    never-null `group_key` string built from the grouped dimensions."""

    PATH: ClassVar[str]
    # The top-level field holding the row's numbers, summed when rows merge.
    METRICS: ClassVar[str]
    # Grouping dimensions this row type adds to the organization's GROUP_BY.
    EXTRA_GROUP_BY: ClassVar[list[str]] = []
    GROUP_MODEL: ClassVar[type[UsageGroup]] = UsageGroup

    starting_at: AwareDatetime
    group_key: str = Field(
        description="The grouped dimensions joined into one string, with empty values for null dimensions."
    )

    @model_validator(mode="before")
    @classmethod
    def _set_group_key(cls, data: Any) -> Any:
        if isinstance(data, dict) and "group_key" not in data:
            group = cls.GROUP_MODEL.model_validate(data["group"])
            data = {
                **data,
                "group_key": "|".join(f"{k}={v}" for k, v in group.key_parts()),
            }
        return data

    def merge(self, other: "ReportRow") -> None:
        """Add `other`'s metrics into this row's. Anthropic returns each
        identity it can't render as its own null-actor row, so several rows
        can share one group_key within a bucket; their totals are the sum."""
        mine = (self.model_extra or {})[self.METRICS]
        theirs = (other.model_extra or {})[self.METRICS]
        setattr(self, self.METRICS, _sum_metrics(mine, theirs))


def _sum_metrics(a: Any, b: Any) -> Any:
    """Sum two metric values of the same shape. Numbers add, decimal strings
    (cost amounts in cents) add exactly and keep their precision, nulls are
    absent values, and any other string (e.g. `currency`) must match."""
    if a is None:
        return b
    if b is None:
        return a
    if isinstance(a, dict) and isinstance(b, dict):
        return {k: _sum_metrics(a.get(k), b.get(k)) for k in a.keys() | b.keys()}
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool):
        return a + b
    if isinstance(a, str) and isinstance(b, str):
        try:
            return str(Decimal(a) + Decimal(b))
        except InvalidOperation:
            if a == b:
                return a
    raise ValueError(f"cannot sum metric values {a!r} and {b!r}")


class UsageRow(ReportRow):
    PATH: ClassVar[str] = "usage"
    METRICS: ClassVar[str] = "usage"


class CostRow(ReportRow):
    PATH: ClassVar[str] = "cost"
    METRICS: ClassVar[str] = "cost"
    # cost_type and token_type don't count toward the API's five-dimension
    # group_by limit.
    EXTRA_GROUP_BY: ClassVar[list[str]] = ["cost_type", "token_type"]
    GROUP_MODEL: ClassVar[type[UsageGroup]] = CostGroup


class ReportPage[RowT: ReportRow](BaseModel):
    data: list[RowT]
    has_more: bool
    next_page: str | None
    # Null when the organization has no exported data yet.
    data_refreshed_at: AwareDatetime | None


__all__ = [
    "ConnectorState",
    "EndpointConfig",
    "ResourceConfig",
    "ResourceState",
]
