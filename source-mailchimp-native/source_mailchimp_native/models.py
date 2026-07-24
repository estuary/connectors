import abc
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, Literal, override

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.capture.common import (
    ResourceState,
    make_cursor_dict,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import (
    BasicAuth,
    LongLivedClientCredentialsOAuth2Credentials,
    OAuth2Spec,
)
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    JsonValue,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic.json_schema import SkipJsonSchema

# Mailchimp's OAuth flow returns a single non-expiring access token and no
# refresh token, so credentials take the long-lived client-credentials shape.
# The spec mirrors the legacy connector's Estuary-registered OAuth app for
# provider "mailchimp" — keep the provider name in sync with that registration.
OAUTH2_SPEC = OAuth2Spec(
    provider="mailchimp",
    authUrlTemplate=(
        "https://login.mailchimp.com/oauth2/authorize?response_type=code"
        r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate="https://login.mailchimp.com/oauth2/token",
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        "grant_type=authorization_code"
        r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
    ),
    accessTokenResponseMap={"access_token": "/access_token"},
)


if TYPE_CHECKING:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = LongLivedClientCredentialsOAuth2Credentials.for_provider(
        OAUTH2_SPEC.provider
    )


class ApiKey(BasicAuth):
    """Mailchimp API keys authenticate over HTTP Basic with the key as the
    password; the username is ignored by the API."""

    credentials_title: Literal["API Key"] = Field(
        default="API Key",
        json_schema_extra={"type": "string"},
    )
    # Mailchimp ignores the Basic-auth username, so it stays out of the config
    # schema entirely; TokenSource still reads it when building the header.
    username: SkipJsonSchema[str] = ""
    password: str = Field(
        title="API Key",
        description=(
            "Your Mailchimp API key, including the data center suffix "
            "(e.g. ends in -us21)."
        ),
        json_schema_extra={"secret": True},
        alias="api_key",
    )


class OAuthMetadata(BaseModel):
    """Response of Mailchimp's OAuth metadata endpoint, which reports the
    account's data-center-specific API endpoint."""

    api_endpoint: str


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: OAuth2Credentials | ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class MailchimpEntity(BaseDocument, abc.ABC, extra="allow"):
    """Base for documents fetched from Mailchimp collection endpoints.

    Holds only the identity every collection shares: `NAME` is the stream name
    and `ITEMS_KEY` is the envelope key the items array lives under (usually
    but not always the path's last segment). How the endpoint URL is formed
    differs by subtree"""

    NAME: ClassVar[str]
    ITEMS_KEY: ClassVar[str]


class MailchimpCollectionEntity(MailchimpEntity):
    """Document from a top-level collection endpoint, addressed by a static
    `PATH` under the account's base URL."""

    PATH: ClassVar[str]


class MailchimpList(MailchimpCollectionEntity):
    NAME: ClassVar[str] = "lists"
    PATH: ClassVar[str] = "lists"
    ITEMS_KEY: ClassVar[str] = "lists"

    id: str


class Campaign(MailchimpCollectionEntity):
    NAME: ClassVar[str] = "campaigns"
    PATH: ClassVar[str] = "campaigns"
    ITEMS_KEY: ClassVar[str] = "campaigns"

    # Declared because it's the collection key; nothing in code reads it.
    id: str

    create_time: AwareDatetime


class Automation(MailchimpCollectionEntity):
    """Classic automation workflow. The endpoint is feature-gated and empty on
    the test account, so this shape is docs-based; the required `id` fails
    loudly if a real payload disagrees."""

    NAME: ClassVar[str] = "automations"
    PATH: ClassVar[str] = "automations"
    ITEMS_KEY: ClassVar[str] = "automations"

    id: str


# A single parent ID. String everywhere except segment IDs, which Mailchimp
# returns as integers (and which can legitimately be 0 — see `IdOnly`).
type ParentId = str | int
# A resolved parent binding: each key is a `ParentLevel.id_field` placeholder
# name (`list_id`, `segment_id`, ...) mapped to the ID it resolved to. Built up
# across the fan-out, spliced into leaf `PATH_TEMPLATE`s, and (for injected keys)
# stamped into `_meta`.
type ParentContext = dict[str, ParentId]


@dataclass(frozen=True)
class ParentIdValidationContext:
    """Validation context stamping parent IDs onto child documents whose
    response items omit them. Only IDs absent from the response body belong
    here — IDs the API already returns (e.g. `list_id` on segment members)
    must not be injected."""

    fields: ParentContext


class MailchimpChildEntity(MailchimpEntity):
    """Base for documents fetched from parent-scoped collection endpoints.

    `PATH_TEMPLATE` carries `{<id_field>}` placeholders (e.g.
    "lists/{list_id}/interest-categories") that `snapshot_children` fills
    from the parent-ID fan-out described by the stream's `ChildSpec`."""

    PATH_TEMPLATE: ClassVar[str]

    class Meta(BaseDocument.Meta, extra="allow"):
        """`extra="allow"` lets context-stamped parent IDs ride in `_meta`;
        `BaseDocument.Meta` otherwise drops unknown keys."""

    meta_: Meta = Field(
        default_factory=lambda: MailchimpChildEntity.Meta(op="u"),
        alias="_meta",
        description="Document metadata",
    )

    @model_validator(mode="before")
    @classmethod
    def _inject_parent_ids(cls, data: Any, info: ValidationInfo) -> Any:
        if isinstance(data, dict) and isinstance(
            info.context, ParentIdValidationContext
        ):
            meta = dict(data.get("_meta") or {})
            meta.update(info.context.fields)
            data["_meta"] = meta
        return data


class InterestCategory(MailchimpChildEntity):
    NAME: ClassVar[str] = "interest_categories"
    PATH_TEMPLATE: ClassVar[str] = "lists/{list_id}/interest-categories"
    ITEMS_KEY: ClassVar[str] = "categories"

    id: str
    list_id: str


class Interest(MailchimpChildEntity):
    """Items carry both parent IDs natively."""

    NAME: ClassVar[str] = "interests"
    PATH_TEMPLATE: ClassVar[str] = (
        "lists/{list_id}/interest-categories/{category_id}/interests"
    )
    ITEMS_KEY: ClassVar[str] = "interests"

    id: str
    list_id: str
    category_id: str


class Tag(MailchimpChildEntity):
    """Tag-search items are bare `{id, name}`; the parent `list_id` is stamped
    into `_meta` (the response omits it). Tags are Mailchimp static segments
    under the hood — this stream is the `{id, name}` projection of the segments
    stream's `type: static` subset.

    Pagination rides offset/count, which tag-search honors but does not
    document; if the provider ever drops it, the walker degrades to a single
    page of up to 1000 tags (fallback enumeration exists via
    `GET /lists/{list_id}/segments?type=static`)."""

    NAME: ClassVar[str] = "tags"
    PATH_TEMPLATE: ClassVar[str] = "lists/{list_id}/tag-search"
    ITEMS_KEY: ClassVar[str] = "tags"

    id: int
    name: str


class SegmentMember(MailchimpChildEntity):
    """Items carry `list_id` natively, so it stays a body field; the parent
    `segment_id` is absent from the response and stamped into `_meta`."""

    NAME: ClassVar[str] = "segment_members"
    PATH_TEMPLATE: ClassVar[str] = "lists/{list_id}/segments/{segment_id}/members"
    ITEMS_KEY: ClassVar[str] = "members"

    id: str
    list_id: str


class AutomationEmail(MailchimpChildEntity):
    """Email in a classic automation workflow. Like its `Automation` parent,
    the endpoint is feature-gated and empty on the test account, so this shape
    is docs-based; the required `id` fails loudly if a real payload disagrees.

    The parent `workflow_id` is stamped into `_meta` (we can't live-verify
    whether the response carries it), and the parent fan-out rejects an empty
    `workflow_id` rather than build a malformed `automations//emails` path."""

    NAME: ClassVar[str] = "automation_emails"
    PATH_TEMPLATE: ClassVar[str] = "automations/{workflow_id}/emails"
    ITEMS_KEY: ClassVar[str] = "emails"

    id: str


@dataclass(frozen=True)
class ParentLevel:
    """A parent collection drained for IDs that fill the `{placeholder}` in a
    child stream's `PATH_TEMPLATE`."""

    # Collection path drained for IDs (e.g. `lists`, `automations`). It is a
    # top-level collection, so the template has no placeholders of its own.
    path_template: str
    # Envelope key the item array lives under in the response (e.g. `lists`,
    # `automations`); also the `fields=<items_key>.id` projection target when
    # draining IDs.
    items_key: str
    # Placeholder name these IDs bind to in the child's PATH_TEMPLATE, and the
    # `_meta` key used when the ID is injected (e.g. `list_id`, `workflow_id`).
    id_field: str


LISTS_PARENT = ParentLevel(path_template="lists", items_key="lists", id_field="list_id")


@dataclass(frozen=True)
class ChildSpec:
    """How `snapshot_children` reaches one child stream: which parent
    collection to drain, and how to bind its IDs to the child requests."""

    # Child document model; its `PATH_TEMPLATE` is filled from the resolved parent
    # ID and its `ITEMS_KEY` locates the items in each response.
    model: type[MailchimpChildEntity]
    # The single parent collection to drain — one child request per parent ID.
    # (Grandchild streams don't ride this path; they have explicit walkers.)
    parent: ParentLevel
    # Whether to stamp the parent ID into each document's `_meta`. Set True only
    # when the response omits it; leave False when the API already returns it
    # (injecting a duplicate is wrong — see `ParentIdValidationContext`).
    inject_parent_id: bool = False


# Interest and SegmentMember are the multi-level (grandchild) streams; they have
# their own explicit walkers in `api.py` (`snapshot_interests`,
# `snapshot_segment_members`) rather than riding this single-level spec path.
SNAPSHOT_CHILD_STREAMS: list[ChildSpec] = [
    ChildSpec(model=InterestCategory, parent=LISTS_PARENT),
    ChildSpec(model=Tag, parent=LISTS_PARENT, inject_parent_id=True),
    ChildSpec(
        model=AutomationEmail,
        parent=ParentLevel(
            path_template="automations",
            items_key="automations",
            id_field="workflow_id",
        ),
        inject_parent_id=True,
    ),
]


class IdOnly(BaseModel):
    """Projection model for draining a parent collection into bare IDs via
    `fields=<items_key>.id` (see `ParentId` for the str/int split).

    An empty-string ID is rejected here: it would template a malformed child
    path (e.g. `automations//emails`) that returns nothing, silently orphaning
    every child rather than failing. The automation streams can't be
    live-verified, so the invariant is enforced rather than trusted. Integer
    IDs (segments) can legitimately be 0, so only the empty string is rejected."""

    id: ParentId

    @field_validator("id")
    @classmethod
    def _reject_empty(cls, value: ParentId) -> ParentId:
        if value == "":
            raise ValueError("parent ID must not be empty")
        return value


class MailchimpIncrementalChildEntity(MailchimpChildEntity, abc.ABC):
    """List-child document with an updated-style cursor, captured
    incrementally rather than by snapshot."""

    # The endpoint's server-side cursor filters: `SINCE_PARAM` lower-bounds
    # results by the cursor field (exclusive of the exact timestamp) and
    # `BEFORE_PARAM` upper-bounds them
    SINCE_PARAM: ClassVar[str]
    BEFORE_PARAM: ClassVar[str]

    @abc.abstractmethod
    def get_cursor(self) -> AwareDatetime: ...


class ListMember(MailchimpIncrementalChildEntity):
    """Member (contact) of a list. `id` is the MD5 of the lowercase email
    address, so it is unique only within its list — the stream key is the
    (`list_id`, `id`) composite. The unfiltered listing silently excludes
    archived members, so the stream walks each list twice: once bare, once
    with `status=archived` (see the per-sweep subtasks in `resources.py`)."""

    NAME: ClassVar[str] = "list_members"
    PATH_TEMPLATE: ClassVar[str] = "lists/{list_id}/members"
    ITEMS_KEY: ClassVar[str] = "members"
    SINCE_PARAM: ClassVar[str] = "since_last_changed"
    BEFORE_PARAM: ClassVar[str] = "before_last_changed"

    id: str
    list_id: str
    email_address: str
    # Cursor. Advances on update, archive, and unarchive, so both directions
    # of the archive transition are observed.
    last_changed: AwareDatetime
    status: str

    @override
    def get_cursor(self) -> AwareDatetime:
        return self.last_changed


class Segment(MailchimpIncrementalChildEntity):
    """List segment. Every `type` partition (`saved`, `static`, `fuzzy`) flows
    through the one endpoint, and the bare listing is the full population —
    segments have no archived/hidden state (unlike members). Member tags
    materialize as `type: static` segments, so this stream is a superset of
    the `tags` stream's projection."""

    NAME: ClassVar[str] = "segments"
    PATH_TEMPLATE: ClassVar[str] = "lists/{list_id}/segments"
    ITEMS_KEY: ClassVar[str] = "segments"
    SINCE_PARAM: ClassVar[str] = "since_updated_at"
    BEFORE_PARAM: ClassVar[str] = "before_updated_at"

    # Integer, unlike most Mailchimp IDs — and legitimately 0-able (see `IdOnly`).
    id: int
    list_id: str
    # Cursor. Advances on rename/definition updates.
    updated_at: AwareDatetime

    @override
    def get_cursor(self) -> AwareDatetime:
        return self.updated_at


class EmailCampaignStub(BaseModel):
    """Minimal `/reports` projection: the email_activity sweep's parent drain
    and recency-gate input."""

    class Opens(BaseModel):
        # Empty string (not null/absent) until the campaign's first open.
        last_open: AwareDatetime | None

        @field_validator("last_open", mode="before")
        @classmethod
        def _empty_to_none(cls, value: object) -> object:
            return None if value == "" else value

    class Clicks(BaseModel):
        # Empty string (not null/absent) until the campaign's first click.
        last_click: AwareDatetime | None

        @field_validator("last_click", mode="before")
        @classmethod
        def _empty_to_none(cls, value: object) -> object:
            return None if value == "" else value

    class Bounces(BaseModel):
        # Counts only — `/reports` carries no bounce timestamps, which is why
        # the settle-window gate clause exists (see `fetch_email_activity`).
        hard_bounces: int
        soft_bounces: int

    id: str
    send_time: AwareDatetime
    opens: Opens
    clicks: Clicks
    bounces: Bounces

    def has_activity_after(self, threshold: datetime) -> bool:
        """Whether any open or click landed strictly after `threshold`."""

        return (
            self.opens.last_open is not None and self.opens.last_open > threshold
        ) or (self.clicks.last_click is not None and self.clicks.last_click > threshold)

    def has_bounces_settling(self, settle_floor: datetime) -> bool:
        """Whether the campaign was sent recently enough that its bounces
        may still be arriving. Bounces move no recency field, so send
        recency is the only retention signal."""

        return self.send_time > settle_floor and (
            self.bounces.hard_bounces > 0 or self.bounces.soft_bounces > 0
        )


@dataclass(frozen=True)
class EmailActivityBackfillCursor:
    """Shape of the email_activity backfill's dict PageCursor.

    - Keys: campaign IDs not yet fully drained, kept sorted.
    - Values: always True (presence is the signal)

    A drained campaign is removed by yielding a JSON merge patch:
    `{"<campaign_id>": null}`.
    """

    remaining: dict[str, bool]

    def __post_init__(self) -> None:
        object.__setattr__(self, "remaining", dict(sorted(self.remaining.items())))

    @classmethod
    def from_cursor_dict(
        cls, cursor_dict: dict[str, bool | None]
    ) -> "EmailActivityBackfillCursor":
        return cls(remaining={k: v for k, v in cursor_dict.items() if v is not None})

    @classmethod
    def from_campaign_ids(
        cls, campaign_ids: list[str]
    ) -> "EmailActivityBackfillCursor":
        return cls(remaining={campaign_id: True for campaign_id in campaign_ids})

    def create_initial_cursor(self) -> dict[str, bool]:
        return make_cursor_dict(
            self.remaining
        )  # pyright: ignore[reportUnknownVariableType]

    def create_completion_patch(self, campaign_id: str) -> dict[str, bool | None]:
        return make_cursor_dict(
            {campaign_id: None}
        )  # pyright: ignore[reportUnknownVariableType]


class RawEmailActivityEvent(BaseModel, extra="allow"):
    """One nested `MemberEmailActivity.activity[]` entry as the API returns it:

    - opens are `{action, timestamp, ip}`
    - clicks add `url`
    - bounces are `{action, type, timestamp}`
    """

    # Declared (rather than riding extra="allow") because it's a component of
    # the collection key.
    action: str
    timestamp: AwareDatetime
    # Bounce-only. Opens/clicks omit the field entirely (flattening and the
    # CDK's document emission both dump with exclude_unset, so they never grow
    # a null `type`).
    type: Literal["hard"] | Literal["soft"] | None = None


class MemberEmailActivity(BaseModel, extra="allow"):
    """Raw `emails[]` item: one recipient with their nested activity events.
    Quiet recipients return an empty `activity[]`, not an absent field.

    Callers gate on the raw events in `activity` and call `flatten` only for
    survivors, so discarded events never pay document construction."""

    # The member side of the flattened document's PK, stamped into `_meta` by
    # `flatten`
    campaign_id: str
    email_id: str
    activity: list[RawEmailActivityEvent]

    def event_cache_key(self, event: RawEmailActivityEvent) -> str:
        return (
            f"{self.campaign_id}/{self.email_id}"
            f"/{event.action}/{event.timestamp.isoformat()}"
        )

    def flatten(self, event: RawEmailActivityEvent) -> "EmailActivityEvent":
        return EmailActivityEvent.model_validate(
            {**event.model_dump(exclude_unset=True), "_meta": self._envelope}
        )

    @cached_property
    def _envelope(self) -> dict[str, JsonValue]:
        return self.model_dump(exclude={"activity"})


class EmailActivityEvent(RawEmailActivityEvent, MailchimpChildEntity):
    """Flattened activity-event engagement document: the raw event plus the
    member envelope in `_meta` and the stream's identity.

    Subclasses `MailchimpChildEntity` rather than
    `MailchimpIncrementalChildEntity`: the email-activity endpoint has no
    before-style upper-bound param, so it can't ride the generic list-children
    fetchers; `SINCE_PARAM` is declared directly."""

    NAME: ClassVar[str] = "email_activity"
    PATH_TEMPLATE: ClassVar[str] = "reports/{campaign_id}/email-activity"
    ITEMS_KEY: ClassVar[str] = "emails"
    SINCE_PARAM: ClassVar[str] = "since"

    class Meta(MailchimpChildEntity.Meta):
        """The recipient-row envelope the flattening dissolves."""

        # Declared (rather than riding extra="allow") because it's a component
        # of the collection key.
        campaign_id: str
        # Member hash (MD5 of the lowercase email address)
        email_id: str

    # fmt: off
    meta_: Meta = Field(  # pyright: ignore[reportGeneralTypeIssues, reportIncompatibleVariableOverride]
        alias="_meta", description="Document metadata"
    )
    # fmt: on

    def get_cursor(self) -> AwareDatetime:
        return self.timestamp
