import abc
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import ClassVar, Literal, override

from estuary_cdk.capture.common import ConnectorState as GenericConnectorState
from estuary_cdk.capture.common import ResourceState
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import AccessToken
from pydantic import AwareDatetime, BaseModel, Field, field_validator, model_validator

from .utils import to_rfc3339_ms


class ApiToken(AccessToken):
    """An Okta API token, sent as `Authorization: SSWS <token>`.

    Created in the Admin Console under Security > API > Tokens. The token
    inherits the admin role of the account that created it and is revoked by
    Okta after 30 days without use.
    """

    credentials_title: Literal["API Token"] = Field(
        default="API Token",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="API Token",
        json_schema_extra={"secret": True, "order": 1},
    )


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    domain: str = Field(
        title="Okta Domain",
        description=(
            "The Okta org domain the Management API is served from, without a scheme. "
            "Examples: dev-123456.okta.com, acme.okta-emea.com, acme.oktapreview.com, "
            "or a custom domain such as login.acme.com."
        ),
        pattern=r"^[A-Za-z0-9.-]+$",
    )
    start_date: AwareDatetime = Field(
        description=(
            "UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. users and groups are "
            "backfilled from the objects last updated after this instant, and system_log "
            "from the events published at or after it; applications and "
            "application_users are always captured in full. If left blank, the start "
            "date will be set to 30 days before the present."
        ),
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: ApiToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

    class Advanced(BaseModel):
        window_size: timedelta = Field(
            description="Date window size for the users and groups backfill in ISO 8601 format. ex: P30D means 30 days, PT6H means 6 hours.",
            title="Window Size",
            default=timedelta(days=30),
            ge=timedelta(hours=1),
            le=timedelta(days=365),
        )

    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

    @field_validator("domain", mode="before")
    @classmethod
    def normalize_domain(cls, value: object) -> object:
        """Reduce a pasted domain to the bare host every request is built on.

        A scheme, trailing slash, surrounding whitespace and upper case are
        tolerated and stripped; a path or an empty value is rejected here with
        a readable message rather than by the field pattern.
        """
        if not isinstance(value, str):
            return value
        host = (
            value.strip()
            .lower()
            .removeprefix("https://")
            .removeprefix("http://")
            .rstrip("/")
        )
        if not host or "/" in host or any(c.isspace() for c in host):
            raise ValueError(
                f"Okta domain must be a bare host such as dev-123456.okta.com, got {value!r}"
            )
        return host


ConnectorState = GenericConnectorState[ResourceState]


class OktaDocument(BaseDocument):
    """A document built from an Okta API row.

    The row's `_links` is dropped: it is HAL navigation whose every href embeds
    the tenant domain, and it can be rebuilt from `id`. Keeping it would only
    add a dozen hostname-bearing strings to every captured document.
    """

    @model_validator(mode="before")
    @classmethod
    def strip_links(cls, data: object) -> object:
        if isinstance(data, dict):
            _ = data.pop("_links", None)
        return data


class IdEntityDocument(OktaDocument):
    """Write schema of the users and groups collections."""

    id: str

    @classmethod
    def deleted(cls, id: str) -> "IdEntityDocument":
        """The delete document for `id`: nothing but the key, since the delete
        time is already stamped on the document by Flow."""
        doc = cls(id=id)
        # Assigned rather than defaulted so that it survives the CDK's
        # exclude_unset serialization.
        doc.meta_ = cls.Meta(op="d")
        return doc


class OktaFilterableEntity(IdEntityDocument, abc.ABC):
    """A top-level Okta object captured by server-side `filter` windows.

    Abstract: subclasses own the filter expression, the list path, the page
    size and the System Log event that records their hard deletion. The window
    is bounded server-side, so no timestamp is read client-side and the model
    declares nothing beyond the key; the timestamps a row carries are passed
    through untouched.

    Okta `filter` timestamps must carry milliseconds (a bare-second bound is a
    400 E0000020) and the comparison is millisecond-precise, while the stored
    timestamps are whole seconds (`Users / lastUpdated boundary`). A window
    `(since, until]` on whole-second bounds therefore covers exactly the
    seconds strictly after `since` through `until` inclusive.
    """

    NAME: ClassVar[str]
    PATH: ClassVar[str]
    FILTER_PARAM: ClassVar[str] = "filter"
    PAGE_SIZE: ClassVar[int]
    DELETE_EVENT_TYPE: ClassVar[str]
    TARGET_TYPE: ClassVar[str]

    @classmethod
    @abc.abstractmethod
    def window_filter(cls, since: datetime, until: datetime) -> str:
        """Okta `filter` expression selecting objects changed in
        `(since, until]`, on every timestamp the object reports change on."""

    @classmethod
    def delete_filter(cls) -> str:
        """System Log `filter` expression selecting the events that record a
        finished hard delete of one of these objects. A failed delete must not
        tombstone an object that still exists, hence the outcome clause."""
        return f'eventType eq "{cls.DELETE_EVENT_TYPE}" and outcome.result eq "SUCCESS"'


class User(OktaFilterableEntity):
    """One user in any lifecycle status, DEPROVISIONED included."""

    NAME: ClassVar[str] = "users"
    PATH: ClassVar[str] = "users"
    # Documented maximum; larger values are silently clamped to 200.
    PAGE_SIZE: ClassVar[int] = 200
    # A hard delete emits `.initiated` then `.completed`; only the latter
    # confirms the user is gone.
    DELETE_EVENT_TYPE: ClassVar[str] = "user.lifecycle.delete.completed"
    TARGET_TYPE: ClassVar[str] = "User"

    @override
    @classmethod
    def window_filter(cls, since: datetime, until: datetime) -> str:
        return (
            f'lastUpdated gt "{to_rfc3339_ms(since)}" '
            f'and lastUpdated le "{to_rfc3339_ms(until)}"'
        )


class Group(OktaFilterableEntity):
    """One group of any type (OKTA_GROUP, APP_GROUP or BUILT_IN).

    Profile edits move `lastUpdated`; membership adds and removes move only
    `lastMembershipUpdated`, which is set to `created` on a new group and is
    never null. A window must test both, or every membership change is missed.
    """

    NAME: ClassVar[str] = "groups"
    PATH: ClassVar[str] = "groups"
    # Above Okta's recommended 200 and below the 10000 maximum (10001 is
    # silently clamped); the standard retry covers the timeout risk Okta
    # names for large pages.
    PAGE_SIZE: ClassVar[int] = 1000
    DELETE_EVENT_TYPE: ClassVar[str] = "group.lifecycle.delete"
    TARGET_TYPE: ClassVar[str] = "UserGroup"

    @override
    @classmethod
    def window_filter(cls, since: datetime, until: datetime) -> str:
        lo = to_rfc3339_ms(since)
        hi = to_rfc3339_ms(until)
        return (
            f'(lastUpdated gt "{lo}" and lastUpdated le "{hi}") '
            f'or (lastMembershipUpdated gt "{lo}" and lastMembershipUpdated le "{hi}")'
        )


class OktaApplication(OktaDocument):
    """One app integration, ACTIVE or INACTIVE; deleted apps are never listed."""

    PATH: ClassVar[str] = "apps"
    # Documented maximum; larger values are silently clamped to 200.
    PAGE_SIZE: ClassVar[int] = 200

    id: str


class SystemLogEvent(BaseDocument, extra="allow"):
    """One System Log event, the atomic audit fact Okta records.

    Only the collection key and the backfill watermark are modelled; every
    other field rides schema inference. Events are immutable once published.
    """

    PATH: ClassVar[str] = "logs"
    SINCE_PARAM: ClassVar[str] = "since"
    UNTIL_PARAM: ClassVar[str] = "until"
    AFTER_PARAM: ClassVar[str] = "after"
    # The `after` token is a position, `<persistence epoch-ms>_<n>`, never a
    # session handle (`System Log / polling resume after`).
    AFTER_TOKEN: ClassVar[re.Pattern[str]] = re.compile(r"\d+_\d+")
    FILTER_PARAM: ClassVar[str] = "filter"
    # Okta rejects anything above 1000 with a 400 (System Log / limit over max).
    PAGE_SIZE: ClassVar[int] = 1000

    uuid: str
    published: AwareDatetime

    def get_cursor(self) -> AwareDatetime:
        return self.published


class DeleteEvent(BaseModel):
    """The slice of a System Log delete event needed to build a delete
    document.
    """

    class Target(BaseModel):
        id: str
        type: str

    target: list[Target]
