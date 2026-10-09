import abc
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import ClassVar, Literal, Self, override
from urllib.parse import parse_qs, urlparse

from estuary_cdk.capture.common import ConnectorState as GenericConnectorState
from estuary_cdk.capture.common import LogCursor, ResourceState
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import AccessToken
from pydantic import AwareDatetime, BaseModel, Field, field_validator, model_validator

from .utils import to_rfc3339_ms

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# A bare, lowercased host name.
HOST_PATTERN = re.compile(r"[a-z0-9.-]+")


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


class EndpointConfig(BaseModel):
    domain: str = Field(
        title="Okta Domain",
        description=(
            "The Okta org domain the Management API is served from, without a scheme. "
            "Examples: dev-123456.okta.com, acme.okta-emea.com, acme.oktapreview.com, "
            "or a custom domain such as login.acme.com."
        ),
    )
    start_date: AwareDatetime = Field(
        description=(
            "UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Users, groups, and "
            "system logs updated before this date will not be captured. If left blank, "
            "all available data will be captured."
        ),
        title="Start Date",
        default=_EPOCH,
    )
    credentials: ApiToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

    @field_validator("domain", mode="before")
    @classmethod
    def normalize_domain(cls, value: object) -> object:
        """Reduce a pasted domain to the bare host every request is built on."""
        if not isinstance(value, str):
            return value
        host = (
            value.strip()
            .lower()
            .removeprefix("https://")
            .removeprefix("http://")
            .rstrip("/")
        )
        if not HOST_PATTERN.fullmatch(host):
            raise ValueError(
                f"Okta domain must be a bare host such as dev-123456.okta.com, got {value!r}"
            )
        return host


ConnectorState = GenericConnectorState[ResourceState]


class BaseOktaDocument(BaseDocument, metaclass=abc.ABCMeta):
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


class IdEntityDocument(BaseOktaDocument):
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


class DeleteEvent(BaseModel):
    """The slice of a System Log delete event needed to build a delete
    document and to hold it back until its updates are committed.
    """

    class Outcome(BaseModel):
        result: str

    class Target(BaseModel):
        id: str
        type: str

    event_type: str = Field(alias="eventType")
    outcome: Outcome
    published: AwareDatetime
    target: list[Target]


class OktaSearchableEntity(IdEntityDocument, abc.ABC):
    """A top-level Okta object captured through server-side `search` windows,
    listed in id order.

    Abstract: subclasses own the window expression, the list path, the page
    size and the System Log event that records their hard deletion. The window
    is bounded server-side, so no timestamp is read client-side and the model
    declares nothing beyond the key; the timestamps a row carries are passed
    through untouched.

    `search` timestamp bounds are millisecond-precise while the stored
    timestamps are whole seconds. A window `(since, until]` on whole-second
    bounds therefore covers exactly the seconds strictly after `since` through
    `until` inclusive.

    `search` sorts and compares ids case-insensitively, the same way for both,
    so `id gt <last id>` resumes an id-ordered walk exactly where it stopped.
    """

    NAME: ClassVar[str]
    PATH: ClassVar[str]
    SEARCH_PARAM: ClassVar[str] = "search"
    SORT_PARAMS: ClassVar[dict[str, str | int]] = {"sortBy": "id", "sortOrder": "asc"}
    PAGE_SIZE: ClassVar[int]
    DELETE_EVENT_TYPE: ClassVar[str]
    SUCCESSFUL_DELETION: ClassVar[str] = "SUCCESS"
    TARGET_TYPE: ClassVar[str]

    @classmethod
    @abc.abstractmethod
    def window_search(cls, since: datetime, until: datetime) -> str:
        """Okta `search` expression selecting objects changed in
        `(since, until]`, on every timestamp the object reports change on."""

    @classmethod
    def page_search(cls, since: datetime, until: datetime, after_id: str | None) -> str:
        """`window_search`, narrowed to ids after `after_id` when given."""
        window = cls.window_search(since, until)
        if after_id is None:
            return window
        return f'({window}) and id gt "{after_id}"'

    @classmethod
    def delete_filter(cls) -> str:
        """System Log `filter` expression selecting the events that record a
        finished hard delete of one of these objects. A failed delete must not
        tombstone an object that still exists, hence the outcome clause."""
        return (
            f'eventType eq "{cls.DELETE_EVENT_TYPE}" '
            f'and outcome.result eq "{cls.SUCCESSFUL_DELETION}"'
        )

    @classmethod
    def deleted_ids(cls, event: DeleteEvent) -> list[str]:
        """Ids of these objects that `event` records as hard-deleted: none
        for an event `delete_filter` does not select."""
        if (
            event.event_type != cls.DELETE_EVENT_TYPE
            or event.outcome.result != cls.SUCCESSFUL_DELETION
        ):
            return []
        return [target.id for target in event.target if target.type == cls.TARGET_TYPE]


class User(OktaSearchableEntity):
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
    def window_search(cls, since: datetime, until: datetime) -> str:
        return (
            f'lastUpdated gt "{to_rfc3339_ms(since)}" '
            f'and lastUpdated le "{to_rfc3339_ms(until)}"'
        )


class Group(OktaSearchableEntity):
    """One group of any type (OKTA_GROUP, APP_GROUP or BUILT_IN).

    Profile edits move `lastUpdated`; membership adds and removes move only
    `lastMembershipUpdated`, which is set to `created` on a new group and is
    never null. A window must test both, or every membership change is missed.

    `search` matches on that profile-only `lastUpdated`, but the row it
    returns carries the search index's own stamp, which also advances on
    membership changes. The captured `lastUpdated` is therefore the group's
    last change of either kind.
    """

    NAME: ClassVar[str] = "groups"
    PATH: ClassVar[str] = "groups"
    # Above Okta's recommended 200 and below the documented 10000 maximum;
    # `search` takes 1000 unclamped. The standard retry covers the timeout
    # risk Okta names for large pages.
    PAGE_SIZE: ClassVar[int] = 1000
    DELETE_EVENT_TYPE: ClassVar[str] = "group.lifecycle.delete"
    TARGET_TYPE: ClassVar[str] = "UserGroup"

    @override
    @classmethod
    def window_search(cls, since: datetime, until: datetime) -> str:
        lo = to_rfc3339_ms(since)
        hi = to_rfc3339_ms(until)
        return (
            f'(lastUpdated gt "{lo}" and lastUpdated le "{hi}") '
            f'or (lastMembershipUpdated gt "{lo}" and lastMembershipUpdated le "{hi}")'
        )


class OktaApplication(BaseOktaDocument):
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
    FILTER_PARAM: ClassVar[str] = "filter"
    # Okta rejects anything above 1000 with a 400 (System Log / limit over max).
    PAGE_SIZE: ClassVar[int] = 1000

    uuid: str
    published: AwareDatetime


@dataclass(frozen=True, order=True)
class AfterToken:
    """A System Log `after` token, ordered by the log position it resumes from.

    Okta documents the token as opaque; its observed form is
    `<persistence epoch-ms>_<n>`, resuming past the first `n` events persisted
    from that ms on. Field order is log order only while those `n` events all
    sit at `epoch_ms`: true of every token held here, since Okta issues its own
    at the last returned event's ms and `from_timestamp` mints `n = 0`.
    """

    epoch_ms: int
    n: int

    @classmethod
    def from_str(cls, token: str) -> Self:
        epoch_ms, n = token.split("_")
        parsed = cls(int(epoch_ms), int(n))
        if str(parsed) != token:
            raise ValueError(f"unrecognized System Log `after` token: {token!r}")
        return parsed

    @classmethod
    def from_url(cls, url: str) -> Self:
        """The `after` token of a Link URL, such as a polling next link."""
        values = parse_qs(urlparse(url).query).get(SystemLogEvent.AFTER_PARAM)
        if not values:
            raise RuntimeError(f"Okta next link carries no `after` parameter: {url}")
        return cls.from_str(values[0])

    @classmethod
    def from_timestamp(cls, at: datetime) -> Self:
        """The token for the first event persisted at or after `at`.

        `n = 0` selects the same events as an inclusive `since`.
        """
        return cls((at - _EPOCH) // timedelta(milliseconds=1), 0)

    @classmethod
    def from_cursor(cls, log_cursor: LogCursor) -> Self:
        match log_cursor:
            case (str() as token,):
                return cls.from_str(token)
            case _:
                raise RuntimeError(
                    f"expected a one-element System Log `after` cursor, got {log_cursor!r}"
                )

    def to_cursor(self) -> LogCursor:
        return (str(self),)

    @property
    def timestamp(self) -> datetime:
        """The persistence instant the token resumes from."""
        return _EPOCH + timedelta(milliseconds=self.epoch_ms)

    @override
    def __str__(self) -> str:
        return f"{self.epoch_ms}_{self.n}"
