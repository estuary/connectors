from datetime import datetime, timedelta, UTC
from typing import Annotated, Any, ClassVar, Literal

from pydantic import AwareDatetime, BaseModel, BeforeValidator, Field

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceConfigWithSchedule,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


# Customer.io offers no OAuth for its REST APIs -- the App API authenticates with a
# long-lived App API Key sent as `Authorization: Bearer <key>`, which is TokenSource's
# default header and token type. Single-arm union, but still discriminated so adding a
# second scheme later stays non-breaking.
class ApiKey(AccessToken):
    credentials_title: Literal["App API Key"] = Field(
        default="App API Key",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="App API Key",
        description="Your Customer.io App API Key, generated under Account Settings > API Credentials. Note that App API Keys are shown only once, and are distinct from Track API and Pipelines API credentials.",
        json_schema_extra={"secret": True, "order": 1},
    )


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    region: Literal["us", "eu"] = Field(
        title="Region",
        description="The Customer.io data region hosting your account, which determines the API base URL. Account Admins can find this under Settings > Account Settings > Data and Privacy. Using the wrong region returns a 401 that is indistinguishable from an invalid key.",
        default="us",
    )
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

    class Advanced(BaseModel):
        window_size: Annotated[
            timedelta,
            Field(
                description="Time window size for each backfill and incremental request, in ISO 8601 format. ex: P1D means 1 day, PT6H means 6 hours.",
                title="Window size",
                default=timedelta(days=1),
                ge=timedelta(minutes=1),
                le=timedelta(days=30),
            ),
        ]
        metrics_rescan_window: Annotated[
            timedelta,
            Field(
                description="How far behind the present to trail a second pass over deliveries, re-reading them once their engagement metrics have settled. Customer.io deliveries have no updated-at field and cannot be filtered by engagement time, so this trailing pass is the only way opens, clicks, bounces and unsubscribes recorded after a delivery was created are captured. Larger values catch more of the long tail at the cost of a longer delay before metrics are final.",
                title="Metrics re-scan window",
                default=timedelta(days=7),
                ge=timedelta(hours=1),
                le=timedelta(days=180),
            ),
        ]

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


class Delivery(BaseDocument, extra="allow"):
    """A single rendered message sent to one recipient.

    Keyed on `id`. `metrics` accretes engagement timestamps for days to weeks
    after `created` is fixed, and no field is filterable by that update time, so
    the stream pairs an incremental pass with a trailing re-scan.
    """

    NAME: ClassVar[str] = "messages"
    PATH: ClassVar[str] = "/v1/messages"
    ITEMS_KEY: ClassVar[str] = "messages"
    SINCE_PARAM: ClassVar[str] = "start_ts"
    BEFORE_PARAM: ClassVar[str] = "end_ts"

    id: str
    # Epoch seconds, as the provider returns them. The document keeps the
    # provider's representation; `get_cursor` is the only place it becomes a
    # datetime, and its return type is what keeps an int out of the LogCursor.
    created: int
    # `<id>:<timestamp>` where the timestamp is the last time the delivery was
    # updated. Captured so consumers can detect change cheaply; nothing in the
    # fetch path branches on it.
    deduplicate_id: str
    # Open key space: alongside the named metrics, Customer.io emits one
    # `link:<n>` and `human_link:<n>` key per tracked link in the message body.
    # An absent key means "not recorded", never zero.
    metrics: dict[str, int]

    def get_cursor(self) -> datetime:
        return datetime.fromtimestamp(self.created, tz=UTC)


def _null_as_empty(value: object) -> object:
    """Treat a JSON null item array as an empty one.

    Several App API endpoints return their item array as `null` rather than `[]`
    when there is nothing to return, and a field default only applies when the
    key is absent -- an explicit null fails validation.
    """
    return [] if value is None else value


class DeliveriesResponse(BaseModel, extra="allow"):
    messages: Annotated[list[Delivery], BeforeValidator(_null_as_empty)] = []
    # Empty string on the final page, and absent entirely on some sibling
    # endpoints in this family, so callers must test falsiness.
    next: str | None = None


class SnapshotPage(BaseModel, extra="allow"):
    """One response from a snapshot endpoint.

    The item array's key varies per endpoint and is not derivable from the path,
    so it is read by name rather than declared -- but `next` is shared, and the
    rows themselves are validated into their document model by the caller.
    """

    # Empty string on some endpoints, absent on others, a token on the rest.
    # Callers must test falsiness rather than `is None` or key presence.
    next: str | None = None

    def items(self, items_key: str) -> list[dict[str, Any]]:
        # Several endpoints return the array as `null` rather than `[]`.
        return (self.model_extra or {}).get(items_key) or []


class SnapshotObject(BaseDocument, extra="allow"):
    """A Customer.io object captured by re-reading it in full each interval.

    No fields are declared deliberately. Snapshot bindings are keyed on
    `/_meta/row_id`, and the CDK writes a bare document to mark a row deleted --
    any required field would fail that write, and only on the first deletion.
    Shapes come from schema inference instead, which also accommodates things
    the provider's spec does not describe: `conditions` on a segment, and the
    seven trigger variants a campaign can take.
    """

    NAME: ClassVar[str]
    PATH: ClassVar[str]
    # Not derivable from the path: /v1/transactional returns `messages`, which
    # collides with the deliveries envelope but holds a different document.
    ITEMS_KEY: ClassVar[str]
    # Rows are addressed positionally by `/_meta/row_id`, so emitting them in a
    # stable order keeps an unchanged row on the same key between passes.
    ORDER_KEY: ClassVar[str] = "id"


class ConfigObject(SnapshotObject):
    """A configuration object whose endpoint returns everything in one response.

    These endpoints accept no query parameters at all -- no paging, no filters.
    """


class PaginatedObject(SnapshotObject):
    """A snapshot object whose endpoint pages with `start`/`limit`.

    Kept a sibling of `ConfigObject` rather than a subclass so that a paginated
    model cannot be registered in `CONFIG_OBJECTS` and silently lose its page
    walk -- it would capture only the first page, with no error.
    """

    EXTRA_PARAMS: ClassVar[dict[str, str]] = {}


class Campaigns(ConfigObject):
    NAME = "campaigns"
    PATH = "/v1/campaigns"
    ITEMS_KEY = "campaigns"


class Broadcasts(ConfigObject):
    NAME = "broadcasts"
    PATH = "/v1/broadcasts"
    ITEMS_KEY = "broadcasts"


class Segments(ConfigObject):
    NAME = "segments"
    PATH = "/v1/segments"
    ITEMS_KEY = "segments"


class TransactionalMessages(ConfigObject):
    NAME = "transactional_messages"
    PATH = "/v1/transactional"
    ITEMS_KEY = "messages"


class SubscriptionTopics(ConfigObject):
    NAME = "subscription_topics"
    PATH = "/v1/subscription_topics"
    ITEMS_KEY = "topics"


class ObjectTypes(ConfigObject):
    NAME = "object_types"
    PATH = "/v1/object_types"
    ITEMS_KEY = "types"


class Collections(ConfigObject):
    NAME = "collections"
    PATH = "/v1/collections"
    ITEMS_KEY = "collections"


CONFIG_OBJECTS: list[type[ConfigObject]] = [
    Campaigns,
    Broadcasts,
    Segments,
    TransactionalMessages,
    SubscriptionTopics,
    ObjectTypes,
    Collections,
]


class Newsletters(PaginatedObject):
    NAME = "newsletters"
    PATH = "/v1/newsletters"
    ITEMS_KEY = "newsletters"


class SenderIdentities(PaginatedObject):
    NAME = "sender_identities"
    PATH = "/v1/sender_identities"
    ITEMS_KEY = "sender_identities"
    # `hidden` defaults to false and omits hidden senders from the response, so
    # the default is a partial view rather than a complete one.
    EXTRA_PARAMS = {"hidden": "true"}


class Optouts(PaginatedObject):
    NAME = "optouts"
    PATH = "/v1/optouts"
    ITEMS_KEY = "optouts"
    # An opt-out row carries no `id`. `cio_id` is Customer.io's own identifier;
    # `customer_id` is caller-assigned and can be reassigned to someone else.
    ORDER_KEY = "cio_id"


PAGINATED_OBJECTS: list[type[PaginatedObject]] = [
    Newsletters,
    SenderIdentities,
    Optouts,
]
