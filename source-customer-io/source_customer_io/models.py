from datetime import datetime, timedelta, UTC
from typing import Annotated, ClassVar, Literal

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


type ItemList[T] = Annotated[list[T], BeforeValidator(_null_as_empty)]


class DeliveriesResponse(BaseModel, extra="allow"):
    messages: ItemList[Delivery] = []
    # Empty string on the final page, and absent entirely on some sibling
    # endpoints in this family, so callers must test falsiness.
    next: str | None = None
