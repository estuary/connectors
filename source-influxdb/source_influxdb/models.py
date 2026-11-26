import re
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import Literal, Self
from urllib.parse import quote

from estuary_cdk.capture.common import BaseDocument, ResourceState
from estuary_cdk.capture.common import ConnectorState as GenericConnectorState
from estuary_cdk.capture.common import ResourceConfig as CommonResourceConfig
from estuary_cdk.flow import AccessToken
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator

ConnectorState = GenericConnectorState[ResourceState]
MIN_TIMESTAMP = -9223372036854775806
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
NANOSECONDS_PER_SECOND = 1_000_000_000
TIMESTAMP = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})"
)


def datetime_to_ns(value: datetime) -> int:
    delta = value - EPOCH
    return (delta.days * 86400 + delta.seconds) * NANOSECONDS_PER_SECOND + delta.microseconds * 1000


def parse_timestamp(value: str) -> int:
    match = TIMESTAMP.fullmatch(value)
    if match is None:
        raise ValueError(f"Invalid InfluxDB timestamp: {value!r}")
    seconds, fraction, offset = match.groups()
    return datetime_to_ns(datetime.fromisoformat(seconds + offset)) + int((fraction or "").ljust(9, "0"))


def default_start_date() -> str:
    return (datetime.now(tz=UTC) - timedelta(days=30)).isoformat()


class ApiToken(AccessToken):
    model_config = ConfigDict(title="API Token", hide_input_in_errors=True)

    credentials_title: Literal["API Token"] = Field(
        default="API Token",
        title="Authentication Method",
        json_schema_extra={"type": "string"},
    )
    access_token: str = Field(
        title="API Token",
        description="Your InfluxDB API token with read access to the buckets you want to capture.",
        min_length=1,
        repr=False,
        json_schema_extra={"secret": True},
    )


class EndpointConfig(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)

    url: str = Field(
        title="URL",
        description="The URL of your InfluxDB 2.x instance, including the scheme and port if needed.",
        examples=["https://us-east-1-1.aws.cloud2.influxdata.com", "http://host:8086"],
    )
    credentials: ApiToken = Field(
        discriminator="credentials_title",
        title="Authentication",
        description="Authenticate using an InfluxDB API token.",
    )
    org: str = Field(
        title="Organization",
        description="The name of your InfluxDB organization.",
        min_length=1,
    )
    start_date: str | None = Field(
        title="Start Date",
        description="Capture points on or after this date, with nanosecond precision. Defaults to 30 days before the first run.",
        default=None,
        json_schema_extra={"format": "date-time"},
    )

    class Advanced(BaseModel):
        lag_seconds: int = Field(
            title="Lag Seconds",
            description="Delay capture by this many seconds to allow points to arrive before their timestamps are read.",
            default=60,
            ge=0,
        )
        lookback_hours: int = Field(
            title="Lookback Hours",
            description="Re-read this many hours before the newest captured point to capture late points and field updates. Zero disables re-reading.",
            default=0,
            ge=0,
        )
        ssl_ca_certificate: str = Field(
            title="SSL CA Certificate",
            description="PEM-encoded CA certificate or certificate bundle to trust in addition to public certificate authorities.",
            default="",
            json_schema_extra={"multiline": True},
        )
        ssl_skip_verify: bool = Field(
            title="Skip SSL Verification",
            description="Skip certificate and hostname verification for HTTPS connections. Use only for trusted networks with self-signed certificates; prefer an SSL CA Certificate.",
            default=False,
        )

        @model_validator(mode="after")
        def validate_tls(self) -> Self:
            if self.ssl_ca_certificate and self.ssl_skip_verify:
                raise ValueError("Provide an SSL CA Certificate or skip verification, not both.")
            return self

    advanced: Advanced = Field(
        default=Advanced(),
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        url = HttpUrl(value)
        if url.username or url.password or url.query or url.fragment:
            raise ValueError("URL must not include credentials, a query, or a fragment.")
        return str(url).rstrip("/")

    @field_validator("start_date")
    @classmethod
    def validate_start_date(cls, value: str | None) -> str | None:
        if value is not None:
            parse_timestamp(value)
        return value

    @cached_property
    def initial_start_date(self) -> str:
        return self.start_date if self.start_date is not None else default_start_date()


class ResourceConfig(CommonResourceConfig):
    interval: timedelta = Field(
        default=timedelta(minutes=5),
        title="Interval",
        description="Interval between updates for this resource.",
        json_schema_extra={"nonsensitive": True},
    )
    bucket: str = Field(
        title="Bucket",
        description="Your InfluxDB bucket name, including any dots.",
        min_length=1,
    )
    measurement: str = Field(
        title="Measurement",
        description="Your InfluxDB measurement name, including any dots.",
        min_length=1,
    )

    @model_validator(mode="after")
    def validate_resource(self) -> Self:
        if self.name != resource_name(*self.components()):
            raise ValueError("Resource name must match the discovered name for its bucket and measurement.")
        return self

    def components(self) -> tuple[str, str]:
        return self.bucket, self.measurement


def resource_name(bucket: str, measurement: str) -> str:
    return "/".join(quote(part, safe="") for part in (bucket, measurement))


class Bucket(BaseModel):
    name: str


class Measurement(BaseModel):
    value: str = Field(alias="_value", min_length=1)


class PointTime(BaseModel):
    time: str = Field(alias="_time")


class FieldRow(PointTime):
    measurement: str = Field(alias="_measurement")
    field: str = Field(alias="_field")
    value: str = Field(alias="_value")


class Point(BaseDocument, extra="allow"):
    time: str = Field(json_schema_extra={"format": "date-time"})
    measurement: str
    tags: dict[str, str]
    fields: dict[str, float | int | bool | str]
    series: str
