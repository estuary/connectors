import re
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal, Self
from urllib.parse import quote

from estuary_cdk.capture.common import BaseDocument, ResourceState
from estuary_cdk.capture.common import ConnectorState as GenericConnectorState
from estuary_cdk.capture.common import ResourceConfig as CommonResourceConfig
from estuary_cdk.flow import BasicAuth, ValidationError
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema

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


class UsernamePassword(BasicAuth):
    username: str = Field(
        title="Username",
        description="Your InfluxDB username for HTTP Basic authentication.",
        min_length=1,
        pattern=r"^[^:]+$",
    )
    password: str = Field(
        title="Password",
        description="Your InfluxDB password.",
        min_length=1,
        repr=False,
        json_schema_extra={"secret": True},
    )


class DatabaseToken(BaseModel):
    credentials_title: Literal["Database Token"] = Field(
        default="Database Token", json_schema_extra={"type": "string"},
    )
    access_token: str = Field(
        title="Database Token",
        description="Your database token, sent with the Bearer authorization scheme used by InfluxDB 3.",
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
    token: Annotated[str, Field(min_length=1)] | SkipJsonSchema[None] = Field(
        default_factory=lambda: None,
        title="API Token",
        description="Your InfluxDB API token with read access to the buckets you want to capture. Provide either this token or Authentication credentials.",
        repr=False,
        json_schema_extra={"secret": True},
    )
    credentials: UsernamePassword | DatabaseToken | None = Field(
        default=None,
        discriminator="credentials_title",
        title="Authentication",
        description="Optional Basic or Bearer authentication instead of an API token. Username & Password works only with a native InfluxDB 1.x server with Flux enabled; compatibility with specific 1.x versions has not been verified. InfluxDB 2.x OSS and Cloud v1-compatibility usernames and passwords are not accepted on /api/v2/query; use an API token there. Your endpoint must still support the v2 Flux query API.",
    )
    org: str = Field(
        title="Organization",
        description="The name of your InfluxDB organization.",
        min_length=1,
    )
    start_date: str = Field(
        title="Start Date",
        description="Capture points with timestamps on or after this date, with nanosecond precision. When omitted, initialize the cursor 30 days before the first run; lookback may read earlier and follows the saved cursor after restarts. Dates before InfluxDB's minimum time (1677-09-21T00:12:43.145224194Z) start at that minimum. Enable a lookback window to capture late points and field changes near the cursor.",
        default_factory=default_start_date,
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
            description="Re-read this many hours before the newest captured point on every poll to capture late points and field updates. An explicit Start Date limits re-reading; an omitted Start Date does not. Zero disables re-reading. Repeated points merge by series and time.",
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
        default_factory=Advanced,
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

    @model_validator(mode="after")
    def validate_authentication(self) -> Self:
        if (self.token is None) == (self.credentials is None):
            raise ValueError("Provide exactly one of token or credentials.")
        return self

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        url = HttpUrl(value)
        if url.username or url.password or url.query or url.fragment:
            raise ValueError("URL must not include credentials, a query, or a fragment.")
        return str(url).rstrip("/")

    @field_validator("start_date")
    @classmethod
    def validate_start_date(cls, value: str) -> str:
        parse_timestamp(value)
        return value

    @property
    def common_headers(self) -> dict[str, str]:
        return {"Authorization": f"Token {self.token}"} if self.token else {}


class ResourceConfig(CommonResourceConfig):
    interval: timedelta = Field(
        default=timedelta(minutes=5),
        title="Interval",
        description="Interval between updates for this resource.",
        json_schema_extra={"nonsensitive": True},
    )
    bucket: str | None = Field(
        default=None,
        title="Bucket",
        description="Your InfluxDB bucket name. Omit both bucket and measurement to parse a legacy resource name at its last dot.",
        min_length=1,
    )
    measurement: str | None = Field(
        default=None,
        title="Measurement",
        description="Your InfluxDB measurement name, including any dots.",
        min_length=1,
    )

    @model_validator(mode="after")
    def validate_resource(self) -> Self:
        if (self.bucket is None) != (self.measurement is None):
            raise ValueError("Provide both bucket and measurement, or neither for a legacy resource name.")
        if self.bucket is None:
            try:
                self.bucket, self.measurement = split_resource_name(self.name)
            except ValidationError as err:
                raise ValueError(err.errors[0]) from err
        elif self.name != resource_name(*self.components()):
            raise ValueError("Resource name must match the discovered name for its bucket and measurement.")
        return self

    def components(self) -> tuple[str, str]:
        assert self.bucket is not None and self.measurement is not None
        return self.bucket, self.measurement


def split_resource_name(name: str) -> tuple[str, str]:
    bucket, separator, measurement = name.rpartition(".")
    if not separator or not bucket or not measurement:
        raise ValidationError(["Resource names must have the form <bucket>.<measurement>."])
    return bucket, measurement


def resource_name(bucket: str, measurement: str) -> str:
    if "." not in measurement:
        return f"{bucket}.{measurement}"
    # Dot-free identifiers cannot collide with any legacy resource name.
    return "/".join(quote(part, safe="").replace(".", "%2E") for part in (bucket, measurement))


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
