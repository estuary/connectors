import re
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import TYPE_CHECKING, Annotated, Any

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import (
    ClientCredentialsOAuth2Credentials,
    OAuth2ClientCredentialsPlacement,
    OAuth2TokenFlowSpec,
)
from estuary_cdk.http import TokenSource
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    create_model,
    field_validator,
    model_validator,
)

ConnectorState = GenericConnectorState[ResourceState]

OAUTH2_SPEC = OAuth2TokenFlowSpec(
    accessTokenUrlTemplate="https://api.criteo.com/oauth2/token",
    accessTokenResponseMap={"access_token": "/access_token"},
)

if TYPE_CHECKING:
    OAuth2Credentials = ClientCredentialsOAuth2Credentials
else:
    OAuth2Credentials = (
        ClientCredentialsOAuth2Credentials.with_client_credentials_placement(
            OAuth2ClientCredentialsPlacement.FORM
        )
    )


class CriteoTokenSource(TokenSource):
    """A `TokenSource` that tolerates Criteo's null `refresh_token`.

    A client credentials grant has no refresh token — RFC 6749 §4.4.3 says one
    should not be issued — and Criteo says so with an explicit `"refresh_token":
    null` rather than by omitting the field, which the CDK's `str` default does
    not accept. source-looker overrides this response for the same reason.
    """

    class AccessTokenResponse(TokenSource.AccessTokenResponse):
        @field_validator("refresh_token", mode="before")
        @classmethod
        def _null_is_absent(cls, value: str | None) -> str:
            # Read the null as "there is no refresh token", which is what the
            # inherited default already means. The field's type is left alone:
            # pydantic fields are invariant, so redeclaring it here would not
            # type check.
            return "" if value is None else value


def default_start_date() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=30)


# Criteo report dimension and metric names are PascalCase identifiers. Enforcing
# that shape keeps them safe to use as Python field names (the per-report document
# model is built from the dimensions) and as unescaped JSON pointer key components.
FIELD_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9]*$")

# A report name becomes a collection name component, so it may not contain
# characters that Flow reserves for name paths.
REPORT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]*$")

# Reports are named by the user but share a binding namespace with the built-in
# entity streams. Prefixing the user's name removes the possibility of a
# collision outright.
REPORT_NAME_PREFIX = "custom_report_"

# Criteo returns the counterpart of every ID/name dimension pair whether or not
# it was asked for, so rows are always at ID granularity. Keying on a name alone
# would collapse two same-named entities onto one key, so the paired ID is added
# to any report that requests only the name.
NAME_TO_ID_DIMENSION = {
    "Ad": "AdId",
    "Adset": "AdsetId",
    "Advertiser": "AdvertiserId",
    "Campaign": "CampaignId",
    "Category": "CategoryId",
    "Channel": "ChannelId",
    "Coupon": "CouponId",
    "MarketingObjective": "MarketingObjectiveId",
}

# Criteo's time dimensions. Which one a report groups by is the `grain` field
# rather than a free-form dimension, so none of them may be listed there.
TIME_DIMENSIONS = frozenset({"Hour", "Day", "Week", "Month", "Year"})


# A report is fetched in multi-day windows and its dimensions form the collection
# key, so a row must carry a timestamp at least as fine as a day. Modelling the
# grain as an enum field, rather than as something the user must remember to put
# in `dimensions`, means it cannot be configured wrong.
class ReportGrain(StrEnum):
    """Time granularity of a report's rows."""

    DAY = "Day"
    HOUR = "Hour"


def _validate_names(values: list[str], pattern: re.Pattern[str], label: str) -> list[str]:
    # Duplicates are silently accepted by Criteo but would produce a duplicated
    # JSON pointer in the collection key, so the user's list is deduplicated
    # while preserving the order they wrote it in.
    deduplicated = list(dict.fromkeys(values))

    if not deduplicated:
        raise ValueError(f"At least one {label} must be specified.")

    invalid = [value for value in deduplicated if not pattern.fullmatch(value)]
    if invalid:
        raise ValueError(
            f"Invalid {label} name(s): {', '.join(invalid)}. "
            f"{label.capitalize()} names must match {pattern.pattern}."
        )

    return deduplicated


class ReportConfig(BaseModel):
    name: str = Field(
        title="Report Name",
        description="Name of this report. The collection it is captured into is named custom_report_<name>. Must be unique across reports.",
    )
    grain: ReportGrain = Field(
        default=ReportGrain.DAY,
        title="Time Grain",
        description="Time granularity of the report. Each row covers one day or one hour, and the grain leads the collection's key.",
    )
    dimensions: list[str] = Field(
        default=[],
        title="Additional Dimensions",
        description="Criteo statistics dimensions to group the report by beyond its time grain, for example CampaignId. These join the grain to form the collection's key.",
    )
    metrics: list[str] = Field(
        title="Metrics",
        description="Criteo statistics metrics to report on, for example Clicks and AdvertiserCost.",
    )
    currency: str = Field(
        default="USD",
        title="Currency",
        description="ISO 4217 currency code (three capital letters) that monetary metrics are reported in.",
    )
    timezone: str = Field(
        default="UTC",
        title="Timezone",
        description="Timezone the report's date dimensions are computed in, as a tz database name such as UTC or America/New_York.",
    )

    @field_validator("name")
    @classmethod
    def _check_name(cls, value: str) -> str:
        if not REPORT_NAME_PATTERN.fullmatch(value):
            raise ValueError(
                f"Invalid report name: {value}. Report names must match {REPORT_NAME_PATTERN.pattern}."
            )
        return value

    @field_validator("dimensions")
    @classmethod
    def _check_dimensions(cls, value: list[str]) -> list[str]:
        # The grain alone is a valid grouping, so an empty list is fine here.
        if not value:
            return []

        dimensions = _validate_names(value, FIELD_NAME_PATTERN, "dimension")

        time_dimensions = TIME_DIMENSIONS.intersection(dimensions)
        if time_dimensions:
            raise ValueError(
                f"Time dimension(s) listed under `dimensions`: {', '.join(sorted(time_dimensions))}. "
                "A report's time granularity is set by its `grain` field instead."
            )

        paired_ids = [
            NAME_TO_ID_DIMENSION[dimension]
            for dimension in dimensions
            if dimension in NAME_TO_ID_DIMENSION
            and NAME_TO_ID_DIMENSION[dimension] not in dimensions
        ]

        return dimensions + paired_ids

    @field_validator("metrics")
    @classmethod
    def _check_metrics(cls, value: list[str]) -> list[str]:
        return _validate_names(value, FIELD_NAME_PATTERN, "metric")

    @field_validator("timezone")
    @classmethod
    def _check_timezone(cls, value: str) -> str:
        """Accept UTC or an Area/Location tz database name, and nothing else.

        Criteo also accepts bare codes (PST) and UTC offsets (UTC-5), but both
        are deliberately rejected: a fixed offset cannot follow daylight saving,
        so such a report drifts an hour from local midnight for part of the year,
        and the codes are ambiguous — IST is India, Israel and Ireland, CST is
        US Central, China and Cuba.

        Requiring the Area/Location form is what makes that stick. The tz
        database itself contains legacy fixed-offset zones named like codes —
        `EST` is permanently UTC-5 and never observes DST, unlike
        `America/New_York` — so a membership check alone would let the very
        confusion this rejects back in under a familiar name. `Etc/GMT±N` is
        excluded for a related reason: its sign is inverted, so `Etc/GMT+5`
        means UTC-5.

        The connector never does arithmetic with this value, so a shape check
        would suffice to pass it through, but resolving it against the real
        database also catches a misspelling like `America/New_Yrok`.

        `tzdata` and `tzdata-legacy` are installed in the connector image, so
        the database is present at runtime.
        """
        if value == "UTC":
            return value

        if value.startswith("Etc/"):
            raise ValueError(
                f"Invalid timezone: {value}. Etc/GMT zones have an inverted sign "
                "(Etc/GMT+5 means UTC-5); name the region instead, for example "
                "America/New_York."
            )

        if "/" not in value:
            raise ValueError(
                f"Invalid timezone: {value}. Use UTC or an Area/Location tz database name "
                "such as America/New_York. Timezone codes and UTC offsets are not accepted: "
                "they are ambiguous or cannot follow daylight saving."
            )

        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as err:
            raise ValueError(
                f"Invalid timezone: {value}. Not a tz database name; expected something "
                "like America/New_York or Europe/London."
            ) from err

        return value

    @property
    def all_dimensions(self) -> list[str]:
        """The grain followed by the configured dimensions.

        This is what Criteo is asked to group by, what the document model
        declares, and what the collection is keyed on.
        """
        return [self.grain.value, *self.dimensions]

    @model_validator(mode="after")
    def _check_no_overlap(self) -> "ReportConfig":
        overlap = set(self.all_dimensions).intersection(self.metrics)
        if overlap:
            raise ValueError(
                f"Field(s) requested as both a dimension and a metric: {', '.join(sorted(overlap))}."
            )
        return self

    @field_validator("currency")
    @classmethod
    def _check_currency(cls, value: str) -> str:
        if not re.fullmatch(r"^[A-Z]{3}$", value):
            raise ValueError(
                f"Invalid currency: {value}. Must be a three-letter uppercase ISO 4217 code."
            )
        return value


class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials = Field(
        title="Authentication",
        description="Criteo API client credentials.",
    )
    start_date: AwareDatetime = Field(
        default_factory=default_start_date,
        title="Start Date",
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Report data before this date will not be replicated. If left blank, the start date will be set to 30 days before the present. Criteo's statistics reports serve at most two years of history, so an earlier start date is clamped to that.",
    )
    advertiser_ids: list[str] = Field(
        default=[],
        title="Advertiser IDs",
        description="Advertiser IDs to capture. If left empty, every advertiser in the API client's portfolio is captured.",
    )
    reports: list[ReportConfig] = Field(
        default=[],
        title="Reports",
        description="Statistics reports to capture.",
    )

    class Advanced(BaseModel):
        report_window_size: Annotated[
            int,
            Field(
                default=4,
                ge=1,
                le=365,
                title="Report Window Size (Days)",
                description="Number of days requested per statistics report call. Criteo caps a single report at 100,000 rows and recommends querying at most 4 consecutive days, so reports with high-cardinality dimensions need a small window.",
            ),
        ]
        report_lookback_days: Annotated[
            int,
            Field(
                default=30,
                ge=1,
                le=365,
                title="Report Lookback (Days)",
                description="Number of days before the cursor that are re-queried on every sweep. Criteo restates statistics as attribution windows close, and the widest standard window is 30 days. At least one day is always re-read, so that a report timezone offset from UTC does not leave the tail of a day uncaptured.",
            ),
        ]

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

    @field_validator("advertiser_ids")
    @classmethod
    def _check_advertiser_ids(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(value))

    @field_validator("reports")
    @classmethod
    def _check_reports(cls, value: list[ReportConfig]) -> list[ReportConfig]:
        names = [report.name for report in value]

        duplicates = {name for name in names if names.count(name) > 1}
        if duplicates:
            raise ValueError(
                f"Duplicate report name(s): {', '.join(sorted(duplicates))}. Each report must have a unique name."
            )

        return value


class ApiProblem(BaseModel, extra="allow"):
    """An entry of a Criteo response's `errors` or `warnings` array."""

    code: str | None = None
    detail: str | None = None
    title: str | None = None
    type: str | None = None
    traceId: str | None = None


class PageMeta(BaseModel, extra="allow"):
    # Criteo marks all three optional; only totalItems drives the pagination walk.
    limit: int | None = None
    offset: int | None = None
    totalItems: int


class CriteoEntity(BaseDocument, extra="allow"):
    """An element of a Criteo `data` array, in the API's own resource shape.

    Only `id` is declared: it is what advertiser resolution reads.
    """

    id: str


class EntityResponse(BaseModel, extra="allow"):
    data: list[CriteoEntity]
    errors: list[ApiProblem] = []
    warnings: list[ApiProblem] = []
    meta: PageMeta | None = None


class ReportEnvelope(BaseModel, extra="allow"):
    """What a report response carries outside its streamed `Rows` array.

    The streaming parser leaves an emptied `Rows` placeholder behind, so the key
    is present on every well-formed response. `rows` stays optional so that a
    response carrying `errors` but no `Rows` reports the API's own message rather
    than a complaint about the envelope shape; the caller checks for None after
    raising for errors.
    """

    rows: list[Any] | None = Field(default=None, alias="Rows")
    errors: list[ApiProblem] = []
    warnings: list[ApiProblem] = []


class ReportRow(BaseDocument, extra="allow"):
    """Base of the per-report document models built by `report_document_model`."""


def report_document_model(report: ReportConfig) -> type[ReportRow]:
    """Build the document model for a report.

    Criteo returns one flat object per row, keyed by the requested dimension and
    metric names. Only the dimensions are declared — including the grain, which
    leads them: they form the collection key, so they must be present and
    non-null on every document. Metrics vary in type across the ~80 Criteo offers
    and are left to schema inference.
    """
    fields: dict[str, Any] = {
        dimension: (str, ...) for dimension in report.all_dimensions
    }
    # A report name may carry `.` and `-`, which don't belong in a class name.
    model_name = re.sub(r"[^A-Za-z0-9]", "", report.name)

    return create_model(
        f"Report{model_name}",
        __base__=ReportRow,
        **fields,
    )


def report_key(report: ReportConfig) -> list[str]:
    return [f"/{dimension}" for dimension in report.all_dimensions]


def report_stream_name(report: ReportConfig) -> str:
    """The binding name for a report, namespaced away from the entity streams."""
    return f"{REPORT_NAME_PREFIX}{report.name}"
