from annotated_types import MinLen
from datetime import datetime, UTC, timedelta
from enum import StrEnum
import json
from typing import Annotated, Any, Optional, TYPE_CHECKING, Union

from estuary_cdk.capture.common import (
    AccessToken,
    BaseDocument,
    BaseOAuth2Credentials,
    OAuth2Spec,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    LogCursor,
    Logger,
)

from pydantic import AwareDatetime, BaseModel, Field, create_model


def urlencode_field(field: str):
    return "{{#urlencode}}{{{ " + field + " }}}{{/urlencode}}"

accessTokenBody = {
    "grant_type": "authorization_code",
    "code": "{{{ code }}}",
    "client_id": "{{{ client_id }}}",
    "client_secret": "{{{ client_secret }}}",
    "redirect_uri": "{{{ redirect_uri }}}",
}

OAUTH2_SPEC = OAuth2Spec(
    provider="google",
    accessTokenBody=json.dumps(accessTokenBody),
    authUrlTemplate=(
        "https://accounts.google.com/o/oauth2/auth?"
        "access_type=offline&"
        "prompt=consent&"
        f"response_type=code&"
        f"client_id={urlencode_field('client_id')}&"
        f"redirect_uri={urlencode_field('redirect_uri')}&"
        f"scope=https://www.googleapis.com/auth/analytics.readonly&"
        f"state={urlencode_field('state')}"
    ),
    accessTokenUrlTemplate="https://oauth2.googleapis.com/token",
    accessTokenResponseMap={
        "refresh_token": "/refresh_token",
    },
    accessTokenHeaders={
        "Content-Type": "application/json",
    },
)


if TYPE_CHECKING:
    OAuth2Credentials = BaseOAuth2Credentials
else:
    OAuth2Credentials = BaseOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    property_id: str = Field(
        description="Your Google Analytics GA4 properity identifier whose events are tracked.",
        title="Property ID"
    )
    custom_reports: str = Field(
        description="A JSON array describing the custom reports you want to sync from the Google Analytics Data API. Reference this connector's documentation for information about how to fill out this field: https://go.estuary.dev/source-google-analytics-data-api-native",
        title="Custom Reports",
        default=""
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
    )
    credentials: OAuth2Credentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class ReportDocument(BaseDocument, extra="allow"):
    pass


# Report represents a valid configured report stream.
class Report(BaseModel, extra="forbid"):
    name: str
    dimensions: Annotated[list[str], MinLen(1)]
    metrics: Annotated[list[str], MinLen(1)]
    # TODO(bair): Improve validation of dimensionFilters and metricFilters.
    # Valid variations of these fields are described here: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/FilterExpression.
    dimensionFilter: Optional[dict[str, Any]] = None
    metricFilter: Optional[dict[str, Any]] = None


def create_report_doc_model(
    report: Report,
):
    dimension_fields: dict[str, Any] = {
        d_name: (str, ...) for d_name in report.dimensions
    }

    metric_fields: dict[str, Any] = {
        m_name: (Union[str, int, float], ...) for m_name in report.metrics
    }

    added_fields: dict[str, Any] = {
        "property_id": (str, ...),
        "report_date": (str, ...),
    }

    model = create_model(
        report.name,
        __base__ = ReportDocument,
        **dimension_fields,
        **metric_fields,
        **added_fields,
    )

    return model


# https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/MetricType
class MetricTypes(StrEnum):
    INTEGER = "TYPE_INTEGER"
    FLOAT = "TYPE_FLOAT"
    SECONDS = "TYPE_SECONDS"
    MILLISECONDS = "TYPE_MILLISECONDS"
    MINUTES = "TYPE_MINUTES"
    HOURS = "TYPE_HOURS"
    STANDARD = "TYPE_STANDARD"
    CURRENCY = "TYPE_CURRENCY"
    FEET = "TYPE_FEET"
    MILES = "TYPE_MILES"
    METERS = "TYPE_METERS"
    KILOMETERS = "TYPE_KILOMETERS"
    UNSPECIFIED = "METRIC_TYPE_UNSPECIFIED"


class DimensionHeader(BaseModel, extra='forbid'):
    name: str


class MetricHeader(BaseModel, extra='forbid'):
    name: str
    type: MetricTypes


class Value(BaseModel, extra="forbid"):
    value: str


class Row(BaseModel, extra="forbid"):
    dimensionValues: list[Value]
    metricValues: list[Value]


class RunReportResponse(BaseModel, extra="allow"):
    dimensionHeaders: list[DimensionHeader]
    metricHeaders: list[MetricHeader]
    # If rows or rowCount is None, that means we requested data for days that haven't started yet.
    rows: Optional[list[Row]] = None
    rowCount: Optional[int] = None

    class Metadata(BaseModel, extra="allow"):
        timeZone: str

    metadata: Metadata
