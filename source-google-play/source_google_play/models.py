from datetime import datetime, UTC, timedelta
from typing import Any, ClassVar
import re

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.flow import (
    GoogleServiceAccount,
    GoogleServiceAccountSpec,
)
from estuary_cdk.incremental_csv_processor import BaseCSVRow

from pydantic import AwareDatetime, BaseModel, Field, model_validator, ValidationInfo


PACKAGE_NAME_FIELD = "Package Name"
ROW_NUMBER_FIELD = "Row Number"
MONTH_FIELD = "Month"
YEAR_FIELD = "Year"

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

GOOGLE_SPEC = GoogleServiceAccountSpec(
    scopes=[
        "https://www.googleapis.com/auth/devstorage.read_only",
    ]
) 


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    bucket: str = Field(
        description="The bucket containing your Google Play data. The bucket starts with pubsite_prod. For example, pubsite_prod_<some_identifier>.",
        title="Bucket",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
    )
    credentials: GoogleServiceAccount = Field(
        discriminator="credentials_title",
        title="Authentication",
    )

ConnectorState = GenericConnectorState[ResourceState]


class BaseValidationContext:
    def __init__(self, **kwargs):
        self.count = 0

    def increment(self):
        self.count += 1


class GooglePlayRow(BaseCSVRow, extra="allow"):
    name: ClassVar[str]
    prefix: ClassVar[str]
    suffix: ClassVar[str | None] = None
    primary_keys: ClassVar[list[str]]
    validation_context_model: ClassVar[type[BaseValidationContext]] = BaseValidationContext

    @classmethod
    def get_glob_pattern(cls, date: datetime | None = None) -> str:
        # By default, match any year and month pattern.
        year_month_pattern = "[0-9]" * 6
        # If a date is provided, match only files for that year and month.
        if date:
            year_month_pattern =  f"{date.year:04d}{date.month:02d}"

        pattern = f"**_{year_month_pattern}"

        if cls.suffix:
            pattern += f"_{cls.suffix}"

        pattern += ".csv"

        return pattern

    package_name: str = Field(alias=PACKAGE_NAME_FIELD)

    # The column naming convention across CSVs is not inherently consistent. ex: Sometimes a column
    # is named "Package name" and other times it's "Package Name". We normalize the column names to
    # be title case, which is the predominant casing convention for these columns before we do
    # perform any transformation.
    @model_validator(mode="before")
    @classmethod
    def _normalize_field_names(cls, data: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:
        normalized_data: dict[str, Any] = {}
        for key, value in data.items():
            normalized_key = key.title()
            normalized_data[normalized_key] = value

        return normalized_data


class Statistics(GooglePlayRow):
    suffix: ClassVar[str | None] = "overview"
    primary_keys: ClassVar[list[str]] = ["/Date", f"/{PACKAGE_NAME_FIELD}"]
    date: str = Field(alias="Date")


class Crashes(Statistics):
    name: ClassVar[str] = "crashes"
    prefix: ClassVar[str] = "stats/crashes"


class Installs(Statistics):
    name: ClassVar[str] = "installs"
    prefix: ClassVar[str] = "stats/installs"


class ReviewValidationContext(BaseValidationContext):
    def __init__(self, filename: str):
        super().__init__()
        self.year, self.month = self._extract_year_month(filename)

    def _extract_year_month(self, filename: str) -> tuple[str, str]:
        """
        Extract YYYYMM from review filenames in various formats:
        - /reviews/reviews_[package_name]_YYYYMM.csv
        - reviews_[package_name]_YYYYMM.csv
        - reviews_[package_name]_YYYYMM_extra.csv

        Args:
            filename: The filename or path

        Returns:
            A tuple containing the year and month as strings.
        """
        # Matches reviews_[anything]_YYYYMM[_optionalstuff].csv
        pattern = r'reviews_[^_]+_(\d{6})(?:_[^.]*)?\.csv$'
        match = re.search(pattern, filename)

        assert match, f"Filename does not match expected pattern: {filename}"

        year_month = match.group(1) # YYYYMM
        year = year_month[:4]
        month = year_month[4:6]

        return (year, month)


class Reviews(GooglePlayRow):
    name: ClassVar[str] = "reviews"
    prefix: ClassVar[str] = "reviews"
    primary_keys: ClassVar[list[str]] = [f"/{YEAR_FIELD}", f"/{MONTH_FIELD}", f"/{PACKAGE_NAME_FIELD}", f"/{ROW_NUMBER_FIELD}"]
    validation_context_model: ClassVar[type[BaseValidationContext]] = ReviewValidationContext

    row_number: int = Field(alias=ROW_NUMBER_FIELD)
    year: str = Field(alias=YEAR_FIELD)
    month: str = Field(alias=MONTH_FIELD)
    updated_at: AwareDatetime = Field(alias="Review Last Update Date And Time")

    @model_validator(mode="before")
    @classmethod
    def _add_primary_key_components(cls, data: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:

        if not info.context or not isinstance(info.context, ReviewValidationContext):
            raise RuntimeError(f"Validation context is not set or is not of type ReviewValidationContext: {info.context}")

        assert YEAR_FIELD not in data, f"{YEAR_FIELD} should not be set before validation."
        assert MONTH_FIELD not in data, f"{MONTH_FIELD} should not be set before validation."
        data[YEAR_FIELD] = info.context.year
        data[MONTH_FIELD] = info.context.month

        assert ROW_NUMBER_FIELD not in data, f"{ROW_NUMBER_FIELD} should not be set before validation."
        data[ROW_NUMBER_FIELD] = info.context.count
        info.context.increment()
        return data


RESOURCES: list[type[GooglePlayRow]] = [
    Crashes,
    Installs,
    Reviews,
]
