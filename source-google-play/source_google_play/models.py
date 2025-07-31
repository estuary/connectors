from datetime import datetime, UTC, timedelta
from typing import Any, ClassVar
import re

from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    LogCursor,
    Logger,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.flow import (
    GoogleServiceAccount,
    GoogleServiceAccountSpec,
)


from pydantic import AwareDatetime, BaseModel, Field, model_validator, ValidationInfo


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


class GooglePlayRow(BaseDocument, extra="allow"):
    name: ClassVar[str]
    prefix: ClassVar[str]
    primary_keys: ClassVar[list[str]]
    validation_context_model: ClassVar[type[BaseValidationContext]] = BaseValidationContext

    @classmethod
    def get_glob_pattern(cls, date: datetime | None = None) -> str:
        # By default, match any year and month pattern.
        year_month_pattern = "[0-9]" * 6
        # If a date is provided, match only files for that year and month.
        if date:
            year_month_pattern =  f"{date.year:04d}{date.month:02d}"

        return f"**_{year_month_pattern}.csv"

    package_name: str
    row_number: int

    @model_validator(mode="before")
    @classmethod
    def _add_row_number(cls, data: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:

        if not info.context or not isinstance(info.context, BaseValidationContext):
            raise RuntimeError(f"Validation context is not set or is not of type BaseValidationContext: {info.context}")

        assert "row_number" not in data, "Row number should not be set before validation."
        data["row_number"] = info.context.count
        info.context.increment()
        return data


class Statistics(GooglePlayRow):
    primary_keys: ClassVar[list[str]] = ["/date", "/package_name", "/row_number"]
    date: str


class Crashes(Statistics):
    name: ClassVar[str] = "crashes"
    prefix: ClassVar[str] = "stats/crashes"


class Installs(Statistics):
    name: ClassVar[str] = "installs"
    prefix: ClassVar[str] = "stats/installs"


class ReviewValidationContext(BaseValidationContext):
    def __init__(self, filename: str):
        super().__init__()
        self.year_month = self._extract_year_month(filename)

    def _extract_year_month(self, filename: str) -> str:
        """
        Extract YYYYMM from review filenames in various formats:
        - /reviews/reviews_[package_name]_YYYYMM.csv
        - reviews_[package_name]_YYYYMM.csv
        - reviews_[package_name]_YYYYMM_extra.csv

        Args:
            filename: The filename or path

        Returns:
            The YYYYMM string.
        """
        # Matches reviews_[anything]_YYYYMM[_optionalstuff].csv
        pattern = r'reviews_[^_]+_(\d{6})(?:_[^.]*)?\.csv$'
        match = re.search(pattern, filename)

        assert match, f"Filename does not match expected pattern: {filename}"

        return match.group(1)


# There _might_ be a "Review Last Update Date and Time" we could use to incrementally
# capture updates within a specific file of Reviews. However, the documentation says it's optional
# and we haven't see what this data actually looks like. Once we see real data, we can
# evaluate whether or not the incremental replication strategy for Reviews can be improved.
class Reviews(GooglePlayRow):
    name: ClassVar[str] = "reviews"
    prefix: ClassVar[str] = "reviews"
    primary_keys: ClassVar[list[str]] = ["/year_month", "/package_name", "/row_number"]
    validation_context_model: ClassVar[type[BaseValidationContext]] = ReviewValidationContext

    year_month: str 

    @model_validator(mode="before")
    @classmethod
    def _add_year_month(cls, data: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:

        if not info.context or not isinstance(info.context, ReviewValidationContext):
            raise RuntimeError(f"Validation context is not set or is not of type ReviewValidationContext: {info.context}")

        assert "year_month" not in data, "year_month should not be set before validation."
        data["year_month"] = info.context.year_month
        return data


RESOURCES: list[type[GooglePlayRow]] = [
    Crashes,
    Installs,
    Reviews,
]
