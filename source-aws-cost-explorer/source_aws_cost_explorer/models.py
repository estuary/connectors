from datetime import timedelta
from typing import ClassVar, Literal

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.capture.common import (
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.document import BaseDocument
from pydantic import AwareDatetime, BaseModel, Field


class AWSAccessKey(BaseModel):
    credentials_title: Literal["AWS Access Key"] = Field(
        default="AWS Access Key", json_schema_extra={"type": "string", "order": 0}
    )
    access_key_id: str = Field(
        title="Access Key ID",
        description="The ID of an AWS access key with the ce:GetCostAndUsage permission.",
        min_length=1,
        json_schema_extra={"secret": True, "order": 1},
    )
    secret_access_key: str = Field(
        title="Secret Access Key",
        description="The secret for your AWS access key.",
        min_length=1,
        json_schema_extra={"secret": True, "order": 2},
    )


class EndpointConfig(BaseModel):
    start_date: AwareDatetime | None = Field(
        default=None,
        description="The UTC date you want to capture costs from. If you leave this empty, each refresh rereads the last 30 days. AWS only keeps the current month and the 13 months before it; costs older than that keep their last captured values.",
        title="Start Date",
    )
    credentials: AWSAccessKey = Field(
        title="Authentication",
        description="The AWS credentials the connector uses to read Cost Explorer.",
        discriminator="credentials_title",
    )


MIN_INTERVAL = timedelta(days=1)


class CostResourceConfig(ResourceConfig):
    # Rows are whole days and AWS bills each request, so polling more often
    # than daily only adds cost. `validate_intervals` enforces the minimum.
    interval: timedelta = Field(
        default=MIN_INTERVAL,
        description="Interval between updates for this resource. Must be at least one day.",
        json_schema_extra={"nonsensitive": True},
    )


ConnectorState = GenericConnectorState[ResourceState]

GroupBy = Literal["SERVICE", "LINKED_ACCOUNT"]


class DateInterval(BaseModel):
    Start: str
    End: str


class Metric(BaseModel):
    Amount: str
    Unit: str


class CostGroup(BaseModel):
    Keys: list[str]
    Metrics: dict[str, Metric]


class CostResult(BaseModel):
    TimePeriod: DateInterval
    Estimated: bool
    Groups: list[CostGroup]
    Total: dict[str, Metric]


class DimensionAttribute(BaseModel):
    Value: str
    Attributes: dict[str, str]


class CostAndUsageResponse(BaseModel):
    ResultsByTime: list[CostResult]
    NextPageToken: str | None = None
    DimensionValueAttributes: list[DimensionAttribute] = Field(default_factory=list)


class CostMetric(BaseModel):
    amount: str
    unit: str


class DailyCost(BaseDocument, extra="allow"):
    name: ClassVar[str] = "daily_costs"
    key: ClassVar[list[str]] = ["/date"]
    group_by: ClassVar[GroupBy | None] = None

    date: str
    period_end: str
    estimated: bool
    metrics: dict[str, CostMetric]

    @classmethod
    def group_fields(
        cls, value: str, attributes: dict[str, dict[str, str]]
    ) -> dict[str, str | None]:
        """Document fields identifying the `group_by` dimension value of a row."""
        return {}


class ServiceCost(DailyCost):
    name = "daily_costs_by_service"
    key = ["/date", "/service"]
    group_by = "SERVICE"

    service: str

    @classmethod
    def group_fields(
        cls, value: str, attributes: dict[str, dict[str, str]]
    ) -> dict[str, str | None]:
        return {"service": value}


class AccountCost(DailyCost):
    name = "daily_costs_by_linked_account"
    key = ["/date", "/linked_account_id"]
    group_by = "LINKED_ACCOUNT"

    linked_account_id: str
    linked_account_name: str | None = None

    @classmethod
    def group_fields(
        cls, value: str, attributes: dict[str, dict[str, str]]
    ) -> dict[str, str | None]:
        return {
            "linked_account_id": value,
            "linked_account_name": attributes.get(value, {}).get("description"),
        }
