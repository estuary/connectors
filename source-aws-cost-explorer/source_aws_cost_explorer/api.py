import asyncio
from collections.abc import AsyncGenerator, Iterator
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from logging import Logger
from typing import TYPE_CHECKING

from boto3.session import Session
from botocore.config import Config
from estuary_cdk.capture.common import LogCursor

from .models import (
    AccountCost,
    AWSAccessKey,
    CostAndUsageResponse,
    CostMetric,
    DailyCost,
    GroupBy,
    Metric,
    ServiceCost,
)

if TYPE_CHECKING:
    from mypy_boto3_ce.type_defs import GetCostAndUsageRequestTypeDef

API = "https://ce.us-east-1.amazonaws.com"
COST_METRICS = (
    "UnblendedCost",
    "BlendedCost",
    "AmortizedCost",
    "NetUnblendedCost",
    "NetAmortizedCost",
)
DEFAULT_LOOKBACK = timedelta(days=30)


async def query_cost_and_usage(
    credentials: AWSAccessKey,
    start: date,
    end: date,
    group_by: GroupBy | None = None,
) -> list[CostAndUsageResponse]:
    """Return a complete daily cost query, or fail without returning partial pages."""

    # Other AWS service overloads lack stubs; the selected ce client is typed.
    # SDK signing: https://docs.aws.amazon.com/cost-management/latest/userguide/ce-api.html
    def request() -> list[CostAndUsageResponse]:
        params: GetCostAndUsageRequestTypeDef = {
            "TimePeriod": {"Start": start.isoformat(), "End": end.isoformat()},
            "Granularity": "DAILY",
            "Metrics": list(COST_METRICS),
        }
        if group_by is not None:
            params["GroupBy"] = [{"Type": "DIMENSION", "Key": group_by}]
        pages: list[CostAndUsageResponse] = []
        with closing(
            Session().client(  # pyright: ignore[reportUnknownMemberType]
                "ce",
                region_name="us-east-1",
                endpoint_url=API,
                aws_access_key_id=credentials.access_key_id,
                aws_secret_access_key=credentials.secret_access_key,
                config=Config(
                    connect_timeout=10,
                    read_timeout=60,
                    retries={"mode": "standard", "total_max_attempts": 5},
                ),
            )
        ) as client:
            while True:
                page = CostAndUsageResponse.model_validate(
                    client.get_cost_and_usage(**params)
                )
                pages.append(page)
                if not page.NextPageToken:
                    return pages
                params["NextPageToken"] = page.NextPageToken

    return await asyncio.to_thread(request)


def utc_now() -> datetime:
    return datetime.now(UTC)


def months_before(day: date, months: int) -> date:
    """Return the 1st of the month `months` before `day`'s month."""
    index = day.year * 12 + day.month - 1 - months
    return date(index // 12, index % 12 + 1, 1)


def available_start(start: date, today: date) -> date:
    """Clamp a requested date to AWS's current month plus 13 prior months."""
    return max(start, months_before(today, 13))


def cost_rows(
    pages: list[CostAndUsageResponse], model: type[DailyCost]
) -> Iterator[DailyCost]:
    """Yield one row per day and group, preserving exact monetary strings."""
    # Account names can arrive on a later page than the account's costs.
    attributes: dict[str, dict[str, str]] = {}
    for page in pages:
        for attribute in page.DimensionValueAttributes:
            attributes.setdefault(attribute.Value, {}).update(attribute.Attributes)

    for page in pages:
        for result in page.ResultsByTime:
            group_metrics = (
                [("", result.Total)]
                if model.group_by is None
                else [(group.Keys[0], group.Metrics) for group in result.Groups]
            )
            for group_value, metrics in group_metrics:
                yield model.model_validate(
                    {
                        "date": result.TimePeriod.Start,
                        "period_end": result.TimePeriod.End,
                        "estimated": result.Estimated,
                        "metrics": monetary_metrics(metrics),
                        **model.group_fields(group_value, attributes),
                    }
                )


def monetary_metrics(metrics: dict[str, Metric]) -> dict[str, CostMetric]:
    return {
        name: CostMetric(amount=metrics[name].Amount, unit=metrics[name].Unit)
        for name in COST_METRICS
    }


async def poll_costs(
    credentials: AWSAccessKey,
    start: date | None,
    model: type[DailyCost],
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[DailyCost | datetime, None]:
    """Reread complete UTC days through yesterday.

    The first poll reads from `start`, or the last `DEFAULT_LOOKBACK` days
    without one. Later polls reread from the month before the previous poll,
    since AWS finalizes a month's costs early in the next; anchoring on the
    cursor also covers days missed while the capture was paused. Rows that
    leave the window keep their last captured values.
    """
    assert isinstance(cursor, datetime)
    poll_started = utc_now()
    if poll_started <= cursor:
        return
    today = poll_started.date()
    refresh_from = months_before(cursor.date(), 1)
    lower = available_start(max(start or today - DEFAULT_LOOKBACK, refresh_from), today)
    if lower < today:
        pages = await query_cost_and_usage(credentials, lower, today, model.group_by)
        count = 0
        for row in cost_rows(pages, model):
            yield row
            count += 1
        log.info(f"Fetched {count} {model.name} from {lower} to {today}")
    yield poll_started


def fetch_daily_costs(
    credentials: AWSAccessKey, start: date | None, log: Logger, cursor: LogCursor
) -> AsyncGenerator[DailyCost | datetime, None]:
    return poll_costs(credentials, start, DailyCost, log, cursor)


def fetch_daily_costs_by_service(
    credentials: AWSAccessKey, start: date | None, log: Logger, cursor: LogCursor
) -> AsyncGenerator[DailyCost | datetime, None]:
    return poll_costs(credentials, start, ServiceCost, log, cursor)


def fetch_daily_costs_by_linked_account(
    credentials: AWSAccessKey, start: date | None, log: Logger, cursor: LogCursor
) -> AsyncGenerator[DailyCost | datetime, None]:
    return poll_costs(credentials, start, AccountCost, log, cursor)
