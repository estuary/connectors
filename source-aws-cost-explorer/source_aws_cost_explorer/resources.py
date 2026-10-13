from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import partial
from logging import Logger

from botocore.exceptions import ClientError
from estuary_cdk.capture import common
from estuary_cdk.capture.common import LogCursor
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPMixin

from .api import (
    fetch_daily_costs,
    fetch_daily_costs_by_linked_account,
    fetch_daily_costs_by_service,
    query_cost_and_usage,
    utc_now,
)
from .models import (
    AccountCost,
    AWSAccessKey,
    CostResourceConfig,
    DailyCost,
    EndpointConfig,
    ServiceCost,
)

CostResource = common.Resource[BaseDocument, CostResourceConfig, common.ResourceState]


@dataclass(frozen=True)
class CostStream:
    model: type[DailyCost]
    fetch_changes: Callable[
        [AWSAccessKey, date | None, Logger, LogCursor],
        AsyncGenerator[DailyCost | datetime, None],
    ]


STREAMS = (
    CostStream(DailyCost, fetch_daily_costs),
    CostStream(ServiceCost, fetch_daily_costs_by_service),
    CostStream(AccountCost, fetch_daily_costs_by_linked_account),
)


async def validate_credentials(
    _log: Logger, _http: HTTPMixin, config: EndpointConfig
) -> None:
    """Confirm the configured credentials authenticate against the provider."""
    # Validation flow follows source-front/source_front/resources.py:28,
    # using signed AWS SDK requests in place of the bearer-token HTTP probe.
    end = utc_now().date()
    try:
        await query_cost_and_usage(config.credentials, end - timedelta(days=1), end)
    except ClientError as err:
        code = err.response.get("Error", {}).get("Code")
        # Cost Explorer's documented common errors, plus InvalidSignatureException
        # for a wrong secret: https://docs.aws.amazon.com/aws-cost-management/latest/APIReference/CommonErrors.html
        if code in {
            "AccessDeniedException",
            "NotAuthorized",
            "UnrecognizedClientException",
            "IncompleteSignature",
            "InvalidSignatureException",
            "ExpiredTokenException",
        }:
            raise ValidationError(
                [
                    f"AWS rejected the credentials or Cost Explorer permissions ({code}). Check the access key, secret, optional session token, and ce:GetCostAndUsage permission."
                ]
            ) from err
        if code in {"DataUnavailableException", "OptInRequired"}:
            raise ValidationError(
                [
                    "Cost Explorer data is unavailable. Enable Cost Explorer in the Billing console and allow time for AWS to prepare the data."
                ]
            ) from err
        raise


def configured_start(config: EndpointConfig) -> date | None:
    if config.start_date is None:
        return None
    return config.start_date.astimezone(UTC).date()


def cost_resource(stream: CostStream, config: EndpointConfig) -> CostResource:
    # Full-window polling with a clock checkpoint follows
    # source-calendly/source_calendly/api.py:216 and resources.py:141.
    # The clock controls cadence; it never filters billed usage or corrections.
    def open(
        binding: CaptureBinding[CostResourceConfig],
        binding_index: int,
        state: common.ResourceState,
        task: common.Task,
        _all_bindings: object,
    ) -> None:
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=partial(
                stream.fetch_changes, config.credentials, configured_start(config)
            ),
        )

    return CostResource(
        name=stream.model.name,
        key=stream.model.key,
        model=stream.model,
        open=open,
        initial_state=common.ResourceState(
            inc=common.ResourceState.Incremental(cursor=datetime.fromtimestamp(0, UTC)),
        ),
        initial_config=CostResourceConfig(name=stream.model.name),
        schema_inference=True,
    )


async def all_resources(
    _log: Logger, _http: HTTPMixin, config: EndpointConfig
) -> list[CostResource]:
    """Discover the three fixed cost streams without making AWS requests."""
    return [cost_resource(stream, config) for stream in STREAMS]
