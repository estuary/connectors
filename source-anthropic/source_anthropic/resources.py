import functools
from datetime import timedelta
from logging import Logger

from estuary_cdk.capture import common, Task
from estuary_cdk.capture.common import ResourceConfig, ResourceState
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    ANTHROPIC_HEADERS,
    API,
    backfill_cost,
    backfill_usage,
    fetch_cost,
    fetch_usage,
)
from .models import (
    GRANULARITIES,
    GROUP_BY,
    CostRow,
    EndpointConfig,
    Granularity,
    ReportRow,
    UsageRow,
    utc_now,
)

INTERVAL = timedelta(hours=1)

# Each report model's incremental and backfill functions. A model missing here
# fails with a KeyError rather than silently getting another model's functions.
FETCHERS = {
    UsageRow: (fetch_usage, backfill_usage),
    CostRow: (fetch_cost, backfill_cost),
}


def _token_source(config: EndpointConfig) -> TokenSource:
    # Anthropic authenticates with the raw key in `x-api-key` (no Bearer prefix).
    return TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_header="x-api-key",
    )


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the API key can read the organization's usage analytics."""
    http.token_source = _token_source(config)
    starting_at = (utc_now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:00:00Z")

    try:
        await http.request(
            log,
            f"{API}/v1/organizations/analytics/usage",
            params={"starting_at": starting_at, "limit": 1},
            headers=dict(ANTHROPIC_HEADERS),
        )
    except HTTPError as err:
        if err.code in (401, 403):
            msg = (
                "The API key was rejected. Use an Admin API key (Claude Console "
                "organizations) or an Analytics API key with the read:analytics "
                f"scope (Claude Enterprise organizations).\n\n{err.message}"
            )
        else:
            msg = f"Encountered error validating API key.\n\n{err.message}"
        raise ValidationError([msg])


def _report_resource(
    http: HTTPMixin,
    config: EndpointConfig,
    model: type[ReportRow],
    granularity: Granularity,
) -> common.Resource:
    name = f"{model.PATH}_{granularity.name}"
    fetch_changes, fetch_page = FETCHERS[model]
    lookback = timedelta(hours=config.advanced.lookback_hours)
    group_by = GROUP_BY[config.organization_type]
    backfills = granularity.rolling_window is None

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_changes, http, granularity, group_by, config.start_date, lookback
            ),
            fetch_page=(
                functools.partial(
                    fetch_page, http, granularity, group_by, config.start_date
                )
                if backfills
                else None
            ),
        )

    cutoff = utc_now()
    return common.Resource(
        name=name,
        key=["/starting_at", "/group_key"],
        model=model,
        open=open,
        initial_state=ResourceState(
            # One interval back, so the first sweep runs at once instead of
            # waiting for the cursor to age. Every sweep re-reads from
            # cursor - lookback, so the earlier cursor only widens that window.
            inc=ResourceState.Incremental(cursor=cutoff - INTERVAL),
            backfill=(
                ResourceState.Backfill(cutoff=cutoff, next_page=None)
                if backfills
                else None
            ),
        ),
        initial_config=ResourceConfig(name=name, interval=INTERVAL),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    http.token_source = _token_source(config)
    return [
        _report_resource(http, config, model, granularity)
        for model in (UsageRow, CostRow)
        for granularity in GRANULARITIES
    ]
