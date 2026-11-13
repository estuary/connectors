import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, cast

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import Resource, open_binding
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    ALL_ORDERS_LAG,
    ALL_ORDERS_MIN_INTERVAL,
    ALL_ORDERS_OVERLAP,
    TICK,
    TRANSACTIONS_LATE_LAG,
    TRANSACTIONS_REALTIME_LAG,
    backfill_all_orders,
    backfill_financial_transactions,
    backfill_orders,
    backfill_sales_and_traffic_by_date,
    backfill_settlement_reports,
    ceil_to_tick,
    fetch_all_orders,
    fetch_financial_transactions,
    fetch_marketplace_participations,
    fetch_orders,
    fetch_sales_and_traffic_by_date,
    fetch_settlement_reports,
    floor_to_day,
    floor_to_tick,
)
from .client import SPAPIClient
from .models import (
    OAUTH2_SPEC,
    AllOrdersRow,
    ConnectorState,
    EndpointConfig,
    FinancialTransaction,
    Order,
    OrdersResourceConfig,
    ResourceState,
    SalesAndTrafficByDate,
    SettlementReportRow,
    lwa_credentials,
)
from .reports import REPORT_MARKETPLACE_LIMIT, RecentReports

ACCESS_TOKEN_HEADER = "x-amz-access-token"

# Subtasks of the financial transactions stream. The realtime pass reads
# transactions soon after they post; the late pass reads the same time again
# for transactions Amazon posts late (it says up to 48 hours); the lookback
# pass reads it a final time, a configured distance behind (longer than the
# late lag), for deferred transactions Amazon has since released.
REALTIME = "realtime"
LATE = "late"
LOOKBACK = "lookback"


def _client(http: HTTPMixin, config: EndpointConfig) -> SPAPIClient:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC,
        credentials=lwa_credentials(config.credentials),
        authorization_header=ACCESS_TOKEN_HEADER,
    )
    return SPAPIClient(
        http=http, base_url=config.region.host(config.advanced.sandbox)
    )


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    client = _client(http, config)

    try:
        participations = await fetch_marketplace_participations(client, log)
    except HTTPError as err:
        # Login with Amazon rejects bad client credentials or refresh tokens
        # with 400 or 401; SP-API rejects an unauthorized application with 403.
        if err.code not in (400, 401, 403):
            raise
        raise ValidationError(
            [
                f"Invalid credentials. Please confirm the client ID, client secret and refresh token are correct, and that the selling account authorized this application in the selected region.\n\n{err.message}"
            ]
        )

    if config.advanced.sandbox:
        log.warning("sandbox mode: skipping the marketplace participation check")
        return

    participating = {p.marketplace.id for p in participations.payload}
    missing = [m for m in config.marketplace_ids if m not in participating]
    if missing:
        raise ValidationError(
            [
                f"The selling account does not participate in marketplace(s) {', '.join(missing)} in the {config.region} region. "
                f"Marketplaces it participates in: {', '.join(sorted(participating)) or 'none'}."
            ]
        )


def orders(log: Logger, client: SPAPIClient, config: EndpointConfig) -> Resource:
    def open(
        binding: CaptureBinding[OrdersResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: list[tuple[CaptureBinding[OrdersResourceConfig], Resource]],
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_orders,
                client,
                config.marketplace_ids,
                config.advanced.window_size,
                config.advanced.overlap,
            ),
            fetch_page=functools.partial(
                backfill_orders,
                client,
                config.marketplace_ids,
                config.start_date,
                config.advanced.window_size,
            ),
        )

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    return Resource(
        name="orders",
        key=["/orderId"],
        model=Order,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff - TICK),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        ),
        initial_config=OrdersResourceConfig(
            name="orders", interval=timedelta(minutes=15)
        ),
        schema_inference=True,
    )


def financial_transactions(
    log: Logger, client: SPAPIClient, config: EndpointConfig
) -> Resource:
    lookback = config.advanced.financial_transactions_lookback

    async def open(
        binding: CaptureBinding[OrdersResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: list[tuple[CaptureBinding[OrdersResourceConfig], Resource]],
    ):
        # A lookback raised on a running capture leaves the stored cursor
        # ahead of the new horizon. Move it back so the newly covered time is
        # read again at the new lookback, rather than skipped while the
        # horizon catches up. Never before start_date. The rewound cursor is
        # checkpointed before any pass runs: the first poll after a rewind
        # finds no full window and saves nothing, so a restart before the
        # next one would otherwise rewind again from a later now.
        assert isinstance(state.inc, dict)
        lookback_state = state.inc[LOOKBACK]
        assert lookback_state is not None
        cursor = cast(datetime, lookback_state.cursor)
        earliest = max(
            floor_to_tick(datetime.now(tz=UTC) - lookback),
            ceil_to_tick(config.start_date),
        )
        if earliest < cursor:
            log.info(
                "financial transactions lookback raised, re-reading from earlier",
                {"cursor": cursor, "rewound_to": earliest},
            )
            lookback_state.cursor = earliest
            await task.checkpoint(
                ConnectorState(
                    bindingStateV1={
                        binding.stateKey: ResourceState(inc={LOOKBACK: lookback_state})
                    }
                )
            )

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                REALTIME: functools.partial(
                    fetch_financial_transactions,
                    client,
                    TRANSACTIONS_REALTIME_LAG,
                    config.advanced.window_size,
                ),
                LATE: functools.partial(
                    fetch_financial_transactions,
                    client,
                    TRANSACTIONS_LATE_LAG,
                    config.advanced.window_size,
                ),
                LOOKBACK: functools.partial(
                    fetch_financial_transactions,
                    client,
                    lookback,
                    config.advanced.window_size,
                ),
            },
            fetch_page=functools.partial(
                backfill_financial_transactions,
                client,
                config.start_date,
                config.advanced.window_size,
            ),
        )

    # The cursor is the next window's inclusive start, so incremental begins
    # exactly where the backfill's exclusive end leaves off.
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    # A re-reading pass starts its lag before the cutoff, but never before
    # start_date. A start_date after the cutoff starts it at the cutoff,
    # where the realtime pass also starts; as with orders, transactions
    # posted between the cutoff and a later start_date are still captured.
    def rereading_cursor(lag: timedelta) -> datetime:
        return max(cutoff - lag, min(ceil_to_tick(config.start_date), cutoff))

    return Resource(
        name="financial_transactions",
        key=["/transactionId"],
        model=FinancialTransaction,
        open=open,
        initial_state=ResourceState(
            inc={
                REALTIME: ResourceState.Incremental(cursor=cutoff),
                LATE: ResourceState.Incremental(
                    cursor=rereading_cursor(TRANSACTIONS_LATE_LAG)
                ),
                LOOKBACK: ResourceState.Incremental(cursor=rereading_cursor(lookback)),
            },
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        ),
        initial_config=OrdersResourceConfig(
            name="financial_transactions", interval=timedelta(minutes=15)
        ),
        schema_inference=True,
    )


def settlement_reports(
    log: Logger, client: SPAPIClient, config: EndpointConfig
) -> Resource:
    def open(
        binding: CaptureBinding[OrdersResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: list[tuple[CaptureBinding[OrdersResourceConfig], Resource]],
    ):
        # Shared by both tasks, so a report either has read is not
        # downloaded again by the other.
        recent = RecentReports()
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_settlement_reports,
                client,
                recent,
                config.start_date,
            ),
            fetch_page=functools.partial(
                backfill_settlement_reports,
                client,
                recent,
                config.start_date,
            ),
        )

    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

    # Settlement reports aren't filtered by the configured marketplaces:
    # whether Amazon sets marketplaceIds on the reports it generates is
    # unverified, and filtering on it could drop them. Rows carry the
    # report's marketplaceIds instead.
    #
    # The binding has no backfill schedule. A scheduled backfill restarts
    # the capture and reads every report Amazon still keeps again, which is
    # 90 days of settlements each time it runs.
    return Resource(
        name="settlement_reports",
        key=["/reportId", "/rowNumber"],
        model=SettlementReportRow,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff - TICK),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        ),
        initial_config=OrdersResourceConfig(
            name="settlement_reports", interval=timedelta(hours=1)
        ),
        schema_inference=True,
    )


def all_orders(log: Logger, client: SPAPIClient, config: EndpointConfig) -> Resource:
    def open(
        binding: CaptureBinding[OrdersResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: list[tuple[CaptureBinding[OrdersResourceConfig], Resource]],
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_all_orders,
                client,
                config.marketplace_ids,
            ),
            fetch_page=functools.partial(
                backfill_all_orders,
                client,
                config.marketplace_ids,
                config.start_date,
            ),
        )

    # The cutoff is ALL_ORDERS_LAG in the past, so no window, backfill or
    # incremental, ends where Amazon may still be recording updates.
    cutoff = datetime.now(tz=UTC).replace(microsecond=0) - ALL_ORDERS_LAG

    # Polled daily: each poll requests a report, and the orders binding
    # already captures orders within minutes.
    return Resource(
        name="all_orders",
        key=["/reportId", "/rowNumber"],
        model=AllOrdersRow,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        ),
        initial_config=OrdersResourceConfig(
            name="all_orders", interval=timedelta(days=1)
        ),
        schema_inference=True,
    )


def sales_and_traffic_by_date(
    log: Logger, client: SPAPIClient, config: EndpointConfig
) -> Resource:
    def open(
        binding: CaptureBinding[OrdersResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: list[tuple[CaptureBinding[OrdersResourceConfig], Resource]],
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_sales_and_traffic_by_date,
                client,
                config.marketplace_ids,
                config.start_date,
            ),
            fetch_page=functools.partial(
                backfill_sales_and_traffic_by_date,
                client,
                config.marketplace_ids,
                config.start_date,
            ),
        )

    # Days are read whole, so the backfill ends with yesterday and
    # incremental's first new day is today, once it is over.
    cutoff = floor_to_day(datetime.now(tz=UTC))

    # Polled daily: a new day completes once a day.
    return Resource(
        name="sales_and_traffic_by_date",
        key=["/marketplaceId", "/date"],
        model=SalesAndTrafficByDate,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=None, cutoff=cutoff),
        ),
        initial_config=OrdersResourceConfig(
            name="sales_and_traffic_by_date", interval=timedelta(days=1)
        ),
        schema_inference=True,
        # Discovered disabled: the report needs the Brand Analytics role,
        # which most applications lack.
        disable=True,
    )


def validate_bindings(config: EndpointConfig, resolved: list[tuple[Any, Resource]]) -> None:
    """
    Fails publication of an all_orders binding the capture couldn't run, so
    it doesn't stop every other binding at each start. Orders-only captures
    may still use up to 50 marketplaces.
    """
    errors: list[str] = []
    for binding, resource in resolved:
        if resource.name != "all_orders":
            continue
        if len(config.marketplace_ids) > REPORT_MARKETPLACE_LIMIT:
            errors.append(
                f"all_orders: createReport accepts at most {REPORT_MARKETPLACE_LIMIT} marketplace IDs; "
                f"{len(config.marketplace_ids)} are configured. Disable the all_orders binding or capture fewer marketplaces."
            )
        if binding.resourceConfig.interval < ALL_ORDERS_MIN_INTERVAL:
            errors.append(
                f"all_orders needs an interval of at least {ALL_ORDERS_MIN_INTERVAL}: each poll reads "
                f"the last {ALL_ORDERS_OVERLAP} again under new keys, so every row is emitted about "
                f"1 + {ALL_ORDERS_OVERLAP} / interval times."
            )
        if binding.resourceConfig.schedule:
            errors.append(
                "all_orders cannot be re-backfilled on a schedule: each re-backfill adds new copies "
                "of every row (rows are keyed by reportId, rowNumber)."
            )
    if errors:
        raise ValidationError(errors)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[Resource]:
    client = _client(http, config)
    return [
        orders(log, client, config),
        financial_transactions(log, client, config),
        settlement_reports(log, client, config),
        all_orders(log, client, config),
        sales_and_traffic_by_date(log, client, config),
    ]
