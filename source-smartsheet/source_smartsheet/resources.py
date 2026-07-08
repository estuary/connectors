import functools
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import (
    ConnectorState,
    ResourceConfigWithSchedule,
    ResourceState,
    SnapshotResource,
    open_binding,  # pyright: ignore[reportUnknownVariableType]
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    backfill_sheet_rows,
    backfill_sheets,
    base_url,
    fetch_sheet_rows,
    fetch_sheets,
    list_all_sheet_ids,
    snapshot_report_rows,
    snapshot_reports,
)
from .models import EndpointConfig, Report, ReportRow, Sheet, SheetRow

# Daily reconciliation schedule for `sheet_rows` — its `rowsModifiedSince`
# cursor has confirmed gaps (blank/never-cell-written rows are invisible to
# it forever) that only a periodic full re-backfill per sheet can close.
DEFAULT_SCHEDULE = "0 0 * * *"

SmartsheetResource = common.Resource[
    Sheet | SheetRow | Report | ReportRow, common.ResourceConfig, ResourceState
]

# Reports can be expensive per cycle (up to 5 detail requests/report by row
# count) -- start conservative relative to the Sheets cluster's 10-minute
# interval.
REPORTS_INTERVAL = timedelta(minutes=30)


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured credentials authenticate against the provider."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    url = f"{base_url(config.region)}/users/me"

    try:
        _ = await http.request(log, url)
    except HTTPError as err:
        msg = "Encountered error validating credentials.\n\n" + err.message
        if err.code == 401:
            msg = f"Invalid API Access Token. Please confirm the provided token is correct.\n\n{err.message}"

        raise ValidationError([msg])


def _generate_sheet_resource_state(
    sheet_ids: list[int], cutoff: datetime
) -> ResourceState:
    # Seam: backfill covers ticks < cutoff, so the cursor seeds one tick back
    # to make the incremental side's first emitted tick exactly cutoff.
    return ResourceState(
        inc={
            f"{sheet_id}": ResourceState.Incremental(
                cursor=cutoff - timedelta(seconds=1)
            )
            for sheet_id in sheet_ids
        },
        backfill={
            f"{sheet_id}": ResourceState.Backfill(cutoff=cutoff, next_page=None)
            for sheet_id in sheet_ids
        },
    )


def _whole_second_cutoff() -> datetime:
    # Backfill/incremental seams are computed in whole ticks (add-stream House
    # rule 6): provider timestamps are whole seconds, so the cutoff is floored
    # to one.
    return datetime.now(tz=UTC).replace(microsecond=0)


async def _patch_missing_sheet_states(
    binding: CaptureBinding[common.ResourceConfig],
    state: ResourceState,
    task: Task,
    sheet_ids: list[int],
    cutoff: datetime,
):
    """New sheets are only discovered when this binding (re)opens, not
    mid-run. When one appears between runs, seed cursor/backfill state for it
    here so it isn't silently skipped"""
    if not (isinstance(state.inc, dict) and isinstance(state.backfill, dict)):
        return

    missing_sheet_ids = [
        sheet_id for sheet_id in sheet_ids if f"{sheet_id}" not in state.inc
    ]
    if not missing_sheet_ids:
        return

    new_states = _generate_sheet_resource_state(missing_sheet_ids, cutoff)
    assert isinstance(new_states.inc, dict)
    assert isinstance(new_states.backfill, dict)

    old_state = deepcopy(state)

    state.inc.update(new_states.inc)
    state.backfill.update(new_states.backfill)

    task.log.info(
        f"Checkpointing state to ensure any new sheet state is persisted for {binding.stateKey}.",
        {
            "prevState": old_state,
            "newState": state,
        },
    )
    await task.checkpoint(ConnectorState(bindingStateV1={binding.stateKey: state}))


async def sheets(http: HTTPMixin, config: EndpointConfig) -> SmartsheetResource:
    cutoff = _whole_second_cutoff()

    async def open(
        binding: CaptureBinding[common.ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_sheets, http, config.region),
            fetch_page=functools.partial(
                backfill_sheets, http, config.region, config.start_date
            ),
        )

    return SmartsheetResource(
        name=Sheet.resource_name,
        key=["/id"],
        model=Sheet,
        open=open,  # pyright: ignore[reportUnknownArgumentType]
        initial_state=ResourceState(
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
            inc=ResourceState.Incremental(cursor=cutoff - timedelta(seconds=1)),
        ),
        initial_config=common.ResourceConfig(
            name=Sheet.resource_name,
            interval=timedelta(minutes=10),
        ),
        schema_inference=True,
    )


async def sheet_rows(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> SmartsheetResource:
    sheet_ids = await list_all_sheet_ids(http, config.region, log)
    cutoff = _whole_second_cutoff()

    incremental_fetchers: dict[str, common.FetchChangesFn[SheetRow]] = {
        f"{sheet_id}": functools.partial(
            fetch_sheet_rows, http, config.region, sheet_id
        )
        for sheet_id in sheet_ids
    }
    backfill_fetchers: dict[str, common.FetchPageFn[SheetRow]] = {
        f"{sheet_id}": functools.partial(
            backfill_sheet_rows, http, config.region, config.start_date, sheet_id
        )
        for sheet_id in sheet_ids
    }

    async def open(
        binding: CaptureBinding[common.ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        await _patch_missing_sheet_states(binding, state, task, sheet_ids, cutoff)

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=incremental_fetchers,
            fetch_page=backfill_fetchers,
        )

    return SmartsheetResource(
        name=SheetRow.resource_name,
        key=["/_meta/sheet_id", "/id"],
        model=SheetRow,
        open=open,  # pyright: ignore[reportUnknownArgumentType]
        initial_state=_generate_sheet_resource_state(sheet_ids, cutoff),
        initial_config=ResourceConfigWithSchedule(
            name=SheetRow.resource_name,
            interval=timedelta(minutes=10),
            schedule=DEFAULT_SCHEDULE,
        ),
        schema_inference=True,
    )


async def reports(http: HTTPMixin, config: EndpointConfig) -> SmartsheetResource:
    def open(
        binding: CaptureBinding[common.ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_reports, http, config.region),
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d")),
        )

    return SnapshotResource(
        name=Report.resource_name,
        key=["/id"],
        model=Report,
        open=open,  # pyright: ignore[reportUnknownArgumentType]
        initial_config=common.ResourceConfig(
            name=Report.resource_name,
            interval=REPORTS_INTERVAL,
        ),
        schema_inference=True,
    )


async def report_rows(http: HTTPMixin, config: EndpointConfig) -> SmartsheetResource:
    def open(
        binding: CaptureBinding[common.ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_report_rows, http, config.region),
            tombstone=BaseDocument(_meta=BaseDocument.Meta(op="d")),
        )

    return SnapshotResource(
        name=ReportRow.resource_name,
        key=["/_meta/report_id", "/id"],
        model=ReportRow,
        open=open,  # pyright: ignore[reportUnknownArgumentType]
        initial_config=common.ResourceConfig(
            name=ReportRow.resource_name,
            interval=REPORTS_INTERVAL,
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[SmartsheetResource]:
    """Enumerate every stream the connector exposes."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    return [
        await sheets(http, config),
        await sheet_rows(log, http, config),
        await reports(http, config),
        await report_rows(http, config),
    ]
