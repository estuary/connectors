from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel, JsonValue

from .models import (
    DetailPageMeta,
    ListPageMeta,
    Report,
    ReportIdValidationContext,
    ReportRow,
    RowValidationContext,
    Sheet,
    SheetIdValidationContext,
    SheetRow,
    SheetSummary,
)


def _format_rfc3339(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


# =============================================================================
# List endpoints — `GET /sheets` and `GET /reports` share one envelope shape,
# one page size, and one completion signal, so a single parametrized
# stream/iterate pair serves both. Items under `data[]` are streamed
# incrementally; the rest of the envelope arrives as the processor's
# remainder (`ListPageMeta`, which documents the per-endpoint pagination
# quirks).
# =============================================================================

LIST_PAGE_SIZE = 100


async def _stream_list_page[ItemT: BaseModel](
    http: HTTPSession,
    log: Logger,
    url: str,
    item_cls: type[ItemT],
    page: int,
    modified_since: datetime | None = None,
) -> IncrementalJsonProcessor[ItemT, ListPageMeta]:
    # `modified_since` is only ever passed for the sheets list: the reports
    # list's `modifiedSince` tracks report-definition edits, not the data a
    # report renders, and is never used (see models.py's Reports header).
    params: dict[str, JsonValue] = {"page": page, "pageSize": LIST_PAGE_SIZE}
    if modified_since is not None:
        params["modifiedSince"] = _format_rfc3339(modified_since)

    _, body = await http.request_stream(log, url, params=params)
    return IncrementalJsonProcessor(body(), "data.item", item_cls, ListPageMeta)


async def _iter_list_items[ItemT: BaseModel](
    http: HTTPSession,
    log: Logger,
    url: str,
    item_cls: type[ItemT],
    modified_since: datetime | None = None,
) -> AsyncGenerator[ItemT, None]:
    page = 1

    while True:
        processor = await _stream_list_page(
            http, log, url, item_cls, page, modified_since
        )
        async for item in processor:
            yield item

        meta = processor.get_remainder()
        # Both list endpoints self-report and clamp safely: pageNumber always
        # echoes what was actually served, even for a page past the last one.
        if meta.pageNumber >= meta.totalPages:
            return

        page += 1


async def list_all_sheet_ids(http: HTTPSession, region: str, log: Logger) -> list[int]:
    return [
        summary.id
        async for summary in _iter_list_items(
            http, log, SheetSummary.build_url(region), SheetSummary
        )
    ]


# =============================================================================
# Detail endpoints — `GET /sheets/{id}` and `GET /reports/{id}` share one
# envelope shape and one positional-pagination contract
# =============================================================================

DETAIL_PAGE_SIZE = 10_000


async def _stream_detail_page[RowT: BaseModel](
    http: HTTPSession,
    log: Logger,
    url: str,
    row_cls: type[RowT],
    page: int,
    page_size: int,
    rows_modified_since: datetime | None = None,
    validation_context: RowValidationContext | None = None,
) -> IncrementalJsonProcessor[RowT, DetailPageMeta]:
    # `rows_modified_since` is only ever passed for sheets: `GET /reports/{id}`
    # has no equivalent row filter.
    params: dict[str, JsonValue] = {"page": page, "pageSize": page_size}
    if rows_modified_since is not None:
        params["rowsModifiedSince"] = _format_rfc3339(rows_modified_since)

    _, body = await http.request_stream(log, url, params=params)
    return IncrementalJsonProcessor(
        body(), "rows.item", row_cls, DetailPageMeta, validation_context
    )


async def _iter_detail_rows[RowT: BaseModel](
    http: HTTPSession,
    log: Logger,
    url: str,
    row_cls: type[RowT],
    page_size: int,
    rows_modified_since: datetime | None = None,
    validation_context: RowValidationContext | None = None,
) -> AsyncGenerator[RowT, None]:
    """Unlike the list endpoints, the detail endpoints silently *clamp* to the
    last valid page instead of erroring or returning empty once `page` goes
    past the entity's real page count, and carry no `pageNumber`/`totalPages`
    field to detect the clamp. The loop bound is therefore computed client-side
    from the remainder's `totalRowCount` after each page drains.

    An empty page ends the walk ONLY when no `rows_modified_since` filter is
    set: pagination is applied before the filter, so a filtered page can
    legitimately be empty mid-walk while later pages still hold matching rows.
    `totalRowCount` is always the entity's real, unfiltered total, so it's a
    correct loop bound whether or not a filter is set."""

    page = 1

    while True:
        processor = await _stream_detail_page(
            http,
            log,
            url,
            row_cls,
            page,
            page_size,
            rows_modified_since,
            validation_context,
        )

        empty = True
        async for row in processor:
            empty = False
            yield row

        meta = processor.get_remainder()
        if rows_modified_since is None and empty:
            return
        if page * page_size >= meta.totalRowCount:
            return

        page += 1


# =============================================================================
# `sheets` stream (metadata)
# =============================================================================

# The sheet list's `modifiedSince` index lags content edits — observed in
# 5s-65s on a live measurement.
LIST_INDEX_LAG_ALLOWANCE = timedelta(minutes=5)


async def _fetch_sheet_metadata(
    http: HTTPSession, region: str, sheet_id: int, log: Logger
) -> Sheet:
    # pageSize=1 is the cheapest call that still returns the full metadata
    # envelope — the one embedded row is drained and discarded; only the
    # remainder is kept.
    processor = await _stream_detail_page(
        http, log, SheetRow.build_url(region, sheet_id), SheetRow, page=1, page_size=1
    )

    async for _ in processor:
        pass

    return Sheet.from_detail(processor.get_remainder())


async def fetch_sheets(
    http: HTTPSession,
    region: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Sheet | LogCursor, None]:
    """Incremental sheet metadata, windowed by the list's `modifiedSince`.

              cursor  cursor+1s   horizon = poll_start − allowance
    ───────────────┼───────┼─────────────┼─────────────┼────▶ time (1s ticks)
                   │       │             │             └ poll_start
    modifiedSince ─┼───────[═════════════╪═════════════╪════▶
    emitted ───────┼───────[═════════════╪═════════════]
                   │       │             └─ next cursor = min(max emitted, horizon)
                   │       └─ queried bound: floor(cursor) + 1s
                   └─ cursor's own second already emitted last poll (safe to skip)
    """
    assert isinstance(log_cursor, datetime)

    poll_start = datetime.now(tz=UTC)
    since = log_cursor.replace(microsecond=0) + timedelta(seconds=1)
    max_modified = log_cursor

    async for summary in _iter_list_items(
        http, log, SheetSummary.build_url(region), SheetSummary, modified_since=since
    ):
        yield await _fetch_sheet_metadata(http, region, summary.id, log)
        max_modified = max(max_modified, summary.modifiedAt)

    new_cursor = min(max_modified, poll_start - LIST_INDEX_LAG_ALLOWANCE)
    if new_cursor > log_cursor:
        yield new_cursor


async def backfill_sheets(
    http: HTTPSession,
    region: str,
    start_date: datetime,
    log: Logger,
    _page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Sheet | PageCursor, None]:
    """Backfill sheet metadata from the unfiltered list walk.

              start_date       cutoff−1s  cutoff (whole second)
    ──────────────┼──────────────────┼──────────┼───▶ time (1s ticks)
                  │                  │          │
    emitted ──────[══════════════════]          │
                  │                  │          └─ first incremental tick
                  │                  └─ last backfilled second
                  └─ configured replication start

    The API offers only positional pagination.
    """
    assert isinstance(cutoff, datetime)

    async for summary in _iter_list_items(
        http, log, SheetSummary.build_url(region), SheetSummary
    ):
        if summary.modifiedAt >= cutoff:
            continue  # already covered by incremental
        if summary.modifiedAt < start_date:
            continue  # before the configured replication window
        yield await _fetch_sheet_metadata(http, region, summary.id, log)


# =============================================================================
# `sheet_rows` stream (row data, child of `sheets`)
# =============================================================================


async def fetch_sheet_rows(
    http: HTTPSession,
    region: str,
    sheet_id: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[SheetRow | LogCursor, None]:
    """Incremental sheet row fetch.

                  cursor  cursor+1s     horizon = last elapsed second
    ───────────────────┼───────┼─────────────────┼────▶ time (1s ticks)
                       │       │                 │
    rowsModifiedSince ─(═══════╪═════════════════╪════▶
    emitted ───────────┼───────[═════════════════]
                       │       │                 └─ present second may still
                       │       │                    take edits
                       │       └─ first emitted second
                       └─ queried bound: the cursor itself (strict `>` at
                          equality)

    The detail endpoint publishes edits immediately (no list-index lag).

    One blind spot: blank/never-cell-written rows are invisible to the filter
    forever. Only a periodic full re-backfill per sheet closes this, which the
    `sheet_rows` resource schedules daily.
    """
    assert isinstance(log_cursor, datetime)

    horizon = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(seconds=1)
    if horizon <= log_cursor:
        return

    ctx = SheetIdValidationContext(sheet_id=sheet_id)
    max_modified = log_cursor

    async for row in _iter_detail_rows(
        http,
        log,
        SheetRow.build_url(region, sheet_id),
        SheetRow,
        DETAIL_PAGE_SIZE,
        rows_modified_since=log_cursor,
        validation_context=ctx,
    ):
        if row.modifiedAt > horizon:
            continue
        yield row

        max_modified = max(max_modified, row.modifiedAt)

    if max_modified > log_cursor:
        yield max_modified


async def backfill_sheet_rows(
    http: HTTPSession,
    region: str,
    start_date: datetime,
    sheet_id: int,
    log: Logger,
    _page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[SheetRow | PageCursor, None]:
    """Backfill rows from the unfiltered detail-page walk.

              start_date       cutoff−1s  cutoff (whole second)
    ──────────────┼──────────────────┼──────────┼───▶ time (1s ticks)
                  │                  │          │
    emitted ──────[══════════════════]          │
                  │                  │          └─ first incremental tick
                  │                  └─ last backfilled second
                  └─ configured replication start

    """
    assert isinstance(cutoff, datetime)

    ctx = SheetIdValidationContext(sheet_id=sheet_id)

    async for row in _iter_detail_rows(
        http,
        log,
        SheetRow.build_url(region, sheet_id),
        SheetRow,
        DETAIL_PAGE_SIZE,
        validation_context=ctx,
    ):
        if row.modifiedAt >= cutoff:
            continue
        if row.modifiedAt < start_date:
            continue  # before the configured replication window

        yield row


# =============================================================================
# Reports cluster: `reports` (catalog metadata) and `report_rows` (row data)
# =============================================================================


async def snapshot_reports(
    http: HTTPSession, region: str, log: Logger
) -> AsyncGenerator[Report, None]:
    async for report in _iter_list_items(http, log, Report.build_url(region), Report):
        yield report


async def snapshot_report_rows(
    http: HTTPSession, region: str, log: Logger
) -> AsyncGenerator[ReportRow, None]:
    report_ids = [
        report.id
        async for report in _iter_list_items(
            http, log, Report.build_url(region), Report
        )
    ]

    for report_id in report_ids:
        ctx = ReportIdValidationContext(report_id=report_id)
        async for row in _iter_detail_rows(
            http,
            log,
            ReportRow.build_url(region, report_id),
            ReportRow,
            DETAIL_PAGE_SIZE,
            validation_context=ctx,
        ):
            yield row
