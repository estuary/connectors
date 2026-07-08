from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel, JsonValue

from .models import (
    DetailPageMeta,
    ListPageMeta,
    RawRow,
    Report,
    ReportIdValidationContext,
    ReportRow,
    ReportSummary,
    Sheet,
    SheetIdValidationContext,
    SheetRow,
    SheetSummary,
)

REGION_BASE_URLS = {
    "us": "https://api.smartsheet.com/2.0",
    "eu": "https://api.smartsheet.eu/2.0",
    "gov": "https://api.smartsheetgov.com/2.0",
    "au": "https://api.smartsheet.au/2.0",
}


def base_url(region: str) -> str:
    return REGION_BASE_URLS[region]


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
    region: str,
    log: Logger,
    path: str,
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

    _, body = await http.request_stream(
        log, f"{base_url(region)}/{path}", params=params
    )
    return IncrementalJsonProcessor(body(), "data.item", item_cls, ListPageMeta)


async def _iter_list_items[ItemT: BaseModel](
    http: HTTPSession,
    region: str,
    log: Logger,
    path: str,
    item_cls: type[ItemT],
    modified_since: datetime | None = None,
) -> AsyncGenerator[ItemT, None]:
    page = 1

    while True:
        processor = await _stream_list_page(
            http, region, log, path, item_cls, page, modified_since
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
        async for summary in _iter_list_items(http, region, log, "sheets", SheetSummary)
    ]


# =============================================================================
# Detail endpoints — `GET /sheets/{id}` and `GET /reports/{id}` share one
# envelope shape and one positional-pagination contract, so a single
# parametrized stream/iterate pair serves both. Rows under `rows[]` are
# streamed incrementally; the entity's metadata envelope arrives as the
# processor's remainder (`DetailPageMeta`). Each pair backs its whole
# cluster: `sheets`/`reports` make one cheap pageSize=1 call and keep only
# the remainder, `sheet_rows`/`report_rows` walk the full row pagination.
# =============================================================================

# The docs' "Limitations" guide caps detail reads at 10,000 rows/request;
# live-confirmed not to 4xx one past it on both endpoints (the `pageSize
# 10001 over-limit probe` requests). Whether a silent clamp exists below the
# cap is unobservable on the seeded data (the probes' entities hold fewer
# rows than a page)
DETAIL_PAGE_SIZE = 10_000


async def _stream_detail_page[RowT: BaseModel](
    http: HTTPSession,
    region: str,
    log: Logger,
    path: str,
    entity_id: int,
    row_cls: type[RowT],
    page: int,
    page_size: int,
    rows_modified_since: datetime | None = None,
    validation_context: object | None = None,
) -> IncrementalJsonProcessor[RowT, DetailPageMeta]:
    # `rows_modified_since` is only ever passed for sheets: `GET /reports/{id}`
    # has no equivalent row filter.
    params: dict[str, JsonValue] = {"page": page, "pageSize": page_size}
    if rows_modified_since is not None:
        params["rowsModifiedSince"] = _format_rfc3339(rows_modified_since)

    _, body = await http.request_stream(
        log, f"{base_url(region)}/{path}/{entity_id}", params=params
    )
    return IncrementalJsonProcessor(
        body(), "rows.item", row_cls, DetailPageMeta, validation_context
    )


async def _iter_detail_rows[RowT: BaseModel](
    http: HTTPSession,
    region: str,
    log: Logger,
    path: str,
    entity_id: int,
    row_cls: type[RowT],
    page_size: int,
    rows_modified_since: datetime | None = None,
    validation_context: object | None = None,
) -> AsyncGenerator[RowT, None]:
    """Paginate a detail endpoint by row position, walking until every row
    has been served.

    Unlike the list endpoints, the detail endpoints silently CLAMP to the
    last valid page instead of erroring or returning empty once `page` goes
    past the entity's real page count, and carry no `pageNumber`/`totalPages`
    field to detect the clamp. The loop bound is therefore computed
    client-side from the remainder's `totalRowCount` after each page drains.

    An empty page ends the walk ONLY when no `rows_modified_since` filter is
    set: pagination is applied before the filter, so a filtered page can
    legitimately be empty mid-walk while later pages still hold matching
    rows. `totalRowCount` is always the entity's real, unfiltered total, so
    it's a correct loop bound whether or not a filter is set.
    """
    page = 1
    while True:
        processor = await _stream_detail_page(
            http,
            region,
            log,
            path,
            entity_id,
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
# (5s, 65s] on a single live measurement (`bruno/Seeding/D2 - Recheck List
# Sheets modifiedSince after edit`), so treat that bound as indicative and
# trail by a comfortably larger window.
LIST_INDEX_LAG_ALLOWANCE = timedelta(minutes=5)


async def _fetch_sheet_metadata(
    http: HTTPSession, region: str, sheet_id: int, log: Logger
) -> Sheet:
    # pageSize=1 is the cheapest call that still returns the full metadata
    # envelope — the one embedded row is drained and discarded; only the
    # remainder is kept.
    processor = await _stream_detail_page(
        http, region, log, "sheets", sheet_id, RawRow, page=1, page_size=1
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
                   │       │             └─ next cursor = min(max modifiedAt
                   │       │                emitted, horizon): the list
                   │       │                index publishes edits late
                   │       │                (LIST_INDEX_LAG_ALLOWANCE), so
                   │       │                the cursor never enters the
                   │       │                window it may not cover yet;
                   │       │                docs in (horizon, poll_start]
                   │       │                re-emit next poll — duplicate
                   │       │                upserts, never skips
                   │       └─ queried bound: floor(cursor) + 1s.
                   │          `modifiedSince` is >=-inclusive at exact
                   │          equality (`bruno/Sheets/List Sheets
                   │          (modifiedSince equality probe)`), so the
                   │          request alone yields exactly the ticks after
                   │          the cursor — no client-side skip; NOT the
                   │          same semantics as `rowsModifiedSince`
                   └─ excluding the cursor's own second is safe: see below

    Docs stamped at or before the cursor's second were already emitted by
    the poll that set it: the cursor never exceeds horizon, so anything in
    that second — even an index-lagged edit — had published by
    second + allowance <= that poll's poll_start, and was seen then.
    """
    assert isinstance(log_cursor, datetime)
    poll_start = datetime.now(tz=UTC)
    since = log_cursor.replace(microsecond=0) + timedelta(seconds=1)
    max_modified = log_cursor
    async for summary in _iter_list_items(
        http, region, log, "sheets", SheetSummary, modified_since=since
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
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Sheet | PageCursor, None]:
    """Backfill sheet metadata from the unfiltered list walk.

              start_date       cutoff−1s  cutoff (whole second)
    ──────────────┼──────────────────┼──────────┼───▶ time (1s ticks)
                  │                  │          │
    emitted ──────[══════════════════]          │
                  │                  │          └─ first incremental tick:
                  │                  │             fetch_sheets' cursor
                  │                  │             seeds at cutoff − 1s
                  │                  └─ last backfilled second (client-side
                  │                     `>= cutoff` skip — the bare list
                  │                     has no server-side time filter)
                  └─ configured replication start; older sheets skipped

    Resume is positional (`page` number): a sheet deleted mid-backfill
    renumbers everything after it, so one innocent sheet is skipped on
    resume — the API offers no value-ordered pagination to key a watermark
    on. Tolerated per House rule 9's carve-out: the walk is short (100
    sheets/page, usually one in-memory run), and a skipped sheet's
    metadata self-heals on its next edit via the incremental task.
    """
    assert isinstance(cutoff, datetime)
    page_num = 1 if page is None else page
    assert isinstance(page_num, int)

    processor = await _stream_list_page(
        http, region, log, "sheets", SheetSummary, page_num
    )
    async for summary in processor:
        if summary.modifiedAt >= cutoff:
            continue  # already covered by incremental once it starts
        if summary.modifiedAt < start_date:
            continue  # before the configured replication window
        yield await _fetch_sheet_metadata(http, region, summary.id, log)
    if page_num < processor.get_remainder().totalPages:
        yield page_num + 1


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
    """Incremental row fetch, windowed by `rowsModifiedSince`.

                  cursor  cursor+1s     horizon = last elapsed second
    ───────────────────┼───────┼─────────────────┼────▶ time (1s ticks)
                       │       │                 │
    rowsModifiedSince ─(═══════╪═════════════════╪════▶
    emitted ───────────┼───────[═════════════════]
                       │       │                 └─ the present second may
                       │       │                    still take edits; rows
                       │       │                    stamped past horizon
                       │       │                    are skipped and served
                       │       │                    by the next poll — the
                       │       │                    one remaining
                       │       │                    client-side skip (no
                       │       │                    upper-bound param
                       │       │                    exists)
                       │       └─ first emitted second
                       └─ queried bound: the cursor itself.
                          `rowsModifiedSince` is strict `>` at exact
                          equality (`bruno/Sheets/Get Sheet
                          (rowsModifiedSince equality probe)`), so the
                          request alone yields exactly the ticks after the
                          cursor; NOT the same semantics as the list's
                          `modifiedSince`

    The detail endpoint publishes edits immediately (no list-index lag —
    `bruno/Seeding/D2`), and an elapsed second is final because rows stamp
    their own "now", so the cursor advances straight to the newest emitted
    `modifiedAt` with no lag allowance and no re-emission.

    Two blind spots:

    - Blank/never-cell-written rows are invisible to the filter forever
      (`bruno/Sheets/Get Sheet (rowsModifiedSince narrows, blank-row gap)`);
      only a periodic full re-backfill per sheet closes this, which the
      `sheet_rows` resource schedules daily.
    - Row deletions leave zero trace in the API — no tombstone, no marker,
      `totalRowCount` silently decrements (`bruno/Seeding/E3`) — so neither
      this fetcher nor a backfill can observe them.
    """
    assert isinstance(log_cursor, datetime)
    horizon = datetime.now(tz=UTC).replace(microsecond=0) - timedelta(seconds=1)
    if horizon <= log_cursor:
        return
    max_modified = log_cursor
    ctx = SheetIdValidationContext(sheet_id=sheet_id)
    async for row in _iter_detail_rows(
        http,
        region,
        log,
        "sheets",
        sheet_id,
        RawRow,
        DETAIL_PAGE_SIZE,
        rows_modified_since=log_cursor,
    ):
        if row.modifiedAt > horizon:
            continue
        yield SheetRow.model_validate(row.model_dump(mode="json"), context=ctx)
        max_modified = max(max_modified, row.modifiedAt)
    if max_modified > log_cursor:
        yield max_modified


async def backfill_sheet_rows(
    http: HTTPSession,
    region: str,
    start_date: datetime,
    sheet_id: int,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[SheetRow | PageCursor, None]:
    """Backfill rows from the unfiltered detail-page walk.

              start_date       cutoff−1s  cutoff (whole second)
    ──────────────┼──────────────────┼──────────┼───▶ time (1s ticks)
                  │                  │          │
    emitted ──────[══════════════════]          │
                  │                  │          └─ first incremental tick:
                  │                  │             fetch_sheet_rows' cursor
                  │                  │             seeds at cutoff − 1s
                  │                  └─ last backfilled second (client-side
                  │                     `>= cutoff` skip — the unfiltered
                  │                     detail walk has no time bound;
                  │                     pages are bounded by totalRowCount,
                  │                     see `_iter_detail_rows`)
                  └─ configured replication start; older rows skipped

    Resume is positional (`page` number): a row deleted mid-backfill
    renumbers everything after it, so one innocent row is skipped on
    resume — the API offers no value-ordered pagination to key a watermark
    on. Tolerated per House rule 9's carve-out: the daily scheduled
    re-backfill bounds the exposure to 24h (deletions themselves are
    already invisible outside that same re-read, see fetch_sheet_rows).
    """
    assert isinstance(cutoff, datetime)
    page_num = 1 if page is None else page
    assert isinstance(page_num, int)

    processor = await _stream_detail_page(
        http, region, log, "sheets", sheet_id, RawRow, page_num, DETAIL_PAGE_SIZE
    )
    ctx = SheetIdValidationContext(sheet_id=sheet_id)
    async for row in processor:
        if row.modifiedAt >= cutoff:
            continue
        if row.modifiedAt < start_date:
            continue  # before the configured replication window
        yield SheetRow.model_validate(row.model_dump(mode="json"), context=ctx)
    if page_num * DETAIL_PAGE_SIZE < processor.get_remainder().totalRowCount:
        yield page_num + 1


# =============================================================================
# Reports cluster: `reports` (catalog metadata) and `report_rows` (row data)
#
# Both streams are Snapshot replication and share the per-report detail
# stream/pagination primitive above. This is a simplified variant of the
# child-entities "snapshot-child" shortcut (source-ashby's
# `snapshot_child_entity`, source-ashby/source_ashby/api.py:110-131):
# Smartsheet's single `GET /reports/{id}` call returns both the parent
# metadata and the first page of child rows in the same response, so there's
# no separate parent-detail call to make.
# =============================================================================


async def _list_report_ids(http: HTTPSession, region: str, log: Logger) -> list[int]:
    # Drain the parent (report) list into bare ids before iterating into
    # per-report detail calls, per child-entities' drain-parents-first
    # guidance -- report list objects carry no filter field we trust (see
    # Classification), so there's nothing else worth holding onto.
    return [
        summary.id
        async for summary in _iter_list_items(
            http, region, log, "reports", ReportSummary
        )
    ]


# =============================================================================
# `reports` stream (metadata)
# =============================================================================


async def snapshot_reports(
    http: HTTPSession, region: str, log: Logger
) -> AsyncGenerator[Report, None]:
    for report_id in await _list_report_ids(http, region, log):
        # pageSize=1 is the cheapest call that still returns the full
        # metadata envelope -- the one embedded row is drained and discarded,
        # only the remainder is kept. `report_rows` independently re-fetches
        # page 1 at the real page size; sharing the fetch across these two
        # independent snapshot bindings isn't a pattern used elsewhere in
        # this codebase and isn't worth the complexity at expected report
        # counts.
        processor = await _stream_detail_page(
            http, region, log, "reports", report_id, ReportRow, page=1, page_size=1
        )
        async for _ in processor:
            pass
        yield Report.from_detail(processor.get_remainder())


# =============================================================================
# `report_rows` stream (row data)
# =============================================================================


async def snapshot_report_rows(
    http: HTTPSession, region: str, log: Logger
) -> AsyncGenerator[ReportRow, None]:
    for report_id in await _list_report_ids(http, region, log):
        ctx = ReportIdValidationContext(report_id=report_id)
        async for row in _iter_detail_rows(
            http,
            region,
            log,
            "reports",
            report_id,
            ReportRow,
            DETAIL_PAGE_SIZE,
            validation_context=ctx,
        ):
            yield row
