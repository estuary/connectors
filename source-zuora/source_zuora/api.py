import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_csv_processor import BaseCSVRow

from .export_manager import ExportManager, ExportTooLargeError
from .models import (
    CatalogObject,
    DescribeCatalog,
    DescribeField,
    DescribeObject,
    ZuoraDocument,
)
from .shared import VERSION_HEADERS

# Maximum date range for a single export job. Zuora's legacy export API
# enforced 30 days; the same cap is kept under AQuA so no single job grows
# unboundedly large and checkpoints stay reasonably frequent.
MAX_EXPORT_WINDOW = timedelta(days=30)

# Hold the leading edge this far behind real time. Zuora's export index is
# eventually consistent, so a record can become visible with an UpdatedDate a
# little in the past. Staying this far back gives such records time to appear
# before the cursor advances past their timestamp.
LAG = timedelta(minutes=5)

# Emit an intermediate checkpoint after roughly this many documents.
CHECKPOINT_INTERVAL = 10_000


def _format_dt_to_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _determine_middle(start: datetime, end: datetime) -> datetime:
    return (start + (end - start) / 2).replace(microsecond=0)


def _second_floor(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def build_query(
    object_name: str,
    fields: list[str],
    cursor_field: str = "UpdatedDate",
    *,
    after: datetime | None = None,
    before: datetime | None = None,
) -> str:
    """Build an Export ZOQL query.

    The optional bounds form a half-open cursor_field window [after, before),
    ordered by cursor_field so checkpoint resume is dupe-free. Omitting both
    yields an unordered full-table export.
    """
    query = f"SELECT {', '.join(fields)} FROM {object_name}"
    conditions: list[str] = []
    if after is not None:
        conditions.append(f"{cursor_field} >= '{_format_dt_to_utc(after)}'")
    if before is not None:
        conditions.append(f"{cursor_field} < '{_format_dt_to_utc(before)}'")
    if conditions:
        query = f"{query} WHERE {' AND '.join(conditions)} ORDER BY {cursor_field}"
    return query


async def discover_object_names(
    base_url: str,
    http: HTTPSession,
    log: Logger,
) -> list[str]:
    """Return all object names available in this Zuora tenant via GET /v1/describe."""
    url = f"{base_url}/v1/describe"
    catalog = _parse_catalog(
        await http.request(log, url, headers=VERSION_HEADERS)
    )
    names = [obj.name for obj in catalog.objects]
    log.debug("discovered objects", {"count": len(names), "objects": names})
    return names


def _parse_catalog(response_bytes: bytes) -> DescribeCatalog:
    root = ET.fromstring(response_bytes)
    objects = [
        CatalogObject(name=name_el.text)
        for name_el in root.findall("./object/name")
        if name_el.text
    ]
    return DescribeCatalog(objects=objects)


async def fetch_object_fields(
    base_url: str,
    http: HTTPSession,
    log: Logger,
    object_name: str,
) -> list[str]:
    """Return the exportable field names for a Zuora object."""
    url = f"{base_url}/v1/describe/{object_name}"
    described = _parse_describe_object(
        await http.request(log, url, headers=VERSION_HEADERS)
    )
    # Field availability in exports is tenant-dependent and describe has been
    # seen disagreeing with what the export engine actually accepts (the reason
    # this connector uses AQuA over the legacy /v1/object/export API), so
    # record exactly how each field was classified and why.
    log.debug(
        "described object",
        {
            "object": object_name,
            "exportable_fields": described.exportable_field_names,
            "excluded_fields": {
                f.name: {"selectable": f.selectable, "contexts": f.contexts}
                for f in described.fields
                if not f.is_exportable
            },
        },
    )
    return described.exportable_field_names


def _parse_describe_object(response_bytes: bytes) -> DescribeObject:
    """Parse a GET /v1/describe/{object} XML body into a DescribeObject."""
    root = ET.fromstring(response_bytes)
    name_el = root.find("./name")

    fields: list[DescribeField] = []
    for field_el in root.findall("./fields/field"):
        field_name_el = field_el.find("name")
        if field_name_el is None or not field_name_el.text:
            continue
        selectable_el = field_el.find("selectable")
        fields.append(
            DescribeField(
                name=field_name_el.text,
                selectable=selectable_el is not None and selectable_el.text == "true",
                contexts=[
                    c.text for c in field_el.findall("./contexts/context") if c.text
                ],
            )
        )

    return DescribeObject(
        name=name_el.text if name_el is not None and name_el.text else "",
        fields=fields,
    )


@dataclass
class _WindowEnd:
    """Terminal item _export_window yields once, after all documents and
    intermediate checkpoints: the largest cursor value seen in the window (None
    if it was empty) and the effective end of the window actually covered (the
    original window_end, or a smaller value if bisection shrank it). Each caller
    turns this into its own final cursor, so the incremental and backfill
    end-of-window advance rules stay distinct while sharing one export engine.
    """
    max_cursor: datetime | None
    covered_end: datetime


async def _export_window(
    object_name: str,
    fields: list[str],
    model: type[ZuoraDocument],
    manager: ExportManager,
    window_start: datetime,
    window_end: datetime,
    log: Logger,
) -> AsyncGenerator[ZuoraDocument | datetime | _WindowEnd, None]:
    """Export the half-open cursor window [window_start, window_end) once,
    bisecting on ExportTooLargeError until a sub-window fits, and stream its rows.

    Yields, in order: each row as a `model` instance; intermediate `datetime`
    checkpoints (a whole-second boundary just past the last fully-elapsed second,
    emitted at most every CHECKPOINT_INTERVAL docs — Zuora filters at second
    granularity, so a checkpoint may never split a second or resume would re-read
    inside it); and finally exactly one `_WindowEnd`. On the smallest splittable
    window still overflowing, raises ExportTooLargeError naming the object and
    window rather than the opaque Zuora file id.
    """
    max_cursor: datetime | None = None
    # count drives intermediate checkpoints and resets at each one; total is the
    # window's whole-lifetime document count, kept separately for the summary log.
    count = 0
    total = 0
    while True:
        query = build_query(
            object_name, fields, model.CURSOR_FIELD,
            after=window_start, before=window_end,
        )
        try:
            async for row in manager.export_rows(query):
                doc = model.model_validate(row)
                cursor = doc.get_cursor()
                if (
                    max_cursor is not None
                    and _second_floor(cursor) > _second_floor(max_cursor)
                    and count >= CHECKPOINT_INTERVAL
                ):
                    yield _second_floor(max_cursor) + timedelta(seconds=1)
                    count = 0
                yield doc
                count += 1
                total += 1
                if max_cursor is None or cursor > max_cursor:
                    max_cursor = cursor
            break
        except ExportTooLargeError as err:
            mid = _determine_middle(window_start, window_end)
            if not (window_start < mid < window_end):
                raise ExportTooLargeError(
                    f"{object_name}: export of the smallest splittable window "
                    f"[{_format_dt_to_utc(window_start)}, {_format_dt_to_utc(window_end)}) "
                    f"still exceeds Zuora's size limit and cannot be narrowed further"
                ) from err
            log.debug(
                "export exceeded size limit, bisecting window",
                {
                    "object": object_name,
                    "window_start": _format_dt_to_utc(window_start),
                    "window_end": _format_dt_to_utc(window_end),
                    "new_window_end": _format_dt_to_utc(mid),
                },
            )
            window_end = mid

    log.debug(
        "export window complete",
        {
            "object": object_name,
            "window_start": _format_dt_to_utc(window_start),
            "covered_end": _format_dt_to_utc(window_end),
            "docs": total,
            "max_cursor": max_cursor.isoformat() if max_cursor is not None else None,
        },
    )
    yield _WindowEnd(max_cursor=max_cursor, covered_end=window_end)


async def fetch_changes(
    object_name: str,
    fields: list[str],
    model: type[ZuoraDocument],
    manager: ExportManager,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZuoraDocument | datetime, None]:
    assert isinstance(log_cursor, datetime)
    now = (datetime.now(UTC) - LAG).replace(microsecond=0)
    if log_cursor >= now:
        return

    window_end = min(log_cursor + MAX_EXPORT_WINDOW, now)

    async for item in _export_window(
        object_name, fields, model, manager, log_cursor, window_end, log
    ):
        if not isinstance(item, _WindowEnd):
            yield item  # a document or an intermediate datetime checkpoint
        elif item.max_cursor is not None:
            # Advance only to just past the last second that held data.
            new_cursor = _second_floor(item.max_cursor) + timedelta(seconds=1)
            log.debug(
                "advancing cursor just past the last second holding data",
                {"object": object_name, "cursor": _format_dt_to_utc(new_cursor)},
            )
            yield new_cursor
        elif item.covered_end < now:
            # A full-sized window that yielded nothing. Advance past all of it
            # rather than leaving the cursor stuck in the past.
            log.debug(
                "empty window, advancing cursor to its end",
                {"object": object_name, "cursor": _format_dt_to_utc(item.covered_end)},
            )
            yield item.covered_end
        else:
            log.debug(
                "empty window at the leading edge, cursor unchanged",
                {"object": object_name, "cursor": _format_dt_to_utc(log_cursor)},
            )


async def fetch_page(
    object_name: str,
    fields: list[str],
    model: type[ZuoraDocument],
    manager: ExportManager,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ZuoraDocument | str, None]:
    assert isinstance(cutoff, datetime)

    if page is None:
        window_start = start_date
    else:
        assert isinstance(page, str)
        window_start = datetime.fromisoformat(page)

    if window_start >= cutoff:
        return

    window_end = min(window_start + MAX_EXPORT_WINDOW, cutoff)

    async for item in _export_window(
        object_name, fields, model, manager, window_start, window_end, log
    ):
        if isinstance(item, _WindowEnd):
            # A non-final window ends on a PageCursor to resume from; the final
            # window (covered_end == cutoff) ends on documents so backfill
            # completes without emitting a further page.
            if item.covered_end < cutoff:
                log.debug(
                    "backfill window complete, resuming from next page cursor",
                    {"object": object_name, "next_page": item.covered_end.isoformat()},
                )
                yield item.covered_end.isoformat()
            else:
                log.debug("backfill reached cutoff", {"object": object_name})
        elif isinstance(item, datetime):
            yield item.isoformat()  # PageCursors are isoformat strings
        else:
            yield item


async def fetch_snapshot(
    object_name: str,
    fields: list[str],
    manager: ExportManager,
    log: Logger,
) -> AsyncGenerator[BaseCSVRow, None]:
    """Full table export for objects with no usable incremental cursor field.

    Snapshot objects have no cursor, so they use BaseCSVRow (empty cells -> None,
    all fields inferred) rather than an incremental ZuoraDocument subclass.

    Note: without a time cursor there's no window to bisect, so a snapshot that
    exceeds Zuora's export size limit raises ExportTooLargeError and fails the
    binding. If that happens for a real object, we should paginate the export
    with Export ZOQL's LIMIT/OFFSET (first-N plus skip rows) to complete it in chunks.
    """
    query = build_query(object_name, fields)
    async for row in manager.export_rows(query):
        yield BaseCSVRow.model_validate(row)
