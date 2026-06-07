import xml.etree.ElementTree as ET
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

# Maximum date range for a single REST export job, enforced by Zuora.
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
    *,
    updated_after: datetime | None = None,
    updated_before: datetime | None = None,
) -> str:
    """Build an Export ZOQL query.

    The optional bounds form a half-open UpdatedDate window
    [updated_after, updated_before). Omitting both yields a full-table export.
    """
    query = f"SELECT {', '.join(fields)} FROM {object_name}"
    conditions: list[str] = []
    if updated_after is not None:
        conditions.append(f"UpdatedDate >= '{_format_dt_to_utc(updated_after)}'")
    if updated_before is not None:
        conditions.append(f"UpdatedDate < '{_format_dt_to_utc(updated_before)}'")
    if conditions:
        query = f"{query} WHERE {' AND '.join(conditions)} ORDER BY UpdatedDate"
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
    return [obj.name for obj in catalog.objects]


def _parse_catalog(response_bytes: bytes) -> DescribeCatalog:
    root = ET.fromstring(response_bytes)
    objects = [
        CatalogObject(name=name_el.text)
        for name_el in root.findall("./object/name")
        if name_el.text
    ]
    return DescribeCatalog(objects=objects)


async def describe_object(
    base_url: str,
    http: HTTPSession,
    log: Logger,
    object_name: str,
) -> list[str]:
    """Return the exportable field names for a Zuora object."""
    url = f"{base_url}/v1/describe/{object_name}"
    return _parse_describe_object(
        await http.request(log, url, headers=VERSION_HEADERS)
    ).exportable_field_names


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


async def fetch_changes(
    object_name: str,
    fields: list[str],
    manager: ExportManager,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZuoraDocument | datetime, None]:
    assert isinstance(log_cursor, datetime)
    now = (datetime.now(UTC) - LAG).replace(microsecond=0)
    if log_cursor >= now:
        return

    window_end = min(log_cursor + MAX_EXPORT_WINDOW, now)

    max_updated: datetime | None = None
    count = 0
    while True:
        query = build_query(
            object_name, fields, updated_after=log_cursor, updated_before=window_end
        )
        try:
            async for row in manager.export_rows(query):
                doc = ZuoraDocument.model_validate(row)
                # Zuora only supports filtering by whole seconds, so we only
                # checkpoint when the entire second advances.
                if (
                    max_updated is not None
                    and _second_floor(doc.UpdatedDate) > _second_floor(max_updated)
                    and count >= CHECKPOINT_INTERVAL
                ):
                    yield _second_floor(max_updated) + timedelta(seconds=1)
                    count = 0
                yield doc
                count += 1
                if max_updated is None or doc.UpdatedDate > max_updated:
                    max_updated = doc.UpdatedDate
            break
        except ExportTooLargeError as err:
            mid = _determine_middle(log_cursor, window_end)
            if not (log_cursor < mid < window_end):
                # Down to the smallest splittable window and still too large.
                # Name the object and window so the failure is actionable; the
                # underlying error only carries an opaque Zuora file id.
                raise ExportTooLargeError(
                    f"{object_name}: export of the smallest splittable window "
                    f"[{_format_dt_to_utc(log_cursor)}, {_format_dt_to_utc(window_end)}) "
                    f"still exceeds Zuora's size limit and cannot be narrowed further"
                ) from err
            window_end = mid

    if max_updated is not None:
        # Advance only to just past the last second that held data.
        yield _second_floor(max_updated) + timedelta(seconds=1)
    elif window_end < now:
        # A full-sized window that yielded nothing. Advance past all of it
        # rather than leaving the cursor stuck in the past.
        yield window_end


async def fetch_page(
    object_name: str,
    fields: list[str],
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

    max_updated: datetime | None = None
    count = 0
    while True:
        query = build_query(
            object_name, fields, updated_after=window_start, updated_before=window_end
        )
        try:
            async for row in manager.export_rows(query):
                doc = ZuoraDocument.model_validate(row)
                # Zuora only supports filtering by whole seconds, so we only
                # checkpoint when the entire second advances.
                if (
                    max_updated is not None
                    and _second_floor(doc.UpdatedDate) > _second_floor(max_updated)
                    and count >= CHECKPOINT_INTERVAL
                ):
                    yield (_second_floor(max_updated) + timedelta(seconds=1)).isoformat()
                    count = 0
                yield doc
                count += 1
                if max_updated is None or doc.UpdatedDate > max_updated:
                    max_updated = doc.UpdatedDate
            break
        except ExportTooLargeError as err:
            mid = _determine_middle(window_start, window_end)
            if not (window_start < mid < window_end):
                # Down to the smallest splittable window and still too large.
                # Name the object and window so the failure is actionable; the
                # underlying error only carries an opaque Zuora file id.
                raise ExportTooLargeError(
                    f"{object_name}: export of the smallest splittable window "
                    f"[{_format_dt_to_utc(window_start)}, {_format_dt_to_utc(window_end)}) "
                    f"still exceeds Zuora's size limit and cannot be narrowed further"
                ) from err
            window_end = mid

    if window_end < cutoff:
        yield window_end.isoformat()


async def fetch_snapshot(
    object_name: str,
    fields: list[str],
    manager: ExportManager,
    log: Logger,
) -> AsyncGenerator[BaseCSVRow, None]:
    """Full table export for objects that do not have an UpdatedDate field.

    Snapshot objects have no cursor, so they use BaseCSVRow (empty cells -> None,
    all fields inferred) rather than the incremental ZuoraDocument model.

    Note: without a time cursor there's no window to bisect, so a snapshot that
    exceeds Zuora's export size limit raises ExportTooLargeError and fails the
    binding. If that happens for a real object, we should paginate the export
    with Export ZOQL's LIMIT/OFFSET (first-N plus skip rows) to complete it in chunks.
    """
    query = build_query(object_name, fields)
    async for row in manager.export_rows(query):
        yield BaseCSVRow.model_validate(row)
