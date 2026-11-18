import asyncio
import math
from contextlib import aclosing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from aiohttp import ClientTimeout
from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_csv_processor import (
    CSVProcessingError,
    IncrementalCSVProcessor,
    IncrementalCSVRowProcessor,
)

from .models import (
    EPOCH,
    MIN_TIMESTAMP,
    NANOSECONDS_PER_SECOND,
    Bucket,
    FieldRow,
    Measurement,
    Point,
    PointTime,
    datetime_to_ns,
    parse_timestamp,
)

WINDOW_SIZE = timedelta(hours=1)
MAX_ROWS = 50_000
MAX_CONCURRENT_READS = 4
# Bound simultaneous window buffers across all bindings during catch-up.
WINDOW_READ_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_READS)
QUERY_TIMEOUT = ClientTimeout(total=None, sock_read=600, sock_connect=60)
METADATA_COLUMNS = {
    "_start", "_stop", "_time", "_measurement", "_field", "_value",
}


@dataclass
class FetchState:
    window_size: int = int(WINDOW_SIZE.total_seconds()) * NANOSECONDS_PER_SECOND
    last_caught_up_at: datetime | None = None
    next_start: int | None = None
    sweep_end: int | None = None
    # Dense catch-up reads window after window; only look ahead past a gap.
    last_window_empty: bool = True


class WindowTooLarge(Exception):
    pass


FLUX_ESCAPES = {"\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t"}


def flux_string(value: str) -> str:
    # Flux string literals accept only \n, \r, \t, \\, \", \${ and \xHH escapes.
    escaped = "".join(
        FLUX_ESCAPES.get(char)
        or (f"\\x{ord(char):02x}" if ord(char) < 0x20 else char)
        for char in value
    )
    return '"' + escaped.replace("${", "\\${") + '"'


async def flux_query(
    log: Logger,
    http: HTTPSession,
    url: str,
    org: str,
    headers: dict[str, str],
    query: str,
    **dialect: object,
):
    _, body = await http.request_stream(
        log,
        f"{url}/api/v2/query",
        method="POST",
        params={"org": org},
        headers={**headers, "Accept": "application/csv"},
        timeout=QUERY_TIMEOUT,
        json={
            "query": query,
            "type": "flux",
            "dialect": {"header": True, "annotations": [], **dialect},
        },
    )
    return body


async def list_buckets(
    log: Logger,
    http: HTTPSession,
    url: str,
    org: str,
    headers: dict[str, str],
) -> AsyncGenerator[Bucket, None]:
    # REST applies authorization after paging, so even an empty page can hide
    # readable buckets later in the store. Flux lists them without pagination.
    body = await flux_query(log, http, url, org, headers, "buckets()")
    async with aclosing(body()) as stream:
        async for bucket in IncrementalCSVProcessor(stream, Bucket):
            yield bucket


async def list_measurements(
    log: Logger,
    http: HTTPSession,
    url: str,
    org: str,
    headers: dict[str, str],
    bucket: str,
) -> list[str]:
    # The pipeline behind schema.measurements, inlined because older Flux releases
    # lack its start and stop parameters and discovery needs the full time range.
    query = (
        f"from(bucket: {flux_string(bucket)})\n"
        f"  |> range(start: time(v: {MIN_TIMESTAMP}), stop: time(v: 9223372036854775807))\n"
        '  |> keep(columns: ["_measurement"])\n'
        "  |> group()\n"
        '  |> distinct(column: "_measurement")'
    )
    body = await flux_query(log, http, url, org, headers, query)
    return sorted({row.value async for row in IncrementalCSVProcessor(body(), Measurement)})


def format_timestamp(value: int) -> str:
    seconds, nanoseconds = divmod(value, NANOSECONDS_PER_SECOND)
    base = (EPOCH + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{nanoseconds:09d}Z" if nanoseconds else f"{base}Z"


def parse_field_value(value: str, datatype: str) -> float | int | bool | str:
    if datatype == "string":
        return value
    if datatype == "boolean" and value in ("true", "false"):
        return value == "true"
    if datatype in ("long", "unsignedLong"):
        number = int(value)
        if datatype == "long" and -(2**63) <= number < 2**63:
            return number
        if datatype == "unsignedLong" and 0 <= number < 2**64:
            return number
    if datatype == "double":
        number = float(value)
        if math.isfinite(number):
            return number
    raise ValueError(f"Invalid InfluxDB field value for type {datatype!r}: {value!r}")


def series_key(measurement: str, tags: dict[str, str]) -> str:
    def escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ")

    def escape_tag(value: str) -> str:
        return escape(value).replace("=", "\\=")

    return ",".join([
        escape(measurement),
        *(f"{escape_tag(key)}={escape_tag(value)}" for key, value in sorted(tags.items())),
    ])


async def first_point_time(
    log: Logger,
    http: HTTPSession,
    url: str,
    org: str,
    headers: dict[str, str],
    bucket: str,
    measurement: str,
    start: int,
    stop: int,
) -> int | None:
    # Select within each series/field before combining timestamps, so mixed
    # field types never share a Flux table and only one timestamp is returned.
    query = (
        f"from(bucket: {flux_string(bucket)})\n"
        f"  |> range(start: time(v: {start}), stop: time(v: {stop}))\n"
        f'  |> filter(fn: (r) => r["_measurement"] == {flux_string(measurement)})\n'
        '  |> first()\n'
        '  |> keep(columns: ["_time"])\n'
        '  |> group()\n'
        '  |> sort(columns: ["_time"])\n'
        '  |> limit(n: 1)'
    )
    body = await flux_query(log, http, url, org, headers, query, dateTimeFormat="RFC3339Nano")

    first: int | None = None
    async for row in IncrementalCSVProcessor(body(), PointTime):
        timestamp = parse_timestamp(row.time)
        if first is not None or not start <= timestamp < stop:
            raise ValueError("InfluxDB returned an invalid first point timestamp")
        first = timestamp
    return first


async def read_points(
    log: Logger,
    http: HTTPSession,
    url: str,
    org: str,
    headers: dict[str, str],
    bucket: str,
    measurement: str,
    start: int,
    stop: int,
) -> list[Point]:
    query = (
        f"from(bucket: {flux_string(bucket)})\n"
        f"  |> range(start: time(v: {start}), stop: time(v: {stop}))\n"
        f'  |> filter(fn: (r) => r["_measurement"] == {flux_string(measurement)})'
    )
    body = await flux_query(
        log, http, url, org, headers, query,
        annotations=["datatype", "group", "default"], dateTimeFormat="RFC3339Nano",
    )

    columns: list[str] = []
    annotations: dict[str, list[str]] = {}
    points: dict[tuple[int, str, tuple[tuple[str, str], ...]], Point] = {}
    row_count = 0

    # Flux returns one table per field and series, with a schema for each table.
    # Keep a window together so fields are complete and points are globally ordered.
    async with aclosing(body()) as stream:
        async for cells in IncrementalCSVRowProcessor(stream):
            if cells[0].startswith("#"):
                if columns:
                    columns = []
                    annotations = {}
                if cells[0] not in ("#datatype", "#group", "#default"):
                    raise CSVProcessingError(f"Unexpected InfluxDB CSV annotation: {cells[0]}")
                annotations[cells[0]] = cells
                continue

            if not columns or cells[:3] == ["", "error", "reference"]:
                columns = cells
                if columns == ["", "error", "reference"]:
                    continue
                if columns[:3] != ["", "result", "table"]:
                    raise CSVProcessingError("Missing InfluxDB CSV protocol columns")
                if len(set(columns[3:])) != len(columns[3:]):
                    raise CSVProcessingError("Duplicate columns in InfluxDB CSV header")
                if set(annotations) != {"#datatype", "#group", "#default"} or any(
                    len(values) != len(columns) for values in annotations.values()
                ):
                    raise CSVProcessingError("Missing or mismatched InfluxDB CSV annotations")
                if any(value not in ("true", "false") for value in annotations["#group"][1:]):
                    raise CSVProcessingError("Invalid InfluxDB CSV group annotation")
                continue

            if len(cells) != len(columns):
                raise CSVProcessingError(
                    f"InfluxDB CSV row has {len(cells)} columns but {len(columns)} were expected"
                )
            if cells == columns:
                continue
            if columns == ["", "error", "reference"]:
                raise RuntimeError(f"InfluxDB query failed: {dict(zip(columns, cells))['error']}")

            row_count += 1
            # A single timestamp cannot be split across cursor checkpoints.
            if row_count > MAX_ROWS and stop - start > 1:
                raise WindowTooLarge

            values = {
                column: cell or default
                for column, cell, default in zip(columns[3:], cells[3:], annotations["#default"][3:], strict=True)
            }
            row = FieldRow.model_validate(values)
            timestamp = parse_timestamp(row.time)
            if row.measurement != measurement or not start <= timestamp < stop:
                raise ValueError("InfluxDB returned a point outside the requested measurement or window")
            tags = {
                column: values[column]
                for column, group in zip(columns[3:], annotations["#group"][3:], strict=True)
                if group == "true" and column not in METADATA_COLUMNS and values[column] != ""
            }
            key = (timestamp, row.measurement, tuple(sorted(tags.items())))
            if key not in points:
                points[key] = Point(
                    time=format_timestamp(timestamp),
                    measurement=row.measurement,
                    tags=tags,
                    fields={},
                    series=series_key(row.measurement, tags),
                )
            points[key].fields[row.field] = parse_field_value(
                row.value, annotations["#datatype"][columns.index("_value", 3)]
            )

    if annotations and not columns:
        raise CSVProcessingError("InfluxDB CSV ended before its table header")
    if columns == ["", "error", "reference"]:
        raise CSVProcessingError("InfluxDB CSV ended before its error message")

    return [points[key] for key in sorted(points)]


async def read_window(
    log: Logger,
    http: HTTPSession,
    url: str,
    org: str,
    headers: dict[str, str],
    bucket: str,
    measurement: str,
    start: int,
    end: int,
    fetch_state: FetchState,
) -> tuple[int, list[Point]]:
    window_size = fetch_state.window_size
    # A timestamp lookup saves requests when a long history is mostly empty.
    # Short spans, and spans right after a window with points, avoid it.
    if fetch_state.last_window_empty and end - start > 2 * window_size:
        async with WINDOW_READ_SEMAPHORE:
            first = await first_point_time(log, http, url, org, headers, bucket, measurement, start, end)
        if first is None:
            return end, []
        start = first

    stop = min(start + window_size, end)
    while True:
        try:
            async with WINDOW_READ_SEMAPHORE:
                points = await read_points(log, http, url, org, headers, bucket, measurement, start, stop)
            if sum(len(point.fields) for point in points) < MAX_ROWS // 2:
                fetch_state.window_size = min(
                    int(WINDOW_SIZE.total_seconds()) * NANOSECONDS_PER_SECOND,
                    fetch_state.window_size * 2,
                )
            fetch_state.last_window_empty = not points
            return stop, points
        except WindowTooLarge:
            fetch_state.window_size = max(1, (stop - start) // 2)
            stop = start + fetch_state.window_size


async def fetch_points(
    http: HTTPSession,
    url: str,
    org: str,
    headers: dict[str, str],
    bucket: str,
    measurement: str,
    lag_seconds: int,
    log: Logger,
    log_cursor: LogCursor,
    *,
    fetch_state: FetchState | None = None,
    interval: timedelta = timedelta(minutes=5),
    lookback_hours: int = 0,
    start_date: str | None = None,
) -> AsyncGenerator[Point | LogCursor, None]:
    """Capture a complete window after the newest captured point.

    Flux range is start-inclusive and stop-exclusive, so reads start at
    cursor + 1ns and stop at most at now - lag. Only captured point times
    advance the cursor; empty windows must not skip later writes.

    With lookback enabled, the start is cursor - lookback, bounded by an
    explicit start date. Each poll drains that range once, including the
    cursor; replays merge by series and time, and replay checkpoints never
    move the newest captured point backward.
    """
    assert isinstance(log_cursor, tuple) and isinstance(log_cursor[0], str)
    if fetch_state is None:
        fetch_state = FetchState()
    if (
        fetch_state.last_caught_up_at is not None
        and datetime.now(tz=UTC) - fetch_state.last_caught_up_at < interval
    ):
        return

    cursor_ns = parse_timestamp(log_cursor[0])
    if lookback_hours and fetch_state.next_start is not None:
        start = fetch_state.next_start
        assert fetch_state.sweep_end is not None
        end = fetch_state.sweep_end
    else:
        start = cursor_ns + 1
        if lookback_hours:
            # Increasing an explicit start date must not skip uncaptured history.
            floor = min(parse_timestamp(start_date), start) if start_date else MIN_TIMESTAMP
            start = max(cursor_ns - lookback_hours * 3600 * NANOSECONDS_PER_SECOND, floor)
        start = max(start, MIN_TIMESTAMP)
        end = datetime_to_ns(datetime.now(tz=UTC) - timedelta(seconds=lag_seconds))

    while start < end:
        stop, points = await read_window(
            log, http, url, org, headers, bucket, measurement, start, end, fetch_state
        )
        if points:
            if lookback_hours:
                fetch_state.next_start = stop
                fetch_state.sweep_end = end
            if stop >= end:
                fetch_state.next_start = fetch_state.sweep_end = None
                fetch_state.last_caught_up_at = datetime.now(tz=UTC)
            for point in points:
                yield point

            # A string tuple preserves nanoseconds (and pre-epoch timestamps)
            # through the CDK's LogCursor JSON serialization.
            cursor = (format_timestamp(max(cursor_ns, parse_timestamp(points[-1].time))),)
            # The CDK may wait on other bindings while committing the cursor.
            del points
            yield cursor
            return

        start = stop

    fetch_state.next_start = fetch_state.sweep_end = None
    fetch_state.last_caught_up_at = datetime.now(tz=UTC)
