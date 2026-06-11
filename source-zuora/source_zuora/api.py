import asyncio
import json
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_csv_processor import IncrementalCSVProcessor

from .models import BasicCredentials, EndpointConfig, ZuoraDocument

# Matches Zuora's "There is no field named Foo." error message.
_INVALID_FIELD_RE = re.compile(r"There is no field named (\w+)\.")

# Per-process cache of fields confirmed unavailable for each object.
# Populated on first 400 per field; prevents re-trying bad fields on every sync cycle.
_unavailable_fields: dict[str, set[str]] = {}

# Maximum date range for a single REST export job (Zuora enforces 30 days
# for incremental queries via the export API).
MAX_EXPORT_DAYS = 30

# Seconds between export job status polls.
POLL_INTERVAL = 10

# Maximum number of polling attempts before giving up (~1 hour).
MAX_POLL_ATTEMPTS = 360

# Retries for transient Zuora export job failures (Failed/Cancelled status).
MAX_RETRIES = 3

# Zuora limits concurrent export jobs per tenant (typically 5).
# This semaphore keeps us well under that limit across all concurrent bindings.
_EXPORT_SEMAPHORE = asyncio.Semaphore(4)


def auth_headers(config: EndpointConfig) -> dict[str, str]:
    """Return extra HTTP headers for BASIC auth.
    OAuth2 tokens are injected automatically by the CDK's token_source.
    """
    if isinstance(config.credentials, BasicCredentials):
        return {
            "apiAccessKeyId": config.credentials.username,
            "apiSecretAccessKey": config.credentials.password,
        }
    return {}


def _fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


async def discover_object_names(
    base_url: str,
    http: HTTPSession,
    log: Logger,
    auth: dict[str, str],
) -> list[str]:
    """Return all object names available in this Zuora tenant via GET /v1/describe."""
    url = f"{base_url}/v1/describe"
    response_bytes = await http.request(log, url, headers=auth or None)
    root = ET.fromstring(response_bytes)
    return [el.text for el in root.findall("./object/name") if el.text]


async def describe_object(
    base_url: str,
    http: HTTPSession,
    log: Logger,
    object_name: str,
    auth: dict[str, str],
) -> tuple[list[str], bool]:
    """Return (selectable_fields, is_queryable) for a Zuora object.

    is_queryable reflects the <queryable> flag in the describe XML; objects
    with queryable=false cannot be used in ZOQL and cannot be exported.
    Defaults to True when the flag is absent.
    """
    url = f"{base_url}/v1/describe/{object_name}"
    response_bytes = await http.request(log, url, headers=auth or None)
    root = ET.fromstring(response_bytes)

    queryable_el = root.find("./queryable")
    is_queryable = queryable_el is None or queryable_el.text == "true"

    fields: list[str] = []
    for field_el in root.findall(".//field"):
        name_el = field_el.find("name")
        selectable_el = field_el.find("selectable")
        if (
            name_el is not None
            and name_el.text
            and selectable_el is not None
            and selectable_el.text == "true"
        ):
            fields.append(name_el.text)
    return fields, is_queryable


async def _run_export_job(
    base_url: str,
    http: HTTPSession,
    log: Logger,
    auth: dict[str, str],
    query: str,
) -> str:
    """Submit an export job, poll until complete, and return the file ID.

    Holds _EXPORT_SEMAPHORE for the full duration so at most 4 jobs run
    concurrently across all bindings, staying within Zuora's per-tenant limit.
    Retries up to MAX_RETRIES times on transient failures with exponential backoff.
    """
    for attempt in range(MAX_RETRIES):
        try:
            async with _EXPORT_SEMAPHORE:
                url = f"{base_url}/v1/object/export"
                payload = {"Format": "csv", "Query": query}
                submit_bytes = await http.request(
                    log, url, method="POST", json=payload, headers=auth or None
                )
                submit_resp = json.loads(submit_bytes)
                if not submit_resp.get("Success", False):
                    raise RuntimeError(
                        f"Export job submission failed for query: {query!r} — {submit_resp}"
                    )
                job_id = submit_resp["Id"]
                log.debug("created export job", {"job_id": job_id})

                poll_url = f"{base_url}/v1/object/export/{job_id}"
                for _ in range(MAX_POLL_ATTEMPTS):
                    response_bytes = await http.request(log, poll_url, headers=auth or None)
                    resp = json.loads(response_bytes)
                    status = resp.get("Status", "")
                    if status == "Completed":
                        return resp["FileId"]
                    if status in ("Cancelled", "Failed"):
                        raise RuntimeError(
                            f"Export job {job_id} {status}: "
                            f"{resp.get('StatusReason', 'no reason given')}"
                        )
                    await asyncio.sleep(POLL_INTERVAL)
                raise RuntimeError(
                    f"Export job {job_id} timed out after {MAX_POLL_ATTEMPTS * POLL_INTERVAL}s"
                )
        except Exception as exc:
            # 4xx errors are not transient — retrying won't help.
            if isinstance(exc, HTTPError) and 400 <= exc.code < 500:
                raise
            if attempt == MAX_RETRIES - 1:
                raise
            wait = 30 * (2**attempt)
            log.warning(
                "Export job failed, retrying",
                {"attempt": attempt + 1, "max": MAX_RETRIES, "wait_s": wait, "error": str(exc)},
            )
            await asyncio.sleep(wait)
    raise RuntimeError("unreachable")


def _active_fields_for(object_name: str, fields: list[str]) -> list[str]:
    """Return fields with any previously-cached unavailable fields removed."""
    known_bad = _unavailable_fields.get(object_name)
    if not known_bad:
        return list(fields)
    return [f for f in fields if f not in known_bad]


async def _run_job_filtering_fields(
    base_url: str,
    http: HTTPSession,
    log: Logger,
    auth: dict[str, str],
    object_name: str,
    active_fields: list[str],
    where_clause: str,
) -> str:
    """Build a ZOQL SELECT, run the export, and return the file ID.

    If Zuora rejects a field as unavailable, it is removed from active_fields
    (mutated in-place), added to the per-process cache so subsequent sync cycles
    skip it immediately, and the query is retried.
    """
    while True:
        query = f"SELECT {', '.join(active_fields)} FROM {object_name}"
        if where_clause:
            query = f"{query} {where_clause}"
        try:
            return await _run_export_job(base_url, http, log, auth, query)
        except Exception as exc:
            m = _INVALID_FIELD_RE.search(str(exc))
            if m and m.group(1) in active_fields:
                bad = m.group(1)
                log.warning("Removing unavailable field", {"object": object_name, "field": bad})
                active_fields.remove(bad)
                _unavailable_fields.setdefault(object_name, set()).add(bad)
                if not active_fields:
                    raise RuntimeError(f"No selectable fields remain for {object_name}") from exc
            else:
                raise


async def _stream_export_rows(
    base_url: str,
    http: HTTPSession,
    log: Logger,
    auth: dict[str, str],
    file_id: str,
) -> AsyncGenerator[dict, None]:
    url = f"{base_url}/v1/files/{file_id}"
    _resp_headers, body_factory = await http.request_stream(
        log, url, headers=auth or None
    )
    async for row in IncrementalCSVProcessor(body_factory()):
        # Zuora prefixes every CSV column with "ObjectName." — strip it so
        # downstream fields match the describe names (e.g. "Id", not "Account.Id").
        yield {k.split(".", 1)[-1]: v for k, v in row.items()}


async def fetch_changes(
    object_name: str,
    fields: list[str],
    base_url: str,
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZuoraDocument | LogCursor, None]:
    """Incrementally fetch records updated since log_cursor using time-windowed exports.

    Processes up to MAX_EXPORT_DAYS per iteration so the CDK can checkpoint
    progress without holding a single export job open for too long.
    """
    if not isinstance(log_cursor, datetime):
        raise TypeError(f"expected datetime log_cursor, got {type(log_cursor)}")
    auth = auth_headers(config)
    active_fields = _active_fields_for(object_name, fields)
    window_start = log_cursor
    now = datetime.now(UTC)

    while window_start < now:
        window_end = min(window_start + timedelta(days=MAX_EXPORT_DAYS), now)
        where = (
            f"WHERE UpdatedDate >= '{_fmt_dt(window_start)}' "
            f"AND UpdatedDate < '{_fmt_dt(window_end)}'"
        )
        file_id = await _run_job_filtering_fields(
            base_url, http, log, auth, object_name, active_fields, where
        )
        async for row in _stream_export_rows(base_url, http, log, auth, file_id):
            yield ZuoraDocument.model_validate(row)
        yield window_end
        window_start = window_end


async def fetch_page(
    object_name: str,
    fields: list[str],
    base_url: str,
    http: HTTPSession,
    config: EndpointConfig,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ZuoraDocument | PageCursor, None]:
    """Backfill records from start_date up to cutoff, processing one window per call.

    The PageCursor is the start datetime of the next window; None means start fresh.
    Yields documents for the current window, then the next window's start as PageCursor.
    Returns without yielding a PageCursor when the backfill is complete.
    """
    if not isinstance(cutoff, datetime):
        raise TypeError(f"expected datetime cutoff, got {type(cutoff)}")
    auth = auth_headers(config)
    active_fields = _active_fields_for(object_name, fields)

    window_start: datetime = page if isinstance(page, datetime) else start_date
    if window_start >= cutoff:
        return

    window_end = min(window_start + timedelta(days=MAX_EXPORT_DAYS), cutoff)
    where = (
        f"WHERE UpdatedDate >= '{_fmt_dt(window_start)}' "
        f"AND UpdatedDate < '{_fmt_dt(window_end)}'"
    )
    file_id = await _run_job_filtering_fields(
        base_url, http, log, auth, object_name, active_fields, where
    )
    async for row in _stream_export_rows(base_url, http, log, auth, file_id):
        yield ZuoraDocument.model_validate(row)

    if window_end < cutoff:
        yield window_end  # more windows remain


async def fetch_snapshot(
    object_name: str,
    fields: list[str],
    base_url: str,
    http: HTTPSession,
    config: EndpointConfig,
    log: Logger,
) -> AsyncGenerator[ZuoraDocument, None]:
    """Full table export for objects that do not have an UpdatedDate field."""
    auth = auth_headers(config)
    active_fields = _active_fields_for(object_name, fields)
    file_id = await _run_job_filtering_fields(
        base_url, http, log, auth, object_name, active_fields, ""
    )
    async for row in _stream_export_rows(base_url, http, log, auth, file_id):
        yield ZuoraDocument.model_validate(row)
