"""
Reads Reports API (2021-06-30) reports: lists them, requests them and waits
for Amazon to finish them, and streams a DONE report's document as rows.
"""

import codecs
import csv
import zlib
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from email.message import Message
from logging import Logger
from typing import Any, AsyncGenerator, AsyncIterator

from estuary_cdk.http import Headers, HTTPError
from estuary_cdk.incremental_csv_processor import CSVConfig, IncrementalCSVProcessor
from pydantic import ValidationError

from .client import Operation, Priority, SPAPIClient
from .models import (
    CreateReportResponse,
    GetReportsResponse,
    ProcessingStatus,
    Report,
    ReportDocument,
)

# Amazon keeps reports for at most 90 days, order reports for 30.
# getReports' createdSince defaults to 90 days.
REPORT_RETENTION = timedelta(days=90)

# Keeps a createdSince near the retention limit inside it by the time Amazon
# checks it. Whether Amazon rejects an older createdSince is unverified.
RETENTION_MARGIN = timedelta(minutes=10)

GZIP_WBITS = 16 + zlib.MAX_WBITS

# Charsets read as a superset that Amazon's documents use: Shift_JIS as
# cp932, Microsoft's extension of it, and ISO-8859-1 as Windows-1252, as the
# web encoding standard does, so bytes 0x80-0x9F are text (a euro sign,
# dashes, curly quotes) rather than control characters. A UTF-8 byte-order
# mark is dropped.
CHARSET_SUPERSETS = {
    "shift_jis": "cp932",
    "iso8859-1": "cp1252",
    "utf-8": "utf-8-sig",
}

# The encoding of a document that names no charset. Amazon's sample code
# expects every document to name one; whether every document does is
# unverified.
DEFAULT_ENCODING = "utf-8-sig"

# How long a requested report is waited on before the capture fails. The
# restart that follows waits on the same report again if it asks for a
# request that report made (see request_report).
REPORT_TIMEOUT = timedelta(hours=2)

# The waits before the first checks of a requested report's status, and
# between every later one.
FIRST_POLL_DELAYS = (timedelta(seconds=15), timedelta(seconds=30))
POLL_INTERVAL = timedelta(seconds=60)

# Amazon cancels a report that has no data, but also one requested too soon
# after another. A cancelled request waits on a same-request report Amazon
# lists, if any, or is made once more this much later. A second
# cancellation is taken to mean the range has no data.
CANCELLED_RETRY_DELAY = timedelta(minutes=5)

# createReport takes at most this many marketplaceIds
# (CreateReportSpecification.marketplaceIds, maxItems 25).
REPORT_MARKETPLACE_LIMIT = 25

# How far back a request looks for the same request in flight or finished,
# such as one made before a restart.
REUSE_LOOKBACK = timedelta(hours=24)

REUSABLE_STATUSES = (
    ProcessingStatus.IN_QUEUE,
    ProcessingStatus.IN_PROGRESS,
    ProcessingStatus.DONE,
)
TERMINAL_STATUSES = (
    ProcessingStatus.DONE,
    ProcessingStatus.CANCELLED,
    ProcessingStatus.FATAL,
)

# How much of a FATAL report's diagnostic document its error quotes.
DIAGNOSTIC_LIMIT = 2000

# Roles a report type needs beyond the Reports API's own. Amazon refuses
# createReport for such a type, with a 403, to an application or selling
# account without the role.
REPORT_TYPE_ROLES = {"GET_SALES_AND_TRAFFIC_REPORT": "Brand Analytics"}


def now() -> datetime:
    return datetime.now(tz=UTC)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class ReportDocumentMissing(RuntimeError):
    """A DONE report has no document to read."""


class ReportDocumentTruncated(RuntimeError):
    """A compressed report document ended before its compressed data did."""


class ReportEncodingError(RuntimeError):
    """A report document isn't text in the encoding it was read with."""


class ReportTimedOut(RuntimeError):
    """Amazon didn't finish a requested report within REPORT_TIMEOUT."""


class ReportFailed(RuntimeError):
    """Amazon aborted a requested report (FATAL)."""


class ReportMismatch(RuntimeError):
    """Amazon lists a requested report with other arguments than were sent."""


class ReportRoleMissing(RuntimeError):
    """Amazon refused to create a report of a type that needs a role."""


@dataclass(frozen=True)
class ReportRequest:
    """
    A report to request: its type, marketplaces, data time range and report
    options, if any. With `data_end_after`, a report of the same start that
    ends after it and no later than `data_end` also counts as made by this
    request. getReports doesn't list a report's options, so a report counts
    as made by this request whatever options it was requested with.
    """

    report_type: str
    marketplace_ids: tuple[str, ...]
    data_start: datetime
    data_end: datetime
    data_end_after: datetime | None = None
    report_options: dict[str, str] | None = None

    def body(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "reportType": self.report_type,
            "marketplaceIds": list(self.marketplace_ids),
            "dataStartTime": _iso(self.data_start),
            "dataEndTime": _iso(self.data_end),
        }
        if self.report_options:
            body["reportOptions"] = dict(self.report_options)
        return body

    def made(self, report: Report) -> bool:
        """
        Whether `report`, of this request's type, was requested with this
        request's arguments.
        """
        # That Amazon lists a report's marketplaceIds, dataStartTime and
        # dataEndTime as they were sent (the same instants, to the second) is
        # unverified. _wait_for fails on a report that doesn't match.
        if sorted(report.marketplaceIds or []) != sorted(self.marketplace_ids):
            return False
        if report.dataStartTime != self.data_start or report.dataEndTime is None:
            return False
        if self.data_end_after is None:
            return report.dataEndTime == self.data_end
        return self.data_end_after < report.dataEndTime <= self.data_end

    def __str__(self) -> str:
        return (
            f"{self.report_type} from {_iso(self.data_start)} "
            f"to {_iso(self.data_end)}"
        )


async def list_reports(
    client: SPAPIClient,
    request_params: dict[str, Any],
    priority: Priority,
    log: Logger,
) -> list[Report]:
    """
    Returns every report matching `request_params`, across all pages, in the
    order Amazon lists them.
    """
    params: dict[str, Any] = {**request_params, "pageSize": Report.PAGE_SIZE}
    reports: list[Report] = []

    while True:
        _, body = await client.get(
            log, Operation.GET_REPORTS, Report.LIST_PATH, params, priority
        )
        page = GetReportsResponse.model_validate_json(
            b"".join([chunk async for chunk in body()])
        )
        reports.extend(page.reports)

        if not page.nextToken:
            return reports

        # Amazon fails a nextToken request that carries any other parameter.
        params = {"nextToken": page.nextToken}


async def fetch_report_document(
    client: SPAPIClient,
    report_document_id: str,
    priority: Priority,
    log: Logger,
) -> ReportDocument:
    _, body = await client.get(
        log,
        Operation.GET_REPORT_DOCUMENT,
        ReportDocument.PATH.format(reportDocumentId=report_document_id),
        {},
        priority,
    )
    return ReportDocument.model_validate_json(b"".join([chunk async for chunk in body()]))


async def get_report(
    client: SPAPIClient,
    report_id: str,
    priority: Priority,
    log: Logger,
) -> Report:
    _, body = await client.get(
        log, Operation.GET_REPORT, Report.PATH.format(reportId=report_id), {}, priority
    )
    return Report.model_validate_json(b"".join([chunk async for chunk in body()]))


async def create_report(
    client: SPAPIClient,
    request: ReportRequest,
    priority: Priority,
    log: Logger,
) -> str:
    """
    Requests a report and returns its id. A report type Amazon limits
    further (see client.REPORT_TYPE_LIMITS) is paced by that limit as well.
    """
    try:
        _, body = await client.post(
            log,
            Operation.CREATE_REPORT,
            Report.LIST_PATH,
            request.body(),
            priority,
            pace=client.report_type_bucket(request.report_type),
        )
    except HTTPError as err:
        role = REPORT_TYPE_ROLES.get(request.report_type)
        if err.code != 403 or role is None:
            raise
        raise ReportRoleMissing(
            f"Amazon refused to create a {request.report_type} report (HTTP 403): "
            f"the application or the selling account lacks the {role} role, which "
            "this report needs. Grant the role, or disable the binding that "
            f"captures this report.\n\n{err.message}"
        ) from err
    response = CreateReportResponse.model_validate_json(
        b"".join([chunk async for chunk in body()])
    )
    log.info(
        "requested report",
        {
            "report_id": response.reportId,
            "report_type": request.report_type,
            "data_start_time": request.data_start,
            "data_end_time": request.data_end,
        },
    )
    return response.reportId


async def _reusable_report(
    client: SPAPIClient,
    request: ReportRequest,
    priority: Priority,
    log: Logger,
    skip: frozenset[str] = frozenset(),
) -> Report | None:
    """
    The newest report of `request` created within REUSE_LOOKBACK that
    Amazon is processing or has finished, if any, other than those in `skip`.
    """
    request_params = {
        "reportTypes": request.report_type,
        "processingStatuses": ",".join(REUSABLE_STATUSES),
        Report.SINCE_PARAM: _iso(now() - REUSE_LOOKBACK),
    }
    reports = await list_reports(client, request_params, priority, log)
    return max(
        (r for r in reports if request.made(r) and r.reportId not in skip),
        key=lambda r: (r.createdTime, r.reportId),
        default=None,
    )


async def _wait_for(
    client: SPAPIClient,
    report_id: str,
    request: ReportRequest,
    priority: Priority,
    log: Logger,
) -> Report:
    """Returns the report once its status is terminal."""
    started = client.clock()
    delays = iter(FIRST_POLL_DELAYS)
    while True:
        await client.sleep(next(delays, POLL_INTERVAL).total_seconds())
        report = await get_report(client, report_id, priority, log)
        if not request.made(report):
            raise ReportMismatch(
                f"Amazon lists report {report_id} as {report.reportType} "
                f"{report.marketplaceIds} {report.dataStartTime}..{report.dataEndTime}, "
                f"not the requested {request} for {list(request.marketplace_ids)}; "
                "reuse after a restart can't find it."
            )
        if report.processingStatus in TERMINAL_STATUSES:
            return report
        if client.clock() - started >= REPORT_TIMEOUT.total_seconds():
            raise ReportTimedOut(
                f"Amazon has not finished report {report_id} ({request}) after "
                f"{REPORT_TIMEOUT}; it is {report.processingStatus}. A restart "
                "whose request this report made waits on it again."
            )


async def _fatal_error(
    client: SPAPIClient,
    report: Report,
    request: ReportRequest,
    priority: Priority,
    log: Logger,
) -> ReportFailed:
    """The error for a FATAL report, quoting its diagnostic document if any."""
    message = f"Amazon aborted report {report.reportId} ({request}) with status FATAL."
    if not report.reportDocumentId:
        return ReportFailed(f"{message} It gives no diagnostic document.")

    # The diagnostic is context only, so failing to read it doesn't hide the
    # FATAL.
    try:
        document, headers, stream = await _download_document(
            client, report.reportDocumentId, priority, log
        )
        content = b"".join([chunk async for chunk in stream])
        encoding = charset_encoding(headers.get("Content-Type")) or DEFAULT_ENCODING
    except (HTTPError, LookupError, ReportDocumentTruncated, ValidationError, zlib.error) as err:
        return ReportFailed(
            f"{message} Its diagnostic document {report.reportDocumentId} could "
            f"not be read: {err}"
        )
    text = content.decode(encoding, errors="replace")[:DIAGNOSTIC_LIMIT]
    return ReportFailed(
        f"{message} Its diagnostic document {document.reportDocumentId} reads:\n{text}"
    )


async def _reused(
    client: SPAPIClient,
    request: ReportRequest,
    priority: Priority,
    log: Logger,
    skip: frozenset[str] = frozenset(),
) -> Report | None:
    """The report _reusable_report finds, run to a terminal status, if any."""
    report = await _reusable_report(client, request, priority, log, skip)
    if report is not None and report.processingStatus != ProcessingStatus.DONE:
        log.info(
            "waiting on a report requested earlier",
            {"report_id": report.reportId, "report_type": request.report_type},
        )
        report = await _wait_for(client, report.reportId, request, priority, log)
    return report


async def request_report(
    client: SPAPIClient,
    request: ReportRequest,
    priority: Priority,
    log: Logger,
) -> Report | None:
    """
    Runs `request` to a terminal status and returns its DONE report, or None
    if Amazon cancels a report of its whole range twice, which is taken to
    mean the range has no data and is logged as a warning.

    A report this request made (see ReportRequest.made) that Amazon is
    processing or has finished within REUSE_LOOKBACK is used instead of
    requesting another, so a request made again, as after a restart, waits
    on or reads the report requested before.
    A cancelled or aborted report is never reused, even while Amazon still
    lists it as processing or finished. A report not finished
    within REPORT_TIMEOUT, or aborted, fails rather than being skipped.
    """
    cancelled: list[str] = []
    seen_cancelled: set[str] = set()
    retry_after_cancel = False
    report = await _reused(client, request, priority, log)
    while report is None or report.processingStatus == ProcessingStatus.CANCELLED:
        if report is not None:
            # A reused report that ends before the request does covers only
            # part of its range, so its cancellation doesn't count.
            if report.dataEndTime == request.data_end:
                cancelled.append(report.reportId)
            seen_cancelled.add(report.reportId)
            retry_after_cancel = True
            # Look for a reusable report again: Amazon may have cancelled this
            # one as too soon because a report of the same request was in flight.
            report = await _reused(
                client, request, priority, log, frozenset(seen_cancelled)
            )
            continue
        if len(cancelled) >= 2:
            log.warning(
                "Amazon cancelled the report twice, so its range is taken to have no data",
                {
                    "report_type": request.report_type,
                    "data_start_time": request.data_start,
                    "data_end_time": request.data_end,
                    "report_ids": cancelled,
                },
            )
            return None
        if retry_after_cancel:
            await client.sleep(CANCELLED_RETRY_DELAY.total_seconds())
        report_id = await create_report(client, request, priority, log)
        # Only a report requested before may end short of the request.
        exact = replace(request, data_end_after=None)
        report = await _wait_for(client, report_id, exact, priority, log)

    if report.processingStatus == ProcessingStatus.FATAL:
        raise await _fatal_error(client, report, request, priority, log)

    return report


def charset_encoding(content_type: str | None) -> str | None:
    """
    The text encoding named by a Content-Type's charset, or None if it names
    none. See CHARSET_SUPERSETS for the charsets read as a superset. A
    charset Python doesn't know fails rather than being guessed at.
    """
    if not content_type:
        return None
    message = Message()
    message["content-type"] = content_type
    charset = message.get_param("charset")
    if not isinstance(charset, str) or not charset:
        return None

    try:
        name = codecs.lookup(charset).name
    except LookupError as err:
        raise LookupError(f"report document has unknown charset {charset!r}") from err

    return CHARSET_SUPERSETS.get(name, name)


async def gunzip(stream: AsyncIterator[bytes]) -> AsyncGenerator[bytes, None]:
    """
    Decompresses a gzip stream as gunzip does, reading every member of a
    file made of several. A stream that ends partway through a member fails
    rather than reading as a shorter document.
    """
    decompressor = zlib.decompressobj(wbits=GZIP_WBITS)
    async for chunk in stream:
        while chunk:
            if decompressor.eof:
                decompressor = zlib.decompressobj(wbits=GZIP_WBITS)
            data = decompressor.decompress(chunk)
            if data:
                yield data
            chunk = decompressor.unused_data if decompressor.eof else b""

    if not decompressor.eof:
        raise ReportDocumentTruncated(
            "the report document's gzip stream ended before its end marker"
        )


async def _download_document(
    client: SPAPIClient,
    report_document_id: str,
    priority: Priority,
    log: Logger,
) -> tuple[ReportDocument, Headers, AsyncGenerator[bytes, None]]:
    """Starts downloading a report document, returning its decompressed bytes as a stream."""
    # The URL expires five minutes after it is issued, so it's requested
    # immediately before every download and never kept.
    document = await fetch_report_document(client, report_document_id, priority, log)
    headers, body = await client.download(
        log, document.url, f"report document {document.reportDocumentId}"
    )
    stream: AsyncGenerator[bytes, None] = body()
    if document.compressionAlgorithm == "GZIP":
        stream = gunzip(stream)
    return document, headers, stream


def _done_document_id(report: Report) -> str:
    """The document id of a DONE report. Any other report fails."""
    if report.processingStatus != ProcessingStatus.DONE:
        raise ValueError(f"report {report.reportId} is {report.processingStatus}, not DONE")
    if not report.reportDocumentId:
        raise ReportDocumentMissing(
            f"Amazon lists report {report.reportId} ({report.reportType}) as DONE "
            "but gives no reportDocumentId to read it from."
        )
    return report.reportDocumentId


async def read_report(
    client: SPAPIClient,
    report: Report,
    priority: Priority,
    log: Logger,
    empty_is_routine: bool = False,
) -> AsyncGenerator[tuple[ReportDocument, dict[str, str]], None]:
    """
    Yields each data row of a DONE report's flat-file document, in file
    order, as its column values exactly as the file holds them, alongside the
    document's metadata. The document is streamed and never stored whole.

    The document is decoded with the charset its Content-Type names, or as
    UTF-8 if it names none. A report without a document, or a document that
    can't be fetched, ends early or isn't text in that encoding, fails
    rather than reading as empty, short or garbled. A document with no data
    rows is logged, as a warning unless `empty_is_routine`.
    """
    document, headers, stream = await _download_document(
        client, _done_document_id(report), priority, log
    )
    log_fields = {
        "report_id": report.reportId,
        "report_document_id": document.reportDocumentId,
        "report_type": report.reportType,
        "content_type": headers.get("Content-Type"),
    }
    encoding = charset_encoding(headers.get("Content-Type"))
    if encoding is None:
        log.warning("report document names no charset, reading it as UTF-8", log_fields)
        encoding = DEFAULT_ENCODING

    # Flat files are tab-separated without quoting: a quote is an ordinary
    # character, and fields never hold tabs or line breaks.
    config = CSVConfig(
        delimiter="\t", quoting=csv.QUOTE_NONE, lineterminator="\n", encoding=encoding
    )

    count = 0
    try:
        async for row in IncrementalCSVProcessor(stream, config=config):
            count += 1
            yield document, row
    except UnicodeDecodeError as err:
        raise ReportEncodingError(
            f"report document {document.reportDocumentId} of report {report.reportId} "
            f"({report.reportType}) isn't {encoding} text. Its Content-Type is "
            f"{headers.get('Content-Type')!r}. {err}"
        ) from err

    if count == 0 and empty_is_routine:
        log.debug("report document has no data rows", log_fields)
    elif count == 0:
        log.warning("report document has no data rows", log_fields)
    else:
        log.info("read report document", {**log_fields, "rows": count, "encoding": encoding})


async def _as_utf8(
    stream: AsyncIterator[bytes],
    encoding: str,
    report: Report,
    document_id: str,
    content_type: str | None,
) -> AsyncGenerator[bytes, None]:
    """Re-encodes a stream of `encoding` text as UTF-8. Bytes that aren't `encoding` text fail."""
    decoder = codecs.getincrementaldecoder(encoding)()
    try:
        async for chunk in stream:
            yield decoder.decode(chunk).encode()
        yield decoder.decode(b"", final=True).encode()
    except UnicodeDecodeError as err:
        raise ReportEncodingError(
            f"report document {document_id} of report {report.reportId} "
            f"({report.reportType}) isn't {encoding} text. Its Content-Type is "
            f"{content_type!r}. {err}"
        ) from err


async def read_json_report(
    client: SPAPIClient,
    report: Report,
    priority: Priority,
    log: Logger,
) -> AsyncGenerator[bytes, None]:
    """
    Starts reading a DONE report's JSON document, returning it as a stream
    of UTF-8 bytes. How much of the document is held at once is up to the
    caller.

    The document is decoded with the charset its Content-Type names, or as
    UTF-8 if it names none. A report without a document, or a document that
    can't be fetched, ends early or isn't text in that encoding, fails.
    """
    document, headers, stream = await _download_document(
        client, _done_document_id(report), priority, log
    )
    content_type = headers.get("Content-Type")
    encoding = charset_encoding(content_type) or DEFAULT_ENCODING
    return _as_utf8(stream, encoding, report, document.reportDocumentId, content_type)


@dataclass
class RecentReports:
    """
    The reports this process has read in full, so a re-listing of the same
    time doesn't download them again. Not persisted: after a restart, every
    report a listing returns is read again, which is harmless because rows
    are keyed by report and row number.
    """

    created: dict[str, datetime] = field(default_factory=dict)

    def __contains__(self, report: Report) -> bool:
        return report.reportId in self.created

    def add(self, report: Report) -> None:
        self.created[report.reportId] = report.createdTime

    def forget_before(self, instant: datetime) -> None:
        """Forgets reports created before `instant`, which are not listed again."""
        self.created = {k: v for k, v in self.created.items() if v >= instant}
