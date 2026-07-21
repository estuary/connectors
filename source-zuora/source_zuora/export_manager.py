import asyncio
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_csv_processor import IncrementalCSVProcessor

from .models import AquaJobResponse, AquaJobStatus
from .shared import VERSION_HEADERS

# AQuA request schema version. With partner/project omitted the job runs in
# stateless mode regardless, so this only selects the response semantics.
AQUA_VERSION = "1.2"

# Informational job/query name shown in Zuora's UI and result file names.
AQUA_JOB_NAME = "estuary-capture-connector"

TERMINAL_FAILURE_STATUSES = (
    AquaJobStatus.ERROR,
    AquaJobStatus.ABORTED,
    AquaJobStatus.CANCELLED,
    AquaJobStatus.FAILED,
)

# An oversized export file (over 2047 MB) fails download with a 403 whose XML
# body carries this marker, e.g. <security:max-object-size>2047MB</...>. A 403
# without it is a different problem — most often the download user lacks the
# "Enable DataSource Exports Reporting" permission — and must not be misread as a
# size overflow. The CDK folds the response body into HTTPError.message for 4xx, so the
# marker is matchable there.
# https://developer.zuora.com/v1-api-reference/api/operation/GET_Files/
_FILE_TOO_LARGE_STATUS = 403
_TOO_LARGE_MARKER = "max-object-size"

# Seconds between export job status polls.
POLL_INTERVAL = 10

# Maximum number of polling attempts before giving up (~1 hour).
MAX_POLL_ATTEMPTS = 360

# Retries for transient Zuora export job failures.
MAX_RETRIES = 3

# Exponential backoff between retries: RETRY_BACKOFF_BASE_SECONDS * FACTOR**attempt.
RETRY_BACKOFF_BASE_SECONDS = 30
RETRY_BACKOFF_FACTOR = 2

# Cap on concurrent export jobs. Zuora allows up to 50 concurrent stateless
# AQuA jobs per tenant; staying far below that leaves room for the tenant's
# other AQuA integrations and keeps our polling load modest.
MAX_CONCURRENT_EXPORTS = 10


class ExportError(Exception):
    """An export job failed at the job level: a rejected submission, a
    Canceled/Failed status, or a timeout. Raised and retried internally by
    ExportManager. Transport-level failures (HTTPError) are not wrapped in this.
    They surface unchanged, having already been retried by the HTTP layer.

    job_id and status are carried as structured attributes, both optional since a
    rejected submission has neither, so callers can emit them as queryable log
    fields instead of parsing them back out of the message string.
    """

    def __init__(
        self,
        message: str,
        *,
        job_id: str | None = None,
        status: AquaJobStatus | None = None,
    ):
        super().__init__(message)
        self.job_id = job_id
        self.status = status


class ExportTooLargeError(Exception):
    """The export's generated file exceeded Zuora's 2047 MB limit, surfaced as a
    403 on download. Not retryable: the caller must narrow the query's time
    window and re-export. Deliberately not an ExportError so the job-level retry
    loop never swallows it.
    """


class ExportManager:
    """Runs Zuora AQuA (Aggregate Query API) export jobs in stateless mode.

    Each export is an async job: submit an Export ZOQL query, poll until it
    reaches a terminal status, then stream the resulting CSV file(s). Callers
    build their own queries (see api.build_query), so the manager stays
    query-agnostic. AQuA is used over the legacy /v1/object/export API because
    some tenants feature-gate fields out of the legacy engine that AQuA (and
    describe's export context) still consider exportable.

    One manager is shared across all of a task's bindings, so its semaphore
    bounds total concurrent export jobs.
    """

    def __init__(self, http: HTTPSession, log: Logger, base_url: str):
        self.http = http
        self.log = log
        self.base_url = base_url
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXPORTS)

    async def export_rows(self, query: str) -> AsyncGenerator[dict, None]:
        # Each segment file is a self-contained CSV with its own header row, so
        # streaming them back-to-back through per-file processors is seamless.
        for file_id in await self._run_job(query):
            async for row in self._fetch_results(file_id):
                yield row

    async def _run_job(self, query: str) -> list[str]:
        """Submit an export job, poll until complete, and return its file IDs
        (multiple when the tenant has AQuA file segmentation enabled).

        Holds the semaphore for the full duration so at most
        MAX_CONCURRENT_EXPORTS jobs run concurrently, staying within Zuora's
        per-tenant limit. Retries up to MAX_RETRIES times on export-job-level
        failures (ExportError) with exponential backoff. Transport errors
        propagate to the caller, already retried by the HTTP layer.
        """
        for attempt in range(MAX_RETRIES):
            try:
                async with self._semaphore:
                    job_id = await self._submit(query)
                    return await self._poll_until_complete(job_id)
            except ExportError as exc:
                if attempt == MAX_RETRIES - 1:
                    raise
                wait = RETRY_BACKOFF_BASE_SECONDS * (RETRY_BACKOFF_FACTOR**attempt)
                self.log.warning(
                    "Export job failed, retrying",
                    {
                        "attempt": attempt + 1,
                        "max": MAX_RETRIES,
                        "wait_s": wait,
                        "job_id": exc.job_id,
                        "status": exc.status,
                        "error": str(exc),
                    },
                )
                await asyncio.sleep(wait)
        # The final attempt always returns or re-raises above, so the loop never
        # exits normally. This assert satisfies the type checker's return path.
        assert False, "unreachable"

    async def _submit(self, query: str) -> str:
        url = f"{self.base_url}/v1/batch-query/"
        # partner/project are deliberately omitted: that runs the job in
        # stateless mode, and cursor windows in the query's WHERE clause carry
        # our incremental state instead.
        payload = {
            "format": "csv",
            "version": AQUA_VERSION,
            "name": AQUA_JOB_NAME,
            # dateTimeUtc keeps datetime output in
            # UTC rather than the tenant's local timezone, which cursor parsing
            # (AwareDatetime) depends on. 
            "dateTimeUtc": "true",
            # useQueryLabels makes CSV headers echo the
            # query's field names (e.g. "AccountNumber"). Without it they are
            # "Object: Field Label" display labels that match nothing in describe.
            "useQueryLabels": "true",
            "queries": [
                {"name": AQUA_JOB_NAME, "query": query, "type": "zoqlexport"}
            ],
        }
        # Logged before the request so a submit-time rejection (e.g. for a field
        # describe claimed was exportable) sits next to the query that caused it
        # in the task logs.
        self.log.debug("submitting export job", {"query": query})
        submit_bytes = await self.http.request(
            self.log, url, method="POST", json=payload, headers=VERSION_HEADERS
        )
        submit = AquaJobResponse.model_validate_json(submit_bytes)
        if submit.message or submit.id is None:
            raise ExportError(
                f"Export job submission failed for query {query!r}: "
                f"{submit.message or 'no error detail'}",
                status=submit.status,
            )
        self.log.debug("created export job", {"job_id": submit.id})
        return submit.id

    async def _check_job(self, job_id: str) -> AquaJobResponse:
        url = f"{self.base_url}/v1/batch-query/jobs/{job_id}"
        return AquaJobResponse.model_validate_json(
            await self.http.request(self.log, url, headers=VERSION_HEADERS)
        )

    async def _poll_until_complete(self, job_id: str) -> list[str]:
        """Poll a submitted job until it completes, returning its file IDs."""
        last_status: AquaJobStatus | None = None
        for attempt in range(MAX_POLL_ATTEMPTS):
            job = await self._check_job(job_id)
            batch = job.batches[0] if job.batches else None
            # Logged only on transitions (not every poll) so a long-running job
            # stays visible in the logs without flooding them.
            if job.status is not last_status:
                last_status = job.status
                self.log.debug(
                    "export job status",
                    {
                        "job_id": job_id,
                        "status": job.status,
                        "record_count": batch.recordCount if batch else None,
                        "elapsed_s": attempt * POLL_INTERVAL,
                    },
                )
            if job.status is AquaJobStatus.COMPLETED:
                file_ids = (
                    batch.segments or ([batch.fileId] if batch.fileId else [])
                ) if batch else []
                if not file_ids:
                    raise ExportError(
                        f"Export job {job_id} completed without any file IDs",
                        job_id=job_id,
                        status=job.status,
                    )
                return file_ids
            if job.status in TERMINAL_FAILURE_STATUSES:
                detail = (batch.message if batch else None) or job.message
                raise ExportError(
                    f"Export job {job_id} {job.status}: "
                    f"{detail or 'no reason given'}",
                    job_id=job_id,
                    status=job.status,
                )
            await asyncio.sleep(POLL_INTERVAL)
        raise ExportError(
            f"Export job {job_id} timed out after {MAX_POLL_ATTEMPTS * POLL_INTERVAL}s",
            job_id=job_id,
        )

    async def _fetch_results(self, file_id: str) -> AsyncGenerator[dict, None]:
        url = f"{self.base_url}/v1/files/{file_id}"
        try:
            _resp_headers, body_factory = await self.http.request_stream(
                self.log, url, headers=VERSION_HEADERS
            )
        except HTTPError as err:
            if err.code == _FILE_TOO_LARGE_STATUS and _TOO_LARGE_MARKER in err.message:
                raise ExportTooLargeError(
                    f"Export file {file_id} exceeds Zuora's size limit"
                ) from err
            raise
        async for row in IncrementalCSVProcessor(body_factory()):
            # Zuora prefixes every CSV column with "ObjectName.". Strip it so
            # fields match the describe names (e.g. "Id", not "Account.Id").
            yield {k.split(".", 1)[-1]: v for k, v in row.items()}
