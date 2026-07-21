import asyncio
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_csv_processor import IncrementalCSVProcessor

from .models import ExportStatus, ExportStatusResponse, ExportSubmitResponse
from .shared import VERSION_HEADERS

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

# Cap on concurrent export jobs. Zuora limits concurrent exports per tenant to
# around 5, so one semaphore shared across a task's bindings keeps us under it.
MAX_CONCURRENT_EXPORTS = 4


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
        status: ExportStatus | None = None,
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
    """Runs Zuora REST Export API jobs.

    Each export is an async job: submit a ZOQL query, poll until it reaches a
    terminal status, then stream the resulting CSV. Callers build their own
    queries (see api.build_query), so the manager stays query-agnostic.

    One manager is shared across all of a task's bindings, so its semaphore
    bounds total concurrent export jobs (Zuora allows ~5 per tenant).
    """

    def __init__(self, http: HTTPSession, log: Logger, base_url: str):
        self.http = http
        self.log = log
        self.base_url = base_url
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXPORTS)

    async def export_rows(self, query: str) -> AsyncGenerator[dict, None]:
        file_id = await self._run_job(query)
        async for row in self._fetch_results(file_id):
            yield row

    async def _run_job(self, query: str) -> str:
        """Submit an export job, poll until complete, and return the file ID.

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
        # exits normally. This assert satisfies the type checker's -> str path.
        assert False, "unreachable"

    async def _submit(self, query: str) -> str:
        url = f"{self.base_url}/v1/object/export"
        payload = {"Format": "csv", "Query": query}
        # Logged before the request so a submit-time rejection (e.g. a 400 for a
        # field describe claimed was exportable) sits next to the query that
        # caused it in the task logs.
        self.log.debug("submitting export job", {"query": query})
        submit_bytes = await self.http.request(
            self.log, url, method="POST", json=payload, headers=VERSION_HEADERS
        )
        submit = ExportSubmitResponse.model_validate_json(submit_bytes)
        if not submit.Success or submit.Id is None:
            reason = "; ".join(
                f"{e.Code}: {e.Message}" for e in submit.Errors
            ) or "no error detail"
            raise ExportError(
                f"Export job submission failed for query {query!r}: {reason}"
            )
        self.log.debug("created export job", {"job_id": submit.Id})
        return submit.Id

    async def _check_job(self, job_id: str) -> ExportStatusResponse:
        url = f"{self.base_url}/v1/object/export/{job_id}"
        return ExportStatusResponse.model_validate_json(
            await self.http.request(self.log, url, headers=VERSION_HEADERS)
        )

    async def _poll_until_complete(self, job_id: str) -> str:
        """Poll a submitted job until it completes, returning its file ID."""
        last_status: ExportStatus | None = None
        for attempt in range(MAX_POLL_ATTEMPTS):
            job = await self._check_job(job_id)
            # Logged only on transitions (not every poll) so a long-running job
            # stays visible in the logs without flooding them.
            if job.Status is not last_status:
                last_status = job.Status
                self.log.debug(
                    "export job status",
                    {
                        "job_id": job_id,
                        "status": job.Status,
                        "file_id": job.FileId,
                        "elapsed_s": attempt * POLL_INTERVAL,
                    },
                )
            if job.Status is ExportStatus.COMPLETED:
                if job.FileId is None:
                    raise ExportError(
                        f"Export job {job_id} completed without a FileId",
                        job_id=job_id,
                        status=job.Status,
                    )
                return job.FileId
            if job.Status in (ExportStatus.CANCELED, ExportStatus.FAILED):
                raise ExportError(
                    f"Export job {job_id} {job.Status}: "
                    f"{job.StatusReason or 'no reason given'}",
                    job_id=job_id,
                    status=job.Status,
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
