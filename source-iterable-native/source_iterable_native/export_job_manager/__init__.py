import asyncio
import time
from datetime import datetime
from typing import AsyncGenerator

from estuary_cdk.capture.common import Logger
from estuary_cdk.http import HTTPError, HTTPSession

from ..models import EventValidationContext, TExportResource
from ..shared import BASE_URL, dt_to_str
from .models import (
    ExportJobCancelledOrFailedError,
    ExportJobError,
    ExportJobState,
    ExportJobTimeoutError,
    GetExportFilesResponse,
    GetRecentExportJobsResponse,
    JobDetails,
    StartExportResponse,
    TruncatedExportJobError,
)


INITIAL_SLEEP = 1
MAX_SLEEP = 300  # 5 minutes
ATTEMPT_LOG_THRESHOLD = 10
MAX_CONCURRENT_JOBS = 4
MAX_WAIT_SECONDS = 24 * 60 * 60  # 24 hours
MAX_FILES_PER_PAGE = 10
MAX_JOB_RETRIES = 3


def _is_url_expired_error(e: HTTPError) -> bool:
    """Check if an HTTPError indicates an expired pre-signed URL."""
    return e.code == 403 and "Request has expired" in e.message


class ExportJobManager:
    def __init__(self, http: HTTPSession, log: Logger):
        self.http = http
        self.log = log
        self.base_export_url = f"{BASE_URL}/export"
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

    async def _submit(
        self,
        data_type_name: str,
        start: datetime,
        end: datetime | None = None,
    ) -> int:
        url = f"{self.base_export_url}/start"

        body = {
            "dataTypeName": data_type_name,
            "startDateTime": dt_to_str(start),
            # application/x-json-stream tells Iterable to
            # compile results in JSONL format.
            "outputFormat": "application/x-json-stream",
        }

        if end:
            body["endDateTime"] = dt_to_str(end)

        response = StartExportResponse.model_validate_json(
            await self.http.request(self.log, url, method="POST", json=body)
        )

        self.log.debug("Submitted export job.", {
            "job id": response.jobId,
            "request details": body,
        })

        return response.jobId


    async def _fetch_job_status_and_files(
        self,
        job_id: int,
        start_after: str | None = None,
    ) -> GetExportFilesResponse:
        """Fetch job status and a page of file URLs."""
        url = f"{self.base_export_url}/{job_id}/files"
        params = {"startAfter": start_after} if start_after else None

        return GetExportFilesResponse.model_validate_json(
            await self.http.request(self.log, url, params=params)
        )


    async def _fetch_results(
        self,
        job_id: int,
        model: type[TExportResource],
        data_type: str,
        start: datetime,
        end: datetime | None,
        validation_context: EventValidationContext | None = None,
    ) -> AsyncGenerator[TExportResource, None]:
        start_after: str | None = None

        while True:
            response = await self._fetch_job_status_and_files(job_id, start_after)
            filenames = [file.file for file in response.files]
            file_urls = {file.file: file.url for file in response.files}

            for filename in filenames:
                self.log.debug(f"Reading export job results.", {
                    "job_id": job_id,
                    "file": filename,
                })

                url = file_urls[filename]
                try:
                    _, lines = await self.http.request_lines(
                        self.log,
                        url,
                        # file.url is a pre-signed URL, and we shouldn't include
                        # the Iterable API key when reading results with that URL.
                        with_token=False
                    )
                except HTTPError as e:
                    # Pre-signed URLs can expire if processing takes too long. This is
                    # unlikely to happen since only 10 files are returned per page, and
                    # file sizes are <= 10 MB. But in case processing does take long
                    # enough for the pre-signed URLs to expire, re-fetch the current
                    # page to get fresh URLs.
                    if not _is_url_expired_error(e):
                        raise

                    refreshed_response = await self._fetch_job_status_and_files(job_id, start_after)
                    file_urls.update({
                        file.file: file.url for file in refreshed_response.files
                    })

                    if filename not in file_urls:
                        raise ExportJobError(
                            f"Could not find file {filename} after refreshing URLs for job {job_id}.",
                            data_type=data_type,
                            start=start,
                            end=end,
                        )

                    url = file_urls[filename]
                    _, lines = await self.http.request_lines(
                        self.log,
                        url,
                        with_token=False
                    )

                async for record in lines():
                    # If Iterable decides to insert extra newlines
                    # into the JSONL results, skip them.
                    if not record:
                        continue

                    yield model.model_validate_json(record, context=validation_context)

            if len(filenames) < MAX_FILES_PER_PAGE:
                break

            start_after = filenames[-1]


    async def _wait_for_completion(
        self,
        job_id: int,
        data_type: str,
        start: datetime,
        end: datetime | None,
    ) -> None:
        delay = INITIAL_SLEEP
        attempt = 1
        poll_start = time.monotonic()

        while True:
            elapsed = time.monotonic() - poll_start
            if elapsed > MAX_WAIT_SECONDS:
                msg = f"Export job {job_id} timed out after {elapsed / 3600:.1f} hours waiting for completion."
                raise ExportJobTimeoutError(msg, data_type, start, end)

            response = await self._fetch_job_status_and_files(job_id)
            match response.jobState:
                case ExportJobState.ENQUEUED | ExportJobState.RUNNING:
                    if delay >= MAX_SLEEP or attempt > ATTEMPT_LOG_THRESHOLD:
                        self.log.info(f"Sleeping for {delay} seconds after attempt #{attempt} of waiting for job completion.", {
                            "job_id": job_id,
                            "job_state": response.jobState,
                        })
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, MAX_SLEEP)
                    attempt += 1
                case ExportJobState.FAILED | ExportJobState.CANCELLING | ExportJobState.CANCELLED:
                    msg = f"Unanticipated state {response.jobState} for export job {job_id}."
                    raise ExportJobCancelledOrFailedError(msg, data_type, start, end)
                case ExportJobState.COMPLETED:
                    if response.exportTruncated:
                        msg = f"Job {job_id} completed but was truncated. Try requesting a smaller date window and try again."
                        raise TruncatedExportJobError(msg, data_type, start, end)
                    return
                case _:
                    msg = f"Unknown state {response.jobState} for export job {job_id}."
                    raise ExportJobError(msg, data_type, start, end)


    async def execute(
        self,
        model: type[TExportResource],
        data_type: str,
        start: datetime,
        end: datetime | None = None,
        validation_context: EventValidationContext | None = None,
    ) -> AsyncGenerator[TExportResource, None]:
        async with self.semaphore:
            attempt = 0
            while True:
                job_id = await self._submit(data_type, start, end)
                try:
                    await self._wait_for_completion(job_id, data_type, start, end)
                    break
                except ExportJobCancelledOrFailedError:
                    attempt += 1
                    if attempt <= MAX_JOB_RETRIES:
                        self.log.warning(f"Export job failed or was cancelled, retrying.", {
                            "job_id": job_id,
                            "attempt": attempt,
                            "max_retries": MAX_JOB_RETRIES,
                            "data_type": data_type,
                            "start": start,
                            "end": end,
                        })
                    else:
                        self.log.error(f"Export job failed after {MAX_JOB_RETRIES} retries.", {
                            "job_id": job_id,
                            "data_type": data_type,
                            "start": start,
                            "end": end,
                        })
                        raise

        async for doc in self._fetch_results(
            job_id,
            model,
            data_type,
            start,
            end,
            validation_context,
        ):
            yield doc


    async def _fetch_recent_export_jobs(self) -> AsyncGenerator[JobDetails, None]:
        url = f"{self.base_export_url}/jobs"

        response = GetRecentExportJobsResponse.model_validate_json(
            await self.http.request(self.log, url)
        )

        for job_detail in response.jobs:
            yield job_detail


    async def _cancel_job(self, job_id: int) -> None:
        url = f"{self.base_export_url}/{job_id}"

        await self.http.request(self.log, url, method="DELETE")


    async def cancel_all_running_and_enqueued_jobs(self) -> None:
        async for job in self._fetch_recent_export_jobs():
            if job.jobState == ExportJobState.RUNNING or job.jobState == ExportJobState.ENQUEUED:
                await self._cancel_job(job.id)
