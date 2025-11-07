import asyncio
import json
from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator, Any
from pydantic import BaseModel, ConfigDict
from enum import StrEnum

from estuary_cdk.http import HTTPSession

from .client import FacebookAPIError, FacebookError, FacebookPaging
from .models import FacebookInsightsResource
from .enums import ActionBreakdown, ApiLevel, Breakdown


class AsyncJobSubmissionResponse(BaseModel):
    """Response from submitting an async insights job."""

    report_run_id: str
    error: FacebookError | None = None

    model_config = ConfigDict(extra="allow")


class AsyncJobStatusResponse(BaseModel):
    """Response from checking async insights job status."""

    id: str
    async_status: str  # Job|Failed|Completed
    async_percent_completion: int
    error: FacebookError | None = None

    model_config = ConfigDict(extra="allow")


class AsyncJobStatus(StrEnum):
    """Status values for async insights jobs."""

    JOB_COMPLETED = "Job Completed"
    JOB_FAILED = "Job Failed"
    JOB_SKIPPED = "Job Skipped"


class FacebookPaginatedInsightsResponse(BaseModel):
    """Paginated response for insights data from async jobs."""

    data: list[dict[str, Any]]
    paging: FacebookPaging | None = None
    error: FacebookError | None = None

    model_config = ConfigDict(extra="allow")


class FacebookInsightsJobManager:
    """
    Facebook async insights job manager.

    Manages the lifecycle of Facebook insights async jobs with:
    - Simple retry logic (no job splitting by levels)
    - Fixed 30-second polling interval
    - Configurable max retries
    """

    def __init__(
        self,
        http: HTTPSession,
        base_url: str,
        log: Logger,
        poll_interval: int = 30,
        max_job_wait_time: timedelta = timedelta(hours=2),
        max_retries: int = 3,
    ):
        self._http = http
        self._base_url = base_url
        self._log = log
        self._poll_interval = poll_interval
        self._max_job_wait_time = max_job_wait_time
        self._max_retries = max_retries

    async def _submit_job(
        self,
        log: Logger,
        account_id: str,
        level: ApiLevel,
        fields: list[str],
        time_range: dict[str, str],
        breakdowns: list[Breakdown] | None = None,
        action_breakdowns: list[ActionBreakdown] | None = None,
        action_attribution_windows: list[str] | None = None,
    ) -> str:
        url = f"{self._base_url}/act_{account_id}/insights"

        params = {
            "level": level,
            "fields": ",".join(fields),
            "time_range": json.dumps(time_range),
            "time_increment": 1,
            "limit": 100,
        }

        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)
        if action_breakdowns:
            params["action_breakdowns"] = ",".join(action_breakdowns)
        if action_attribution_windows:
            params["action_attribution_windows"] = ",".join(action_attribution_windows)

        log.debug(f"Submitting async insights job with params: {params}")

        response = AsyncJobSubmissionResponse.model_validate_json(
            await self._http.request(log, url, method="POST", params=params)
        )

        if response.error:
            raise FacebookAPIError(error=response.error)

        log.info(f"Submitted async insights job: {response.report_run_id}")
        return response.report_run_id

    async def _check_job_status(
        self, log: Logger, job_id: str
    ) -> AsyncJobStatusResponse:
        """Check the status of an async insights job using Pydantic models."""
        url = f"{self._base_url}/{job_id}"

        log.debug(f"Checking status for job: {job_id}")

        response = AsyncJobStatusResponse.model_validate_json(
            await self._http.request(log, url)
        )

        if response.error:
            raise FacebookAPIError(error=response.error)

        return response

    async def _fetch_job_results(
        self, log: Logger, job_id: str
    ) -> AsyncGenerator[dict[str, Any], None]:
        url = f"{self._base_url}/{job_id}/insights"
        params = {"limit": 100}

        log.debug(f"Fetching results for job: {job_id}")

        while True:
            response = FacebookPaginatedInsightsResponse.model_validate_json(
                await self._http.request(log, url, params=params)
            )

            if response.error:
                raise FacebookAPIError(error=response.error)

            log.debug(f"Got {len(response.data)} results from job {job_id}")

            for item in response.data:
                yield item

            if not response.paging or not response.paging.next:
                log.debug(f"No more pages for job {job_id}")
                break

            url = response.paging.next
            params = {}  # Next URL already has params

    async def fetch_insights(
        self,
        log: Logger,
        model: type[FacebookInsightsResource],
        account_id: str,
        time_range: dict[str, str],
        max_wait_time: timedelta | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        job_id = await self._submit_with_retry(
            log=log,
            model=model,
            account_id=account_id,
            time_range=time_range,
            max_wait_time=max_wait_time,
        )

        async for result in self._fetch_results(log, job_id, account_id, model.level):
            yield result

    async def _wait_for_completion(
        self,
        log: Logger,
        job_id: str,
        max_wait_time: timedelta | None = None,
    ) -> None:
        max_wait = max_wait_time or self._max_job_wait_time
        start_time = datetime.now(UTC)

        while datetime.now(UTC) - start_time < max_wait:
            try:
                status = await self._check_job_status(log, job_id)

                log.debug(
                    f"Job {job_id} status: {status.async_status} ({status.async_percent_completion}%)"
                )

                if status.async_status == AsyncJobStatus.JOB_COMPLETED:
                    log.info(f"Job {job_id} completed successfully")
                    return

                elif status.async_status == AsyncJobStatus.JOB_FAILED:
                    error_msg = f"Job {job_id} failed"
                    log.error(error_msg)

                    raise FacebookAPIError(
                        error=FacebookError(
                            message=error_msg, type="JobFailure", code=100
                        )
                    )

                elif status.async_status == AsyncJobStatus.JOB_SKIPPED:
                    error_msg = f"Job {job_id} was skipped"
                    log.warning(error_msg)

                    raise FacebookAPIError(
                        error=FacebookError(
                            message=error_msg, type="JobSkipped", code=100
                        )
                    )

                log.debug(
                    f"Job {job_id} running: {status.async_percent_completion}% complete"
                )
                await asyncio.sleep(self._poll_interval)

            except FacebookAPIError as e:
                if "does not exist" in str(e).lower():
                    error_msg = f"Job {job_id} no longer exists"
                    log.error(error_msg)
                    raise
                else:
                    log.warning(
                        f"Error checking job {job_id} status: {e}. Retrying in {self._poll_interval}s"
                    )
                    await asyncio.sleep(self._poll_interval)

        error_msg = f"Job {job_id} timed out after {max_wait}"
        log.error(error_msg)

        raise FacebookAPIError(
            error=FacebookError(message=error_msg, type="JobTimeout", code=100)
        )

    async def _submit_with_retry(
        self,
        log: Logger,
        model: type[FacebookInsightsResource],
        account_id: str,
        time_range: dict[str, str],
        max_wait_time: timedelta | None = None,
    ) -> str:
        attempt = 0
        job_id = None

        while attempt <= self._max_retries:
            try:
                if attempt > 0:
                    log.info(
                        f"Retrying job for account {account_id} (previous job: {job_id}, attempt {attempt}/{self._max_retries})"
                    )

                job_id = await self._submit_job(
                    log=log,
                    account_id=account_id,
                    level=model.level,
                    fields=model.fields,
                    time_range=time_range,
                    breakdowns=model.breakdowns,
                    action_breakdowns=model.action_breakdowns,
                )

                log.debug(f"Submitted job {job_id} for account {account_id}")

                await self._wait_for_completion(log, job_id, max_wait_time)

                return job_id

            except FacebookAPIError as e:
                attempt += 1

                if attempt > self._max_retries:
                    error_msg = (
                        f"Job for account {account_id} failed after {self._max_retries} retries. "
                        f"Last error: {e}"
                    )
                    log.error(error_msg)
                    raise

                log.warning(
                    f"Job for account {account_id} failed (attempt {attempt}/{self._max_retries}): {e}. "
                    f"Will retry..."
                )
        error_msg = (
            f"Job for account {account_id} failed after {self._max_retries} retries"
        )
        log.error(error_msg)

        raise FacebookAPIError(
            error=FacebookError(message=error_msg, type="JobRetryExhausted", code=100)
        )

    async def _fetch_results(
        self,
        log: Logger,
        job_id: str,
        account_id: str,
        level: ApiLevel,
    ) -> AsyncGenerator[dict[str, Any], None]:
        try:
            async for item in self._fetch_job_results(log, job_id):
                if isinstance(item, dict):
                    item["account_id"] = account_id
                    item["level"] = level
                    yield item

        except FacebookAPIError as e:
            error_msg = f"Failed to fetch results for job {job_id}: {e}"
            log.error(error_msg)
            raise
