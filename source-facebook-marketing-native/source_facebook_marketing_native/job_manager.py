from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator, Any
import asyncio

from .client import FacebookAPIClient, FacebookAPIError
from .models import FacebookInsightsJobStatus, FacebookInsightsResource


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
        client: FacebookAPIClient,
        max_concurrent_jobs: int = 10,
        poll_interval: int = 30,
        max_job_wait_time: timedelta = timedelta(hours=2),
        max_retries: int = 3,
    ):
        self._client = client
        self._max_concurrent_jobs = max_concurrent_jobs
        self._poll_interval = poll_interval
        self._max_job_wait_time = max_job_wait_time
        self._max_retries = max_retries

    async def fetch_insights(
        self,
        log: Logger,
        model: type[FacebookInsightsResource],
        account_id: str,
        date_range: dict[str, str],
        max_wait_time: timedelta | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        job_id = await self._submit_with_retry(
            log=log,
            account_id=account_id,
            level=model.level,
            fields=model.fields,
            date_range=date_range,
            breakdowns=model.breakdowns,
            action_breakdowns=model.action_breakdowns,
            time_increment=model.time_increment,
            max_wait_time=max_wait_time,
        )

        async for result in self._fetch_results(log, job_id, account_id, model.level):
            yield result

    async def _submit_job(
        self,
        log: Logger,
        account_id: str,
        level: str,
        fields: list[str],
        date_range: dict[str, str],
        breakdowns: list[str] | None = None,
        action_breakdowns: list[str] | None = None,
        time_increment: int = 1,
    ) -> str:
        """Submit an insights job and return the job ID."""
        try:
            job_id = await self._client.get_insights_async_job(
                log=log,
                account_id=account_id,
                level=level,
                fields=fields,
                time_range=date_range,
                breakdowns=breakdowns,
                action_breakdowns=action_breakdowns,
                time_increment=time_increment,
            )

            log.info(
                f"Submitted insights job {job_id} for account {account_id} at {level} level"
            )
            return job_id

        except FacebookAPIError as e:
            error_msg = f"Failed to submit insights job for account {account_id}: {e}"
            log.error(error_msg)
            raise

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
                status_data = await self._client.get_insights_job_status(log, job_id)
                status = FacebookInsightsJobStatus.model_validate(status_data)

                log.debug(
                    f"Job {job_id} status: {status.async_status} ({status.async_percent_completion}%)"
                )

                if status.async_status == "Job Completed":
                    log.info(f"Job {job_id} completed successfully")
                    return

                elif status.async_status == "Job Failed":
                    error_msg = f"Job {job_id} failed"
                    log.error(error_msg)
                    from .models import FacebookError
                    raise FacebookAPIError(
                        error=FacebookError(
                            message=error_msg, type="JobFailure", code=100
                        )
                    )

                elif status.async_status == "Job Skipped":
                    error_msg = f"Job {job_id} was skipped"
                    log.warning(error_msg)
                    from .models import FacebookError
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
        from .models import FacebookError
        raise FacebookAPIError(
            error=FacebookError(message=error_msg, type="JobTimeout", code=100)
        )

    async def _submit_with_retry(
        self,
        log: Logger,
        account_id: str,
        level: str,
        fields: list[str],
        date_range: dict[str, str],
        breakdowns: list[str] | None = None,
        action_breakdowns: list[str] | None = None,
        time_increment: int = 1,
        max_wait_time: timedelta | None = None,
    ) -> str:
        attempt = 0

        while attempt <= self._max_retries:
            try:
                if attempt > 0:
                    log.info(
                        f"Retrying job for account {account_id} (attempt {attempt}/{self._max_retries})"
                    )

                job_id = await self._submit_job(
                    log=log,
                    account_id=account_id,
                    level=level,
                    fields=fields,
                    date_range=date_range,
                    breakdowns=breakdowns,
                    action_breakdowns=action_breakdowns,
                    time_increment=time_increment,
                )

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

                # The retry happens immediately in next loop iteration
                # The inherent delay comes from the 30s polling interval that detected the failure

        error_msg = f"Job for account {account_id} failed after {self._max_retries} retries"
        log.error(error_msg)
        from .models import FacebookError
        raise FacebookAPIError(
            error=FacebookError(message=error_msg, type="JobRetryExhausted", code=100)
        )

    async def _fetch_results(
        self,
        log: Logger,
        job_id: str,
        account_id: str,
        level: str,
    ) -> AsyncGenerator[dict[str, Any], None]:
        try:
            async for item in self._client.get_insights_job_result(log, job_id):
                if isinstance(item, dict):
                    item["account_id"] = account_id
                    item["level"] = level
                    yield item

        except FacebookAPIError as e:
            error_msg = f"Failed to fetch results for job {job_id}: {e}"
            log.error(error_msg)
            raise
