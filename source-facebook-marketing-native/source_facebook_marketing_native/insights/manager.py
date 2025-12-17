"""Facebook Insights Job Manager for async job processing with automatic splitting."""

import asyncio
import json
from collections import deque
from datetime import datetime, timedelta, UTC
from dateutil.relativedelta import relativedelta
from logging import Logger
from typing import AsyncGenerator, Any

from estuary_cdk.http import HTTPSession, HTTPError

from ..client import FacebookAPIError, FacebookError
from ..models import DATA_RETENTION_PERIOD, FacebookInsightsResource
from ..enums import ActionBreakdown, ApiLevel, AttributionWindow, Breakdown

from .errors import (
    CannotSplitFurtherError,
    DataLimitExceededError,
    try_parse_facebook_api_error,
)
from .types import (
    AsyncJobStatus,
    AsyncJobStatusResponse,
    AsyncJobSubmissionResponse,
    FacebookPaginatedInsightsResponse,
    FilterField,
    InsightRecord,
    InsightsFilter,
    InsightsJob,
    InternalJobErrorType,
    JobOutcome,
    JobScope,
    JobSplit,
    JobSuccess,
    PROPAGATING_JOB_ERRORS,
    TimeRange,
)
from .metrics import JobMetrics


# Job retry configuration - each split job gets a fresh retry budget
MAX_RETRIES: int = 3

# Per-account concurrency limit for actually-executing jobs (via semaphore)
# Facebook rate limits are per ad account
MAX_CONCURRENT_JOBS: int = 100

# Maximum jobs queued per fetch_insights invocation (may be waiting on semaphore)
MAX_PENDING_PER_FETCH: int = 100

# Maximum time to wait for a single job before timing out
MAX_JOB_WAIT_TIME: timedelta = timedelta(hours=1)

# Polling interval for checking async job status
POLL_INTERVAL_SECONDS: int = 30

# Lookback window for entity ID discovery (28-day attribution + 1 day buffer)
ENTITY_ID_LOOKBACK_DAYS: int = 29

# Page sizes for API requests
INSIGHTS_PAGE_SIZE: int = 100
DISCOVERY_PAGE_SIZE: int = 500


class FacebookInsightsJobManager:

    def __init__(
        self,
        http: HTTPSession,
        base_url: str,
        log: Logger,
        account_id: str,
    ):
        self._http = http
        self._base_url = base_url
        self._log = log
        self._account_id = account_id
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

    def _describe_job(self, job: InsightsJob) -> str:
        """
        Create human-readable job description for logging.

        Format: "SCOPE job (N entities) [depth=D, from PARENT]"
        Examples:
            - "ACCOUNT job [depth=0]"
            - "campaigns job (2 entities) [depth=1, from ACCOUNT]"
            - "adsets job (1 entities) [depth=2, from campaigns]"
        """
        if job.scope == JobScope.ACCOUNT:
            return f"ACCOUNT job [depth={job.depth}]"

        count = len(job.entity_ids) if job.entity_ids else 0
        parent_info = f", from {job.parent_scope.value}" if job.parent_scope else ""
        return f"{job.scope.value} job ({count} entities) [depth={job.depth}{parent_info}]"

    def _build_filter(self, job: InsightsJob) -> InsightsFilter | None:
        """Build InsightsFilter from job scope and entity IDs."""
        if job.scope == JobScope.ACCOUNT or not job.entity_ids:
            return None

        field_map: dict[JobScope, FilterField] = {
            JobScope.CAMPAIGNS: FilterField.CAMPAIGN,
            JobScope.ADSETS: FilterField.ADSET,
            JobScope.ADS: FilterField.AD,
        }

        return InsightsFilter(
            field=field_map[job.scope],
            value=job.entity_ids,
        )

    async def _execute_job(
        self,
        log: Logger,
        job: InsightsJob,
        model: type[FacebookInsightsResource],
        account_id: str,
        time_range: TimeRange,
        result_queue: asyncio.Queue[InsightRecord],
    ) -> JobOutcome:
        """
        Execute a single insights job with retry logic.

        Returns JobSuccess if records were streamed to the queue, or JobSplit
        if the job was split into child jobs. Raises CannotSplitFurtherError
        if a single-ad job fails and cannot be split further.
        """
        job_desc = self._describe_job(job)
        last_error: Exception | None = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                log.info(f"Executing {job_desc} (attempt {attempt}/{MAX_RETRIES})")

                filtering = self._build_filter(job)
                job_id = await self._submit_job(
                    log=log,
                    account_id=account_id,
                    level=model.level,
                    fields=model.fields,
                    time_range=time_range,
                    breakdowns=model.breakdowns,
                    action_breakdowns=model.action_breakdowns,
                    action_attribution_windows=model.action_attribution_windows,
                    filtering=filtering,
                )

                await self._wait_for_completion(log, job_id, job_desc)

                records_count = 0
                async for record in self._fetch_results(log, job_id, account_id, model.level):
                    await result_queue.put(record)
                    records_count += 1

                log.info(f"Completed {job_desc}: {records_count} records")
                return JobSuccess(records_count=records_count)

            except HTTPError as e:
                converted = try_parse_facebook_api_error(e)
                if converted is None:
                    raise
                if isinstance(converted, DataLimitExceededError):
                    log.info(f"{job_desc} hit data limits, splitting immediately")
                    return await self._split_or_raise(log, job, account_id, time_range, job_desc)
                last_error = converted
                log.warning(f"{job_desc} failed (attempt {attempt}/{MAX_RETRIES}): {converted}")

            except DataLimitExceededError:
                log.info(f"{job_desc} hit data limits, splitting immediately")
                return await self._split_or_raise(log, job, account_id, time_range, job_desc)

            except FacebookAPIError as e:
                last_error = e
                log.warning(f"{job_desc} failed (attempt {attempt}/{MAX_RETRIES}): {e}")

        # All retries exhausted
        log.warning(f"{job_desc} failed after {MAX_RETRIES} attempts: {last_error}")
        return await self._split_or_raise(log, job, account_id, time_range, job_desc, last_error)

    async def _split_or_raise(
        self,
        log: Logger,
        job: InsightsJob,
        account_id: str,
        time_range: TimeRange,
        job_desc: str,
        error: Exception | None = None,
    ) -> JobSplit:
        """Split a job into children or raise CannotSplitFurtherError if atomic."""
        if not job.can_split():
            raise CannotSplitFurtherError(f"{job_desc} cannot be split: {error}")
        children = await self._split_job(log, job, account_id, time_range)
        return JobSplit(children=children)

    async def _submit_job(
        self,
        log: Logger,
        account_id: str,
        level: ApiLevel,
        fields: list[str],
        time_range: TimeRange,
        breakdowns: list[Breakdown] | None = None,
        action_breakdowns: list[ActionBreakdown] | None = None,
        action_attribution_windows: list[AttributionWindow] | None = None,
        filtering: InsightsFilter | None = None,
    ) -> str:
        url = f"{self._base_url}/act_{account_id}/insights"

        params = {
            "level": level,
            "fields": ",".join(fields),
            "time_range": json.dumps(time_range),
            "time_increment": 1,
            "limit": INSIGHTS_PAGE_SIZE,
        }

        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)
        if action_breakdowns:
            params["action_breakdowns"] = ",".join(action_breakdowns)
        if action_attribution_windows:
            params["action_attribution_windows"] = ",".join(action_attribution_windows)
        if filtering:
            params["filtering"] = json.dumps([filtering.to_dict()])

        filter_desc = f" with {filtering.field} filter ({len(filtering.value)} entities)" if filtering else ""
        log.debug(f"Submitting async insights job{filter_desc}", {"params": params})

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
    ) -> AsyncGenerator[InsightRecord, None]:
        url = f"{self._base_url}/{job_id}/insights"
        params = {"limit": INSIGHTS_PAGE_SIZE}

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
            params = {}

    async def _discover_entity_ids(
        self,
        log: Logger,
        account_id: str,
        level: ApiLevel,
        id_field: str,
        time_range: TimeRange,
        parent_filter: InsightsFilter | None = None,
    ) -> list[str]:
        """
        Discover entity IDs that have insights data within the lookback window.

        Used when splitting jobs by hierarchy descent to find child entities
        (e.g., campaigns for an account, adsets for a campaign). Uses a
        synchronous insights query with minimal fields for efficiency.
        """
        original_since = datetime.strptime(time_range["since"], "%Y-%m-%d")
        lookback_start = original_since - timedelta(days=ENTITY_ID_LOOKBACK_DAYS)

        # Respect 37-month data retention limit
        oldest_allowed = datetime.now(UTC) - relativedelta(months=DATA_RETENTION_PERIOD)
        if lookback_start < oldest_allowed.replace(tzinfo=None):
            lookback_start = oldest_allowed.replace(tzinfo=None)

        lookback_range = {
            "since": lookback_start.strftime("%Y-%m-%d"),
            "until": time_range["until"],
        }

        url = f"{self._base_url}/act_{account_id}/insights"
        params: dict[str, Any] = {
            "level": level.value,
            "fields": id_field,
            "time_range": json.dumps(lookback_range),
            "limit": DISCOVERY_PAGE_SIZE,
        }

        if parent_filter:
            params["filtering"] = json.dumps([parent_filter.to_dict()])

        entity_ids: set[str] = set()

        while True:
            response = FacebookPaginatedInsightsResponse.model_validate_json(
                await self._http.request(log, url, params=params)
            )

            if response.error:
                raise FacebookAPIError(error=response.error)

            for item in response.data:
                if id_field in item:
                    entity_ids.add(item[id_field])

            if not response.paging or not response.paging.next:
                break

            url = response.paging.next
            params = {}

        log.info(
            f"Discovered {len(entity_ids)} {level.value}s via insights lookback",
            {
                "account_id": account_id,
                "level": level.value,
                "lookback_since": lookback_range["since"],
                "parent_filter": parent_filter.field if parent_filter else None,
            },
        )

        if len(entity_ids) > 5000:
            log.warning(
                f"Large account: {len(entity_ids)} {level.value}s discovered",
                {"account_id": account_id},
            )

        return list(entity_ids)

    def _binary_split(
        self,
        entity_ids: list[str],
        scope: JobScope,
        depth: int,
        parent_scope: JobScope,
    ) -> list[InsightsJob]:
        """
        Split a list of entity IDs into at most 2 jobs.

        Handles edge cases:
        - Empty list: Returns empty list (no data exists)
        - Single ID: Returns single job
        - 2+ IDs: Splits in half (first half gets smaller portion on odd counts)

        Args:
            entity_ids: List of entity IDs to split.
            scope: The JobScope for the resulting jobs.
            depth: Split depth for the child jobs.
            parent_scope: Parent job's scope for logging context.

        Returns:
            List of InsightsJob instances (0, 1, or 2 jobs).
        """
        if not entity_ids:
            return []

        if len(entity_ids) == 1:
            return [InsightsJob(
                scope=scope,
                entity_ids=entity_ids,
                depth=depth,
                parent_scope=parent_scope,
            )]

        mid = len(entity_ids) // 2
        return [
            InsightsJob(
                scope=scope,
                entity_ids=entity_ids[:mid],
                depth=depth,
                parent_scope=parent_scope,
            ),
            InsightsJob(
                scope=scope,
                entity_ids=entity_ids[mid:],
                depth=depth,
                parent_scope=parent_scope,
            ),
        ]

    async def _split_job(
        self,
        log: Logger,
        failed_job: InsightsJob,
        account_id: str,
        time_range: TimeRange,
    ) -> list[InsightsJob]:
        """
        Split a failed job into smaller jobs using logarithmic bisection.

        Strategy:
        1. Prefer binary splitting at the current level (fewer API calls)
        2. Only descend the hierarchy when we have a single entity that fails
        3. Stop at single-ad level (atomic, cannot split further)

        Decision tree:
        - ACCOUNT scope: Discover campaigns -> binary split into CAMPAIGNS jobs
        - Multiple entity_ids: Binary split at current scope level
        - Single CAMPAIGN: Discover adsets -> binary split into ADSETS jobs
        - Single ADSET: Discover ads -> binary split into ADS jobs
        - Single AD: Raise CannotSplitFurtherError (atomic level)

        Args:
            log: Logger for debugging.
            failed_job: The job that failed and needs splitting.
            account_id: Facebook ad account ID.
            time_range: Original time range for the insights query.

        Returns:
            List of child jobs to execute. May be empty if no data exists.

        Raises:
            CannotSplitFurtherError: If job is at atomic level (single ad).
        """
        child_depth = failed_job.depth + 1
        job_desc = self._describe_job(failed_job)

        # Case 1: Account scope - discover campaigns
        if failed_job.scope == JobScope.ACCOUNT:
            campaign_ids = await self._discover_entity_ids(
                log, account_id, ApiLevel.CAMPAIGN, "campaign_id", time_range
            )
            children = self._binary_split(
                campaign_ids, JobScope.CAMPAIGNS, child_depth, JobScope.ACCOUNT
            )
            log.info(
                f"Split {job_desc} -> {len(children)} CAMPAIGNS jobs "
                f"({len(campaign_ids)} campaigns discovered, child_depth={child_depth})"
            )
            return children

        # Case 2: Multiple entities - binary split at current level
        if failed_job.entity_ids and len(failed_job.entity_ids) > 1:
            children = self._binary_split(
                failed_job.entity_ids, failed_job.scope, child_depth, failed_job.scope
            )
            # children always have entity_ids set since we created them from _binary_split
            first_count = len(children[0].entity_ids) if children[0].entity_ids else 0
            second_count = len(children[1].entity_ids) if children[1].entity_ids else 0
            log.info(
                f"Binary split {job_desc}: "
                f"{len(failed_job.entity_ids)} entities -> {first_count} + {second_count} "
                f"(child_depth={child_depth})"
            )
            return children

        # Case 3: Single campaign - descend to adsets
        if failed_job.scope == JobScope.CAMPAIGNS:
            parent_filter = InsightsFilter(
                field=FilterField.CAMPAIGN, value=failed_job.entity_ids or []
            )
            adset_ids = await self._discover_entity_ids(
                log, account_id, ApiLevel.ADSET, "adset_id", time_range, parent_filter
            )
            children = self._binary_split(
                adset_ids, JobScope.ADSETS, child_depth, JobScope.CAMPAIGNS
            )
            log.info(
                f"Split {job_desc} -> {len(children)} ADSETS jobs "
                f"({len(adset_ids)} adsets discovered, child_depth={child_depth})"
            )
            return children

        # Case 4: Single adset - descend to ads
        if failed_job.scope == JobScope.ADSETS:
            parent_filter = InsightsFilter(
                field=FilterField.ADSET, value=failed_job.entity_ids or []
            )
            ad_ids = await self._discover_entity_ids(
                log, account_id, ApiLevel.AD, "ad_id", time_range, parent_filter
            )
            children = self._binary_split(
                ad_ids, JobScope.ADS, child_depth, JobScope.ADSETS
            )
            log.info(
                f"Split {job_desc} -> {len(children)} ADS jobs "
                f"({len(ad_ids)} ads discovered, child_depth={child_depth})"
            )
            return children

        # Case 5: Single ad - cannot split further
        raise CannotSplitFurtherError(
            f"Cannot split job further: {failed_job.scope} with "
            f"entity_ids={failed_job.entity_ids}"
        )

    async def _cancel_pending(
        self, pending: dict[asyncio.Task[Any], InsightsJob]
    ) -> None:
        """Cancel all pending tasks and wait for them to complete."""
        for task in pending.keys():
            task.cancel()

        if pending:
            await asyncio.gather(*pending.keys(), return_exceptions=True)

    async def fetch_insights(
        self,
        log: Logger,
        model: type[FacebookInsightsResource],
        account_id: str,
        time_range: TimeRange,
    ) -> AsyncGenerator[InsightRecord, None]:
        """
        Fetch insights with automatic job splitting on failure.

        Manages a job queue and executes jobs concurrently. Results are
        yielded as soon as each job completes, not batched.
        """
        queue: deque[InsightsJob] = deque([InsightsJob(scope=JobScope.ACCOUNT, depth=0)])
        pending: dict[asyncio.Task[JobOutcome], InsightsJob] = {}
        result_queue: asyncio.Queue[InsightRecord] = asyncio.Queue()
        metrics = JobMetrics()
        last_progress_log = 0

        async def execute_with_semaphore(job: InsightsJob) -> JobOutcome:
            """Wrapper to acquire semaphore before execution."""
            async with self._semaphore:
                return await self._execute_job(
                    log, job, model, account_id, time_range, result_queue
                )

        try:
            log.info(
                f"Starting job processing for {time_range['since']} to {time_range['until']} "
                f"(account={account_id})"
            )

            while queue or pending:
                while queue and len(pending) < MAX_PENDING_PER_FETCH:
                    job = queue.popleft()
                    task = asyncio.create_task(execute_with_semaphore(job))
                    pending[task] = job
                    metrics.track_submit(job)
                    log.info(
                        f"Launched {self._describe_job(job)}, "
                        f"queue={len(queue)}, pending={len(pending)}"
                    )

                # Drain result_queue while waiting for tasks
                while not result_queue.empty():
                    record = result_queue.get_nowait()
                    metrics.records += 1
                    yield record

                if not pending:
                    break

                # Wait with short timeout to periodically drain the queue
                done, _ = await asyncio.wait(
                    pending.keys(),
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=0.1,
                )

                for task in done:
                    job = pending.pop(task)
                    job_desc = self._describe_job(job)

                    try:
                        outcome = task.result()

                        match outcome:
                            case JobSuccess(records_count=count):
                                metrics.track_complete(job)
                                log.debug(f"{job_desc} completed with {count} records")
                            case JobSplit(children=children) if children:
                                metrics.track_split(job)
                                for child in reversed(children):
                                    queue.appendleft(child)
                                log.debug(
                                    f"{job_desc} split into {len(children)} children, "
                                    f"queue={len(queue)}"
                                )
                            case JobSplit():
                                # Empty children = no data for this scope
                                metrics.track_complete(job)
                                log.debug(f"{job_desc} split found no entities")

                    except CannotSplitFurtherError as e:
                        metrics.failed += 1
                        log.error(f"Unrecoverable job failure: {e}")
                        await self._cancel_pending(pending)
                        raise

                    except Exception as e:
                        metrics.failed += 1
                        log.error(f"Unexpected error in {job_desc}: {e}")
                        await self._cancel_pending(pending)
                        raise

                # Log tree progress periodically (every 10 completed/split jobs)
                total_processed = metrics.completed + metrics.split
                if total_processed >= last_progress_log + 10:
                    last_progress_log = total_processed
                    log.info(
                        f"Tree progress: {metrics.tree_progress_summary()}, "
                        f"queue={len(queue)}, pending={len(pending)}, "
                        f"records={metrics.records}"
                    )

            # Final drain of any remaining records
            while not result_queue.empty():
                record = result_queue.get_nowait()
                metrics.records += 1
                yield record

        finally:
            log.info(
                f"Job processing complete for {time_range['since']} to {time_range['until']}: "
                f"submitted={metrics.submitted}, completed={metrics.completed}, "
                f"split={metrics.split}, failed={metrics.failed}, records={metrics.records}, "
                f"max_depth={metrics.max_depth}"
            )
            log.info(f"Final tree breakdown: {metrics.tree_progress_summary()}")

    async def _wait_for_completion(
        self,
        log: Logger,
        job_id: str,
        job_desc: str | None = None,
    ) -> None:
        """Wait for an async job to complete, polling at regular intervals."""
        start_time = datetime.now(UTC)
        desc = job_desc or f"job {job_id}"
        last_logged_milestone = 0  # Track last logged 25% milestone to avoid duplicates

        while datetime.now(UTC) - start_time < MAX_JOB_WAIT_TIME:
            try:
                status = await self._check_job_status(log, job_id)

                log.debug(
                    f"{desc}: status={status.async_status}, "
                    f"progress={status.async_percent_completion}%, job_id={job_id}"
                )

                if status.async_status == AsyncJobStatus.JOB_COMPLETED:
                    log.info(f"{desc}: completed successfully (job_id={job_id})")
                    return

                elif status.async_status == AsyncJobStatus.JOB_FAILED:
                    error_msg = f"{desc}: job failed at {status.async_percent_completion}% (job_id={job_id})"
                    log.error(error_msg)

                    raise FacebookAPIError(
                        error=FacebookError(
                            message=error_msg,
                            type=InternalJobErrorType.JOB_FAILURE,
                            code=100,
                        )
                    )

                elif status.async_status == AsyncJobStatus.JOB_SKIPPED:
                    error_msg = f"{desc}: job was skipped (job_id={job_id})"
                    log.warning(error_msg)

                    raise FacebookAPIError(
                        error=FacebookError(
                            message=error_msg,
                            type=InternalJobErrorType.JOB_SKIPPED,
                            code=100,
                        )
                    )

                # Log progress at INFO level at 25% milestones (25%, 50%, 75%)
                # Skip 0% (not started) and 100% (handled by completion check above)
                current_milestone = (status.async_percent_completion // 25) * 25
                if current_milestone > last_logged_milestone and current_milestone < 100:
                    log.info(
                        f"{desc}: {status.async_percent_completion}% complete (job_id={job_id})"
                    )
                    last_logged_milestone = current_milestone

                await asyncio.sleep(POLL_INTERVAL_SECONDS)

            except FacebookAPIError as e:
                # Re-raise job failure/skip/timeout errors - these should propagate
                if e.error and e.error.type in PROPAGATING_JOB_ERRORS:
                    raise
                # Re-raise if job no longer exists
                if "does not exist" in str(e).lower():
                    error_msg = f"{desc}: job no longer exists (job_id={job_id})"
                    log.error(error_msg)
                    raise
                # Transient status check errors - retry
                log.warning(
                    f"{desc}: error checking status: {e}. Retrying in {POLL_INTERVAL_SECONDS}s (job_id={job_id})"
                )
                await asyncio.sleep(POLL_INTERVAL_SECONDS)

        error_msg = f"{desc}: timed out after {MAX_JOB_WAIT_TIME} (job_id={job_id})"
        log.error(error_msg)

        raise FacebookAPIError(
            error=FacebookError(
                message=error_msg,
                type=InternalJobErrorType.JOB_TIMEOUT,
                code=100,
            )
        )

    async def _fetch_results(
        self,
        log: Logger,
        job_id: str,
        account_id: str,
        level: ApiLevel,
    ) -> AsyncGenerator[InsightRecord, None]:
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
