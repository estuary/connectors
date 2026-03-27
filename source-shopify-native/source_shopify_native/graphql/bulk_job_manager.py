import asyncio
import re
import time
from logging import Logger
from typing import Any

import aiohttp
from estuary_cdk.http import HTTPError

from source_shopify_native.models import (
    BulkCancelData,
    BulkOperationDetails,
    BulkOperationErrorCodes,
    BulkOperationsData,
    BulkOperationStatuses,
    BulkOperationUserErrors,
    BulkSpecificData,
    BulkSubmitData,
    ShopifyGraphQLResource,
)

from .client import QueryError, ShopifyGraphQLClient

BULK_QUERY_CONCURRENCY_LIMIT_EXCEEDED_ERROR = (
    "A bulk query operation for this app and shop is already in progress"
)
JOB_ID_PATTERN = re.compile(r"gid://shopify/BulkOperation/\d+")
INITIAL_SLEEP = 1
MAX_SLEEP = 2
SIX_HOURS = 6 * 60 * 60
CANCEL_TIMEOUT = 5 * 60  # 5 minutes
MAX_CONCURRENT_BULK_OPS = 5
MAX_QUERY_REQUEST_ATTEMPTS = 5
MAX_QUERY_REQUEST_RETRY_INTERVAL = 60  # 1 minute


class BulkJobError(RuntimeError):
    """Exception raised for error when executing a bulk GraphQL query job."""

    def __init__(
        self,
        message: str,
        query: str | None = None,
        errors: list[BulkOperationUserErrors] | None = None,
    ):
        self.message = message
        self.query = query
        self.errors = errors

        self.details: dict[str, Any] = {"message": self.message}

        if self.errors:
            self.details["errors"] = self.errors
        if self.query:
            self.details["query"] = self.query

        super().__init__(self.details)

    def __str__(self):
        return f"BulkJobError: {self.message}"

    def __repr__(self):
        return f"BulkJobError: {self.message},query: {self.query},errors: {self.errors}"


class BulkJobManager:
    def __init__(self, client: ShopifyGraphQLClient, log: Logger):
        self.client = client
        self.log = log
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_BULK_OPS)
        self._tracked_jobs: set[str] = set()

    async def check_connectivity(self) -> None:
        """Verify that the store's credentials are valid by issuing a lightweight API call."""
        await self._get_running_jobs()

    async def cancel_current(self):
        running_jobs = await self._get_running_jobs()

        if not running_jobs:
            return

        # First, issue cancel requests for all running jobs
        job_statuses: dict[str, BulkOperationStatuses] = {}
        for job_details in running_jobs:
            job_id = job_details.id
            self.log.info(f"[{self.client.store}] Cancelling bulk job {job_id}.")
            status = await self._cancel(job_id)
            job_statuses[job_id] = status

        # Then, wait for all jobs to finish cancelling
        for job_id, status in job_statuses.items():
            deadline = time.monotonic() + CANCEL_TIMEOUT
            while True:
                match status:
                    case BulkOperationStatuses.CANCELED | BulkOperationStatuses.COMPLETED:
                        self.log.info(f"[{self.client.store}] Bulk job {job_id} is {status}.")
                        break
                    case BulkOperationStatuses.CANCELING:
                        if time.monotonic() >= deadline:
                            self.log.warning(
                                f"[{self.client.store}] Timed out waiting for bulk job {job_id} to cancel "
                                f"after {CANCEL_TIMEOUT}s. Will retry on next startup."
                            )
                            break
                        self.log.info(f"[{self.client.store}] Waiting for bulk job {job_id} to be CANCELED.")
                        await asyncio.sleep(5)

                        details = await self._get_job(job_id)
                        status = details.status
                    case _:
                        raise BulkJobError(f"Unable to cancel bulk job {job_id}.")

    async def _retryable_request[T](self, query: str, data_model: type[T]) -> T:
        last_exception = None

        for attempt_count in range(1, MAX_QUERY_REQUEST_ATTEMPTS + 1):
            try:
                return await self.client.request(query, data_model, log=self.log)
            except QueryError:
                raise
            except HTTPError as e:
                if e.code < 500:
                    raise
                last_exception = e
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                last_exception = e

            self.log.warning(
                f"[{self.client.store}] Failed to request query execution (attempt {attempt_count}/{MAX_QUERY_REQUEST_ATTEMPTS})",
                {
                    "error": str(last_exception),
                    "error_type": type(last_exception).__name__,
                    "data_model": data_model.__name__,
                },
            )

            if attempt_count < MAX_QUERY_REQUEST_ATTEMPTS:
                await asyncio.sleep(MAX_QUERY_REQUEST_RETRY_INTERVAL)

        raise BulkJobError(
            f"Failed to request query execution {MAX_QUERY_REQUEST_ATTEMPTS} times",
            query=query,
        ) from last_exception

    # Get all running bulk query jobs using the new bulkOperations query (API 2026-01+)
    async def _get_running_jobs(self) -> list[BulkOperationDetails]:
        # Shopify allows max 5 concurrent bulk ops per store, so first:10 is sufficient
        # to capture all running query jobs without pagination.
        # Reference: https://shopify.dev/docs/api/usage/bulk-operations/queries#view-all-running-operations
        query = """
            query {
                bulkOperations(first: 10, query: "status:RUNNING type:QUERY") {
                    edges {
                        node {
                            type
                            id
                            status
                            createdAt
                            completedAt
                            url
                            errorCode
                            query
                        }
                    }
                }
            }
        """

        data = await self._retryable_request(
            query,
            data_model=BulkOperationsData,
        )

        return [edge.node for edge in data.bulkOperations.edges]

    async def _get_job(self, job_id: str) -> BulkOperationDetails:
        query = f"""
            query {{
                node(id: "{job_id}") {{
                    ... on BulkOperation{{
                        type
                        id
                        status
                        createdAt
                        completedAt
                        url
                        errorCode
                    }}
                }}
            }}
        """

        data = await self._retryable_request(
            query,
            data_model=BulkSpecificData,
        )

        return data.node

    # Cancel a running bulk job
    async def _cancel(self, job_id: str) -> BulkOperationStatuses:
        query = f"""
            mutation {{
                bulkOperationCancel(id: "{job_id}") {{
                    bulkOperation {{
                        type
                        id
                        status
                        createdAt
                        completedAt
                        url
                        errorCode
                    }}
                    userErrors {{
                        field
                        message
                    }}
                }}
            }}
        """

        self.log.debug(f"[{self.client.store}] Trying to cancel job {job_id}.")

        data = await self._retryable_request(
            query,
            data_model=BulkCancelData,
        )

        status = data.bulkOperationCancel.bulkOperation.status

        match status:
            case BulkOperationStatuses.CANCELED:
                self.log.debug(f"[{self.client.store}] Bulk job {job_id} has been cancelled.")
            case BulkOperationStatuses.CANCELING:
                self.log.debug(f"[{self.client.store}] Bulk job {job_id} is being cancelled.")
            case _:
                self.log.debug(
                    f"[{self.client.store}] Could not cancel bulk job {job_id}.",
                    {
                        "status": status,
                        "errors": data.bulkOperationCancel.userErrors,
                    },
                )

        return status

    # Submits a bulk job & fetches the result URL
    async def execute(
        self, model: type[ShopifyGraphQLResource], query: str
    ) -> str | None:
        # Shopify API 2026-01+ supports up to 5 concurrent bulk query jobs per store.
        async with self.semaphore:
            job_id = await self._submit(query)
            try:
                delay = INITIAL_SLEEP
                total_sleep = 0

                is_running = False

                while True:
                    details = await self._get_job(job_id)
                    match details.status:
                        case BulkOperationStatuses.COMPLETED:
                            self.log.info(
                                f"[{self.client.store}] Job {job_id} has completed.",
                                {
                                    "stream": model.NAME,
                                    "details": details,
                                },
                            )
                            return details.url
                        case (
                            BulkOperationStatuses.CREATED
                            | BulkOperationStatuses.RUNNING
                        ):
                            if not is_running:
                                self.log.info(
                                    f"[{self.client.store}] Job {job_id} is {details.status}. Sleeping to await job completion.",
                                    {
                                        "stream": model.NAME,
                                    },
                                )

                                is_running = True

                            total_sleep += delay

                            if total_sleep > SIX_HOURS:
                                self.log.warning(
                                    f"[{self.client.store}] Shopify has been working on job {job_id} for over {SIX_HOURS / (60 * 60)} hours. "
                                    "This is likely due to a large amount of data being processed within the job. "
                                    "Consider reducing how much data is processed in a single job by reducing the advanced date window setting in the config.",
                                    {
                                        "stream": model.NAME,
                                        "total_sleep": total_sleep,
                                    },
                                )

                            await asyncio.sleep(delay)
                            delay = min(delay * 2, MAX_SLEEP)
                        case (
                            BulkOperationStatuses.CANCELED
                            | BulkOperationStatuses.CANCELING
                            | BulkOperationStatuses.EXPIRED
                            | BulkOperationStatuses.FAILED
                        ):
                            msg = f"Unanticipated status {details.status} for job {job_id}. Error code: {details.errorCode}."
                            if (
                                details.errorCode
                                and details.errorCode
                                == BulkOperationErrorCodes.ACCESS_DENIED
                            ):
                                msg = (
                                    f"Bulk job {job_id} has failed because the provided credentials do not have sufficient permissions."
                                    f" If authenticating with an access token, ensure it is granted the permissions listed at"
                                    f" https://docs.estuary.dev/reference/Connectors/capture-connectors/shopify-native/#access-token-permissions."
                                )
                            raise BulkJobError(msg)
                        case _:
                            raise BulkJobError(
                                f"Unknown status {details.status} for job {job_id}."
                            )
            finally:
                self._tracked_jobs.discard(job_id)

    def _build_submit_error(
        self, errors: list[BulkOperationUserErrors], query: str
    ) -> BulkJobError:
        concurrency_limit_error = next(
            (
                error
                for error in errors
                if BULK_QUERY_CONCURRENCY_LIMIT_EXCEEDED_ERROR in error.message
            ),
            None,
        )

        if concurrency_limit_error is None:
            for error in errors:
                self.log.warning(f"[{self.client.store}] {error.message}")
            return BulkJobError(
                message="Errors when submitting query.",
                query=query,
                errors=errors,
            )

        reported_job_ids: set[str] = set(
            JOB_ID_PATTERN.findall(concurrency_limit_error.message)
        )

        msg_parts = [
            "Bulk query operation rejected due to concurrency limits.",
        ]

        if not reported_job_ids:
            msg_parts.append(
                f"Could not identify specific job IDs from Shopify's error response."
                f" Raw errors: {'; '.join(e.message for e in errors)}"
            )
            return BulkJobError(message=" ".join(msg_parts), errors=errors)

        our_jobs = reported_job_ids & self._tracked_jobs
        external_jobs = reported_job_ids - self._tracked_jobs

        msg_parts.append(
            f"Reported running jobs: {', '.join(sorted(reported_job_ids))}."
        )
        if our_jobs:
            msg_parts.append(
                f"Jobs tracked by this job manager: {', '.join(sorted(our_jobs))}."
            )
        if external_jobs:
            msg_parts.append(
                f"Jobs not tracked by this job manager: {', '.join(sorted(external_jobs))}."
                " Please prevent the other application from submitting bulk query operations to Shopify."
            )

        return BulkJobError(
            message=" ".join(msg_parts),
            errors=errors,
        )

    # Submit a bulk job for processing
    async def _submit(self, query: str) -> str:
        # groupObjects controls whether nested objects are grouped with their parent.
        # Default changed to false in API 2026-01, but we need true for proper nested resource parsing.
        query = f"""
            mutation {{
            bulkOperationRunQuery(
                query: \"\"\"{query}\"\"\",
                groupObjects: true
            ) {{
                bulkOperation {{
                type
                id
                status
                createdAt
                completedAt
                url
                errorCode
                }}
                userErrors {{
                field
                message
                code
                }}
            }}
            }}
        """

        data = await self._retryable_request(
            query,
            data_model=BulkSubmitData,
        )

        details = data.bulkOperationRunQuery.bulkOperation

        if details is None:
            raise self._build_submit_error(data.bulkOperationRunQuery.userErrors, query)

        self._tracked_jobs.add(details.id)

        return details.id
