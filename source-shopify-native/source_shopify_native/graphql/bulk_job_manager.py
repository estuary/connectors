import asyncio
from logging import Logger
from typing import Any

from estuary_cdk.http import HTTPSession
from source_shopify_native.models import (
    BulkJobCancelResponse,
    BulkCurrentJobResponse,
    BulkSpecificJobResponse,
    BulkJobSubmitResponse,
    BulkOperationDetails,
    BulkOperationStatuses,
    BulkOperationTypes,
    UserErrors,
)

VERSION = "2025-01"
BULK_QUERY_ALREADY_EXISTS_ERROR = r"A bulk query operation for this app and shop is already in progress"
INITIAL_SLEEP = 5
MAX_SLEEP = 150

bulk_job_lock = asyncio.Lock()

class BulkJobError(RuntimeError):
    """Exception raised for error when executing a bulk GraphQL query job."""

    def __init__(self, message: str, query: str | None = None, errors: list[UserErrors] | None = None):
        self.message = message
        self.query = query
        self.errors = errors

        self.details: dict[str, Any] = {
            "message": self.message
        }

        if self.errors:
            self.details["errors"] = self.errors
        if self.query:
            self.details["query"] = self.query

        super().__init__(self.details)

    def __str__(self):
        return f"BulkJobError: {self.message}"

    def __repr__(self):
        return (
            f"BulkJobError: {self.message},"
            f"query: {self.query},"
            f"errors: {self.errors}"
        )


class BulkJobManager:
    def __init__(self, http: HTTPSession, log: Logger, store: str):
        self.http = http
        self.log = log
        self.url = f"https://{store}.myshopify.com/admin/api/{VERSION}/graphql.json"

    # Cancel currently running job
    async def cancel_current(self):
        current_job_details = await self._get_currently_running_job()

        if current_job_details is None:
            return

        # Do not cancel ongoing bulk mutations.
        if current_job_details.type == BulkOperationTypes.MUTATION:
            self.log.debug("Currently running query is a mutation. Not cancelling it.", {
                "current_job_details": current_job_details,
            })
            return
        elif current_job_details.status != BulkOperationStatuses.RUNNING:
            self.log.debug("No currently running bulk query.", {
                "current_job_details": current_job_details,
            })
            return

        is_canceled = await self._cancel(current_job_details.id)

        if is_canceled:
            self.log.info(f"Canceled job {current_job_details.id}.")
        else:
            self.log.warning(f"Unabled to cancel job {current_job_details.id}.")


    # Get currently running job
    async def _get_currently_running_job(self) -> BulkOperationDetails |  None:
        query = f"""
            query {{
                currentBulkOperation {{
                    type
                    id
                    status
                    createdAt
                    completedAt
                    url
                    errorCode
                }}
            }}
        """

        response = BulkCurrentJobResponse.model_validate_json(
            await self.http.request(self.log, self.url, method="POST", json={"query": query})
        )

        return response.data.currentBulkOperation


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

        response = BulkSpecificJobResponse.model_validate_json(
            await self.http.request(self.log, self.url, method="POST", json={"query": query})
        )

        return response.data.node


    # Cancel a running bulk job
    async def _cancel(self, job_id: str) -> bool:
        query = f"""
            mutation {{
                bulkOperationCancel(id: {job_id}) {{
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

        self.log.debug(f"Trying to cancel job {job_id}.")
        response = BulkJobCancelResponse.model_validate_json(
            await self.http.request(self.log, self.url, method="POST", json={"query": query})
        )

        status = response.data.bulkOperationCancel.bulkOperation.status

        match status:
            case BulkOperationStatuses.CANCELED:
                self.log.debug(f"Bulk job {job_id} has been cancelled.")
            case BulkOperationStatuses.CANCELING:
                self.log.debug(f"Bulk job {job_id} is being cancelled.")
            case _:
                self.log.debug(f"Could not cancel bulk job {job_id}.", {
                    "status": status,
                    "errors": response.data.bulkOperationCancel.userErrors
                })

        return status == "CANCELED"


    # Submits a bulk job & fetches the result URL
    async def execute(self, query: str) -> str | None:
        # Only a single bulk query job can be executed at a time via Shopify's API.
        async with bulk_job_lock:
            job_id = await self._submit(query)

            delay = INITIAL_SLEEP

            while True:
                details = await self._get_job(job_id)
                match details.status:
                    case BulkOperationStatuses.COMPLETED:
                        self.log.info(f"Job {job_id} has completed.", details)
                        return details.url
                    case BulkOperationStatuses.CREATED | BulkOperationStatuses.RUNNING:
                        self.log.info(f"Job {job_id} is {details.status}. Sleeping {delay} seconds to await job completion.")
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, MAX_SLEEP)
                    case BulkOperationStatuses.CANCELED | BulkOperationStatuses.CANCELING | BulkOperationStatuses.EXPIRED | BulkOperationStatuses.FAILED:
                        raise BulkJobError(f"Unanticipated status {details.status} for job {job_id}. Error code: {details.errorCode}.")
                    case _:
                        raise BulkJobError(f"Unknown status {details.status} for job {job_id}.")

    # Submit a bulk job for processing
    async def _submit(self, query: str) -> str:
        query = f"""
            mutation {{
                bulkOperationRunQuery(
                    query: \"\"\"{query}\"\"\"
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

        response = BulkJobSubmitResponse.model_validate_json(
            await self.http.request(self.log, self.url, method="POST", json={"query": query})
        )

        details = response.data.bulkOperationRunQuery.bulkOperation

        if details is None:
            errors = response.data.bulkOperationRunQuery.userErrors

            is_ongoing_query_conflict = any(BULK_QUERY_ALREADY_EXISTS_ERROR in error.message for error in errors)

            if is_ongoing_query_conflict:
                msg = (
                    "Another application is submitting bulk query operations to Shopify's API, preventing"
                    " this connector from extracting data. Please prevent prevent the other application from"
                    " submitting bulk query operations to Shopify."
                )
                raise BulkJobError(
                    message = msg,
                    errors = errors,
                )
            else:
                raise BulkJobError(
                    message = "Errors when submitting query.",
                    query = query,
                    errors = response.data.bulkOperationRunQuery.userErrors,
                )

        return details.id
