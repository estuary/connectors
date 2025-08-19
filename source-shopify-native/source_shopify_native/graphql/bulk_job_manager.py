import asyncio
from logging import Logger
from typing import Any

from source_shopify_native.models import (
    BulkCancelData,
    BulkCurrentData,
    BulkSpecificData,
    BulkSubmitData,
    BulkOperationDetails,
    BulkOperationErrorCodes,
    BulkOperationStatuses,
    BulkOperationTypes,
    BulkOperationUserErrors,
    ShopifyGraphQLResource,
)

from .client import ShopifyGraphQLClient


BULK_QUERY_ALREADY_EXISTS_ERROR = (
    r"A bulk query operation for this app and shop is already in progress"
)
INITIAL_SLEEP = 1
MAX_SLEEP = 2
SIX_HOURS = 6 * 60 * 60

bulk_job_lock = asyncio.Lock()


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

    # Cancel currently running job
    async def cancel_current(self):
        current_job_details = await self._get_currently_running_job()

        if current_job_details is None:
            return

        # Do not cancel ongoing bulk mutations.
        if current_job_details.type == BulkOperationTypes.MUTATION:
            self.log.debug(
                "Currently running query is a mutation. Not cancelling it.",
                {
                    "current_job_details": current_job_details,
                },
            )
            return
        elif current_job_details.status != BulkOperationStatuses.RUNNING:
            self.log.debug(
                "No currently running bulk query.",
                {
                    "current_job_details": current_job_details,
                },
            )
            return

        job_id = current_job_details.id
        status = await self._cancel(job_id)

        # Shopify doesn't let us submit any new jobs until the current job is completely canceled.
        # We wait until the current job is canceled or completed to return, then the connector can
        # successfully submit new jobs.
        while True:
            match status:
                case BulkOperationStatuses.CANCELED | BulkOperationStatuses.COMPLETED:
                    self.log.info(f"Previous bulk job {job_id} is {status}.")
                    return
                case BulkOperationStatuses.CANCELING:
                    self.log.info(f"Waiting for bulk job {job_id} to be CANCELED.")
                    await asyncio.sleep(5)

                    details = await self._get_job(job_id)
                    status = details.status
                case _:
                    raise BulkJobError(f"Unable to cancel bulk job {job_id}.")

    # Get currently running job
    async def _get_currently_running_job(self) -> BulkOperationDetails | None:
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

        data = await self.client.request(
            query,
            data_model=BulkCurrentData,
            log=self.log,
        )

        return data.currentBulkOperation

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

        data = await self.client.request(
            query,
            data_model=BulkSpecificData,
            log=self.log,
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

        self.log.debug(f"Trying to cancel job {job_id}.")

        data = await self.client.request(
            query,
            data_model=BulkCancelData,
            log=self.log,
        )

        status = data.bulkOperationCancel.bulkOperation.status

        match status:
            case BulkOperationStatuses.CANCELED:
                self.log.debug(f"Bulk job {job_id} has been cancelled.")
            case BulkOperationStatuses.CANCELING:
                self.log.debug(f"Bulk job {job_id} is being cancelled.")
            case _:
                self.log.debug(
                    f"Could not cancel bulk job {job_id}.",
                    {
                        "status": status,
                        "errors": data.bulkOperationCancel.userErrors,
                    },
                )

        return status

    # Submits a bulk job & fetches the result URL
    async def execute(self, model: type[ShopifyGraphQLResource], query: str) -> str | None:
        # Only a single bulk query job can be executed at a time via Shopify's API.
        async with bulk_job_lock:
            job_id = await self._submit(query)

            delay = INITIAL_SLEEP
            total_sleep = 0

            is_running = False

            while True:
                details = await self._get_job(job_id)
                match details.status:
                    case BulkOperationStatuses.COMPLETED:
                        self.log.info(f"Job {job_id} has completed.", {
                            "stream": model.NAME,
                            "details": details,
                        })
                        return details.url
                    case BulkOperationStatuses.CREATED | BulkOperationStatuses.RUNNING:
                        if not is_running:
                            self.log.info(
                                f"Job {job_id} is {details.status}. Sleeping to await job completion.", {
                                    "stream": model.NAME,
                                }
                            )

                            is_running = True

                        total_sleep += delay

                        if total_sleep > SIX_HOURS:
                            self.log.warning(
                                f"Shopify has been working on job {job_id} for over {SIX_HOURS / (60 * 60)} hours. "
                                "This is likely due to a large amount of data being processed within the job. "
                                "Consider reducing how much data is processed in a single job by reducing the advanced date window setting in the config.", {
                                    "stream": model.NAME,
                                    "total_sleep": total_sleep,
                                }
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

        data = await self.client.request(
            query,
            data_model=BulkSubmitData,
            log=self.log,
        )

        details = data.bulkOperationRunQuery.bulkOperation

        if details is None:
            errors = data.bulkOperationRunQuery.userErrors

            is_ongoing_query_conflict = any(
                BULK_QUERY_ALREADY_EXISTS_ERROR in error.message for error in errors
            )

            if is_ongoing_query_conflict:
                msg = (
                    "Another application is submitting bulk query operations to Shopify's API, preventing"
                    " this connector from extracting data. Please prevent prevent the other application from"
                    " submitting bulk query operations to Shopify."
                )
                raise BulkJobError(
                    message=msg,
                    errors=errors,
                )
            else:
                errors = [error.message for error in errors]
                for error in errors:
                    self.log.warning(error)

                raise BulkJobError(
                    message="Errors when submitting query.",
                    query=query,
                    errors=data.bulkOperationRunQuery.userErrors,
                )

        return details.id
