import asyncio
from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.http import HTTPSession, HTTPError
from estuary_cdk.incremental_csv_processor import CSVConfig, IncrementalCSVProcessor
from .shared import build_query, should_retry, VERSION
from .models import (
    BulkJobError,
    FieldDetailsDict,
    CursorFields,
    BulkJobStates,
    BulkJobSubmitResponse,
    BulkJobCheckStatusResponse,
    SalesforceDataSource,
    SalesforceRecord,
    ValidationContext,
)

INITIAL_SLEEP = 0.2
MAX_SLEEP = 300
ATTEMPT_LOG_THRESHOLD = 10
MAX_BULK_QUERY_SET_SIZE = 25_000

COUNT_HEADER = "Sforce-NumberOfRecords"
CANNOT_FETCH_COMPOUND_DATA = r"Selecting compound data not supported in Bulk Query"
NOT_SUPPORTED_BY_BULK_API = r"is not supported by the Bulk API"
DAILY_MAX_BULK_API_QUERY_VOLUME_EXCEEDED = r"Max bulk v2 query result size stored (1000000000) kb per 24 hrs has been exceeded"
DAILY_MAX_BULK_API_QUERY_LIMIT_EXCEEDED = r"Max bulk v2 query jobs (10000) per 24 hrs has been reached"


CSV_CONFIG = CSVConfig(
    delimiter=',',
    quotechar='"',
    lineterminator='\n',
    encoding='utf-8'
)

BULK_VALIDATION_CONTEXT = ValidationContext(SalesforceDataSource.BULK_API)


class BulkJobManager:
    def __init__(self, http: HTTPSession, log: Logger, instance_url: str):
        self.http = http
        self.log = log
        self.base_url = f"{instance_url}/services/data/v{VERSION}/jobs/query"


    async def _submit(
            self,
            object_name: str, 
            field_names: list[str],
            cursor_field: CursorFields | None = None,
            start: datetime | None = None,
            end: datetime | None = None,
        ) -> str:
        query = build_query(object_name, field_names, cursor_field, start, end)

        body = {
            "operation": "queryAll",
            "query" : query,
        }

        try:
            self.log.debug("Submitting bulk job.", {
                "object_name": object_name,
                "start": start,
                "end": end,
                "query": query,
            })

            response = BulkJobSubmitResponse.model_validate_json(
                await self.http.request(self.log, self.base_url, method="POST", json=body, should_retry=should_retry)
            )
        except HTTPError as err:
            if err.code == 400 and CANNOT_FETCH_COMPOUND_DATA in err.message:
                msg = "Complex fields cannot be fetched via the Bulk API."
                raise BulkJobError(msg, body['query'], err.message)
            elif err.code == 400 and NOT_SUPPORTED_BY_BULK_API in err.message:
                msg = f"Object {object_name} is not supported by the Bulk API."
                raise BulkJobError(msg, body["query"], err.message)
            elif err.code == 400 and DAILY_MAX_BULK_API_QUERY_VOLUME_EXCEEDED in err.message:
                msg = "Maximum size of bulk results per rolling 24 hour period (1 TB) has been exceeded."
                raise BulkJobError(msg, body['query'], err.message)
            elif err.code == 400 and DAILY_MAX_BULK_API_QUERY_LIMIT_EXCEEDED in err.message:
                msg = "Maximum number of bulk jobs per rolling 24 hour period (10,000) has been exceeded."
                raise BulkJobError(msg, body["query"], err.message)
            else:
                raise

        self.log.debug("Submitted bulk job.", {
            "job_id": response.id,
            "object_name": object_name,
            "start": start,
            "end": end,
            "query": query,
        })

        return response.id


    async def _check_job(self, job_id: str) -> BulkJobCheckStatusResponse:
        url = f"{self.base_url}/{job_id}"

        self.log.debug("Checking bulk job status.", {
            "job_id": job_id,
        })

        response = BulkJobCheckStatusResponse.model_validate_json(
            await self.http.request(self.log, url, should_retry=should_retry)
        )

        self.log.debug("Received bulk job status.", {
            "job_id": job_id,
            "response": response
        })

        return response


    async def _fetch_results(self, job_id: str, model_cls: type[SalesforceRecord]) -> AsyncGenerator[SalesforceRecord, None]:
        url = f"{self.base_url}/{job_id}/results"
        request_headers = {"Accept-Encoding": "gzip"}
        params: dict[str, str | int] = {
            "maxRecords": MAX_BULK_QUERY_SET_SIZE,
        }

        while True:
            self.log.debug("Fetching page of results for bulk job.", {
                "job_id": job_id,
                "url": url,
                "params": params,
            })

            headers, body = await self.http.request_stream(self.log, url, params=params, headers=request_headers, should_retry=should_retry)
            count: str | None = headers.get(COUNT_HEADER)

            if count is None or int(count) == 0:
                return

            expected = int(count)
            received = 0

            processor = IncrementalCSVProcessor(
                body(),
                model_cls,
                CSV_CONFIG,
                validation_context=BULK_VALIDATION_CONTEXT,
            )
            async for record in processor:
                yield record
                received += 1

            if received != expected:
                msg = f"Record count mismatch for job {job_id}. Expected {expected} records but received {received}."
                if params.get('locator'):
                    msg += f" Locator parameter: {params['locator']}."
                raise BulkJobError(msg)

            next_page = headers.get('Sforce-Locator', None)

            if next_page == 'null' or not next_page:
                return

            params['locator'] = next_page


    async def execute(
        self,
        object_name: str, 
        fields: FieldDetailsDict,
        model_cls: type[SalesforceRecord],
        cursor_field: CursorFields | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> AsyncGenerator[SalesforceRecord, None]:
        job_id = await self._submit(object_name, list(fields.keys()), cursor_field, start, end)

        delay = INITIAL_SLEEP
        attempt = 1

        while True:
            job_details = await self._check_job(job_id)
            match job_details.state:
                case BulkJobStates.JOB_COMPLETE:
                    if job_details.numberRecordsProcessed == 0:
                        self.log.debug("Bulk job returned no results.", {
                            "job_id": job_id,
                            "object_name": object_name,
                            "start": start,
                            "end": end,
                        })
                        return
                    break
                case BulkJobStates.UPLOAD_COMPLETE | BulkJobStates.IN_PROGRESS:
                    if delay >= MAX_SLEEP or attempt > ATTEMPT_LOG_THRESHOLD:
                        self.log.info(f"Sleeping for {delay} seconds after attempt #{attempt} of waiting for job completion.", {
                            "job_details": job_details,
                        })
                    await asyncio.sleep(delay)  
                    delay = min(delay * 2, MAX_SLEEP)
                    attempt += 1
                case BulkJobStates.ABORTED | BulkJobStates.FAILED:
                    msg = f"Unanticipated status {job_details.state} for job {job_id}."
                    msg += (
                        f"\nRetries: {job_details.retries}."
                        f"\nTotal processing time {job_details.totalProcessingTime}."
                        f"\nNumber of records processed: {job_details.numberRecordsProcessed}."
                    )
                    raise BulkJobError(msg, error=job_details.errorMessage)
                case _:
                    raise BulkJobError(f"Unknown status {job_details.state} for job {job_id}.")

        assert isinstance(job_details.numberRecordsProcessed, int)
        received = 0

        self.log.debug("Bulk job completed. Fetching results.", {
            "job_id": job_id,
            "job_details": job_details,
            "object_name": object_name,
            "start": start,
            "end": end,
        })

        async for result in self._fetch_results(job_id, model_cls):
            yield result
            received += 1

        if received != job_details.numberRecordsProcessed:
            msg = f"Record count mismatch for job {job_id}. Expected {job_details.numberRecordsProcessed} total records but received {received}."
            raise BulkJobError(msg)

        self.log.debug("Finished fetching results for bulk job.", {
            "job_id": job_id,
            "object_name": object_name,
            "start": start,
            "end": end,
            "count": received,
        })
