import asyncio
import aiocsv
import aiocsv.protocols
import codecs
import csv
from datetime import datetime
from logging import Logger
import re
import sys
from typing import Any, AsyncGenerator

from estuary_cdk.http import HTTPSession, HTTPError
from .shared import build_query, VERSION
from .models import (
    SoapTypes,
    FieldDetailsDict,
    CursorFields,
    BulkJobStates,
    BulkJobSubmitResponse,
    BulkJobCheckStatusResponse,
)

INITIAL_SLEEP = 0.2
MAX_SLEEP = 300
ATTEMPT_LOG_THRESHOLD = 10

COUNT_HEADER = "Sforce-NumberOfRecords"
CANNOT_FETCH_COMPOUND_DATA = r"Selecting compound data not supported in Bulk Query"
NOT_SUPPORTED_BY_BULK_API = r"is not supported by the Bulk API"
DAILY_MAX_BULK_API_QUERY_VOLUME_EXCEEDED = r"Max bulk v2 query result size stored (1000000000) kb per 24 hrs has been exceeded"


# Python's csv module has a default field size limit of 131,072 bytes, and it will raise an _csv.Error exception if a field value
# is larger than that limit. Some users have fields larger than 131,072 bytes, so we max out the limit.
csv.field_size_limit(sys.maxsize)


class BulkJobError(RuntimeError):
    """Exception raised for error when executing a bulk query job."""
    def __init__(self, message: str, query: str | None = None, error: str | None = None):
        self.message = message
        self.query = query
        self.errors = error

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
        body = {
            "operation": "queryAll",
            "query" : build_query(object_name, field_names, cursor_field, start, end),
        }

        try:
            response = BulkJobSubmitResponse.model_validate_json(
                await self.http.request(self.log, self.base_url, method="POST", json=body)
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
            else:
                raise

        return response.id


    async def _check_job(self, job_id: str) -> BulkJobCheckStatusResponse:
        url = f"{self.base_url}/{job_id}"

        response = BulkJobCheckStatusResponse.model_validate_json(
            await self.http.request(self.log, url)
        )

        return response


    async def _process_csv_lines(
        self,
        byte_generator: AsyncGenerator[bytes, None],
    ) -> AsyncGenerator[dict[str, str], None]:
        class AsyncByteReader(aiocsv.protocols.WithAsyncRead):
            def __init__(self, byte_gen: AsyncGenerator[bytes, None]):
                self.byte_gen = byte_gen
                self.decoder = codecs.getincrementaldecoder("utf-8")()

            async def read(self, size: int = -1) -> str:
                try:
                    chunk = await self.byte_gen.__anext__()
                    return self.decoder.decode(chunk)
                except StopAsyncIteration:
                    # There should be no bytes left in the buffer after completely reading the CSV.
                    if self.decoder.buffer != b"":
                        raise BulkJobError("There were leftover bytes in the incremental decoder after reading the entire CSV.")
                    raise

        byte_reader = AsyncByteReader(byte_generator)
        async for row in aiocsv.AsyncDictReader(byte_reader):
            yield row


    async def _fetch_results(self, job_id: str) -> AsyncGenerator[dict[str, str], None]:
        url = f"{self.base_url}/{job_id}/results"
        request_headers = {"Accept-Encoding": "gzip"}
        params: dict[str, str] = {}

        while True:
            headers, body = await self.http.request_stream(self.log, url, params=params, headers=request_headers)
            count: str | None = headers.get(COUNT_HEADER)

            if count is None or int(count) == 0:
                return

            expected = int(count)
            received = 0

            async for record in self._process_csv_lines(body()):
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


    def _bool_str_to_bool(self, string: str) -> bool:
        lowercase_string = string.lower()
        if lowercase_string == "true":
            return True
        elif lowercase_string == "false":
            return False
        else:
            raise ValueError(f"No boolean equivalent for {string}.")


    def _str_to_number(self, string: str) -> int | float:
        try:
            return int(string)
        except ValueError:
            return float(string)


    def _str_to_anytype(self, string: str) -> str | bool | int | float:
        if string.lower() in ["true", "false"]:
            return self._bool_str_to_bool(string)

        try:
            return self._str_to_number(string)
        except ValueError:
            return string


    def _transform_value(
        self,
        name: str,
        soap_type: SoapTypes,
        value: str,
    ) -> Any:
        try:
            match soap_type:
                case SoapTypes.ID | SoapTypes.STRING | SoapTypes.DATE | SoapTypes.DATETIME | SoapTypes.TIME | SoapTypes.BASE64:
                    transformed_value = value
                case SoapTypes.BOOLEAN:
                    transformed_value = self._bool_str_to_bool(value)
                case SoapTypes.INTEGER | SoapTypes.LONG:
                    transformed_value = int(value)
                case SoapTypes.DOUBLE:
                    transformed_value = float(value)
                case SoapTypes.ANY_TYPE:
                    transformed_value = self._str_to_anytype(value)
                case _:
                    raise BulkJobError(f"Unanticipated field type {soap_type} for field {name}. Please reach out to Estuary support for help resolving this issue.")

            return transformed_value
        # The Salesforce reported type for custom fields are not always correct. If conversion to the Salesforce reported
        # type fails, we don't transform the value & we rely on schema inference to do its job.
        except ValueError:
            return value


    # Field values are always strings since they're parsed from a CSV file. We use the field types from Salesforce's
    # schemas to transform fields into the appropriate type before yielding a record.
    def _transform_fields(
        self,
        record: dict[str, str],
        fields: FieldDetailsDict,
    ) -> dict[str, Any]:
        transformed: dict[str, Any] = {}

        for field in record:
            name = field
            pre_value = record[name]

            transformed_value: Any = None

            # Salesforce represents null values with empty strings in Bulk API responses.
            if pre_value == "":
                transformed_value = None
            else:
                transformed_value = self._transform_value(name, fields[name].soapType, pre_value)

            transformed[name] = transformed_value

        return transformed


    async def execute(
        self,
        object_name: str, 
        fields: FieldDetailsDict,
        cursor_field: CursorFields | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> AsyncGenerator[dict[str, str], None]:
        job_id = await self._submit(object_name, list(fields.keys()), cursor_field, start, end)

        delay = INITIAL_SLEEP
        attempt = 1

        while True:
            job_details = await self._check_job(job_id)
            match job_details.state:
                case BulkJobStates.JOB_COMPLETE:
                    if job_details.numberRecordsProcessed == 0:
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

        async for result in self._fetch_results(job_id):
            yield self._transform_fields(result, fields)
            received += 1

        if received != job_details.numberRecordsProcessed:
            msg = f"Record count mismatch for job {job_id}. Expected {job_details.numberRecordsProcessed} total records but received {received}."
            raise BulkJobError(msg)
