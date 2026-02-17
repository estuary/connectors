from datetime import datetime
from logging import Logger
from typing import Any, AsyncGenerator, TypedDict

from estuary_cdk.http import HTTPSession
from .shared import build_query, should_retry, str_to_dt, VERSION
from .models import (
    CursorFields,
    FieldDetailsDict,
    QueryResponse,
    SalesforceDataSource,
    SalesforceRecord,
    ValidationContext,
)


# The maximum allowed length for the combined URI and headers is 16,384 bytes.
# I'm assuming the headers and base url take up at max 2,500 bytes, but we could adjust this (or calculate it dynamically) later if this assumption doesn't hold.
# https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm#:~:text=In%20each%20REST%20call%2C%20the%20maximum%20length%20for%20the%20combined%20URI%20and%20headers%20is%2016%2C384%20bytes.
MAX_URI_LENGTH = 16_384
MAX_FIELDS_LENGTH = MAX_URI_LENGTH - 2_500

REST_VALIDATION_CONTEXT = ValidationContext(SalesforceDataSource.REST_API)

class Query:
    def __init__(self, http: HTTPSession, log: Logger, base_url: str, query: str):
        self.http = http
        self.log = log
        self.base_url = base_url
        self.query = query
        self.query_locator = None
        self.done = False
        self.total_size = 0
        self.records_yielded = 0

    def _set_query_locator(self, next_records_url: str | None) -> None:
        if next_records_url is None:
            self.query_locator = None
            return

        parts = next_records_url.strip('/').split('/')
        if len(parts) == 5:
            self.query_locator = parts[-1]
        else:
            self.query_locator = None

    async def _fetch_single_page(self) -> AsyncGenerator[dict[str, Any], None]:
        if self.done:
            raise RuntimeError("No additional pages to fetch.")

        url = self.base_url
        params: dict[str, str] = {}
        if not self.query_locator:
            params['q'] = self.query
        else:
            url += f'/{self.query_locator}'

        self.log.debug("Fetching single page of REST API query.", {
            "url": url,
            "params": params,
        })

        response = QueryResponse.model_validate_json(
            await self.http.request(self.log, url, params=params, should_retry=should_retry)
        )

        count = 0

        for record in response.records:
            yield record
            count += 1

        self.log.debug("Processing page of REST API query.", {
            "url": url,
            "params": params,
            "count": count,
        })

        self.records_yielded += count
        self.done = response.done
        self.total_size = response.totalSize
        self._set_query_locator(response.nextRecordsUrl)


# RecordAndChunksCompleted contains an in-progress record that's being built-up from multiple Queries
# that are fetching separate chunks of available fields. Fields are merged into the record
# when they are received. chunks_completed_count is used to determine when a record has
# been completed & all of its fields have been merged into it; once chunks_completed_count
# is the same as the number of total field chunks, the record is completely built and can be yielded.
class RecordAndChunksCompleted(TypedDict):
    record: dict[str, Any]
    chunks_completed_count: int


class RestQueryManager:
    def __init__(self, http: HTTPSession, log: Logger, instance_url: str):
        self.http = http
        self.log = log
        self.base_url = f"{instance_url}/services/data/v{VERSION}/queryAll"

    def _chunk_fields(self, fields: FieldDetailsDict, cursor_field: CursorFields) -> list[list[str]]:
        # The Id and cursor field are required later to merge together documents across
        # chunks and detect if a document was updated between querys.
        mandatory_fields: list[str] = ['Id', cursor_field]
        mandatory_fields_length = sum([len(field) for field in mandatory_fields])

        field_names = [f for f in list(fields.keys()) if f not in mandatory_fields]
        chunks: list[list[str]] = []

        chunk_fields_length = mandatory_fields_length
        chunk: list[str] = [*mandatory_fields]
        for field in field_names:
            if chunk_fields_length + len(field) > MAX_FIELDS_LENGTH:
                chunks.append(chunk)

                chunk = [field, *mandatory_fields]
                chunk_fields_length = len(field) + mandatory_fields_length
            else:
                chunk.append(field)
                chunk_fields_length += len(field)

        if len(chunk) > len(mandatory_fields):
            chunks.append(chunk)

        return chunks


    async def execute(
        self,
        object_name: str,
        fields: FieldDetailsDict,
        model_cls: type[SalesforceRecord],
        cursor_field: CursorFields,
        start: datetime,
        end: datetime,
    ) -> AsyncGenerator[SalesforceRecord, None]:
        # All fields are chunked across separate queries to avoid Salesforce's URI length limits. Results from
        # each query are merged together before yielding the complete record.
        field_chunks = self._chunk_fields(fields, cursor_field)

        queries: list[Query] = []
        for chunk in field_chunks:
            q = Query(self.http, self.log, self.base_url, build_query(object_name, chunk, cursor_field, start, end))
            queries.append(q)

        records: dict[str, RecordAndChunksCompleted] = {}

        self.log.debug("Executing REST API queries.", {
            "object_name": object_name,
            "start": start,
            "end": end,
            "len(queries)": len(queries),
        })

        while True:
            # next_query is the query that's yielded the fewest records so far. Since Salesforce dynamically reduces the page size of queries
            # depending on how much data is returned for each query, the page size for each query can be different.
            # We cache a record in memory until all of its fields are fetched. Each query's records_yielded attribute is used to gauge the
            # overall progress of each query. Executing the query that's made the least progress ensures the number of incomplete records we
            # hold in memory is bounded.
            next_query: Query | None = None
            for query in queries:
                if query.done:
                    continue
                elif next_query is None:
                    next_query = query
                elif query.records_yielded < next_query.records_yielded:
                    next_query = query

            assert isinstance(next_query, Query)

            async for partial_record in next_query._fetch_single_page():
                id = partial_record["Id"]

                # Create a new entry if we haven't seen this record's id yet. Since records are queried in ascending order 
                # of their cursor field, records are always added to the records dictionary in the same ascending order.
                if id not in records:
                    records[id] = {
                            "record": partial_record,
                            "chunks_completed_count": 1,
                        }
                else:
                    records[id]["record"] = records[id]["record"] | partial_record
                    records[id]["chunks_completed_count"] += 1

            # Dictionaries maintain the order items were added, and since items were added in ascending order of their cursor field,
            # we can iterate over their ids with list(records.keys()) and know the ids are in the same ascending order of record's cursor field.
            # This ensures records are yielded in ascending order.
            for id in list(records.keys()):
                chunk_completed_count = records[id]["chunks_completed_count"]
                record = records[id]["record"]

                # Do not emit a record if we haven't fetched all of its fields yet.
                if chunk_completed_count < len(field_chunks):
                    continue

                # If the record was updated after our end date, we ignore it since we should capture it on a future sweep.
                # This means the connector ignores records that are updated between chunked queries since they may not reflect the
                # current state of the record in Saleforce.
                if str_to_dt(record[cursor_field]) <= end:
                    yield model_cls.model_validate(
                        record, context=REST_VALIDATION_CONTEXT
                    )

                # Delete completed records to avoid keeping them in memory.
                del records[id]
            if all([q.done for q in queries]):
                # Any records that were updated between kicking off individual queries will not have all fields & won't have been yielded after all queries are done.
                # These are ignored since they should be captured on a future incremental sweep.
                if len(records) > 0:
                    self.log.debug(f"There were {len(records)} records that were not yielded when all queries completed. These updated records will be picked up on the next incremental sweep.")
                
                self.log.debug("Finished executing REST API queries.", {
                    "object_name": object_name,
                    "start": start,
                    "end": end,
                    "len(queries)": len(queries),
                })
                return
