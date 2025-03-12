from datetime import datetime
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.http import HTTPSession
from .shared import async_enumerate, build_query, dt_to_str, str_to_dt, VERSION
from .models import (
    CursorFields,
    SoapTypes,
    FieldDetailsDict,
    QueryResponse,
)


# The maximum allowed length for the combined URI and headers is 16,384 bytes.
# I'm assuming the headers and base url take up at max 2,500 bytes, but we could adjust this (or calculate it dynamically) later if this assumption doesn't hold.
# https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm#:~:text=In%20each%20REST%20call%2C%20the%20maximum%20length%20for%20the%20combined%20URI%20and%20headers%20is%2016%2C384%20bytes.
MAX_URI_LENGTH = 16_384
MAX_FIELDS_LENGTH = MAX_URI_LENGTH - 2_500

class Query:
    def __init__(self, http: HTTPSession, log: Logger, base_url: str, query: str):
        self.http = http
        self.log = log
        self.base_url = base_url
        self.query = query
        self.query_locator = None
        self.done = False
        self.total_size = 0

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

        response = QueryResponse.model_validate_json(
            await self.http.request(self.log, url, params=params)
        )

        for record in response.records:
            yield record

        self.done = response.done
        self.total_size = response.totalSize
        self._set_query_locator(response.nextRecordsUrl)


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
            if (
                len(chunk) > 0 and 
                chunk_fields_length + len(field) > MAX_FIELDS_LENGTH
            ):
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
        cursor_field: CursorFields,
        start: datetime,
        end: datetime,
    ) -> AsyncGenerator[dict[str, Any], None]:
        # All fields are chunked across separate queries to avoid Salesforce's URI length limits. Results from
        # each query are merged together before yielding the complete record.
        field_chunks = self._chunk_fields(fields, cursor_field)

        queries: list[Query] = []
        for chunk in field_chunks:
            q = Query(self.http, self.log, self.base_url, build_query(object_name, chunk, cursor_field, start, end))
            queries.append(q)

        while True:
            records: list[dict[str, Any]] = []
            for query in queries:
                async for index, partial_record in async_enumerate(query._fetch_single_page()):
                    if index == len(records):
                        records.append(partial_record)
                    else:
                        # We require chunked records be returned in the same order across queries.
                        assert records[index]['Id'] == partial_record['Id']
                        records[index] = records[index] | partial_record

            for record in records:
                # If the record was updated after our end date, we ignore it since we should capture it on a future sweep.
                # This means the connector ignores records that are updated between chunked queries since they may not reflect the
                # current state of the record in Saleforce.
                if str_to_dt(record[cursor_field]) <= end:
                    # The datetime field formats between REST and Bulk API are different, so we try
                    # to convert any cursor fields to the same format to keep values consistent between them.
                    for field, details in fields.items():
                        if details.soapType == SoapTypes.DATETIME and isinstance(record[field], str):
                            try:
                                record[field] = dt_to_str(str_to_dt(record[field]))
                            except ValueError:
                                pass

                    # REST API query results have an extraneous "attributes" field with a small amount of metadata.
                    # This metadata isn't present in the Bulk API response, so we remove it from records fetched
                    # via the REST API.
                    if 'attributes' in record:
                        del record['attributes']

                    yield record

            if all([q.done for q in queries]):
                return
            elif not all([not q.done for q in queries]):
                raise RuntimeError("Not all queries completed at the same time!")
