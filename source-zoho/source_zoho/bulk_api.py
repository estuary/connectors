from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Collection, Generator
from datetime import datetime
from logging import Logger
from typing import NamedTuple

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_csv_processor import IncrementalCSVProcessor
from estuary_cdk.unzip_stream import UnzipStream

from .utils import dt_to_ts

from .models import (
    BulkJobCreateResponse,
    BulkJobDetails,
    BulkJobDetailsResponse,
    BulkJobState,
    FieldTypeFilter,
    ZohoModule,
)

INITIAL_SLEEP = 0.2
MAX_SLEEP = 300
ATTEMPT_LOG_THRESHOLD = 10


def _sleep_time_gen() -> Generator[float]:
    sleep_time_in_seconds = INITIAL_SLEEP

    while True:
        yield min(sleep_time_in_seconds, MAX_SLEEP)
        sleep_time_in_seconds *= 2


class TimeRange(NamedTuple):
    start: datetime
    end: datetime


BackfillState = TimeRange | str


class BulkJobError(RuntimeError):
    """Exception raised for errors when executing a bulk read job."""

    def __init__(self, message: str, error: str | None = None):
        self.message: str = message
        self.errors: str | None = error
        super().__init__(self.message)


class BulkJobManager:
    base_url: str
    http: HTTPSession
    log: Logger
    bulk_url: str

    def __init__(self, base_url: str, http: HTTPSession, log: Logger):
        self.base_url = base_url
        self.http = http
        self.log = log
        self.bulk_url = self.base_url + "/crm/bulk/v8/read"

    async def _submit(
        self,
        module_name: str,
        fields: Collection[str],
        state: BackfillState,
    ) -> str:
        match state:
            case TimeRange(start, end):
                body = {
                    "query": {
                        "module": {"api_name": module_name},
                        "fields": list(fields),
                        "criteria": {
                            "group_operator": "and",
                            "group": [
                                {
                                    "field": {"api_name": "Modified_Time"},
                                    "comparator": "greater_equal",
                                    "value": dt_to_ts(start),
                                },
                                {
                                    "field": {"api_name": "Modified_Time"},
                                    "comparator": "less_than",
                                    "value": dt_to_ts(end),
                                },
                            ],
                        },
                    }
                }
            case str(page_token):
                body = {"query": {"page_token": page_token}}

        self.log.debug(
            "Submitting bulk read job",
            {"module": module_name, "state": state},
        )

        response_data = BulkJobCreateResponse.model_validate_json(
            await self.http.request(self.log, self.bulk_url, method="POST", json=body)
        )

        assert (
            len(response_data.data) == 1
        ), "The data array having more than one object is undocumented behavior"
        result = response_data.data[0]

        if result.status != "success":
            raise BulkJobError(
                f"Failed to create bulk job: {result.message}",
                error=result.code,
            )

        job_id = result.details.id

        self.log.debug(
            "Submitted bulk read job",
            {"job_id": job_id, "module": module_name, "state": state},
        )

        return job_id

    async def _check_status(self, job_id: str) -> BulkJobDetails:
        url = f"{self.bulk_url}/{job_id}"
        self.log.debug("Checking bulk job status", {"job_id": job_id})

        response_data = BulkJobDetailsResponse.model_validate_json(
            await self.http.request(self.log, url)
        )

        assert (
            len(response_data.data) == 1
        ), "The data array having more than one object is undocumented behavior"
        result = response_data.data[0]

        self.log.debug(
            "Received bulk job status",
            {"job_id": job_id, "state": result.state},
        )

        return result

    async def _fetch_results(
        self, download_url: str, model: type[ZohoModule]
    ) -> AsyncGenerator[ZohoModule, None]:
        self.log.debug("Downloading bulk job results", {"download_url": download_url})

        _, body = await self.http.request_stream(self.log, download_url)

        async def unzipped_csv():
            async for chunk in UnzipStream(body()):
                yield chunk

        processor = IncrementalCSVProcessor(
            unzipped_csv(),
            model,
        )

        async for record in processor:
            yield record

    async def execute(
        self,
        module: type[ZohoModule],
        fields: Collection[str],
        state: BackfillState,
    ) -> AsyncGenerator[ZohoModule | str | None, None]:
        """Execute a bulk read job, yielding records and the next page token.

        When state is a TimeRange, starts a new bulk job with date criteria.
        When state is a page token string, continues from that checkpoint.
        Yields records first, then either the next page token or None if done.
        """
        job_id = await self._submit(module.api_name, fields, state)
        job_details = None

        for attempt, sleep_delay in enumerate(_sleep_time_gen(), start=1):
            job_details = await self._check_status(job_id)

            if job_details.state == BulkJobState.COMPLETED:
                break

            if sleep_delay >= MAX_SLEEP or attempt > ATTEMPT_LOG_THRESHOLD:
                self.log.info(
                    f"Waiting for bulk job completion (attempt #{attempt}, delay {sleep_delay}s)",
                    {"job_id": job_id, "state": job_details.state},
                )

            await asyncio.sleep(sleep_delay)

        assert job_details is not None

        if job_details.result is None:
            raise ValueError(
                f"Bulk job completed but no result available for job id {job_id}",
            )

        download_url = self.base_url + job_details.result.download_path

        self.log.debug(
            "Bulk job completed, fetching results",
            {
                "job_id": job_id,
                "state": state,
                "count": job_details.result.count,
                "more_records": job_details.result.more_records,
            },
        )

        async for record in self._fetch_results(download_url, module):
            yield record

        self.log.debug(
            "Finished fetching bulk job results",
            {"job_id": job_id, "module": module.api_name, "state": state},
        )

        if not job_details.result.more_records:
            return

        next_page_token = job_details.result.next_page_token
        assert (
            next_page_token is not None
        ), "A next page token should be present if we have more records to fetch"

        yield next_page_token


async def backfill_module(
    start_date: datetime,
    module: type[ZohoModule],
    bulk_job_manager: BulkJobManager,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
    is_initiated_by_connector: bool,
) -> AsyncGenerator[ZohoModule | PageCursor, None]:
    """Use Bulk Read API for ALL backfills (including formula refresh).

    Checkpoints using page tokens from the Zoho Bulk Read API. When `page` is None,
    starts a new backfill from start_date. When `page` is a page token, continues
    from that checkpoint.

    It is important to note that page tokens expire after 24 hours. In the event of a crash loop
    or prolonged downtime, manually issuing a new backfill may be necessary.
    """
    assert isinstance(page, str | None)
    assert isinstance(cutoff, datetime)

    if is_initiated_by_connector and not module.has_formula_fields():
        # Formula refresh only applies to modules with formula fields
        return

    fields_to_query = module.get_field_api_names(
        FieldTypeFilter.FORMULA if is_initiated_by_connector else FieldTypeFilter.ALL
    )

    state: BackfillState = page if page is not None else TimeRange(start_date, cutoff)

    log.info(
        "Initiating backfill",
        {"state": state},
    )

    async for item in bulk_job_manager.execute(module, fields_to_query, state):
        yield item
