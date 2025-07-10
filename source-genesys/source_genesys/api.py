import asyncio

from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator, Any
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession, RateLimiter

from .models import (
    GenesysStream,
    GenesysResource,
    Users,
    SnapshotResponse,
    Conversation,
    CreateJobResponse,
    CheckJobStatusResponse,
    JobResultsResponse,
)

COMMON_API = "https://api"
DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def base_url(domain: str) -> str:
    return f"{COMMON_API}.{domain}/api/v2"


async def snapshot_resources(
    http: HTTPSession,
    domain: str,
    stream: type[GenesysStream],
    log: Logger,
) -> AsyncGenerator[GenesysResource, None]:
    url = f"{base_url(domain)}/{stream.path}"

    params = {
        "pageSize": stream.page_size,
        "pageNumber": 1,
        "sortOrder": "ASC",
    }

    if stream.extra_params:
        params.update(stream.extra_params)

    lastPageNumber = 1
    while params["pageNumber"] <= lastPageNumber:
        response = SnapshotResponse.model_validate_json(
            await http.request(log, url, params=params)
        )

        resources = response.entities
        for resource in resources:
            yield resource

        params["pageNumber"] += 1
        lastPageNumber = response.pageCount


async def fetch_conversations(
        http: HTTPSession,
        domain: str,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[Conversation | LogCursor, None]:
    """
    Submits an asynchronous job for all conversations
    that have ended between start_date and the present.
    """
    assert isinstance(log_cursor, datetime)

    updated_cursor = log_cursor

    async for record in _perform_conversation_job(
        http, domain, log, log_cursor
    ): 
        # Async analytics jobs return any conversations that started or ended within the requested date window.
        # The stream's cursor should be updated to the record's most recent datetime.
        most_recent_conversation_dt = record.conversationEnd or record.conversationStart
        if most_recent_conversation_dt > log_cursor:
            updated_cursor = max(updated_cursor, most_recent_conversation_dt)
            yield record

    if updated_cursor > log_cursor:
        yield updated_cursor


async def _perform_conversation_job(
        http: HTTPSession,
        domain: str,
        log: Logger,
        start_date: datetime,
        end_date: datetime | None = None,
) -> AsyncGenerator[Conversation, None]:
    """
    Requests the Genesys API to perform an async job & paginates through the results.

    API docs - https://developer.genesys.cloud/routing/conversations/conversations-apis#post-api-v2-analytics-conversations-details-jobs
    """

    if end_date is None:
        end_date = datetime.now(tz=UTC)

    # Submit job.
    url = f"{COMMON_API}.{domain}/api/v2/analytics/conversations/details/jobs"
    body = {
        "interval": f"{_dt_to_str(start_date)}/{_dt_to_str(end_date)}",
    }

    log.info(f"Submitting Genesys analytics job for conversations between {start_date} and {end_date}.")

    response = CreateJobResponse.model_validate_json(
        await http.request(log, url, method="POST", json=body)
    )

    job_id = response.jobId

    # Query job status.
    url = f"{COMMON_API}.{domain}/api/v2/analytics/conversations/details/jobs/{job_id}"

    state = "QUEUED"
    rate_limiter = RateLimiter()
    # 3 seconds anecdotally seems like the minimum time for a job to complete.
    rate_limiter.delay = 3.0 
    while state != "FULFILLED":
        delay = rate_limiter.delay
        log.info(f"Sleeping for {delay} seconds while Genesys analytics job completes. Current Genesys job status is {state}.")
        await asyncio.sleep(delay)
        response = CheckJobStatusResponse.model_validate_json(
            await http.request(log, url)
        )

        state = response.state

        match state:
            case "FAILED":
                raise RuntimeError("Conversations job failed. Please inspect the error and update job details.", response.errorMessage)
            case "CANCELLED" | "EXPIRED":
                raise RuntimeError(f"Conversations job status is {state}. Job request body was: ", body)

        rate_limiter.update(delay, True)

    # Paginate through results.
    url = f"{COMMON_API}.{domain}/api/v2/analytics/conversations/details/jobs/{job_id}/results"
    params: dict[str, str | int] = {
        "pageSize": 1000,
    }

    pageCount = 0
    most_recent_start = start_date

    while True:
        response = JobResultsResponse.model_validate_json(
            await http.request(log, url, params=params)
        )
        pageCount += 1

        conversations = response.conversations
        for conversation in conversations:
            most_recent_start = conversation.conversationStart
            yield conversation

        log.info(f"Processed page {pageCount} containing conversations starting on or before {most_recent_start}.")

        cursor = response.cursor
        if not cursor:
            break

        params.update({
            "cursor": cursor,
        })
