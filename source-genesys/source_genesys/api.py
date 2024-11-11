import asyncio

from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator, Any
from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession, RateLimiter

from .models import (
    User,
    UserResponse,
    Conversation,
    ConversationQueryResponse,
    CreateJobResponse,
    CheckJobStatusResponse,
    JobResultsResponse,
)

COMMON_API = "https://api"
DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# Some of the /v2/analytics/*/query endpoints only allow queries covering intervals of 7 days or fewer.
MAX_QUERY_DATE_WINDOW = 7
MAX_QUERY_PAGE_SIZE = 100


def _dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)

def _str_to_dt(date: str) -> datetime:
    return datetime.fromisoformat(date)


async def snapshot_users(
        http: HTTPSession,
        domain: str,
        log: Logger,
) -> AsyncGenerator[User, None]:
    """
    API docs - https://developer.genesys.cloud/useragentman/users/#get-api-v2-users
    """
    url = f"{COMMON_API}.{domain}/api/v2/users"

    params = {
        "pageSize": 500,
        "pageNumber": 1,
        "sortOrder": "ASC",
    }

    lastPageNumber = 1

    while params["pageNumber"] <= lastPageNumber:
        response = UserResponse.model_validate_json(
            await http.request(log, url, params=params)
        )

        users = response.entities
        for user in users:
            yield user

        params["pageNumber"] += 1
        lastPageNumber = response.pageCount


async def fetch_conversations(
        http: HTTPSession,
        domain: str,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[Conversation | LogCursor, None]:
    """
    There is not a single endpoint that can be used to retrieve updated conversations in real-time,
    so `fetch_conversations` relies on two separate endpoints:

    /api/v2/analytics/conversations/details/query - provides created conversations in real-time
    /api/v2/analytics/conversations/details/jobs - provides updated converversations with a delay
    """
    assert isinstance(log_cursor, datetime)

    most_recent_created_cursor = log_cursor

    async for record in fetch_created_conversations(http, domain, log, log_cursor):
        record_dt = record.conversationStart
        if record_dt > log_cursor:
            most_recent_created_cursor = record.conversationStart
            yield record

    # If any real-time documents were yielded, check the past two days for conversations that have ended.
    if most_recent_created_cursor != log_cursor:
        async for record in fetch_updated_conversations(http, domain, log, log_cursor - timedelta(days=1)):
                yield record

        yield most_recent_created_cursor


async def fetch_created_conversations(
        http: HTTPSession,
        domain: str,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[Conversation, None]:
    """
    Retrieves created conversations from the log_cursor to the present.

    API docs - https://developer.genesys.cloud/routing/conversations/conversations-apis#post-api-v2-analytics-conversations-details-query
    """
    assert isinstance(log_cursor, datetime)
    url = f"{COMMON_API}.{domain}/api/v2/analytics/conversations/details/query"

    lower_bound = log_cursor
    now = datetime.now(tz=UTC)

    # Iterate from our last seen cursor value to the present in 7 day windows.
    while lower_bound < now:
        upper_bound = min(lower_bound + timedelta(days=MAX_QUERY_DATE_WINDOW), now)
        pageNumber = 1

        # Paginate through this date window's results.
        while True:
            # This endpoint returns results in ascending order of the conversationStart field by default,
            # so we don't need to specify any ordering options.
            body = {
                "interval": f"{_dt_to_str(lower_bound)}/{_dt_to_str(upper_bound)}",
                "paging": {
                    "pageSize": MAX_QUERY_PAGE_SIZE,
                    "pageNumber": pageNumber,
                }
            }

            response = ConversationQueryResponse.model_validate_json(
                await http.request(log, url, method="POST", json=body)
            )

            conversations = response.conversations

            # If there are no more results to page through, move on to the next date window.
            if conversations is None:
                break

            for conversation in conversations: 
                yield conversation

            pageNumber += 1

        lower_bound = upper_bound


def _build_conversation_job_body(
        start_date: datetime,
        end_date: datetime,
        exclude_ongoing_conversations: bool = True,
) -> dict[str, Any]:
    body = {
        "interval": f"{_dt_to_str(start_date)}/{_dt_to_str(end_date)}",
    }

    if exclude_ongoing_conversations:
        filter_condition = {
            # Order results in ascending order of when they completed.
            "orderBy": "conversationEnd",
            # Only include conversations that have ended.
            "conversationFilters": [
                {
                "predicates": [
                    {
                    "type": "dimension",
                    "dimension": "conversationEnd",
                    "operator": "exists"
                    }
                ],
                "type": "and"
                }
            ]
        }

        body.update(filter_condition)

    return body


async def _perform_conversation_job(
        http: HTTPSession,
        domain: str,
        log: Logger,
        start_date: datetime,
        end_date: datetime = datetime.now(tz=UTC),
        exclude_ongoing_conversations: bool = True,
) -> AsyncGenerator[Conversation, None]:
    """
    Requests the Genesys API to perform an async job & paginates through the results.

    API docs - https://developer.genesys.cloud/routing/conversations/conversations-apis#post-api-v2-analytics-conversations-details-jobs
    """
    # Submit job.
    url = f"{COMMON_API}.{domain}/api/v2/analytics/conversations/details/jobs"
    body = _build_conversation_job_body(start_date, end_date, exclude_ongoing_conversations)

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

    while True:
        response = JobResultsResponse.model_validate_json(
            await http.request(log, url, params=params)
        )

        conversations = response.conversations
        for conversation in conversations:
            yield conversation

        cursor = response.cursor
        if not cursor:
            break

        params.update({
            "cursor": cursor,
        })


async def fetch_updated_conversations(
        http: HTTPSession,
        domain: str,
        log: Logger,
        start_date: datetime,
) -> AsyncGenerator[Conversation, None]:
    """
    Submits an asynchronous job for all conversations
    that have ended between start_date and the present.
    """
    async for result in _perform_conversation_job(
        http,
        domain,
        log,
        start_date,
    ): 
        yield result


async def backfill_conversations(
        http: HTTPSession,
        domain: str,
        log: Logger,
        page_cursor: PageCursor | None,
        cutoff: LogCursor,
) -> AsyncGenerator[Conversation | PageCursor, None]:
    """
    Submits an asynchronous job for all conversations from the page_cursor (config start date) and the cutoff.
    """
    assert isinstance(page_cursor, str)
    assert isinstance(cutoff, datetime)

    start_date = _str_to_dt(page_cursor)

    # Since cutoff is shifted backwards 2 days, it's possible for the cutoff to be before the start date.
    # In this situation, incremental replication will cover everything that should be backfilled, and the
    # backfill can safely return.
    if start_date > cutoff:
        return

    async for result in _perform_conversation_job(
        http,
        domain,
        log,
        start_date,
        end_date=cutoff,
        exclude_ongoing_conversations=False,
    ):
        yield result
