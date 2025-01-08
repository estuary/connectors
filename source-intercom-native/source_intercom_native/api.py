from datetime import datetime, UTC, timedelta
import json
from logging import Logger
import re
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession, HTTPError
from pydantic import TypeAdapter

from .models import (
    IntercomResource,
    TimestampedResource,
    ContactsSearchResponse,
    ConversationsSearchResponse,
    ConversationResponse,
    SegmentsResponse,
    CompanyListResponse,
    CompanySegmentsResponse,
)


API = "https://api.intercom.io"
SEARCH_PAGE_SIZE = 150
COMPANIES_LIST_LIMIT = 10_000

COMPANIES_LIST_LIMIT_REACHED_REGEX = r"page limit reached, please use scroll API"

def _dt_to_s(dt: datetime) -> int:
    return int(dt.timestamp())


def _s_to_dt(s: int) -> datetime:
    return datetime.fromtimestamp(s, tz=UTC)


async def snapshot_resources(
        http: HTTPSession,
        path: str,
        response_field: str,
        query_param: str | None,
        log: Logger,
) -> AsyncGenerator[IntercomResource, None]:
    url = f"{API}/{path}"
    params = None if query_param is None else {"model": query_param}

    response = json.loads(
        await http.request(log, url, params=params)
    )

    resources = TypeAdapter(list[IntercomResource]).validate_python(response[f'{response_field}'])

    for resource in resources:
        yield resource


def _generate_contacts_search_request_body(
        lower_bound: int,
        upper_bound: int,
        next_page_cursor: str | None = None
    ):
    # Intercom's search endpoints support complex queries within the request body. We filter and sort results
    # on the updated_at field, requesting all results with an updated at on or between the lower & upper bounds.
    #
    # Although the request body contains datetimes, Intercom filters contacts based on just the date.
    # Docs reference: https://developers.intercom.com/docs/references/rest-api/api.intercom.io/contacts/searchcontacts#section/Searching-for-Timestamp-Fields
    #
    # Results are sorted in descending order to improve incremental sync speeds, since the connector would have to
    # page through previously seen results if they were sorted in ascending order.
    query = {
            "operator": "OR",
            "value": [
                # Query results with updated_at on days between bounds (exclusive).
                {
                    "operator": "AND",
                    "value": [
                        {
                            "field": "updated_at",
                            "operator": ">",
                            "value": lower_bound,
                        },
                        {
                            "field": "updated_at",
                            "operator": "<",
                            "value": upper_bound,
                        }
                    ]
                },
                # Query results with update_at on the same days as the bounds.
                {
                    "operator": "OR",
                    "value": [
                        {
                            "field": "updated_at",
                            "operator": "=",
                            "value": lower_bound,
                        },
                        {
                            "field": "updated_at",
                            "operator": "=",
                            "value": upper_bound,
                        }
                    ]
                }
            ],
        }

    pagination: dict[str, str | int] = {
        "per_page": SEARCH_PAGE_SIZE,
    }

    if next_page_cursor:
        pagination["starting_after"] = next_page_cursor

    return {
        "query": query,
        "pagination": pagination,
        "sort": {
            "field": "updated_at",
            "order": "desc",
        }
    }


def _generate_conversations_search_request_body(
        lower_bound: int,
        upper_bound: int,
        next_page_cursor: str | None = None
    ):
    # Intercom's search endpoints support complex queries within the request body. We filter and sort results
    # on the updated_at field, requesting all results with an updated at on or between the lower & upper bounds.
    #
    # Unlike the contacts search endpoint, the conversations search endpoint *does* filter based on exact Unix timestamps.
    query = {
            "operator": "AND",
            "value": [
                {
                    "field": "updated_at",
                    "operator": ">",
                    "value": lower_bound,
                },
                {
                    "field": "updated_at",
                    "operator": "<=",
                    "value": upper_bound,
                }
            ],
        }

    pagination: dict[str, str | int] = {
        "per_page": SEARCH_PAGE_SIZE,
    }

    if next_page_cursor:
        pagination["starting_after"] = next_page_cursor

    return {
        "query": query,
        "pagination": pagination,
        "sort": {
                "field": "updated_at",
                "order": "ascending",
            },
    }


async def fetch_contacts(
    http: HTTPSession,
    window_size: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    start = _dt_to_s(log_cursor)
    max_end = _dt_to_s(log_cursor + timedelta(days=window_size))
    end = min(max_end, _dt_to_s(datetime.now(tz=UTC)))

    last_seen_ts = start

    url = f"{API}/contacts/search"
    body = _generate_contacts_search_request_body(start, end)

    pagination_ended_early = False
    while True:
        response = ContactsSearchResponse.model_validate_json(
                await http.request(log, url, "POST", json=body)
        )

        page_num = response.pages.page
        total_pages = response.pages.total_pages

        if total_pages == 0:
            break

        if page_num == 1 or page_num % 25 == 0 or page_num == total_pages:
            log.info(f"Processing page {page_num} of {total_pages}.", {
                'window_start': _s_to_dt(start),
                'window_end': _s_to_dt(end)
            })

        for contact in response.data:
            updated_at = contact.updated_at
            if updated_at < start:
                # We request that results are returned in descending order of updated_at,
                # so we stop processing results once we see one before the current date window.
                if page_num > 1:
                    log.info(f"Ending pagination early after processing {page_num} pages. Remaining pages contain already replicated data.")

                pagination_ended_early = True
                break

            if updated_at > last_seen_ts:
                last_seen_ts = updated_at
            if updated_at > start:
                yield contact

        if pagination_ended_early or response.pages.next is None:
            break

        body = _generate_contacts_search_request_body(start, end, response.pages.next.starting_after)

    # Results are returned in descending order, so we can't yield a cursor until pagination is complete.
    if end == max_end:
        yield _s_to_dt(max_end)
    elif last_seen_ts > start:
        yield _s_to_dt(last_seen_ts)


async def fetch_conversations(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    start = _dt_to_s(log_cursor)
    end = int(datetime.now(tz=UTC).timestamp())

    last_seen_ts = start

    url = f"{API}/conversations/search"
    body = _generate_conversations_search_request_body(start, end)

    while True:
        response = ConversationsSearchResponse.model_validate_json(
                await http.request(log, url, "POST", json=body)
        )

        page_num = response.pages.page
        total_pages = response.pages.total_pages

        if total_pages == 0:
            break

        if page_num == 1 or page_num % 25 == 0 or page_num == total_pages:
            log.info(f"Processing page {page_num} of {total_pages}.", {
                'start': _s_to_dt(start),
            })

        for conversation in response.conversations:
            if conversation.updated_at > last_seen_ts:
                # Checkpoint any yielded documents if the next result was updated later than
                # the prior results and it's not the first result we've seen.
                if last_seen_ts != start:
                    yield _s_to_dt(last_seen_ts)

                last_seen_ts = conversation.updated_at

            if conversation.updated_at > start:
                    yield conversation

        if response.pages.next is None:
            yield _s_to_dt(last_seen_ts)
            break

        body = _generate_conversations_search_request_body(start, end, response.pages.next.starting_after)


async def fetch_conversations_parts(
    http: HTTPSession,
    window_size: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    start = _dt_to_s(log_cursor)
    max_end = _dt_to_s(log_cursor + timedelta(days=window_size))
    end = min(max_end, _dt_to_s(datetime.now(tz=UTC)))

    last_seen_ts = start

    url = f"{API}/conversations/search"
    body = _generate_conversations_search_request_body(start, end)

    while True:
        response = ConversationsSearchResponse.model_validate_json(
                await http.request(log, url, "POST", json=body)
        )

        page_num = response.pages.page
        total_pages = response.pages.total_pages

        if total_pages == 0:
            break

        if page_num == 1 or page_num % 25 == 0 or page_num == total_pages:
            log.info(f"Processing page {page_num} of {total_pages}.", {
                'window_start': _s_to_dt(start),
                'window_end': _s_to_dt(end)
            })

        for conversation in response.conversations:
            if conversation.updated_at > last_seen_ts:
                last_seen_ts = conversation.updated_at

            if conversation.updated_at > start:
                async for part in _fetch_part(http, log, conversation):
                    if part.updated_at > start:
                        yield part

        if response.pages.next is None:
            break

        body = _generate_conversations_search_request_body(start, end, response.pages.next.starting_after)

    # Since a conversation part's updated_at could be after the window_start but before the parent
    # conversation's updated_at, we can't yield a new cursor until after checking the entire date window.
    if end == max_end:
        yield _s_to_dt(max_end)
    elif last_seen_ts > start:
        yield _s_to_dt(last_seen_ts)


async def _fetch_part(
        http: HTTPSession,
        log: Logger,
        conversation: TimestampedResource
) -> AsyncGenerator[TimestampedResource, None]:
    url = f"{API}/conversations/{conversation.id}"

    response = ConversationResponse.model_validate_json(
        await http.request(log, url)
    )

    for part in response.conversation_parts.conversation_parts:
        # Add conversation_id to the conversation part to align with the Airbyte connector.
        part.conversation_id = conversation.id #type: ignore
        yield part


async def fetch_segments(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    log_cursor_ts = _dt_to_s(log_cursor)
    last_seen_ts = log_cursor_ts

    url = f"{API}/segments"
    params={"include_count": 'true'}

    response = SegmentsResponse.model_validate_json(
        await http.request(log, url, params=params)
    )

    for result in response.data:
        updated_at = result.updated_at

        if updated_at > last_seen_ts:
            last_seen_ts = updated_at
        if updated_at > log_cursor_ts:
            yield result

    if last_seen_ts > log_cursor_ts:
        yield _s_to_dt(last_seen_ts)


async def fetch_companies(
        http: HTTPSession,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    log_cursor_ts = _dt_to_s(log_cursor)
    last_seen_ts = log_cursor_ts

    url = f"{API}/companies/list"

    current_page = 1
    params = {
        "per_page": 60,
        "page": current_page
    }

    exceeds_list_limit = False

    while True:
        try:
            response = CompanyListResponse.model_validate_json(
                await http.request(log, url, method="POST", params=params)
            )
        except HTTPError as err:
            # End pagination and checkpoint any documents if we hit the limit for the /companies/list endpoint.
            # If support for the /companies/scroll endpoint is added, we can re-evaluate whether to break or
            # fail here & tell users to use the /companies/scroll endpoint option.
            if err.code == 400 and bool(re.search(COMPANIES_LIST_LIMIT_REACHED_REGEX, err.message, re.DOTALL)):
                break
            else:
                raise

        if not exceeds_list_limit and response.total_count > COMPANIES_LIST_LIMIT:
            log.warning(f"{response.total_count} companies found."
                        " This is greater than the maximum number of companies returned by the /companies/list endpoint, and the connector could be missing data."
                        f" Contact Estuary support to request this stream use the /companies/scroll endpoint to retrieve more than {COMPANIES_LIST_LIMIT} companies.")
            exceeds_list_limit = True

        for company in response.data:
            if company.updated_at > last_seen_ts:
                last_seen_ts = company.updated_at
            if company.updated_at > log_cursor_ts:
                yield company

        if current_page >= response.pages.total_pages:
            break

        current_page += 1
        params['page'] = current_page

    # Results are not returned sorted by a timestamp field,
    # so we can't yield a cursor until pagination is complete.
    if last_seen_ts > log_cursor_ts:
        yield _s_to_dt(last_seen_ts)


async def fetch_company_segments(
        http: HTTPSession,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    log_cursor_ts = _dt_to_s(log_cursor)
    last_seen_ts = log_cursor_ts

    url = f"{API}/companies/list"

    current_page = 1
    params = {
        "per_page": 60,
        "page": current_page
    }

    exceeds_list_limit = False

    while True:
        try:
            response = CompanyListResponse.model_validate_json(
                await http.request(log, url, method="POST", params=params)
            )
        except HTTPError as err:
            # End pagination and checkpoint any documents if we hit the limit for the /companies/list endpoint.
            # If support for the /companies/scroll endpoint is added, we can re-evaluate whether to break or
            # fail here & tell users to use the /companies/scroll endpoint option.
            if err.code == 400 and bool(re.search(COMPANIES_LIST_LIMIT_REACHED_REGEX, err.message, re.DOTALL)):
                break
            else:
                raise

        if not exceeds_list_limit and response.total_count > COMPANIES_LIST_LIMIT:
            log.warning(f"{response.total_count} companies found."
                        " This is greater than the maximum number of companies returned by the /companies/list endpoint, and the connector could be missing data."
                        f" Contact Estuary support to request this stream use the /companies/scroll endpoint to retrieve more than {COMPANIES_LIST_LIMIT} companies.")
            exceeds_list_limit = True

        for company in response.data:
            segments_url = f"{API}/companies/{company.id}/segments"

            company_segments = CompanySegmentsResponse.model_validate_json(
                await http.request(log, segments_url)
            )

            for segment in company_segments.data:
                if segment.updated_at > last_seen_ts:
                    last_seen_ts = segment.updated_at
                if segment.updated_at > log_cursor_ts:
                    yield segment

        if current_page >= response.pages.total_pages:
            break

        current_page += 1
        params['page'] = current_page

    # Results are not returned sorted by a timestamp field,
    # so we can't yield a cursor until pagination is complete.
    if last_seen_ts > log_cursor_ts:
        yield _s_to_dt(last_seen_ts)
