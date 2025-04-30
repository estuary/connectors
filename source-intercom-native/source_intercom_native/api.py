import asyncio
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
    Contact,
    NestedTag,
    ContactTagsResponse,
    ContactsSearchResponse,
    TicketsSearchResponse,
    ConversationsSearchResponse,
    ConversationResponse,
    SegmentsResponse,
    CompanyListResponse,
    CompanyScrollResponse,
    CompanySegmentsResponse,
)


API = "https://api.intercom.io"
SEARCH_PAGE_SIZE = 150
COMPANIES_LIST_LIMIT = 10_000

COMPANIES_LIST_LIMIT_REACHED_REGEX = r"page limit reached, please use scroll API"
COMPANIES_SCROLL_IN_USE_BY_OTHER_APPLICATION_REGEX = r"scroll already exists for this workspace"

companies_scroll_lock = asyncio.Lock()
parts_semaphore = asyncio.Semaphore(15)


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


def _generate_contacts_search_body(
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


def _generate_conversations_or_tickets_search_body(
        lower_bound: int,
        upper_bound: int,
        next_page_cursor: str | None = None
    ):
    # Intercom's search endpoints support complex queries within the request body. We filter and sort results
    # on the updated_at field, requesting all results with an updated at on or between the lower & upper bounds.
    #
    # Unlike the contacts search endpoint, the conversations and tickets search endpoints *do* filter based on exact Unix timestamps.
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


def _is_page_number_to_log(page_number: int, total_pages: int) -> bool:

    return total_pages > 1 and (
        page_number == 1 or
        page_number % 25 == 0 or
        page_number == total_pages
    )


def _is_large_date_window(start: int, end: int) -> bool:
    delta = _s_to_dt(end) - _s_to_dt(start)
    return delta > timedelta(hours=1)


async def _hydrate_contact(
    http: HTTPSession,
    contact: Contact,
    log: Logger,
) -> Contact:
    if contact.tags.has_more:
        url = f"{API}/contacts/{contact.id}/tags"
        response = ContactTagsResponse.model_validate_json(
            await http.request(log, url)
        )

        # Tags and nested tags have different shapes. We have to transform tags into nested tags
        # to ensure the tags in a Contact are always the same shape.
        nested_tags = []
        for tag in response.data:
            nested_tag = NestedTag.model_validate({
                "id": tag.id,
                "type": tag.type,
                "url": f"/tags/{tag.id}"
            })
            nested_tags.append(nested_tag)

        contact.tags.data = nested_tags
        contact.tags.has_more = False
    return contact


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
    body = _generate_contacts_search_body(start, end)

    should_log_progress = _is_large_date_window(start, end)
    pagination_ended_early = False
    while True:
        response = ContactsSearchResponse.model_validate_json(
                await http.request(log, url, "POST", json=body)
        )

        page_num = response.pages.page
        total_pages = response.pages.total_pages

        if total_pages == 0:
            break

        if should_log_progress and _is_page_number_to_log(page_num, total_pages):
            log.info(f"Processing page {page_num} of {total_pages}.", {
                'window_start': _s_to_dt(start),
                'window_end': _s_to_dt(end)
            })

        for contact in response.data:
            updated_at = contact.updated_at
            if updated_at < start:
                # We request that results are returned in descending order of updated_at,
                # so we stop processing results once we see one before the current date window.
                if should_log_progress and page_num > 1:
                    log.info(f"Ending pagination early after processing {page_num} pages. Remaining pages contain already replicated data.")

                pagination_ended_early = True
                break

            if updated_at > last_seen_ts:
                last_seen_ts = updated_at
            if updated_at > start:
                # Nested subresources within a contact are capped at 10 elements, even if more exist.
                # We hydrate the contact with the additional subresources if they aren't all present.
                yield await _hydrate_contact(http, contact, log)

        if pagination_ended_early or response.pages.next is None:
            break

        body = _generate_contacts_search_body(start, end, response.pages.next.starting_after)

    # Results are returned in descending order, so we can't yield a cursor until pagination is complete.
    if end == max_end:
        yield _s_to_dt(max_end)
    elif last_seen_ts > start:
        yield _s_to_dt(last_seen_ts)


async def fetch_tickets(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    start = _dt_to_s(log_cursor)
    end = int(datetime.now(tz=UTC).timestamp())

    last_seen_ts = start

    url = f"{API}/tickets/search"
    body = _generate_conversations_or_tickets_search_body(start, end)

    while True:
        response = TicketsSearchResponse.model_validate_json(
                await http.request(log, url, "POST", json=body)
        )

        page_num = response.pages.page
        total_pages = response.pages.total_pages

        if total_pages == 0:
            break

        if _is_page_number_to_log(page_num, total_pages):
            log.info(f"Processing page {page_num} of {total_pages}.", {
                'start': _s_to_dt(start),
            })

        if (
            last_seen_ts > start
            and response.tickets
            and response.tickets[0].updated_at > last_seen_ts
        ):
            yield _s_to_dt(last_seen_ts)

        for ticket in response.tickets:
            if ticket.updated_at > last_seen_ts:
                last_seen_ts = ticket.updated_at

            if ticket.updated_at > start:
                yield ticket

        if response.pages.next is None:
            yield _s_to_dt(last_seen_ts)
            break

        body = _generate_conversations_or_tickets_search_body(start, end, response.pages.next.starting_after)


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
    body = _generate_conversations_or_tickets_search_body(start, end)

    count = 0

    while True:
        response = ConversationsSearchResponse.model_validate_json(
                await http.request(log, url, "POST", json=body)
        )

        page_num = response.pages.page
        total_pages = response.pages.total_pages

        if total_pages == 0:
            break

        if _is_page_number_to_log(page_num, total_pages):
            log.info(f"Processing page {page_num} of {total_pages}.", {
                'start': _s_to_dt(start),
            })

        for conversation in response.conversations:
            # It's possible to update multiple conversations at the same time in Intercom, making it difficult to find
            # a safe spot to checkpoint between pages returned by the API. To checkpoint more frequently when processing these bulk updates,
            # conversations looks for any safe spot to checkpoint after yielding SEARCH_PAGE_SIZE documents.
            if conversation.updated_at > last_seen_ts:
                if count >= SEARCH_PAGE_SIZE:
                    yield _s_to_dt(last_seen_ts)
                    count = 0

                last_seen_ts = conversation.updated_at

            if conversation.updated_at > start:
                yield conversation
                count += 1

        if response.pages.next is None:
            yield _s_to_dt(last_seen_ts)
            break

        body = _generate_conversations_or_tickets_search_body(start, end, response.pages.next.starting_after)


async def fetch_conversations_parts(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    conversation_ids: list[str] = []

    async for conversation_or_dt in fetch_conversations(http, log, log_cursor):
        if isinstance(conversation_or_dt, TimestampedResource):
            conversation_ids.append(conversation_or_dt.id)
        else:
            for coro in asyncio.as_completed(
                [
                    _fetch_parts(http, log, conversation_id)
                    for conversation_id in conversation_ids
                ]
            ):
                parts = await coro
                for part in parts:
                    yield part

            conversation_ids = []

            yield conversation_or_dt


async def _fetch_parts(
        http: HTTPSession,
        log: Logger,
        conversation_id: str
) -> list[TimestampedResource]:
    async with parts_semaphore:
        url = f"{API}/conversations/{conversation_id}"

        response = ConversationResponse.model_validate_json(
            await http.request(log, url)
        )

        for part in response.conversation_parts.conversation_parts:
            # Add conversation_id to the conversation part to align with the Airbyte connector.
            part.conversation_id = conversation_id #type: ignore

        return response.conversation_parts.conversation_parts


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


async def _list_companies(
        http: HTTPSession,
        log: Logger,
) -> AsyncGenerator[TimestampedResource, None]:
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
            if err.code == 400 and bool(re.search(COMPANIES_LIST_LIMIT_REACHED_REGEX, err.message, re.DOTALL)):
                break
            else:
                raise

        if not exceeds_list_limit and response.total_count > COMPANIES_LIST_LIMIT:
            log.warning(f"{response.total_count} companies found."
                        " This is greater than the maximum number of companies returned by the /companies/list endpoint, and the connector could be missing data."
                        f" Consider configuring the connector to use the /companies/scroll endpoint to retrieve more than {COMPANIES_LIST_LIMIT} companies.")
            exceeds_list_limit = True

        for company in response.data:
            yield company

        if current_page >= response.pages.total_pages:
            break

        current_page += 1
        params['page'] = current_page


async def _scroll_companies(
        http: HTTPSession,
        log: Logger,
) -> AsyncGenerator[TimestampedResource, None]:
    url = f"{API}/companies/scroll"
    params = {}

    # The Intercom API only lets a single "scroll" through /companies/scroll happen at a time,
    # and the companies_scroll_lock prevents the connector from attempting multiple concurrent "scrolls".
    async with companies_scroll_lock:
        while True:
            try:
                response = CompanyScrollResponse.model_validate_json(
                    await http.request(log, url, method="GET", params=params)
                )
            except HTTPError as err:
                if err.code == 400 and bool(re.search(COMPANIES_SCROLL_IN_USE_BY_OTHER_APPLICATION_REGEX, err.message, re.DOTALL)):
                    log.error(
                        "Unable to access the /companies/scroll endpoint because it's in use by a different application."
                        " Please ensure no other application is using the /companies/scroll endpoint or "
                        " configure the connector to use the alternative /companies/list endpoint."
                        )
                raise

            if len(response.data) == 0:
                break

            for company in response.data:
                yield company

            params['scroll_param'] = response.scroll_param


async def fetch_companies(
        http: HTTPSession,
        use_list_endpoint: bool,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    log_cursor_ts = _dt_to_s(log_cursor)
    last_seen_ts = log_cursor_ts

    companies_func = _list_companies if use_list_endpoint else _scroll_companies

    async for company in companies_func(http, log):
        if company.updated_at > last_seen_ts:
            last_seen_ts = company.updated_at
        if company.updated_at > log_cursor_ts:
            yield company

    # Results are not returned sorted by a timestamp field,
    # so we can't yield a cursor until pagination is complete.
    if last_seen_ts > log_cursor_ts:
        yield _s_to_dt(last_seen_ts)


async def fetch_company_segments(
        http: HTTPSession,
        use_list_endpoint: bool,
        log: Logger,
        log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    log_cursor_ts = _dt_to_s(log_cursor)
    last_seen_ts = log_cursor_ts

    company_ids: list[str] = []

    companies_func = _list_companies if use_list_endpoint else _scroll_companies

    # Fetch & buffer company ids to avoid using the /companies/scroll endpoint for longer than necessary and
    # avoid exceeding the one minute timeout for a single "scroll" if we were to fetch segments while scrolling.
    async for company in companies_func(http, log):
        company_ids.append(company.id)

    for id in company_ids:
        segments_url = f"{API}/companies/{id}/segments"

        try:
            company_segments = CompanySegmentsResponse.model_validate_json(
                await http.request(log, segments_url)
            )
        except HTTPError as err:
            if err.code == 404 and 'Company Not Found' in err.message:
                continue
            else:
                raise

        for segment in company_segments.data:
            if segment.updated_at > last_seen_ts:
                last_seen_ts = segment.updated_at
            if segment.updated_at > log_cursor_ts:
                yield segment

    # Results are not returned sorted by a timestamp field,
    # so we can't yield a cursor until pagination is complete.
    if last_seen_ts > log_cursor_ts:
        yield _s_to_dt(last_seen_ts)
