import asyncio
from asyncio.exceptions import CancelledError
from dataclasses import dataclass, field
from datetime import UTC, datetime
from logging import Logger
from typing import (
    AsyncGenerator,
)

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession
from estuary_cdk.utils import format_error_message

from ..models import (
    ContactList,
    ContactListMembership,
    ContactListMembershipResponse,
    Names,
)
from .contact_lists import request_contact_lists_in_time_range, request_contact_lists
from .shared import (
    HUB,
    EPOCH_PLUS_ONE_SECOND,
    delayed_offset,
)

async def _request_contact_list_memberships(
    http: HTTPSession,
    log: Logger,
    contact_list: ContactList,
    start: datetime,
    end: datetime | None = None,
) -> AsyncGenerator[ContactListMembership, None]:
    url = f"{HUB}/crm/v3/lists/{contact_list.listId}/memberships/join-order"
    params: dict[str, int | str] = {"limit": 250}

    while True:
        response = ContactListMembershipResponse.model_validate_json(
            await http.request(log, url, params=params),
            context=contact_list,
        )

        for item in response.results:
            if end and item.membershipTimestamp >= end:
                continue
            if item.membershipTimestamp < start:
                # The endpoint's sorted in descending join date order, so we are now caught up
                return

            yield item

        if (next_cursor := response.get_next_cursor()) is None:
            return
        params["after"] = next_cursor


@dataclass
class ContactListMembershipControl:
    """Coordination for concurrent contact list membership fetches."""

    # We expect users to have tens of thousands of lists to fetch.
    # This semaphore limits the number of tasks we keep in memory at any given time
    task_slots: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(20))
    results: asyncio.Queue[ContactListMembership | None] = field(
        default_factory=lambda: asyncio.Queue(1)
    )
    finished_tasks: int = 0
    first_error: str | None = None


async def _list_memberships_worker(
    http: HTTPSession,
    log: Logger,
    contact_list: ContactList,
    context: ContactListMembershipControl,
    start: datetime,
    end: datetime | None = None,
) -> None:
    try:
        async for item in _request_contact_list_memberships(
            http, log, contact_list, start, end
        ):
            await context.results.put(item)

        log.debug(
            "finished fetching memberships for list",
            {"listId": contact_list.listId},
        )
        await context.results.put(None)
    except CancelledError as e:
        # If no worker has been cancelled or encountered an exception yet,
        # that means this worker was cancelled from something other than the
        # TaskGroup. A non-CancelledError should be raised so the
        # TaskGroup cancels the remaining tasks & propagates an exception
        # up through the connector.
        if not context.first_error:
            msg = format_error_message(e)
            context.first_error = msg

            raise Exception(
                f"Contact list {contact_list.listId} worker was unexpectedly cancelled: {msg}"
            )
        # If a different worker already failed, then this worker task was cancelled by
        # the TaskGroup as part of its exception handling and it's safe to propagate the
        # CancelledError.
        else:
            raise e
    except BaseException as e:
        msg = format_error_message(e)
        if not context.first_error:
            context.first_error = msg

        log.error(
            f"Contact list {contact_list.listId} worker encountered an error.",
            {
                "exception": msg,
            },
        )
        raise e


async def _request_all_contact_list_memberships(
    http: HTTPSession, log: Logger, start: datetime, end: datetime | None = None
) -> AsyncGenerator[ContactListMembership, None]:
    log.info(
        "fetching contact list memberships",
        {
            "start": start.isoformat(),
            "end": end.isoformat() if end else None,
        },
    )

    control = ContactListMembershipControl()
    all_lists = [
        item
        async for item in request_contact_lists_in_time_range(
            http,
            log,
            # We only care about listIds, so we do not want to make an extra request
            #  asking for all the additional properties there are
            False,
            start,
            end,
        )
    ]
    if not all_lists:
        return

    async with asyncio.TaskGroup() as tg:

        async def dispatcher() -> None:
            for contact_list in all_lists:
                _ = await control.task_slots.acquire()
                _ = tg.create_task(
                    _list_memberships_worker(
                        http, log, contact_list, control, start, end
                    )
                )

        _ = tg.create_task(dispatcher())

        while True:
            item = await control.results.get()
            if item is None:
                # A worker task has completed
                control.finished_tasks += 1
                control.task_slots.release()

                if control.finished_tasks == len(all_lists):
                    break
            else:
                yield item

        log.info("fetched all list memberships")


async def fetch_contact_list_memberships_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ContactListMembership | PageCursor, None]:
    assert isinstance(page, int | None)
    assert isinstance(cutoff, datetime)

    next_list = await anext(
        (
            item
            async for item in request_contact_lists(http, log, False)
            if page is None or item.listId > page
        ),
        None,
    )
    if next_list is None:
        return

    async for membership in _request_contact_list_memberships(
        http, log, next_list, EPOCH_PLUS_ONE_SECOND
    ):
        yield membership

    yield next_list.listId


async def fetch_contact_list_memberships(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[ContactListMembership | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    # We are deliberately not implementing the same delayed stream mechanism
    # used in CRM object APIs. We have no way to filter/sort by `hs_lastmodifieddate`
    # and we don't want to paginate through all items twice
    start_date = log_cursor - delayed_offset
    now = datetime.now(tz=UTC)

    async for item in _request_all_contact_list_memberships(http, log, start_date):
        key = f"{item.listId}:{item.membershipTimestamp}"

        if cache.should_yield(
            Names.contact_list_memberships, key, item.membershipTimestamp
        ):
            yield item

    yield now
