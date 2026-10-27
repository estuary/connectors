import asyncio
from asyncio.exceptions import CancelledError
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from logging import Logger

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession
from estuary_cdk.utils import format_error_message

from ..models import (
    ContactListMembership,
    ContactListMembershipResponse,
    Names,
)
from .contact_lists import request_contact_lists, request_contact_lists_in_time_range
from .shared import (
    DELAYED_LAG,
    EPOCH_PLUS_ONE_SECOND,
    HUB,
)

AD_HOC_LIST_TYPE = "ADHOC"


async def _request_contact_list_memberships(
    http: HTTPSession,
    log: Logger,
    list_id: int,
    start: datetime,
    end: datetime | None = None,
) -> AsyncGenerator[ContactListMembership]:
    url = f"{HUB}/crm/v3/lists/{list_id}/memberships/join-order"
    params: dict[str, int | str] = {"limit": 250}

    while True:
        response = ContactListMembershipResponse.model_validate_json(
            await http.request(log, url, params=params),
            context=list_id,
        )

        for item in response.results:
            if end and item.membershipTimestamp >= end:
                continue
            if item.membershipTimestamp < start:
                # Results are newest-first, so we are now caught up. HubSpot's
                # OpenAPI spec says ascending; live responses contradict it, so
                # don't trust it.
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
    list_id: int,
    processing_type: str,
    context: ContactListMembershipControl,
    start: datetime,
    end: datetime | None = None,
) -> None:
    try:
        async for item in _request_contact_list_memberships(
            http, log, list_id, start, end
        ):
            await context.results.put(item)

        log.debug(
            "finished fetching memberships for list",
            {"listId": list_id},
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

            raise RuntimeError(
                f"Contact list {list_id} worker was unexpectedly cancelled: {msg}"
            )
        # If a different worker already failed, then this worker task was cancelled by
        # the TaskGroup as part of its exception handling and it's safe to propagate the
        # CancelledError.
        else:
            raise
    except BaseException as e:
        msg = format_error_message(e)
        if not context.first_error:
            context.first_error = msg

        log.error(
            f"Contact list {list_id} worker encountered an error.",
            {
                "exception": msg,
                "processing_type": processing_type,
            },
        )
        raise


async def _request_all_contact_list_memberships(
    http: HTTPSession, log: Logger, start: datetime, end: datetime | None = None
) -> AsyncGenerator[ContactListMembership]:
    log.info(
        "fetching contact list memberships",
        extra={
            "start": start.isoformat(),
            "end": end.isoformat() if end else None,
        },
    )

    control = ContactListMembershipControl()
    # Processing types are kept so a failing worker can report what the
    # lists search said about its list.
    all_lists: list[tuple[int, str]] = []
    adhoc_list_ids: list[int] = []
    async for item in request_contact_lists_in_time_range(
        http,
        log,
        # We only care about listIds, so we do not want to make an extra request
        #  asking for all the additional properties there are
        False,
        start,
        end,
    ):
        # The memberships endpoint rejects ADHOC lists with a 400.
        if item.processingType == AD_HOC_LIST_TYPE:
            adhoc_list_ids.append(item.listId)
        else:
            all_lists.append((item.listId, item.processingType))

    if adhoc_list_ids:
        log.warning(
            "skipping ADHOC contact lists",
            extra={"list_ids": adhoc_list_ids},
        )
    if not all_lists:
        return

    async with asyncio.TaskGroup() as tg:

        async def dispatcher() -> None:
            for list_id, processing_type in all_lists:
                _ = await control.task_slots.acquire()
                _ = tg.create_task(
                    _list_memberships_worker(
                        http, log, list_id, processing_type, control, start, end
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
) -> AsyncGenerator[ContactListMembership | PageCursor]:
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
        http, log, next_list.listId, EPOCH_PLUS_ONE_SECOND
    ):
        yield membership

    yield next_list.listId


async def fetch_contact_list_memberships(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[ContactListMembership | LogCursor]:
    assert isinstance(log_cursor, datetime)

    # We are deliberately not implementing the same delayed stream mechanism
    # used in CRM object APIs. We have no way to filter/sort by `hs_lastmodifieddate`
    # and we don't want to paginate through all items twice
    start_date = log_cursor - DELAYED_LAG
    now = datetime.now(tz=UTC)

    async for item in _request_all_contact_list_memberships(http, log, start_date):
        key = f"{item.listId}:{item.recordId}"

        if cache.should_yield(
            Names.contact_list_memberships, key, item.membershipTimestamp
        ):
            yield item

    # Entries older than this run's lower bound can never be looked up again,
    # since later runs start no earlier.
    evicted = cache.cleanup(Names.contact_list_memberships, start_date)
    log.info("evicted contact list memberships from cache", extra={"evicted": evicted})

    yield now
