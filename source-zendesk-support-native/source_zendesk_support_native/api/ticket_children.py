import asyncio
import functools
import itertools
from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator, Callable, TypeVar

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    ZendeskResource,
    TimestampedResource,
    AbbreviatedTicket,
    IncrementalCursorPaginatedResponse,
    SideConversation,
    SideConversationsResponse,
    TicketChildResourceValidationContext,
    TicketsListResponse,
    TicketsShowManyResponse,
)

from .shared import (
    CURSOR_PAGINATION_PAGE_SIZE,
    MIN_INCREMENTAL_WINDOW_SIZE,
    url_base,
    dt_to_s,
    s_to_dt,
    is_timestamp,
)

from .cursor_exports import (
    _fetch_incremental_cursor_export_resources,
)
from .incremental_keyset_paginated import fetch_incremental_keyset_paginated_ids_by_page

# Zendesk's rate limit is shared account-wide with the customer's other integrations,
# so each binding keeps only a few per-ticket requests in flight.
TICKET_CHILD_CONCURRENCY = 5

# Zendesk documents that /tickets/show_many accepts up to 100 ids.
SHOW_MANY_MAX_IDS = 100


async def _fetch_ticket_child_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    ticket_id: int,
    log: Logger,
) -> AsyncGenerator[ZendeskResource, None]:
        url = f"{url_base(subdomain)}/tickets/{ticket_id}/{path}"
        params: dict[str, str | int] = {
            "page[size]": CURSOR_PAGINATION_PAGE_SIZE,
        }
        context = TicketChildResourceValidationContext(ticket_id=ticket_id)

        while True:
            response = response_model.model_validate_json(
                await http.request(log, url, params=params),
                context=context
            )

            for resource in response.resources:
                yield resource

            if not response.meta.has_more:
                break

            if response.meta.after_cursor:
                params["page[after]"] = response.meta.after_cursor


T = TypeVar("T")


async def _fetch_ticket_children_concurrently(
    ticket_ids: list[int],
    fetch_children: Callable[[int], AsyncGenerator[T, None]],
) -> AsyncGenerator[T, None]:
    """
    Yields the children of every ticket in `ticket_ids`, in no particular
    order, keeping at most TICKET_CHILD_CONCURRENCY tickets in flight or
    buffered at a time. Raises the first error any ticket's fetch raises.
    """
    semaphore = asyncio.Semaphore(TICKET_CHILD_CONCURRENCY)

    async def _collect(ticket_id: int) -> list[T]:
        await semaphore.acquire()
        return [child async for child in fetch_children(ticket_id)]

    tasks = [asyncio.create_task(_collect(ticket_id)) for ticket_id in ticket_ids]
    try:
        for next_done in asyncio.as_completed(tasks):
            for child in await next_done:
                yield child
            # Released only after a ticket's children are yielded so memory
            # stays bounded when emitting is slower than fetching.
            semaphore.release()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def fetch_ticket_child_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, tuple)
    cursor = log_cursor[0]
    assert isinstance(cursor, str)

    start_date: datetime | None = None

    if is_timestamp(cursor):
        start_date = s_to_dt(int(cursor))
        cursor = None

    tickets_generator = _fetch_incremental_cursor_export_resources(http, subdomain, "tickets", start_date, cursor, log)

    tickets: list[AbbreviatedTicket] = []

    while True:
        # Fetching comments for each ticket as we're streaming a tickets response often triggers aiohttp's TimeoutError.
        # To avoid these TimeoutErrors, we fetch all ticket ids in a single response, then fetch the child resources for 
        # those ticket ids.

        next_page_cursor: str | None = None
        async for result in tickets_generator:
            if isinstance(result, TimestampedResource):
                tickets.append(AbbreviatedTicket(
                    id=result.id, 
                    status=result.status,
                    updated_at=result.updated_at
                ))
            elif isinstance(result, str):
                next_page_cursor = result
                break

        if len(tickets) > 0 and next_page_cursor:
            async for child_resource in _fetch_ticket_children_concurrently(
                [ticket.id for ticket in tickets if ticket.status != 'deleted'],
                functools.partial(_fetch_ticket_child_resources, http, subdomain, path, response_model, log=log),
            ):
                yield child_resource

            yield (next_page_cursor,)
            tickets = []
        elif not next_page_cursor:
            break


async def backfill_ticket_child_resources(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ZendeskResource | PageCursor, None]:
    if page is not None:
        assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    tickets_generator = _fetch_incremental_cursor_export_resources(http, subdomain, "tickets", start_date, page, log)

    tickets: list[AbbreviatedTicket] = []

    while True:
        next_page_cursor: str | None = None
        async for result in tickets_generator:
            if isinstance(result, TimestampedResource):
                tickets.append(AbbreviatedTicket(
                    id=result.id, 
                    status=result.status,
                    updated_at=result.updated_at
                ))
            elif isinstance(result, str):
                next_page_cursor = result
                break

        if len(tickets) > 0 and next_page_cursor:
            tickets_before_cutoff = list(itertools.takewhile(lambda t: t.updated_at < cutoff, tickets))

            async for child_resource in _fetch_ticket_children_concurrently(
                [ticket.id for ticket in tickets_before_cutoff if ticket.status != 'deleted'],
                functools.partial(_fetch_ticket_child_resources, http, subdomain, path, response_model, log=log),
            ):
                yield child_resource

            if len(tickets_before_cutoff) < len(tickets):
                return

            yield next_page_cursor
            tickets = []
        elif not next_page_cursor:
            break


async def _fetch_side_conversations(
    http: HTTPSession,
    subdomain: str,
    ticket_id: int,
    log: Logger,
) -> AsyncGenerator[SideConversation, None]:
    # Side conversations are scoped per-ticket: GET /tickets/{ticket_id}/side_conversations.
    # The endpoint uses classic next_page URL-based pagination.
    url: str | None = f"{url_base(subdomain)}/tickets/{ticket_id}/side_conversations"

    while url:
        response = SideConversationsResponse.model_validate_json(
            await http.request(log, url)
        )

        for resource in response.side_conversations:
            yield resource

        url = response.next_page


# Side conversations are ticket child resources, but they're validated with
# a different Pydantic model than all other ticket child resources so the
# side_conversations stream can't reuse
# `fetch_ticket_child_resources`/`backfill_ticket_child_resources` as-is.
# That refactor would take more effort than I want to spend on this, 
# so I'm accepting this duplicate code & defering that refactor to
# some future point in time.
async def fetch_side_conversations(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[SideConversation | LogCursor, None]:
    assert isinstance(log_cursor, tuple)
    cursor = log_cursor[0]
    assert isinstance(cursor, str)

    start_date: datetime | None = None

    if is_timestamp(cursor):
        start_date = s_to_dt(int(cursor))
        cursor = None

    tickets_generator = _fetch_incremental_cursor_export_resources(
        http, subdomain, "tickets", start_date, cursor, log
    )

    tickets: list[AbbreviatedTicket] = []

    while True:
        next_page_cursor: str | None = None
        async for result in tickets_generator:
            if isinstance(result, TimestampedResource):
                tickets.append(AbbreviatedTicket(
                    id=result.id,
                    status=result.status,
                    updated_at=result.updated_at,
                ))
            elif isinstance(result, str):
                next_page_cursor = result
                break

        if len(tickets) > 0 and next_page_cursor:
            async for side_conv in _fetch_ticket_children_concurrently(
                [ticket.id for ticket in tickets if ticket.status != 'deleted'],
                functools.partial(_fetch_side_conversations, http, subdomain, log=log),
            ):
                yield side_conv

            yield (next_page_cursor,)
            tickets = []
        elif not next_page_cursor:
            break


async def backfill_side_conversations(
    http: HTTPSession,
    subdomain: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[SideConversation | PageCursor, None]:
    if page is not None:
        assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    tickets_generator = _fetch_incremental_cursor_export_resources(
        http, subdomain, "tickets", start_date, page, log
    )

    tickets: list[AbbreviatedTicket] = []

    while True:
        next_page_cursor: str | None = None
        async for result in tickets_generator:
            if isinstance(result, TimestampedResource):
                tickets.append(AbbreviatedTicket(
                    id=result.id,
                    status=result.status,
                    updated_at=result.updated_at,
                ))
            elif isinstance(result, str):
                next_page_cursor = result
                break

        if len(tickets) > 0 and next_page_cursor:
            tickets_before_cutoff = list(itertools.takewhile(lambda t: t.updated_at < cutoff, tickets))

            async for side_conv in _fetch_ticket_children_concurrently(
                [ticket.id for ticket in tickets_before_cutoff if ticket.status != "deleted"],
                functools.partial(_fetch_side_conversations, http, subdomain, log=log),
            ):
                yield side_conv

            if len(tickets_before_cutoff) < len(tickets):
                return

            yield next_page_cursor
            tickets = []
        elif not next_page_cursor:
            break


async def fetch_ticket_metrics(
    http: HTTPSession,
    subdomain: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZendeskResource | LogCursor, None]:
    assert isinstance(log_cursor, tuple)
    cursor = log_cursor[0]
    assert isinstance(cursor, str)

    start_date: datetime | None = None

    if is_timestamp(cursor):
        start_date = s_to_dt(int(cursor))
        cursor = None

    sideload_params = {
        "include": "metric_sets"
    }

    generator = _fetch_incremental_cursor_export_resources(http, subdomain, "tickets", start_date, cursor, log, sideload_params)

    async for result in generator:
        if isinstance(result, str):
            yield (result,)
        else:
            metrics = result.metric_set
            # Deleted tickets have no metrics, so we have to check that the metric set exists before yielding it.
            if metrics is not None:
                yield ZendeskResource.model_validate(metrics)


async def backfill_ticket_metrics(
    http: HTTPSession,
    subdomain: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ZendeskResource | PageCursor, None]:
    if page is not None:
        assert isinstance(page, str)
    assert isinstance(cutoff, datetime)

    sideload_params = {
        "include": "metric_sets"
    }

    generator = _fetch_incremental_cursor_export_resources(http, subdomain, "tickets", start_date, page, log, sideload_params)

    async for result in generator:

        if isinstance(result, str):
            yield result
        elif result.updated_at < cutoff:
            metrics = result.metric_set
            # Deleted tickets have no metrics, so we have to check that the metric set exists before yielding it.
            if metrics is not None:
                yield ZendeskResource.model_validate(metrics)
        else:
            return


async def fetch_child_resources_of_tickets(
    http: HTTPSession,
    subdomain: str,
    path: str,
    response_model: type[IncrementalCursorPaginatedResponse],
    ticket_ids: list[int],
    log: Logger,
) -> AsyncGenerator[ZendeskResource, None]:
    async for child in _fetch_ticket_children_concurrently(
        ticket_ids,
        functools.partial(_fetch_ticket_child_resources, http, subdomain, path, response_model, log=log),
    ):
        yield child


async def fetch_side_conversations_of_tickets(
    http: HTTPSession,
    subdomain: str,
    ticket_ids: list[int],
    log: Logger,
) -> AsyncGenerator[SideConversation, None]:
    async for side_conversation in _fetch_ticket_children_concurrently(
        ticket_ids,
        functools.partial(_fetch_side_conversations, http, subdomain, log=log),
    ):
        yield side_conversation


async def fetch_metric_sets_of_tickets(
    http: HTTPSession,
    subdomain: str,
    ticket_ids: list[int],
    log: Logger,
) -> AsyncGenerator[ZendeskResource, None]:
    url = f"{url_base(subdomain)}/tickets/show_many"

    for start in range(0, len(ticket_ids), SHOW_MANY_MAX_IDS):
        batch = ticket_ids[start:start + SHOW_MANY_MAX_IDS]
        response = TicketsShowManyResponse.model_validate_json(
            await http.request(log, url, params={
                "ids": ",".join(str(ticket_id) for ticket_id in batch),
                "include": "metric_sets",
            })
        )

        # show_many returns every requested ticket on one page, so we can't
        # assume metric sets past a next page would ever be read.
        if response.next_page is not None:
            raise RuntimeError(f"/tickets/show_many returned a next page for {len(batch)} ids.")

        for metric_set in response.metric_sets:
            yield metric_set


async def fetch_incremental_keyset_paginated_ticket_children[ChildT](
    http: HTTPSession,
    subdomain: str,
    fetch_children: Callable[[list[int], Logger], AsyncGenerator[ChildT, None]],
    lag: timedelta,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ChildT | LogCursor, None]:
    """
    Yields the children of every ticket updated after `log_cursor` and at or
    before the horizon, then the last of those tickets' updated_at. A yielded
    datetime means the children of every ticket updated at or before it have
    been yielded.

              log_cursor      log_cursor + 1s       horizon
    ───────────────┼─────────────────┼──────────────────┼──────▶ time (1s ticks)
                   │                 │                  │
    page[after] ───┼─────────────────[══════════════════╪══════▶
    tickets ───────┼─────────────────[══════════════════]
                   │                 │                  └─ now - lag
                   │                 └─ seek (log_cursor + 1s, 0)
                   └─ the children of every ticket updated at or
                      before this second were already emitted

    A changed ticket's children are all fetched and emitted again. Each page of
    tickets is read completely before any children are fetched, so no
    response stays open while they are.
    """
    assert isinstance(log_cursor, datetime)

    horizon = (datetime.now(tz=UTC) - lag).replace(microsecond=0)

    if horizon - log_cursor < MIN_INCREMENTAL_WINDOW_SIZE:
        return

    generator = fetch_incremental_keyset_paginated_ids_by_page(
        http, subdomain, "tickets", "tickets", TicketsListResponse.REQUEST_PARAMS, log_cursor, horizon, log,
    )

    ticket_ids: list[int] = []
    last_seen_dt: datetime | None = None

    async for result in generator:
        if isinstance(result, datetime):
            async for child in fetch_children(ticket_ids, log):
                yield child
            ticket_ids = []
            yield result
        else:
            ticket_ids.append(result.id)
            last_seen_dt = result.updated_at

    if ticket_ids:
        async for child in fetch_children(ticket_ids, log):
            yield child

    if last_seen_dt is not None:
        yield last_seen_dt


async def backfill_incremental_keyset_paginated_ticket_children[ChildT](
    http: HTTPSession,
    subdomain: str,
    fetch_children: Callable[[list[int], Logger], AsyncGenerator[ChildT, None]],
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ChildT | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    if page is None:
        lower_bound = start_date
    else:
        assert isinstance(page, int)
        lower_bound = s_to_dt(page)

    generator = fetch_incremental_keyset_paginated_ids_by_page(
        http, subdomain, "tickets", "tickets", TicketsListResponse.REQUEST_PARAMS, lower_bound, cutoff, log,
    )

    ticket_ids: list[int] = []

    async for result in generator:
        if isinstance(result, datetime):
            async for child in fetch_children(ticket_ids, log):
                yield child
            ticket_ids = []
            yield dt_to_s(result)
        else:
            ticket_ids.append(result.id)

    if ticket_ids:
        async for child in fetch_children(ticket_ids, log):
            yield child
