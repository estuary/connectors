from datetime import UTC, datetime
from logging import Logger
from types import NoneType
from typing import (
    AsyncGenerator,
)

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession

from ..models import (
    OBJECT_TYPE_IDS,
    ContactList,
    ContactListMembership,
    ContactListMembershipResponse,
    ListSearch,
    Names,
)
from .shared import (
    ms_to_dt,
    HUB,
    EPOCH_PLUS_ONE_SECOND,
    DELAYED_LAG,
)
from .properties import fetch_properties

async def _request_list_page(
    http: HTTPSession, log: Logger, offset: int, prop_names: list[str] = []
) -> ListSearch:
    url = f"{HUB}/crm/v3/lists/search"

    request_body = {
        "count": 500,
        "offset": offset,
        "sort": "HS_LIST_ID",  # Required for list membership backfill checkpoints
        "additionalProperties": prop_names,
    }

    return ListSearch.model_validate_json(
        await http.request(
            log,
            url,
            method="POST",
            json=request_body,
        )
    )


async def request_contact_lists(
    http: HTTPSession, log: Logger, should_fetch_all_properties: bool
) -> AsyncGenerator[ContactList, None]:
    """
    The lists search API is different enough that it requires its own
    integration logic. Markedly, it is currently impossible to filter
    or sort results based on the `hs_lastmodifieddate` property.
    In-memory filtering is the most viable option at the moment.

    Results are in ascending list id order.
    """
    pagination_offset = 0

    if should_fetch_all_properties:
        all_props = (
            await fetch_properties(log, http, OBJECT_TYPE_IDS["contact_list"])
        ).results
        prop_names = [p.name for p in all_props]
    else:
        prop_names = ["hs_lastmodifieddate"]

    while True:
        response = await _request_list_page(http, log, pagination_offset, prop_names)

        for item in response.lists:
            if isinstance(item, ContactList):
                yield item

        pagination_offset = response.offset
        if not response.hasMore:
            return


async def check_contact_lists_access(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[ContactList, None]:
    """Lightweight wrapper for permission checking contact lists endpoint."""
    async for contact_list in request_contact_lists(http, log, False):
        yield contact_list


async def check_contact_list_memberships_access(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[ContactListMembership, None]:
    """Lightweight wrapper for permission checking contact list memberships endpoint."""
    async for contact_list in request_contact_lists(http, log, False):
        url = f"{HUB}/crm/v3/lists/{contact_list.listId}/memberships"

        response = ContactListMembershipResponse.model_validate_json(
            (await http.request(log, url, params={"limit": 1})), context=contact_list
        )
        for membership in response.results:
            yield membership


async def request_contact_lists_in_time_range(
    http: HTTPSession,
    log: Logger,
    should_fetch_all_properties: bool,
    start: datetime,
    end: datetime | None = None,
) -> AsyncGenerator[ContactList, None]:
    async for item in request_contact_lists(http, log, should_fetch_all_properties):
        last_modified_date = item.get_cursor_value()

        if last_modified_date < start:
            continue
        if end and last_modified_date >= end:
            continue

        yield item


async def fetch_contact_lists_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ContactList, None]:
    assert isinstance(page, NoneType)
    assert isinstance(cutoff, datetime)

    page_ts = (
        datetime.fromisoformat(page) if page is not None else EPOCH_PLUS_ONE_SECOND
    )

    async for item in request_contact_lists_in_time_range(
        http, log, True, page_ts, cutoff
    ):
        yield item

    return


async def fetch_contact_lists(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[ContactList | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    # We are deliberately not implementing the same delayed stream mechanism
    # used in CRM object APIs. We have no way to filter/sort by `hs_lastmodifieddate`
    # and we don't want to paginate through all items twice
    start_date = log_cursor - DELAYED_LAG
    now = datetime.now(tz=UTC)

    async for item in request_contact_lists_in_time_range(http, log, True, start_date):
        log.debug("processing contact list", {"listId": item.listId})
        timestamp = ms_to_dt(int(item.additionalProperties["hs_lastmodifieddate"]))

        if cache.should_yield(Names.contact_lists, item.listId, timestamp):
            yield item

    log.info("fetched all contact lists")
    yield now