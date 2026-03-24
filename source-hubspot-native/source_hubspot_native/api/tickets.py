from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
)

from estuary_cdk.http import HTTPSession
from pydantic import TypeAdapter

from ..models import (
    Names,
    OldRecentTicket,
    Ticket,
)
from .object_with_associations import fetch_changes_with_associations
from .search_objects import fetch_search_objects
from .shared import (
    ms_to_dt,
    HUB,
)


def fetch_recent_tickets(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Ticket], None]:

    async def fetch_ids():
        # This API will return a maximum of 1000 tickets, and does not appear to
        # ever return an error. It just ends at the 1000 most recently modified
        # tickets.
        url = f"{HUB}/crm-objects/v1/change-log/tickets"
        params = {"timestamp": int(since.timestamp() * 1000) - 1}

        result = TypeAdapter(list[OldRecentTicket]).validate_json(
            await http.request(log, url, params=params)
        )
        for r in result:
            yield (ms_to_dt(r.timestamp), str(r.objectId))

    return fetch_changes_with_associations(
        Names.tickets, Ticket, fetch_ids(), log, http, with_history, since, until
    )


def fetch_delayed_tickets(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Ticket], None]:

    return fetch_changes_with_associations(
        Names.tickets, Ticket,
        fetch_search_objects(Names.tickets, log, http, since, until),
        log, http, with_history, since, until,
    )
