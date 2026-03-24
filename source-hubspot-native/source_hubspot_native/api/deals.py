from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
)

from estuary_cdk.http import HTTPSession

from ..models import (
    Deal,
    Names,
    OldRecentDeals,
)
from .object_with_associations import fetch_changes_with_associations
from .search_objects import fetch_search_objects
from .shared import (
    ms_to_dt,
    HUB,
)


def fetch_recent_deals(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Deal], None]:

    async def fetch_ids():
        count = 0
        page = None
        while True:
            if count >= 9_900:
                log.warn("limit of 9,900 recent deals reached")
                return

            url = f"{HUB}/deals/v1/deal/recent/modified"
            params = {"count": 100, "offset": page} if page else {"count": 1}

            result = OldRecentDeals.model_validate_json(
                await http.request(log, url, params=params)
            )
            for r in result.results:
                yield (ms_to_dt(r.properties.hs_lastmodifieddate.timestamp), str(r.dealId))
                count += 1

            if not (result.hasMore and result.offset):
                return
            page = result.offset

    return fetch_changes_with_associations(
        Names.deals, Deal, fetch_ids(), log, http, with_history, since, until
    )


def fetch_delayed_deals(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Deal], None]:

    return fetch_changes_with_associations(
        Names.deals, Deal,
        fetch_search_objects(Names.deals, log, http, since, until),
        log, http, with_history, since, until,
    )
