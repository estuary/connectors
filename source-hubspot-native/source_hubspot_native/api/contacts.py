from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
)

from estuary_cdk.http import HTTPSession

from ..models import (
    Contact,
    Names,
    OldRecentContacts,
)
from .object_with_associations import fetch_changes_with_associations
from .search_objects import fetch_search_objects
from .shared import (
    ms_to_dt,
    HUB,
)

def fetch_recent_contacts(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Contact], None]:

    async def fetch_ids():
        count = 0
        page = None
        while True:
            if count >= 9_900:
                # There is actually no documented limit on the number of contacts
                # that can be returned by this API, other than that it goes back a
                # maximum of 30 days. But since there is no way to filter the
                # response by `until`, we impose the same limit on the number of
                # recent IDs that will be fetched here as other ID fetchers to
                # prevent cases of trying to cycle through huge numbers of results
                # if the LogCursor hasn't been updated in a long time.
                log.warn("limit of 9,900 recent contacts reached")
                return

            url = f"{HUB}/contacts/v1/lists/recently_updated/contacts/recent"
            params = {"count": 100, "timeOffset": page} if page else {"count": 1}

            result = OldRecentContacts.model_validate_json(
                await http.request(log, url, params=params)
            )
            for r in result.contacts:
                yield (ms_to_dt(int(r.properties.lastmodifieddate.value)), str(r.vid))
                count += 1

            if not (result.has_more and result.time_offset):
                return
            page = result.time_offset

    return fetch_changes_with_associations(
        Names.contacts, Contact, fetch_ids(), log, http, with_history, since, until
    )


def fetch_delayed_contacts(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Contact], None]:

    return fetch_changes_with_associations(
        Names.contacts, Contact,
        fetch_search_objects(Names.contacts, log, http, since, until, "lastmodifieddate"),
        log, http, with_history, since, until,
    )
