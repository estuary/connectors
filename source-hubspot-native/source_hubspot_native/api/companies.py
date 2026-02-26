from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
    Iterable,
)

from estuary_cdk.capture.common import PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    Company,
    Names,
    OldRecentCompanies,
)
from .object_with_associations import fetch_changes_with_associations
from .search_objects import fetch_search_objects
from .shared import (
    ms_to_dt,
    HUB,
)


def fetch_recent_companies(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Company], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        if count >= 9_900:
            log.warn("limit of 9,900 recent companies reached")
            return [], None

        url = f"{HUB}/companies/v2/companies/recent/modified"
        params = {"count": 100, "offset": page} if page else {"count": 1}

        result = OldRecentCompanies.model_validate_json(
            await http.request(log, url, params=params)
        )
        return (
            (ms_to_dt(r.properties.hs_lastmodifieddate.timestamp), str(r.companyId))
            for r in result.results
        ), result.hasMore and result.offset

    return fetch_changes_with_associations(
        Names.companies, Company, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_companies(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Company], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(
            Names.companies, log, http, since, until, page
        )

    return fetch_changes_with_associations(
        Names.companies, Company, do_fetch, log, http, with_history, since, until
    )
