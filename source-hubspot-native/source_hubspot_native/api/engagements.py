from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
)

from estuary_cdk.http import HTTPSession

from ..models import (
    Engagement,
    Names,
    OldRecentEngagements,
)
from .object_with_associations import fetch_changes_with_associations
from .shared import (
    ms_to_dt,
    HUB,
    MustBackfillBinding,
)


async def _fetch_engagement_ids(
    log: Logger, http: HTTPSession, since: datetime,
):
    count = 0
    page = None
    while True:
        if count >= 9_900:
            # "Engagements" as we are capturing them has a 10k limit on how many
            # items the API can return, and there is no other API that can be used
            # to get them within a certain time window. The only option here is to
            # re-backfill.
            log.warn("limit of 9,900 recent engagements reached")
            raise MustBackfillBinding

        url = f"{HUB}/engagements/v1/engagements/recent/modified"
        params = {"count": 100, "offset": page} if page else {"count": 1}

        result = OldRecentEngagements.model_validate_json(
            await http.request(log, url, params=params)
        )
        for r in result.results:
            ts = ms_to_dt(r.engagement.lastUpdated)
            # This API returns results newest-first, so once we
            # see a record at or before `since` there's nothing
            # left worth fetching.
            if ts <= since:
                return
            yield (ts, str(r.engagement.id))
            count += 1

        if not (result.hasMore and result.offset):
            return
        page = result.offset


def fetch_recent_engagements(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Engagement], None]:
    return fetch_changes_with_associations(
        Names.engagements,
        Engagement,
        _fetch_engagement_ids(log, http, since),
        log,
        http,
        with_history,
        since,
        until,
    )


def fetch_delayed_engagements(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Engagement], None]:
    # There is no way to fetch engagements other than starting with the most
    # recent and reading backward, so this is the same process as fetching
    # "recent" engagements. It relies on the filtering in
    # `fetch_changes_with_associations` to ignore the more recent items.
    return fetch_changes_with_associations(
        Names.engagements,
        Engagement,
        _fetch_engagement_ids(log, http, since),
        log,
        http,
        with_history,
        since,
        until,
    )
