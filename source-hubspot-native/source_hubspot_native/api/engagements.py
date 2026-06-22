import functools
from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
    Iterable,
)

from estuary_cdk.capture.common import PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    Engagement,
    EngagementsModifiedAfter,
    Names,
    OldRecentEngagements,
    TimestampedId,
    TimestampedObject,
)
from .object_with_associations import fetch_changes_with_associations
from .shared import (
    dt_to_ms,
    ms_to_dt,
    HUB,
)


async def _fetch_engagements_modified_after(
    log: Logger,
    http: HTTPSession,
    since: datetime,
    until: datetime,
    page: PageCursor,
    count: int,
) -> tuple[Iterable[TimestampedId], PageCursor]:
    # Unlike the "recent/modified" endpoint used by the realtime stream,
    # "modified/after" is cursor-paginated with no 10k offset cap and reads
    # forward from a timestamp. The results aren't ordered, so we fully
    # enumerate the result set here and return everything at once.
    url = f"{HUB}/engagements/v1/engagements/modified/after"
    output: list[TimestampedId] = []
    after: int | str = dt_to_ms(since)

    while True:
        params: dict[str, str | int] = {"after": after, "limit": 250}
        result = EngagementsModifiedAfter.model_validate_json(
            await http.request(log, url, params=params)
        )
        output.extend(
            TimestampedId(ts, str(r.engagement.id))
            for r in result.results
            if (ts := ms_to_dt(r.engagement.lastUpdated)) <= until
        )
        if not result.hasMore or not result.after:
            break
        after = result.after

    return output, None


async def _fetch_recently_modified_engagements(
    log: Logger, http: HTTPSession, since: datetime, page: PageCursor, count: int
) -> tuple[Iterable[TimestampedId], PageCursor]:
    if count >= 9_900:
        # The /engagements/recent/modified endpoint has a 10k limit on how many
        # items the API can return. We rely on the delayed stream that
        # uses the /engagements/modified/after endpoint to capture
        # any engagements the real-time stream missed due to this 10k limit.
        log.warning("limit of 9,900 recent engagements reached")
        return [], None

    url = f"{HUB}/engagements/v1/engagements/recent/modified"
    params = {"count": 100, "offset": page} if page else {"count": 1}

    result = OldRecentEngagements.model_validate_json(
        await http.request(log, url, params=params)
    )
    next_page: PageCursor = result.hasMore and result.offset
    records: list[TimestampedId] = []
    for r in result.results:
        ts = ms_to_dt(r.engagement.lastUpdated)
        records.append(TimestampedId(ts, str(r.engagement.id)))
        if ts <= since:
            # This endpoint returns records newest-first, so once a page reaches
            # one as old as `since` there's nothing older worth paging for.
            next_page = None

    return records, next_page


def fetch_recent_engagements(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[TimestampedObject[Engagement], None]:
    return fetch_changes_with_associations(
        Names.engagements,
        Engagement,
        functools.partial(_fetch_recently_modified_engagements, log, http, since),
        log,
        http,
        with_history,
        since,
        until,
    )


def fetch_delayed_engagements(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[TimestampedObject[Engagement], None]:
    return fetch_changes_with_associations(
        Names.engagements,
        Engagement,
        functools.partial(_fetch_engagements_modified_after, log, http, since, until),
        log,
        http,
        with_history,
        since,
        until,
    )
