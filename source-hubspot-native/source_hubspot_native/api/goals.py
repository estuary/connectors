from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
    Iterable,
)

from estuary_cdk.capture.common import PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    Goals,
    Names,
    TimestampedId,
    TimestampedObject,
)
from .object_with_associations import (
    fetch_changes_with_associations,
    fetch_chunked_changes_with_associations,
)
from .search_objects import fetch_search_objects


def fetch_recent_goals(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[TimestampedObject[Goals], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[TimestampedId], PageCursor]:
        return await fetch_search_objects(
            Names.goals, log, http, since, until, page,
            should_crash_on_unordered_results=False,
        )

    return fetch_changes_with_associations(
        Names.goals, Goals, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_goals(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[TimestampedObject[Goals] | datetime, None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[TimestampedId], PageCursor]:
        return await fetch_search_objects(Names.goals, log, http, since, until, page)

    return fetch_chunked_changes_with_associations(
        Names.goals, Goals, do_fetch, log, http, with_history, since, until
    )
