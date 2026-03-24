from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
)

from estuary_cdk.http import HTTPSession

from ..models import (
    LineItem,
    Names,
)
from .object_with_associations import fetch_changes_with_associations
from .search_objects import fetch_search_objects


def fetch_recent_line_items(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, LineItem], None]:

    return fetch_changes_with_associations(
        Names.line_items, LineItem,
        fetch_search_objects(Names.line_items, log, http, since, until),
        log, http, with_history, since, until,
    )


def fetch_delayed_line_items(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, LineItem], None]:

    return fetch_changes_with_associations(
        Names.line_items, LineItem,
        fetch_search_objects(Names.line_items, log, http, since, until),
        log, http, with_history, since, until,
    )
