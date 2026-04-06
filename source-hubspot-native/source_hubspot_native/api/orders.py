from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
)

from estuary_cdk.http import HTTPSession

from ..models import (
    Names,
    Order,
)
from .object_with_associations import fetch_changes_with_associations
from .search_objects import fetch_search_objects


def fetch_recent_orders(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Order], None]:

    return fetch_changes_with_associations(
        Names.orders, Order,
        fetch_search_objects(Names.orders, log, http, since, until, ignore_out_of_order_results=True),
        log, http, with_history, since, until,
    )


def fetch_delayed_orders(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Order], None]:

    return fetch_changes_with_associations(
        Names.orders, Order,
        fetch_search_objects(Names.orders, log, http, since, until),
        log, http, with_history, since, until,
    )
