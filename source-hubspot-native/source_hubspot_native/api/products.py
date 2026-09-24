from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
    Iterable,
)

from estuary_cdk.capture.common import PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    Names,
    PageResult,
    Product,
    TimestampedId,
    TimestampedObject,
)
from .object_with_associations import (
    fetch_changes_with_associations,
    fetch_chunked_changes_with_associations,
)
from .search_objects import fetch_search_objects
from .shared import HUB


async def check_products_access(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[Product, None]:
    """Lightweight wrapper for permission checking the products endpoint."""
    url = f"{HUB}/crm/v3/objects/{Names.products}"

    response = PageResult[Product].model_validate_json(
        await http.request(log, url, params={"limit": 1})
    )

    for record in response.results:
        yield record


def fetch_recent_products(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[TimestampedObject[Product], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[TimestampedId], PageCursor]:
        return await fetch_search_objects(Names.products, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.products, Product, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_products(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[TimestampedObject[Product] | datetime, None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[TimestampedId], PageCursor]:
        return await fetch_search_objects(Names.products, log, http, since, until, page)

    return fetch_chunked_changes_with_associations(
        Names.products, Product, do_fetch, log, http, with_history
    )
