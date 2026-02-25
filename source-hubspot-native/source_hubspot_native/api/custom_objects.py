from datetime import datetime
from logging import Logger
from typing import (
    AsyncGenerator,
    Iterable,
)

from estuary_cdk.capture.common import PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    CustomObject,
    CustomObjectSchema,
    PageResult,
)
from .object_with_associations import fetch_changes_with_associations
from .search_objects import fetch_search_objects
from .shared import HUB


def fetch_recent_custom_objects(
    object_name: str,
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, CustomObject], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(object_name, log, http, since, until, page)

    return fetch_changes_with_associations(
        object_name, CustomObject, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_custom_objects(
    object_name: str,
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime,
) -> AsyncGenerator[tuple[datetime, str, CustomObject], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(object_name, log, http, since, until, page)

    return fetch_changes_with_associations(
        object_name, CustomObject, do_fetch, log, http, with_history, since, until
    )

async def list_custom_objects(
    log: Logger,
    http: HTTPSession,
) -> list[str]:

    url = f"{HUB}/crm/v3/schemas"
    # Note: The schemas endpoint always returns all items in a single call, so there's never
    # pagination.
    result = PageResult[CustomObjectSchema].model_validate_json(
        await http.request(log, url, method="GET")
    )

    return [r.name for r in result.results if not r.archived]
