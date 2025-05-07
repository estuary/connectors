from datetime import UTC, datetime
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor

from .models import (
    DynamicRecordModel,
    JSONRecord,
    XMLRecord,
)
from .sage import Sage


async def fetch_changes(
    object: str,
    sage: Sage,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[DynamicRecordModel | LogCursor, None]:
    assert isinstance(cursor, datetime)

    last = cursor
    async for rec in sage.fetch_since(object, cursor):
        last = max(last, rec.WHENMODIFIED)
        yield rec

    if last > cursor:
        yield last


async def fetch_page(
    object: str,
    sage: Sage,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[DynamicRecordModel | PageCursor, None]:
    assert isinstance(page, int | None)
    assert isinstance(cutoff, datetime)

    last: int | None = None
    async for rec in sage.fetch_all(object, page):
        last = rec.RECORDNO
        if rec.WHENMODIFIED < cutoff:
            yield rec

    if last is not None:
        yield last
