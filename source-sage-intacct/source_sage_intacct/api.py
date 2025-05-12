from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import AsyncGenerator

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.capture.common import LogCursor, PageCursor

from .models import (
    IncrementalResource,
    SnapshotResource,
)
from .sage import Sage


# This function is kind of intense, but what it's doing is repeatedly issuing
# queries to the Sage API to walk records that have a WHENMODIFIED timestamp
# greater than the cursor, in ascending timestamp order. There are a number of
# things to consider when doing this:
# - The API returns a fixed amount of records per response, so more than 1
#   request is going to be needed if there are a lot of changes.
# - Each request is an entirely new query, so it's not sufficient to use offset
#   based pagination: Consider a case where records get deleted concurrently
#   with our queries, where if we were to send a request "starting after the
#   2000'th", that might end up skipping some records that have shifted up in
#   the overall order.
# - It's possible for many records to have the same WHENMODIFIED timestamp,
#   meaning we don't know if there are more records with the same timestamp as
#   the final record on this page at the start of the next page, and it's also
#   possible for "cycles" to occur if every single record on the page has the
#   same timestamp.
#
# These factors all influence a strategy where only the last timestamp we know
# we have seen all the records for is checkpointed, and a separate query can be
# used for cycle breaking which is based on sorting by RECORDNO at a specific
# WHENMODIFIED timestamp.
#
# At some point it may be worth generalizing this into a more generic version,
# since it is a fairly common pattern in APIs.
async def fetch_changes(
    object: str,
    sage: Sage,
    horizon: timedelta | None,
    page_size: int,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    assert isinstance(cursor, datetime)
    now = datetime.now(UTC)

    last_completed_ts = cursor
    total_count = 0
    while True:
        cursor = last_completed_ts
        last_record_ts = None
        this_count = 0
        async for rec in sage.fetch_since(object, cursor):
            rec = IncrementalResource.model_validate(rec.model_dump())
            if last_record_ts and rec.WHENMODIFIED != last_record_ts:
                # Got a timestamp change, which means the timestamp of the prior
                # record has been fully read.
                last_completed_ts = last_record_ts

            if horizon and rec.WHENMODIFIED > now - horizon:
                # Stop short if the record is too recent. This will trigger the
                # "read fewer documents than the page size" condition a little
                # further down.
                break

            this_count += 1
            last_record_ts = rec.WHENMODIFIED
            if cache.should_yield(object, rec.RECORDNO, last_record_ts):
                yield rec

        total_count += this_count
        if this_count < page_size:
            # No more documents, which means that the most recent document's
            # timestamp has been fully read. The count could be 0 if there were
            # no new documents or if page sizes lined up just right.
            if last_record_ts and total_count > 0:
                yield last_record_ts
            break

        if last_completed_ts != cursor:
            # If the page was full, this checkpoint will never be equal to the
            # timestamp of the last emitted record since we don't know if more
            # records with that same timestamp are on the next page of results.
            # Most commonly this will result in the next call to `fetch_since`
            # returning the same record again as its first result, which will be
            # filtered by the cache.
            yield last_completed_ts
            continue

        # This is a cycle: We got a full page of records with the exact same
        # WHENMODIFIED timestamp.
        assert last_record_ts is not None
        last_record_no = None
        while True:
            this_count = 0
            async for rec in sage.fetch_at(object, last_record_ts, last_record_no):
                rec = IncrementalResource.model_validate(rec.model_dump())
                last_record_no = rec.RECORDNO
                this_count += 1
                if cache.should_yield(object, rec.RECORDNO, rec.WHENMODIFIED):
                    yield rec

            if this_count < page_size:
                # We have now read all of the records with this same timestamp.
                last_completed_ts = last_record_ts
                yield last_completed_ts
                break


async def fetch_page(
    object: str,
    sage: Sage,
    page_size: int,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[IncrementalResource | PageCursor, None]:
    assert isinstance(page, int | None)
    assert isinstance(cutoff, datetime)

    last: int | None = None
    count = 0
    async for rec in sage.fetch_all(object, page):
        rec = IncrementalResource.model_validate(rec.model_dump())
        last = rec.RECORDNO
        count += 1
        if rec.WHENMODIFIED < cutoff:
            yield rec

    if count < page_size:
        return

    yield last


async def snapshot(
    object: str,
    sage: Sage,
    page_size: int,
    log: Logger,
) -> AsyncGenerator[SnapshotResource, None]:
    page = None
    while True:
        count = 0
        async for rec in sage.fetch_all(object, page):
            rec = SnapshotResource.model_validate(rec.model_dump())
            assert rec.RECORDNO is not None
            page = rec.RECORDNO
            count += 1
            yield rec

        if count < page_size:
            return
