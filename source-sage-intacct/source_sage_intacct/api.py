from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, Callable

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.capture.common import LogCursor, PageCursor

from .models import (
    CreationRecord,
    DeletionEvent,
    DeletionRecord,
    GLDetailBackfillRecord,
    GLDetailRecord,
    IncrementalResource,
    ObjectKeyNotRecordNo,
    SnapshotResource,
    parse_backfill_record,
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
# used for cycle breaking which is based on sorting by `id_field` at a specific
# WHENMODIFIED timestamp.
async def _fetch_records_generic[
    T: (
        IncrementalResource,
        CreationRecord,
        DeletionEvent,
        GLDetailRecord,
    )
](
    object: str,
    cls: type[T],
    id_field: str,
    horizon: timedelta | None,
    page_size: int,
    cursor: LogCursor,
    fetch_since: Callable[[str, datetime], AsyncGenerator[Any, None]],
    fetch_at: Callable[[str, datetime, Any], AsyncGenerator[Any, None]],
) -> AsyncGenerator[T | LogCursor, None]:
    assert isinstance(cursor, datetime)
    now = datetime.now(UTC)

    last_completed_ts = cursor
    total_count = 0
    while True:
        cursor = last_completed_ts
        last_record_ts = None
        this_count = 0
        async for rec in fetch_since(object, cursor):
            rec = cls.model_validate(rec.model_dump())
            rec_ts = rec.cursor_value()
            if last_record_ts and rec_ts != last_record_ts:
                # Got a timestamp change, which means the timestamp of the prior
                # record has been fully read.
                last_completed_ts = last_record_ts

            if horizon and rec_ts > now - horizon:
                # Stop short if the record is too recent. This will trigger the
                # "read fewer documents than the page size" condition a little
                # further down.
                break

            this_count += 1
            last_record_ts = rec_ts
            if cache.should_yield(object, getattr(rec, id_field), rec_ts):
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
        last_record_identifier = None
        while True:
            this_count = 0
            async for rec in fetch_at(object, last_record_ts, last_record_identifier):
                rec = cls.model_validate(rec.model_dump())
                last_record_identifier = getattr(rec, id_field)
                this_count += 1
                if cache.should_yield(
                    object, getattr(rec, id_field), rec.cursor_value()
                ):
                    yield rec

            if this_count < page_size:
                # We have now read all of the records with this same timestamp.
                last_completed_ts = last_record_ts
                yield last_completed_ts
                break


async def fetch_changes(
    object: str,
    sage: Sage,
    horizon: timedelta | None,
    page_size: int,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[IncrementalResource | LogCursor, None]:
    async for item in _fetch_records_generic(
        object=object,
        cls=IncrementalResource,
        id_field="RECORDNO",
        horizon=horizon,
        page_size=page_size,
        cursor=cursor,
        fetch_since=sage.fetch_since,
        fetch_at=sage.fetch_at,
    ):
        yield item


async def fetch_creations(
    object: str,
    sage: Sage,
    horizon: timedelta | None,
    page_size: int,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[CreationRecord | LogCursor, None]:
    async for item in _fetch_records_generic(
        object=object,
        cls=CreationRecord,
        id_field="RECORDNO",
        horizon=horizon,
        page_size=page_size,
        cursor=cursor,
        fetch_since=sage.fetch_created_since,
        fetch_at=sage.fetch_created_at,
    ):
        yield item


async def fetch_deletions(
    object: str,
    sage: Sage,
    horizon: timedelta | None,
    page_size: int,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[DeletionRecord | LogCursor, None]:
    async for item in _fetch_records_generic(
        object=object,
        cls=DeletionEvent,
        id_field="ID",
        horizon=horizon,
        page_size=page_size,
        cursor=cursor,
        fetch_since=sage.fetch_deleted,
        fetch_at=sage.fetch_deleted_at,
    ):
        if not isinstance(item, DeletionEvent):
            yield item
            continue

        # Sage records some objects' deletions under their user-facing text ID
        # rather than their RECORDNO. Such a tombstone cannot be addressed to a
        # document in the RECORDNO-keyed collection, so it is skipped.
        try:
            doc = DeletionRecord.from_event(item)
        except ObjectKeyNotRecordNo:
            log.debug(
                "skipping deletion whose OBJECTKEY is not a RECORDNO",
                {
                    "object": object,
                    "object_key": item.OBJECTKEY,
                    "access_time": item.ACCESSTIME,
                    "audit_id": item.ID,
                },
            )
            continue

        yield doc


async def fetch_page(
    object: str,
    sage: Sage,
    page_size: int,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[IncrementalResource | CreationRecord | PageCursor, None]:
    assert isinstance(page, int | None)
    assert isinstance(cutoff, datetime)

    last: int | None = None
    count = 0
    async for rec in sage.fetch_all(object, page):
        doc = parse_backfill_record(rec.model_dump())
        last = doc.RECORDNO
        count += 1
        if doc.cursor_value() < cutoff:
            yield doc

    if count < page_size:
        return

    yield last


async def fetch_gldetail(
    sage: Sage,
    horizon: timedelta | None,
    page_size: int,
    log: Logger,
    cursor: LogCursor,
) -> AsyncGenerator[GLDetailRecord | LogCursor, None]:
    async for item in _fetch_records_generic(
        object="GLDETAIL",
        cls=GLDetailRecord,
        id_field="RECORDNO",
        horizon=horizon,
        page_size=page_size,
        cursor=cursor,
        fetch_since=sage.fetch_since,
        fetch_at=sage.fetch_at,
    ):
        yield item


async def backfill_gldetail(
    sage: Sage,
    page_size: int,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[GLDetailBackfillRecord | PageCursor, None]:
    """The page cursor is the BATCHKEY at which the next page starts.

    Pages are in BATCHKEY order because a sort on the text RECORDNO of the
    whole object is slow, and its text order is not the order of the batches.

    Do not add a WHENMODIFIED filter to the request for the cutoff. Sage
    drops the rows where it is empty, and a backfill that sends backfill
    signals then removes those rows from the destination.
    """
    assert isinstance(page, int | None)
    assert isinstance(cutoff, datetime)

    def is_before_cutoff(doc: GLDetailBackfillRecord) -> bool:
        return doc.WHENMODIFIED is None or doc.WHENMODIFIED < cutoff

    count = 0
    first_batch_key: int | None = None
    # Hold the newest batch back until the next batch starts. A full page can
    # end inside a batch, and the next page starts with that batch again, so
    # an early emit would emit its rows two times.
    batch: list[GLDetailBackfillRecord] = []
    async for rec in sage.fetch_gldetail_page(page):
        doc = GLDetailBackfillRecord.model_validate(rec.model_dump())
        count += 1
        if first_batch_key is None:
            first_batch_key = doc.BATCHKEY

        if batch and doc.BATCHKEY != batch[-1].BATCHKEY:
            for held in batch:
                if is_before_cutoff(held):
                    yield held
            batch = []

        batch.append(doc)

    if count < page_size:
        for held in batch:
            if is_before_cutoff(held):
                yield held
        return

    last_batch_key = batch[-1].BATCHKEY
    if last_batch_key != first_batch_key:
        # The CDK does not check that a page cursor moves forward. A cursor
        # that does not move would repeat this request without end, and Sage
        # bills each call.
        if page is not None and last_batch_key <= page:
            raise RuntimeError(
                f"GLDETAIL page from BATCHKEY {page} is not in BATCHKEY order: "
                f"it ends at {last_batch_key}"
            )
        yield last_batch_key
        return

    # One batch fills the page, so a page that starts at this batch can never
    # get past it. Read this batch alone, in RECORDNO order.
    after: str | None = None
    seen: set[str] = set()
    while True:
        count = 0
        seen_before = len(seen)
        async for rec in sage.fetch_gldetail_batch(last_batch_key, after):
            doc = GLDetailBackfillRecord.model_validate(rec.model_dump())
            after = doc.RECORDNO
            count += 1
            if doc.RECORDNO in seen:
                continue
            seen.add(doc.RECORDNO)
            if is_before_cutoff(doc):
                yield doc

        if count < page_size:
            break
        if len(seen) == seen_before:
            # A page with no new row means the RECORDNO bound does not move.
            # Stop, because the loop would not end and Sage bills each call.
            raise RuntimeError(
                f"GLDETAIL batch {last_batch_key} cannot be read in RECORDNO "
                f"order: the page after {after!r} returned no new row"
            )

    yield last_batch_key + 1


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
