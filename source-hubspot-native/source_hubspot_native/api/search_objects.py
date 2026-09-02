from datetime import UTC, datetime, timedelta
from enum import StrEnum, auto
from logging import Logger
from typing import (
    Any,
    Iterable,
)

from estuary_cdk.capture.common import PageCursor
from estuary_cdk.http import HTTPSession

from ..models import (
    CustomObjectSearchResult,
    SearchPageResult,
    TimestampedId,
)
from .shared import (
    dt_to_str,
    str_to_dt,
    HUB,
)

# HubSpot's Search API serves at most this many results for one query; paging
# past it returns a 400.
SEARCH_RESULT_CAP = 10_000
SEARCH_PAGE_LIMIT = 200
# Offset of the last page a query can serve. Its records show where the cap
# falls in time.
PEEK_OFFSET = SEARCH_RESULT_CAP - SEARCH_PAGE_LIMIT

ONE_MS = timedelta(milliseconds=1)


class _CapReached(Exception):
    """Paging a chunk would cross the offset cap despite its reported total."""


class _SplitMethod(StrEnum):
    """How a chunk's end was chosen from the page at the cap."""

    # The earliest in-window timestamp at the cap, less one millisecond.
    PEEK = auto()
    # Fewer than SEARCH_PAGE_LIMIT records sit at the cap, so the rest of the
    # window fits in one search.
    FITS = auto()
    # The peek offered no usable timestamp, or a peeked chunk still overflowed,
    # so the range is halved.
    HALVE = auto()
    # Every record at the cap shares the window's first millisecond; drain it.
    CYCLE = auto()


def _floor_to_ms(dt: datetime) -> datetime:
    """`dt` with its sub-millisecond part removed. HubSpot compares timestamps
    at millisecond resolution and ignores the microseconds in a request's
    bounds, so flooring ours keeps every comparison here consistent with what
    the search actually returns."""
    return dt - timedelta(microseconds=dt.microsecond % 1000)


def _midpoint(start: datetime, end: datetime) -> datetime:
    """The whole millisecond halfway from `start` to `end`; `start` itself when
    they are a millisecond or less apart."""
    return start + ONE_MS * ((end - start) // ONE_MS // 2)


def _choose_chunk_end(
    start: datetime, end: datetime, peeked_timestamps: list[datetime]
) -> tuple[_SplitMethod, datetime]:
    """Decide where a chunk ends from the last-modified timestamps HubSpot
    reports for the records at the cap. A short page means fewer than the cap
    remain, so the whole window fits. Otherwise only a timestamp inside the
    window can end a chunk. For CYCLE the returned end is the window's start."""
    if len(peeked_timestamps) < SEARCH_PAGE_LIMIT:
        return _SplitMethod.FITS, end

    in_window_timestamps = [ts for ts in peeked_timestamps if start < ts <= end]
    if in_window_timestamps:
        return _SplitMethod.PEEK, min(in_window_timestamps) - ONE_MS
    if all(ts <= start for ts in peeked_timestamps):
        return _SplitMethod.CYCLE, start
    return _SplitMethod.HALVE, _midpoint(start, end)


async def _request_search_page(
    log: Logger,
    http: HTTPSession,
    object_name: str,
    last_modified_property_name: str,
    start: datetime,
    end: datetime | None,
    after: int | None,
) -> tuple[SearchPageResult[CustomObjectSearchResult], dict[str, Any]]:
    """Request one page of Search API results for records whose last-modified
    property is in [start, end], or at or after `start` when `end` is None,
    sorted ascending, SEARCH_PAGE_LIMIT per page. `after` is HubSpot's offset
    for the page; None requests the first. Also returns the request body, which
    the ordering check logs when it fails."""
    filter = (
        {
            "propertyName": last_modified_property_name,
            "operator": "BETWEEN",
            "value": dt_to_str(start),
            "highValue": dt_to_str(end),
        }
        if end
        else {
            "propertyName": last_modified_property_name,
            "operator": "GTE",
            "value": dt_to_str(start),
        }
    )

    input: dict[str, Any] = {
        "filters": [filter],
        "sorts": [
            {"propertyName": last_modified_property_name, "direction": "ASCENDING"}
        ],
        "limit": SEARCH_PAGE_LIMIT,
    }
    if after is not None:
        input["after"] = after

    url = f"{HUB}/crm/v3/objects/{object_name}/search"
    result = SearchPageResult[CustomObjectSearchResult].model_validate_json(
        await http.request(log, url, method="POST", json=input)
    )
    return result, input


async def _fetch_all_ids_between(
    log: Logger,
    http: HTTPSession,
    object_name: str,
    last_modified_property_name: str,
    start: datetime,
    end: datetime | None,
    first_page: tuple[SearchPageResult[CustomObjectSearchResult], dict[str, Any]],
    should_crash_on_unordered_results: bool,
) -> tuple[list[TimestampedId], int]:
    """Fetch the id and last-modified timestamp of the records HubSpot returns
    for [start, end], paging until it reports no more. The caller has already
    requested `first_page` to check the range's total, so reading continues
    from it. The range must hold at most SEARCH_RESULT_CAP records: if paging
    would cross the cap, raise _CapReached so the caller can narrow it. Returns
    the records sorted by timestamp and the number of pages read."""
    output_items: set[TimestampedId] = set()
    max_updated = start
    pages = 0
    result, input = first_page

    while True:
        pages += 1

        for r in result.results:
            this_mod_time = r.properties.hs_lastmodifieddate

            if this_mod_time < start:
                # The search API will return records with a modification time
                # before the requested "since" (the start of the window) if
                # their updatedAt timestamp is within the same millisecond,
                # effectively ignoring the microseconds part of the range
                # criteria. These spurious results can be safely ignored in the
                # rare case that there is a record with a modification time
                # within the same millisecond as requested at the start of the
                # time window, but some smaller fraction of a second earlier.
                log.info(
                    "ignoring search result with record modification time that is earlier than minimum search window",
                    {"id": r.id, "this_mod_time": this_mod_time, "since": start},
                )
                continue

            if end and this_mod_time > end:
                log.info(
                    "ignoring search result with record modification time that is later than maximum search window",
                    {"id": r.id, "this_mod_time": this_mod_time, "until": end},
                )
                continue

            if this_mod_time < max_updated:
                if should_crash_on_unordered_results:
                    log.error("search query input", input)
                    raise Exception(
                        f"search query returned records out of order for {r.id} with {this_mod_time} < {max_updated}"
                    )
                # The realtime stream is best-effort and allowed to be
                # incomplete, so an out-of-order result is skipped rather
                # than treated as fatal. The delayed stream will capture
                # any records the realtime stream skips.
                continue

            max_updated = this_mod_time
            output_items.add(TimestampedId(this_mod_time, str(r.id)))

        if not result.paging:
            return sorted(output_items), pages

        after = int(result.paging.next.after)
        # The caller should have already bounded the search so there are
        # at most SEARCH_RESULT_CAP results returned by the search, but this
        # sanity check covers the case where HubSpot's `total` under-reported
        # the range or where records joined the result set while we paged: a
        # record indexed late, or one modified during an open-ended realtime
        # search. Requesting an offset at or past the cap gets a 400, so raise
        # instead and let the caller narrow the range.
        if after + SEARCH_PAGE_LIMIT > SEARCH_RESULT_CAP:
            raise _CapReached()

        result, input = await _request_search_page(
            log, http, object_name, last_modified_property_name, start, end, after
        )


async def fetch_search_objects(
    object_name: str,
    log: Logger,
    http: HTTPSession,
    since: datetime,
    until: datetime | None,
    page: PageCursor,
    last_modified_property_name: str = "hs_lastmodifieddate",
    should_crash_on_unordered_results: bool = True,
) -> tuple[Iterable[TimestampedId], PageCursor]:
    """
    Retrieve one chunk of the records modified in [since, until] (or from
    `since` onward when `until` is None), plus a resume PageCursor for the rest
    of the window, or None once the window has been read.

    HubSpot's Search API serves at most 10,000 results per query. Rather than
    page up to that cap and rely on result order to know what was covered, a
    window holding more records is split by time into chunks the API can return
    completely, and every record in a chunk is read in whatever order it comes.

                 start          chunk_end          until
    ───────────────┼────────────────┼────────────────┼──▶ time (1 ms ticks)
                   │                │                │
    emitted ───────[════════════════]                │
    resume ────────┼────────────────(════════════════]
                   │                └─ chosen so the chunk holds at most
                   │                   10,000 records; equals until when
                   │                   the whole window fits
                   └─ inclusive; page replaces since when resuming

    `emitted` is what this call returns: the records HubSpot's filter
    matches in [start, chunk_end], inclusive at both ends at its millisecond
    resolution. `resume` is what the next call covers: its cursor is
    chunk_end + 1 ms, the first millisecond this chunk did not cover, and is
    None once the chunk reaches until.

    A chunk end is found by peeking at the last page the cap allows (offset
    9,800): the earliest timestamp there that lies inside the window, less one
    millisecond, bounds a chunk of at most 9,800 records. A fresh search then
    confirms the chunk's total before it is read, so the peek only sizes the
    chunk and never decides completeness. When the peek offers no usable
    timestamp the window is halved instead, and a chunk that is a single
    millisecond yet still exceeds the cap is drained with
    fetch_search_objects_modified_at.
    """
    start = _floor_to_ms(str_to_dt(page) if isinstance(page, str) else since)
    until = _floor_to_ms(until) if until is not None else None
    # Realtime callers pass no `until`. The present bounds any splitting they need.
    end = until if until is not None else _floor_to_ms(datetime.now(tz=UTC))

    async def request_page(chunk_end: datetime | None, after: int | None):
        return await _request_search_page(
            log, http, object_name, last_modified_property_name, start, chunk_end, after
        )

    async def fetch_all_ids(
        chunk_end: datetime | None,
        first_page: tuple[SearchPageResult[CustomObjectSearchResult], dict[str, Any]],
    ) -> tuple[list[TimestampedId], int]:
        return await _fetch_all_ids_between(
            log, http, object_name, last_modified_property_name, start, chunk_end,
            first_page, should_crash_on_unordered_results,
        )

    def resume_after(chunk_end: datetime) -> PageCursor:
        resume = chunk_end + ONE_MS
        return dt_to_str(resume) if resume <= end else None

    async def drain_instant() -> tuple[Iterable[TimestampedId], PageCursor]:
        # More than 10,000 records share a single millisecond, so splitting by
        # time can't make progress. Drain every record at that instant and step
        # the resume cursor forward by the minimum (1ms) amount.
        log.info(
            "cycle detected for lastmodifieddate, fetching all ids for records modified at that instant",
            {"object_name": object_name, "instant": start},
        )
        items = await fetch_search_objects_modified_at(
            object_name, log, http, start, last_modified_property_name
        )
        return sorted(items), resume_after(start)

    if page is None:
        # A fresh window. Its first page is needed regardless and carries the
        # window's total.
        first_page = await request_page(until, None)
        if first_page[0].total <= SEARCH_RESULT_CAP:
            try:
                items, _ = await fetch_all_ids(until, first_page)
                return items, None
            except _CapReached:
                log.warning(
                    "search paged past the cap despite its total; splitting the window",
                    {"object_name": object_name, "start": start, "total": first_page[0].total},
                )

    # The window contains over SEARCH_RESULT_CAP records or we're resuming
    # inside one that was. Peek at the last page the cap allows to see
    # where the cap falls in time.
    peek, _ = await request_page(until, PEEK_OFFSET)
    method, chunk_end = _choose_chunk_end(
        start, end, [r.properties.hs_lastmodifieddate for r in peek.results]
    )
    if method is _SplitMethod.CYCLE:
        return await drain_instant()

    while True:
        if chunk_end < end:
            log.info(
                "search window split",
                {
                    "object_name": object_name,
                    "start": start,
                    "end": end,
                    "chunk_end": chunk_end,
                    "method": method,
                },
            )

        first_page = await request_page(chunk_end, None)
        if first_page[0].total <= SEARCH_RESULT_CAP:
            try:
                items, pages = await fetch_all_ids(chunk_end, first_page)
            except _CapReached:
                # The chunk held more than the cap after all, so it needs
                # narrowing just as if its total had said so. Fall through to
                # the same halving (or drain) below.
                pass
            else:
                log.info(
                    "search window chunk complete",
                    {
                        "object_name": object_name,
                        "start": start,
                        "chunk_end": chunk_end,
                        "count": len(items),
                        "pages": pages,
                        "total": first_page[0].total,
                    },
                )
                return items, resume_after(chunk_end)

        if chunk_end <= start:
            return await drain_instant()

        chunk_end, method = _midpoint(start, chunk_end), _SplitMethod.HALVE


async def fetch_search_objects_modified_at(
    object_name: str,
    log: Logger,
    http: HTTPSession,
    modified: datetime,
    last_modified_property_name: str = "hs_lastmodifieddate",
) -> set[TimestampedId]:
    """
    Fetch all of the ids of the given object that were modified at the given
    time. Used exclusively for breaking out of cycles in the search API
    resulting from more than 10,000 records being modified at the same time,
    which is unfortunately a thing that can happen.

    To simplify the pagination strategy, the actual `paging` result isn't used
    at all other than to see when we have reached the end, and the search query
    just always asks for ids larger than what it had previously seen.
    """

    url = f"{HUB}/crm/v3/objects/{object_name}/search"
    limit = 200
    output_items: set[TimestampedId] = set()
    id_cursor: int | None = None
    round = 0

    while True:
        filters: list[dict[str, Any]] = [
            {
                "propertyName": last_modified_property_name,
                "operator": "EQ",
                "value": dt_to_str(modified),
            }
        ]

        if id_cursor:
            filters.append(
                {
                    "propertyName": "hs_object_id",
                    "operator": "GT",
                    "value": id_cursor,
                }
            )

        input = {
            "filters": filters,
            "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}],
            "limit": limit,
        }

        result: SearchPageResult[CustomObjectSearchResult] = SearchPageResult[
            CustomObjectSearchResult
        ].model_validate_json(await http.request(log, url, method="POST", json=input))

        for r in result.results:
            if id_cursor and r.id <= id_cursor:
                # This should _really_ never happen, but HubSpot is weird so if
                # it does I want to know about it and will come back later and
                # figure it out.
                raise Exception(f"unexpected id order: {r.id} <= {id_cursor}")
            id_cursor = r.id
            output_items.add(
                TimestampedId(r.properties.hs_lastmodifieddate, str(r.id))
            )

        # Log every 10,000 returned records, since there are 200 per page.
        if round % 50 == 0:
            log.info(
                "fetching ids for records modified at instant",
                {
                    "object_name": object_name,
                    "instant": modified,
                    "count": len(output_items),
                    "remaining": result.total,
                },
            )

        if not result.paging:
            break

        round += 1

    return output_items
