from datetime import datetime, timedelta
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
)
from .shared import (
    dt_to_str,
    str_to_dt,
    HUB,
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
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    """
    Retrieve a single chunk of records modified at or after 'since' and at or
    before 'until' if provided, in ascending order of last-modified time.

    The HubSpot Search API has an undocumented maximum offset of 10,000 items.
    Rather than enumerate the whole window in one call, this returns one chunk
    that ends at the offset boundary, along with a resume PageCursor (a
    last-modified timestamp string) to continue from on the next call. When the
    whole window has been read, the returned PageCursor is None.

    Chunks never leave gaps. A chunk contains every record in the window
    modified at or before the chunk's newest timestamp, except records already
    returned by an earlier chunk. The returned cursor is the first millisecond the
    chunk does NOT cover. Records the search read at that millisecond are
    withheld from the chunk, since the offset cap may have cut that millisecond
    off partway. Passing the cursor back as 'page' starts a fresh search at
    that millisecond, which re-reads it in full and continues on.

    Multiple records can have the same "last modified" property value, and
    indeed large runs of these may have the same value. When more than 10,000
    records share a single millisecond (a "cycle"), the whole instant is drained
    via fetch_search_objects_modified_at and the resume cursor steps forward by
    one millisecond.
    """

    if isinstance(page, str):
        since = str_to_dt(page)

    url = f"{HUB}/crm/v3/objects/{object_name}/search"
    limit = 200
    output_items: set[tuple[datetime, str]] = set()
    cursor: int | None = None
    max_updated: datetime = since
    original_total: int | None = None

    while True:
        filter = (
            {
                "propertyName": last_modified_property_name,
                "operator": "BETWEEN",
                "value": dt_to_str(since),
                "highValue": dt_to_str(until),
            }
            if until
            else {
                "propertyName": last_modified_property_name,
                "operator": "GTE",
                "value": dt_to_str(since),
            }
        )

        input = {
            "filters": [filter],
            "sorts": [
                {"propertyName": last_modified_property_name, "direction": "ASCENDING"}
            ],
            "limit": limit,
        }
        if cursor:
            input["after"] = cursor

        result: SearchPageResult[CustomObjectSearchResult] = SearchPageResult[
            CustomObjectSearchResult
        ].model_validate_json(await http.request(log, url, method="POST", json=input))

        if not original_total:
            # Record the total from the original entire request range for
            # logging.
            original_total = result.total

        for r in result.results:
            this_mod_time = r.properties.hs_lastmodifieddate

            if this_mod_time < since:
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
                    {"id": r.id, "this_mod_time": this_mod_time, "since": since},
                )
                continue

            if until and this_mod_time > until:
                log.info(
                    "ignoring search result with record modification time that is later than maximum search window",
                    {"id": r.id, "this_mod_time": this_mod_time, "until": until},
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
            output_items.add((this_mod_time, str(r.id)))

        if not result.paging:
            # The whole window has been read and no more chunks remain.
            return sorted(output_items), None

        cursor = int(result.paging.next.after)
        if cursor + limit <= 10_000:
            # Still within the offset cap; keep paginating this same search.
            continue

        # Hit the 10,000-offset cap. End this chunk here and return a resume
        # cursor so the caller can continue with a fresh search.
        if since == max_updated:
            # More than 10,000 records share a single millisecond, so paginating
            # by time can't make progress. Drain every record at that instant
            # and step the resume cursor forward by the minimum (1ms) amount.
            log.info(
                "cycle detected for lastmodifieddate, fetching all ids for records modified at that instant",
                {"object_name": object_name, "instant": since},
            )
            output_items.update(
                await fetch_search_objects_modified_at(
                    object_name, log, http, max_updated, last_modified_property_name
                )
            )

            # HubSpot APIs use millisecond resolution, so move the time cursor
            # forward by that minimum amount now that we know we have all of the
            # records modified at the common `max_updated` time.
            max_updated = max_updated + timedelta(milliseconds=1)

            chunk = sorted(output_items)

            if until and max_updated > until:
                # Unlikely edge case, but resuming past `until` would otherwise
                # result in an error from the API. The window is fully read.
                return chunk, None
        else:
            # Withhold the in-progress `max_updated` millisecond from the chunk;
            # the offset cap may have cut it off partway, so the next call
            # re-reads it.
            chunk = sorted(item for item in output_items if item[0] < max_updated)

        log.info(
            "search window chunk complete; resuming with a new search",
            {
                "object_name": object_name,
                "since": since,
                "until": until,
                "max_updated": dt_to_str(max_updated),
                "count": len(output_items),
                "total": original_total,
            },
        )

        return chunk, dt_to_str(max_updated)


async def fetch_search_objects_modified_at(
    object_name: str,
    log: Logger,
    http: HTTPSession,
    modified: datetime,
    last_modified_property_name: str = "hs_lastmodifieddate",
) -> set[tuple[datetime, str]]:
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
    output_items: set[tuple[datetime, str]] = set()
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
            output_items.add((r.properties.hs_lastmodifieddate, str(r.id)))

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
