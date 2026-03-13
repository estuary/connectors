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
    HUB,
)


async def fetch_search_objects(
    object_name: str,
    log: Logger,
    http: HTTPSession,
    since: datetime,
    until: datetime | None,
    _: PageCursor,
    last_modified_property_name: str = "hs_lastmodifieddate",
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    """
    Retrieve all records between 'since' and 'until' (or the present time).

    The HubSpot Search API has an undocumented maximum offset of 10,000 items,
    so the logic here will completely enumerate all available records, kicking
    off new searches if needed to work around that limit. The returned
    PageCursor is always None.

    Multiple records can have the same "last modified" property value, and
    indeed large runs of these may have the same value. So a "new" search will
    inevitably grab some records again, and deduplication is handled here via
    the set.
    """

    url = f"{HUB}/crm/v3/objects/{object_name}/search"
    limit = 200
    output_items: set[tuple[datetime, str]] = set()
    cursor: int | None = None
    max_updated: datetime = since
    original_since = since
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
                log.error("search query input", input)
                raise Exception(
                    f"search query returned records out of order for {r.id} with {this_mod_time} < {max_updated}"
                )

            max_updated = this_mod_time
            output_items.add((this_mod_time, str(r.id)))

        if not result.paging:
            if since is not original_since:
                log.info(
                    "finished multi-request fetch",
                    {
                        "final_count": len(output_items),
                        "total": original_total,
                    },
                )
            break

        cursor = int(result.paging.next.after)
        if cursor + limit > 10_000:
            # Further attempts to read this search result will not accepted by
            # the search API, so a new search must be initiated to complete the
            # request.
            cursor = None

            if since == max_updated:
                log.info(
                    "cycle detected for lastmodifieddate, fetching all ids for records modified at that instant",
                    {"object_name": object_name, "instant": since},
                )
                output_items.update(
                    await fetch_search_objects_modified_at(
                        object_name, log, http, max_updated, last_modified_property_name
                    )
                )

                # HubSpot APIs use millisecond resolution, so move the time
                # cursor forward by that minimum amount now that we know we have
                # all of the records modified at the common `max_updated` time.
                max_updated = max_updated + timedelta(milliseconds=1)

                if until and max_updated > until:
                    # Unlikely edge case, but this would otherwise result in an
                    # error from the API.
                    break

            since = max_updated
            log.info(
                "initiating a new search to satisfy requested time range",
                {
                    "object_name": object_name,
                    "since": since,
                    "until": until,
                    "original_since": original_since,
                    "count": len(output_items),
                    "total": original_total,
                },
            )

    # Sort newest to oldest to match the convention of most of "fetch ID"
    # functions.
    return sorted(list(output_items), reverse=True), None


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
