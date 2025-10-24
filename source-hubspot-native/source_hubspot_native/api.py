import asyncio
import functools
import itertools
from asyncio.exceptions import CancelledError
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from logging import Logger
from types import NoneType
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
)

import estuary_cdk.emitted_changes_cache as cache
from estuary_cdk.buffer_ordered import buffer_ordered
from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
    Triggers,
)
from estuary_cdk.http import HTTPSession
from estuary_cdk.utils import format_error_message
from pydantic import TypeAdapter

from .models import (
    OBJECT_TYPE_IDS,
    Association,
    BatchResult,
    Company,
    Contact,
    ContactList,
    ContactListMembership,
    ContactListMembershipResponse,
    ListSearch,
    CRMObject,
    CustomObject,
    CustomObjectSchema,
    CustomObjectSearchResult,
    Deal,
    DealPipelines,
    EmailEvent,
    EmailEventsResponse,
    Engagement,
    FeedbackSubmission,
    Form,
    FormSubmission,
    FormSubmissionContext,
    Goals,
    LineItem,
    MarketingEmail,
    Names,
    OldRecentCompanies,
    OldRecentContacts,
    OldRecentDeals,
    OldRecentEngagements,
    OldRecentTicket,
    Owner,
    PageResult,
    Product,
    Properties,
    SearchPageResult,
    Ticket,
)


# Hubspot returns a 500 internal server error if querying for data
EPOCH_PLUS_ONE_SECOND = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=1)
HUB = "https://api.hubapi.com"
MARKETING_EMAILS_PAGE_SIZE = 300

properties_cache: dict[str, Properties] = {}


async def fetch_properties(
    log: Logger, http: HTTPSession, object_name: str
) -> Properties:
    if object_name in properties_cache:
        return properties_cache[object_name]

    url = f"{HUB}/crm/v3/properties/{object_name}"
    properties_cache[object_name] = Properties.model_validate_json(
        await http.request(log, url)
    )
    for p in properties_cache[object_name].results:
        p.hubspotObject = object_name

    return properties_cache[object_name]


async def fetch_deal_pipelines(log: Logger, http: HTTPSession) -> DealPipelines:
    url = f"{HUB}/crm-pipelines/v1/pipelines/deals"

    return DealPipelines.model_validate_json(await http.request(log, url))


async def fetch_owners(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Owner, None]:
    url = f"{HUB}/crm/v3/owners"
    after: str | None = None

    input: dict[str, Any] = {
        "limit": 500,
    }

    while True:
        if after:
            input["after"] = after

        result = PageResult[Owner].model_validate_json(
            await http.request(log, url, method="GET", params=input)
        )

        for owner in result.results:
            yield owner

        if not result.paging:
            break

        after = result.paging.next.after


async def fetch_forms(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Form, None]:
    url = f"{HUB}/marketing/v3/forms"
    after: str | None = None

    input: dict[str, Any] = {
        "limit": 500,
    }

    while True:
        if after:
            input["after"] = after

        result = PageResult[Form].model_validate_json(
            await http.request(log, url, method="GET", params=input)
        )

        for owner in result.results:
            yield owner

        if not result.paging:
            break

        after = result.paging.next.after


async def _fetch_form_submissions_since(
    http: HTTPSession,
    log: Logger,
    form_id: str,
    last_submitted_at: int,
) -> AsyncGenerator[FormSubmission, None]:
    url = f"{HUB}/form-integrations/v1/submissions/forms/{form_id}"
    after: str | None = None
    params: dict[str, str | int] = {
        "limit": 50,
    }

    validation_context = FormSubmissionContext(form_id)

    while True:
        if after:
            params["after"] = after

        result = PageResult[FormSubmission].model_validate_json(
            await http.request(log, url, params=params), context=validation_context
        )

        for form_submission in result.results:
            # Form submissions are returned in reverse chronological order.
            # We can safely stop paginating once we see a submission with
            # a timestamp before the previous sweep.
            if form_submission.submittedAt > last_submitted_at:
                yield form_submission
            else:
                return

        if not result.paging:
            return

        after = result.paging.next.after


async def fetch_form_submissions(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[FormSubmission | LogCursor, None]:
    assert isinstance(log_cursor, int)
    form_ids: list[str] = []

    async for form in fetch_forms(http, log):
        form_ids.append(form.id)

    latest_submitted_at = log_cursor

    for id in form_ids:
        async for submission in _fetch_form_submissions_since(
            http, log, id, log_cursor
        ):
            if submission.submittedAt > latest_submitted_at:
                latest_submitted_at = submission.submittedAt

            yield submission

    if latest_submitted_at != log_cursor:
        yield latest_submitted_at


async def fetch_page_with_associations(
    # Closed over via functools.partial:
    cls: type[CRMObject],
    http: HTTPSession,
    with_history: bool,
    object_name: str,
    # Remainder is common.FetchPageFn:
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[CRMObject | str, None]:

    assert isinstance(cutoff, datetime)

    url = f"{HUB}/crm/v3/objects/{object_name}"
    output: list[CRMObject] = []
    properties = await fetch_properties(log, http, object_name)

    # There is a limit on how large a URL can be when making a GET request to HubSpot. Exactly what
    # this limit is is a bit mysterious to me. Empirical testing indicates that a single property
    # with an alphanumeric name ~32k characters long will push it over the limit. However, this ~32k
    # length limit assumption does not seem to hold when considering huge amounts of shorter
    # property names, which is more common. The chunk size here is a best guess then based on
    # something that works for actual rea;-world use cases. Note that the length is effectively
    # doubled by the fact that we request both properties and properties with history.
    #
    # If this calculation results in more than one chunk of properties to retrieve based on the
    # cumulative byte lengths, we will issue multiple requests for different sets of properties and
    # combine the results together in the output documents.
    chunked_properties = _chunk_props(
        [p.name for p in properties.results if not p.calculated],
        5 * 1024,
    )

    for props in chunked_properties:
        property_names = ",".join(props)

        input = {
            "limit": 100,
            "properties": property_names,
        }
        if with_history:
            input["propertiesWithHistory"] = property_names
            input["limit"] = 50
        if len(cls.ASSOCIATED_ENTITIES) > 0:
            input["associations"] = ",".join(cls.ASSOCIATED_ENTITIES)
        if page:
            input["after"] = page

        _cls: Any = cls  # Silence mypy false-positive.
        result: PageResult[CRMObject] = PageResult[_cls].model_validate_json(
            await http.request(log, url, method="GET", params=input)
        )

        for idx, doc in enumerate(result.results):
            if idx == len(output):
                # Populate the document at idx the first time around.
                output.append(doc)
            else:
                # When fetching values for additional chunks of properties, we require that
                # documents are received in the same order.
                assert output[idx].id == doc.id

                # An additional requirement is that if a document gets updated while we are fetching
                # a separate chunk of properties, that its `updatedAt` value will be increased to
                # the point that it will be beyond the cutoff for the backfill and the updated
                # document will be captured via the incremental stream. This will prevent any
                # inconsistencies arising from a document being updated in the midst of us fetching
                # its properties.
                if output[idx].updatedAt != doc.updatedAt:
                    assert doc.updatedAt >= cutoff
                    output[idx].updatedAt = (
                        doc.updatedAt
                    )  # We'll discard this document per the check a little further down.

                output[idx].properties.update(doc.properties)
                if with_history:
                    output[idx].propertiesWithHistory.update(doc.propertiesWithHistory)

    for doc in output:
        if doc.updatedAt < cutoff:
            yield doc

    if result.paging:
        yield result.paging.next.after


async def _fetch_batch(
    log: Logger,
    cls: type[CRMObject],
    http: HTTPSession,
    with_history: bool,
    object_name: str,
    ids: Iterable[str],
) -> BatchResult[CRMObject]:

    url = f"{HUB}/crm/v3/objects/{object_name}/batch/read"
    properties = await fetch_properties(log, http, object_name)
    property_names = [p.name for p in properties.results if not p.calculated]

    input = {
        "inputs": [{"id": id} for id in ids],
        "properties": property_names,
    }
    if with_history:
        input["propertiesWithHistory"] = property_names

    _cls: Any = cls  # Silence mypy false-positive.
    return BatchResult[_cls].model_validate_json(
        await http.request(log, url, method="POST", json=input)
    )


async def fetch_association(
    log: Logger,
    cls: type[CRMObject],
    http: HTTPSession,
    object_name: str,
    ids: Iterable[str],
    associated_entity: str,
) -> BatchResult[Association]:
    url = f"{HUB}/crm/v4/associations/{object_name}/{associated_entity}/batch/read"
    input = {"inputs": [{"id": id} for id in ids]}

    return BatchResult[Association].model_validate_json(
        await http.request(log, url, method="POST", json=input)
    )


async def fetch_batch_with_associations(
    log: Logger,
    cls: type[CRMObject],
    http: HTTPSession,
    with_history: bool,
    object_name: str,
    ids: list[str],
) -> BatchResult[CRMObject]:

    batch, all_associated = await asyncio.gather(
        _fetch_batch(log, cls, http, with_history, object_name, ids),
        asyncio.gather(
            *(
                fetch_association(log, cls, http, object_name, ids, e)
                for e in cls.ASSOCIATED_ENTITIES
            )
        ),
    )
    # Index CRM records so we can attach associations.
    index = {r.id: r for r in batch.results}

    for associated_entity, associated in zip(cls.ASSOCIATED_ENTITIES, all_associated):
        for result in associated.results:
            setattr(
                index[result.from_.id],
                associated_entity,
                [to.toObjectId for to in result.to],
            )

    return batch


FetchRecentFn = Callable[
    [Logger, HTTPSession, bool, datetime, datetime | None],
    AsyncGenerator[tuple[datetime, str, Any], None],
]
"""
Returns a stream of (timestamp, key, document) tuples that represent a
potentially incomplete stream of very recent documents. The timestamp is used to
checkpoint the next log cursor. The key is used for updating the emitted changes
cache with the timestamp of the document.

Documents may be returned in any order, but iteration will be stopped upon
seeing an entry that's as-old or older than the datetime cursor.

The first datetime parameter represents the "since" value, which is the oldest
documents that are required. The second datetime parameter is an "until" value,
which if not None is a hint that more recent documents are not needed.
"""

FetchDelayedFn = Callable[
    [Logger, HTTPSession, bool, datetime, datetime],
    AsyncGenerator[tuple[datetime, str, Any], None],
]
"""
Returns a stream of (timestamp, key, document) tuples that represent a complete
stream of not-so-recent documents. The key is used for seeing if a more recent
change event document has already been emitted by the FetchRecentFn.

Similar to FetchRecentFn, documents may be returned in any order and iteration
stops when seeing an entry that's as-old or older than the "since" datetime
cursor, which is the first datetime parameter. The second datetime parameter
represents an "until" value, and documents more recent than this are discarded
and should usually not even be retrieved if possible.
"""

# In-memory cache of how far along the "delayed" change stream is.
last_delayed_fetch_end: dict[str, datetime] = {}

# How far back the delayed stream will search up until. This needs to be far
# back enough that the HubSpot APIs return consistent data.
delayed_offset = timedelta(hours=1)

# The minimum amount of time must be available for the delay stream to be
# called. It can't be called every time `process_changes` is invoked since
# FetchDelayedFn is usually using a slower API than FetchRecentFn, and calling
# FetchDelayedFn every time would negate the benefit of having a faster
# FetchRecentFn.
delayed_fetch_minimum_window = timedelta(minutes=5)

# Limit the maximum time window we will ask FetchRecentFn to get changes for.
# This is to prevent the time window from becoming excessively large if there
# are relatively brief outages in the connector (on the order of a day or two),
# which otherwise would require a checkpoint containing all of the documents
# from the last log_cursor up until the present time. This allows for a form of
# incremental progress to be made, which is helpful if there are ~millions of
# documents to catch up on. The HubSpot APIs are pretty flaky and will
# occasionally return non-retryable errors for no reason and a successful
# checkpoint requires that to never happen, so really large checkpoints can
# become effectively impossible to complete.
#
# Generally moving the cursor forward in these fixed windows is a bad idea when
# trying to cover time windows on the scales of ~years (as in for a backfill),
# but incremental bindings shouldn't fall more than ~days behind in the worst of
# cases so requesting an hour at a time won't take too long.
max_fetch_recent_window = timedelta(hours=1)


class MustBackfillBinding(Exception):
    pass


async def process_changes(
    object_name: str,
    fetch_recent: FetchRecentFn,
    fetch_delayed: FetchDelayedFn,
    http: HTTPSession,
    with_history: bool,
    # Remainder is common.FetchChangesFn:
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Any | LogCursor, None]:
    """
    High-level processing of changes event documents, where there is both a
    stream of very recent documents that is potentially incomplete, and a
    trailing stream that is delayed but 100% complete.

    Every time this function is invoked, the FetchRecentFn is called and any
    recent documents are emitted. If a sufficient amount of time has passed
    since the last time the delayed stream was polled, the FetchDelayedFn is
    called and those documents are emitted to ensure all documents are captured.
    This process will inevitably result in some duplicate documents being
    emitted, but some duplicates are better than missing data completely.

    The LogCursor is based on the position of the recent stream in time. To keep
    track of the progress of both the recent and delayed streams with a single
    LogCursor, there's a few of important details to note:

    - The delayed stream will only ever read documents as recent as
      `delayed_offset` (1 hour).
    - The delayed stream will always be polled up to the LogCursor timestamp
      minus `delayed_offset` if the LogCursor to be emitted would represent a
      delayed stream "window" of at least `delayed_fetch_minimum_window` (5
      minutes).
    - On connector initialization, the delayed stream is assumed to have last
      polled up to the persisted LogCursor, less `delayed_offset` _and_
      `delayed_fetch_minimum_window`. This ensures that when the connector
      restarts the delayed stream does not resume any later than where it last
      left off, and will in fact always resume from just prior to the oldest
      point it may have left off.
    - While the connector is running, the delayed stream progress is kept in
      memory.
    """
    assert isinstance(log_cursor, datetime)

    now_time = datetime.now(UTC)
    fetch_recent_end_time: datetime | None = None
    if now_time - log_cursor > max_fetch_recent_window:
        # Limit the maximum requested window for FetchRecentFn if it's been a
        # while since the LogCursor was updated.
        fetch_recent_end_time = log_cursor + max_fetch_recent_window

    max_ts: datetime = log_cursor
    try:
        async for ts, key, obj in fetch_recent(
            log, http, with_history, log_cursor, fetch_recent_end_time
        ):
            if fetch_recent_end_time and ts > fetch_recent_end_time:
                continue
            elif ts > log_cursor:
                max_ts = max(max_ts, ts)
                cache.add_recent(object_name, key, ts)
                yield obj
            else:
                break
    except MustBackfillBinding:
        log.info("triggering automatic backfill for %s", object_name)
        yield Triggers.BACKFILL

    if fetch_recent_end_time:
        # Assume all recent documents up until fetch_recent_end_time were
        # returned by FetchRecentFn. It's fine if this is not strictly true
        # since FetchDelayedFn will eventually fill in any missed documents.
        # This also ensures that the cursor is always kept moving forward even
        # if there are no new recent documents for a long time, which is
        # important since it moves the delayed stream forward in that case.
        max_ts = fetch_recent_end_time

    delayed_fetch_next_start = last_delayed_fetch_end.setdefault(
        object_name, log_cursor - delayed_offset - delayed_fetch_minimum_window
    )
    delayed_fetch_next_end = max_ts - delayed_offset

    cache_hits = 0
    delayed_emitted = 0
    if delayed_fetch_next_end - delayed_fetch_next_start > delayed_fetch_minimum_window:
        # Poll the delayed stream for documents if we need to.
        try:
            async for ts, key, obj in fetch_delayed(
                log,
                http,
                with_history,
                delayed_fetch_next_start,
                delayed_fetch_next_end,
            ):
                if ts > delayed_fetch_next_end:
                    # In case the FetchDelayedFn is unable to filter based on
                    # `delayed_fetch_next_end`.
                    continue
                elif ts > delayed_fetch_next_start:
                    if cache.has_as_recent_as(object_name, key, ts):
                        cache_hits += 1
                        continue

                    delayed_emitted += 1
                    yield obj
                else:
                    break
        except MustBackfillBinding:
            log.info("triggering automatic backfill for %s", object_name)
            yield Triggers.BACKFILL

        last_delayed_fetch_end[object_name] = delayed_fetch_next_end
        evicted = cache.cleanup(object_name, delayed_fetch_next_end)

        log.info(
            "fetched delayed events for stream",
            {
                "object_name": object_name,
                "since": delayed_fetch_next_start,
                "until": delayed_fetch_next_end,
                "emitted": delayed_emitted,
                "cache_hits": cache_hits,
                "evicted": evicted,
                "new_size": cache.count_for(object_name),
            },
        )

    if max_ts != log_cursor:
        yield max_ts


_FetchIdsFn = Callable[
    [PageCursor, int],
    Awaitable[tuple[Iterable[tuple[datetime, str]], PageCursor]],
]
"""
Returns a stream of object IDs that can be used to fetch the full object details
along with its associations. Used in `fetch_changes_with_associations`.

IDs may be returned in any order, but iteration will be stopped upon seeing an
entry that's as-old or older than the datetime cursor. Entries newer than the
until datetime will be discarded.
"""


async def fetch_changes_with_associations(
    object_name: str,
    cls: type[CRMObject],
    fetcher: _FetchIdsFn,
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, CRMObject], None]:

    # Walk pages of recent IDs until we see one which is as-old
    # as `since`, or no pages remain.
    recent: list[tuple[datetime, str]] = []
    next_page: PageCursor = None
    count = 0

    while True:
        iter, next_page = await fetcher(next_page, count)

        for ts, id in iter:
            count += 1
            if until and ts > until:
                continue
            elif ts > since:
                # TODO(whb): It may be worth consulting the emitted changes
                # cache here to see if we have already emitted a more recent
                # change event before we do all the associations fetching work.
                # Before implementing that I'd like to make sure that the
                # top-level filtering works in production. Since the delayed
                # changes stream only runs every 5 minutes or so it shouldn't be
                # a huge load on the connector.
                recent.append((ts, id))
            else:
                next_page = None

        if not next_page:
            break

    recent.sort()  # Oldest updates first.

    async def _do_batch_fetch(
        batch: list[tuple[datetime, str]],
    ) -> Iterable[tuple[datetime, str, CRMObject]]:
        # Enable lookup of datetimes for IDs from the result batch.
        dts = {id: dt for dt, id in batch}

        attempt = 1
        while True:
            try:
                documents: BatchResult[CRMObject] = await fetch_batch_with_associations(
                    log, cls, http, with_history, object_name, [id for _, id in batch]
                )
                break
            except Exception as e:
                if attempt == 5:
                    raise
                log.warning(
                    "failed to fetch batch with associations (will retry)",
                    {"error": str(e), "attempt": attempt},
                )
                await asyncio.sleep(attempt * 2)
                attempt += 1

        return ((dts[str(doc.id)], str(doc.id), doc) for doc in documents.results)

    async def _batches_gen() -> (
        AsyncGenerator[Awaitable[Iterable[tuple[datetime, str, CRMObject]]], None]
    ):
        for batch_it in itertools.batched(recent, 50 if with_history else 100):
            yield _do_batch_fetch(list(batch_it))

    total = len(recent)
    if total >= 10_000:
        log.info(
            "will process large batch of changes with associations", {"total": total}
        )

    count = 0
    async for res in buffer_ordered(_batches_gen(), 3):
        for ts, id, doc in res:
            count += 1
            if count > 0 and count % 10_000 == 0:
                log.info(
                    "fetching changes with associations",
                    {"count": count, "total": total},
                )
            yield ts, id, doc


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
                "value": _dt_to_str(since),
                "highValue": _dt_to_str(until),
            }
            if until
            else {
                "propertyName": last_modified_property_name,
                "operator": "GTE",
                "value": _dt_to_str(since),
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
                "value": _dt_to_str(modified),
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


def fetch_recent_companies(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Company], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        if count >= 9_900:
            log.warn("limit of 9,900 recent companies reached")
            return [], None

        url = f"{HUB}/companies/v2/companies/recent/modified"
        params = {"count": 100, "offset": page} if page else {"count": 1}

        result = OldRecentCompanies.model_validate_json(
            await http.request(log, url, params=params)
        )
        return (
            (_ms_to_dt(r.properties.hs_lastmodifieddate.timestamp), str(r.companyId))
            for r in result.results
        ), result.hasMore and result.offset

    return fetch_changes_with_associations(
        Names.companies, Company, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_companies(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Company], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(
            Names.companies, log, http, since, until, page
        )

    return fetch_changes_with_associations(
        Names.companies, Company, do_fetch, log, http, with_history, since, until
    )


def fetch_recent_contacts(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Contact], None]:
    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        if count >= 9_900:
            # There is actually no documented limit on the number of contacts
            # that can be returned by this API, other than that it goes back a
            # maximum of 30 days. But since there is no way to filter the
            # response by `until`, we impose the same limit on the number of
            # recent IDs that will be fetched here as other ID fetchers to
            # prevent cases of trying to cycle through huge numbers of results
            # if the LogCursor hasn't been updated in a long time.
            log.warn("limit of 9,900 recent contacts reached")
            return [], None

        url = f"{HUB}/contacts/v1/lists/recently_updated/contacts/recent"
        params = {"count": 100, "timeOffset": page} if page else {"count": 1}

        result = OldRecentContacts.model_validate_json(
            await http.request(log, url, params=params)
        )
        return (
            (_ms_to_dt(int(r.properties.lastmodifieddate.value)), str(r.vid))
            for r in result.contacts
        ), result.has_more and result.time_offset

    return fetch_changes_with_associations(
        Names.contacts, Contact, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_contacts(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Contact], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(
            Names.contacts, log, http, since, until, page, "lastmodifieddate"
        )

    return fetch_changes_with_associations(
        Names.contacts, Contact, do_fetch, log, http, with_history, since, until
    )


def fetch_recent_deals(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Deal], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        if count >= 9_900:
            log.warn("limit of 9,900 recent deals reached")
            return [], None

        url = f"{HUB}/deals/v1/deal/recent/modified"
        params = {"count": 100, "offset": page} if page else {"count": 1}

        result = OldRecentDeals.model_validate_json(
            await http.request(log, url, params=params)
        )

        return (
            (_ms_to_dt(r.properties.hs_lastmodifieddate.timestamp), str(r.dealId))
            for r in result.results
        ), result.hasMore and result.offset

    return fetch_changes_with_associations(
        Names.deals, Deal, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_deals(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Deal], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.deals, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.deals, Deal, do_fetch, log, http, with_history, since, until
    )


async def _fetch_engagements(
    log: Logger, http: HTTPSession, page: PageCursor, count: int
) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
    if count >= 9_900:
        # "Engagements" as we are capturing them has a 10k limit on how many
        # items the API can return, and there is no other API that can be used
        # to get them within a certain time window. The only option here is to
        # re-backfill.
        log.warn("limit of 9,900 recent engagements reached")
        raise MustBackfillBinding

    url = f"{HUB}/engagements/v1/engagements/recent/modified"
    params = {"count": 100, "offset": page} if page else {"count": 1}

    result = OldRecentEngagements.model_validate_json(
        await http.request(log, url, params=params)
    )
    return (
        (_ms_to_dt(r.engagement.lastUpdated), str(r.engagement.id))
        for r in result.results
    ), result.hasMore and result.offset


def fetch_recent_engagements(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Engagement], None]:
    return fetch_changes_with_associations(
        Names.engagements,
        Engagement,
        functools.partial(_fetch_engagements, log, http),
        log,
        http,
        with_history,
        since,
        until,
    )


def fetch_delayed_engagements(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Engagement], None]:
    # There is no way to fetch engagements other than starting with the most
    # recent and reading backward, so this is the same process as fetching
    # "recent" engagements. It relies on the filtering in
    # `fetch_changes_with_associations` to ignore the more recent items.
    return fetch_changes_with_associations(
        Names.engagements,
        Engagement,
        functools.partial(_fetch_engagements, log, http),
        log,
        http,
        with_history,
        since,
        until,
    )


def fetch_recent_tickets(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Ticket], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        # This API will return a maximum of 1000 tickets, and does not appear to
        # ever return an error. It just ends at the 1000 most recently modified
        # tickets.
        url = f"{HUB}/crm-objects/v1/change-log/tickets"
        params = {"timestamp": int(since.timestamp() * 1000) - 1}

        result = TypeAdapter(list[OldRecentTicket]).validate_json(
            await http.request(log, url, params=params)
        )
        return ((_ms_to_dt(r.timestamp), str(r.objectId)) for r in result), None

    return fetch_changes_with_associations(
        Names.tickets, Ticket, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_tickets(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Ticket], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.tickets, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.tickets, Ticket, do_fetch, log, http, with_history, since, until
    )


def fetch_recent_products(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Product], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.products, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.products, Product, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_products(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Product], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.products, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.products, Product, do_fetch, log, http, with_history, since, until
    )


def fetch_recent_line_items(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, LineItem], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(
            Names.line_items, log, http, since, until, page
        )

    return fetch_changes_with_associations(
        Names.line_items, LineItem, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_line_items(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, LineItem], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(
            Names.line_items, log, http, since, until, page
        )

    return fetch_changes_with_associations(
        Names.line_items, LineItem, do_fetch, log, http, with_history, since, until
    )


def fetch_recent_goals(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Goals], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.goals, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.goals, Goals, do_fetch, log, http, with_history, since, until
    )


def fetch_delayed_goals(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, Goals], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(Names.goals, log, http, since, until, page)

    return fetch_changes_with_associations(
        Names.goals, Goals, do_fetch, log, http, with_history, since, until
    )


def fetch_recent_feedback_submissions(
    log: Logger,
    http: HTTPSession,
    with_history: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, FeedbackSubmission], None]:

    async def do_fetch(
        page: PageCursor, count: int
    ) -> tuple[Iterable[tuple[datetime, str]], PageCursor]:
        return await fetch_search_objects(
            Names.feedback_submissions, log, http, since, until, page
        )

    return fetch_changes_with_associations(
        Names.feedback_submissions,
        FeedbackSubmission,
        do_fetch,
        log,
        http,
        with_history,
        since,
        until,
    )


def fetch_delayed_feedback_submissions(
    log: Logger, http: HTTPSession, with_history: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, FeedbackSubmission], None]:
    return fetch_recent_feedback_submissions(log, http, with_history, since, until)


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


async def fetch_email_events_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[EmailEvent | PageCursor, None]:

    assert isinstance(cutoff, datetime)

    url = f"{HUB}/email/public/v1/events"
    input: Dict[str, Any] = {
        "endTimestamp": _dt_to_ms(cutoff) - 1,  # endTimestamp is inclusive.
        "limit": 1000,
    }
    if page:
        input["offset"] = page

    result = EmailEventsResponse.model_validate_json(
        await http.request(log, url, params=input)
    )

    for event in result.events:
        yield event

    if result.hasMore:
        yield result.offset


async def _fetch_email_events(
    log: Logger, http: HTTPSession, since: datetime, until: datetime | None
) -> AsyncGenerator[tuple[datetime, str, EmailEvent], None]:
    url = f"{HUB}/email/public/v1/events"

    input: Dict[str, Any] = {
        "startTimestamp": _dt_to_ms(since),
        "limit": 1000,
    }
    if until:
        input["endTimestamp"] = _dt_to_ms(until)

    while True:
        result = EmailEventsResponse.model_validate_json(
            await http.request(log, url, params=input)
        )

        for event in result.events:
            yield event.created, event.id, event

        if not result.hasMore:
            break

        input["offset"] = result.offset


def fetch_recent_email_events(
    log: Logger, http: HTTPSession, _: bool, since: datetime, until: datetime | None
) -> AsyncGenerator[tuple[datetime, str, EmailEvent], None]:

    return _fetch_email_events(log, http, since + timedelta(milliseconds=1), until)


def fetch_delayed_email_events(
    log: Logger, http: HTTPSession, _: bool, since: datetime, until: datetime
) -> AsyncGenerator[tuple[datetime, str, EmailEvent], None]:

    return _fetch_email_events(log, http, since, until)


async def _paginate_through_marketing_emails(
    log: Logger,
    http: HTTPSession,
    params: dict[str, str | int],
) -> AsyncGenerator[MarketingEmail, None]:
    url = f"{HUB}/marketing/v3/emails"

    input: dict[str, str | int] = {}
    input.update(params)

    while True:
        response = PageResult[MarketingEmail].model_validate_json(
            await http.request(log, url, params=input)
        )

        for email in response.results:
            yield email

        if not response.paging:
            break

        input["after"] = response.paging.next.after


async def _fetch_marketing_emails_updated_between(
    log: Logger,
    http: HTTPSession,
    start: datetime,
    end: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, MarketingEmail], None]:
    if not end:
        end = datetime.now(tz=UTC)

    assert start < end

    params = {
        "limit": MARKETING_EMAILS_PAGE_SIZE,
        "sort": "updatedAt",
        # updatedAfter is an exclusive lower bound filter on the updatedAt field.
        "updatedAfter": _dt_to_str(start),
        # updatedBefore is an exclusive upper bound filter on the updatedAt field.
        "updatedBefore": _dt_to_str(end),
    }

    # The /marketing/v3/emails API has a bug: when "includeStats=true", HubSpot returns
    # an incomplete subset of emails, omitting some records entirely. When "includeStats=false",
    # all emails are returned but without statistics. To work around this API limitation and
    # ensure we capture all records, we use a two-pass approach: 1) fetch emails with stats
    # enabled and yield all returned records, 2) fetch all emails without stats and yield
    # only those records that were missing from the first pass.
    # Additionally, we have to make separate queries for archived and non-archived records.
    # NOTE: This means yielded documents are *not* ordered by the updatedAt field.

    seen_ids: set[str] = set()
    for archived_query_param in ["false", "true"]:
        for include_stats_query_param in ["true", "false"]:
            params["archived"] = archived_query_param
            params["includeStats"] = include_stats_query_param
            async for email in _paginate_through_marketing_emails(log, http, params):
                if email.id not in seen_ids:
                    seen_ids.add(email.id)
                    yield (email.updatedAt, email.id, email)


async def fetch_marketing_emails_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[MarketingEmail | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    start = EPOCH_PLUS_ONE_SECOND

    # Since we must make multiple separate queries to fetch all marketing emails updated in a
    # certain date window, results are unordered and we cannot emit a checkpoint until all
    # results in the date window from start to end are yielded. For now, fetch_marketing_emails_page
    # is simple and tries to fetch all marketing emails in a single sweep. Typically, most accounts don't
    # have tons of marketing emails, so this backfill doesn't take very long to complete.
    async for _, _, email in _fetch_marketing_emails_updated_between(
        log, http, start=start, end=cutoff
    ):
        yield email


def fetch_recent_marketing_emails(
    log: Logger,
    http: HTTPSession,
    _: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, MarketingEmail], None]:

    return _fetch_marketing_emails_updated_between(log, http, since, until)


def fetch_delayed_marketing_emails(
    log: Logger,
    http: HTTPSession,
    _: bool,
    since: datetime,
    until: datetime,
) -> AsyncGenerator[tuple[datetime, str, MarketingEmail], None]:

    return _fetch_marketing_emails_updated_between(log, http, since, until)


async def _request_list_page(
    http: HTTPSession, log: Logger, offset: int, prop_names: list[str] = []
) -> ListSearch:
    url = f"{HUB}/crm/v3/lists/search"

    request_body = {
        "count": 500,
        "offset": offset,
        "sort": "HS_LIST_ID",  # Required for list membership backfill checkpoints
        "additionalProperties": prop_names,
    }

    return ListSearch.model_validate_json(
        await http.request(
            log,
            url,
            method="POST",
            json=request_body,
        )
    )


async def _request_contact_lists(
    http: HTTPSession, log: Logger, should_fetch_all_properties: bool
) -> AsyncGenerator[ContactList, None]:
    """
    The lists search API is different enough that it requires its own
    integration logic. Markedly, it is currently impossible to filter
    or sort results based on the `hs_lastmodifieddate` property.
    In-memory filtering is the most viable option at the moment.

    Results are in ascending list id order.
    """
    pagination_offset = 0

    if should_fetch_all_properties:
        all_props = (
            await fetch_properties(log, http, OBJECT_TYPE_IDS["contact_list"])
        ).results
        prop_names = [p.name for p in all_props]
    else:
        prop_names = ["hs_lastmodifieddate"]

    while True:
        response = await _request_list_page(http, log, pagination_offset, prop_names)

        for item in response.lists:
            if isinstance(item, ContactList):
                yield item

        pagination_offset = response.offset
        if not response.hasMore:
            return


async def check_contact_lists_access(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[ContactList, None]:
    """Lightweight wrapper for permission checking contact lists endpoint."""
    async for contact_list in _request_contact_lists(http, log, False):
        yield contact_list


async def check_contact_list_memberships_access(
    http: HTTPSession, log: Logger
) -> AsyncGenerator[ContactListMembership, None]:
    """Lightweight wrapper for permission checking contact list memberships endpoint."""
    async for contact_list in _request_contact_lists(http, log, False):
        url = f"{HUB}/crm/v3/lists/{contact_list.listId}/memberships"

        response = ContactListMembershipResponse.model_validate_json(
            (await http.request(log, url, params={"limit": 1})), context=contact_list
        )
        for membership in response.results:
            yield membership


async def _request_contact_lists_in_time_range(
    http: HTTPSession,
    log: Logger,
    should_fetch_all_properties: bool,
    start: datetime,
    end: datetime | None = None,
) -> AsyncGenerator[ContactList, None]:
    async for item in _request_contact_lists(http, log, should_fetch_all_properties):
        last_modified_date = item.get_cursor_value()

        if last_modified_date < start:
            continue
        if end and last_modified_date >= end:
            continue

        yield item


async def fetch_contact_lists_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ContactList, None]:
    assert isinstance(page, NoneType)
    assert isinstance(cutoff, datetime)

    page_ts = (
        datetime.fromisoformat(page) if page is not None else EPOCH_PLUS_ONE_SECOND
    )

    async for item in _request_contact_lists_in_time_range(
        http, log, True, page_ts, cutoff
    ):
        yield item

    return


async def fetch_contact_lists(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[ContactList | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    # We are deliberately not implementing the same delayed stream mechanism
    # used in CRM object APIs. We have no way to filter/sort by `hs_lastmodifieddate`
    # and we don't want to paginate through all items twice
    start_date = log_cursor - delayed_offset
    now = datetime.now(tz=UTC)

    async for item in _request_contact_lists_in_time_range(http, log, True, start_date):
        log.info("Processing contact list", {"listId": item.listId})
        timestamp = _ms_to_dt(int(item.additionalProperties["hs_lastmodifieddate"]))

        if cache.should_yield(Names.contact_lists, item.listId, timestamp):
            yield item

    yield now


async def _request_contact_list_memberships(
    http: HTTPSession,
    log: Logger,
    contact_list: ContactList,
    start: datetime,
    end: datetime | None = None,
) -> AsyncGenerator[ContactListMembership, None]:
    url = f"{HUB}/crm/v3/lists/{contact_list.listId}/memberships"
    params = {"limit": 250}

    response = ContactListMembershipResponse.model_validate_json(
        await http.request(log, url, params=params),
        context=contact_list,
    )

    for item in response.results:
        if item.membershipTimestamp < start:
            continue
        if end and item.membershipTimestamp >= end:
            continue

        yield item

        if (next_cursor := response.get_next_cursor()) is None:
            break
        params["after"] = next_cursor


@dataclass
class ContactListMembershipControl:
    """Coordination for concurrent contact list membership fetches."""

    # We expect users to have tens of thousands of lists to fetch.
    # This semaphore limits the number of tasks we keep in memory at any given time
    task_slots: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(5))
    results: asyncio.Queue[ContactListMembership | None] = field(
        default_factory=lambda: asyncio.Queue(1)
    )
    finished_tasks: int = 0
    first_error: str | None = None


async def _list_memberships_worker(
    http: HTTPSession,
    log: Logger,
    contact_list: ContactList,
    context: ContactListMembershipControl,
    start: datetime,
    end: datetime | None = None,
) -> None:
    try:
        async for item in _request_contact_list_memberships(
            http, log, contact_list, start, end
        ):
            await context.results.put(item)

        log.info(
            "Finished fetching memberships for list",
            {"listId": contact_list.listId},
        )
        await context.results.put(None)
    except CancelledError as e:
        # If no worker has been cancelled or encountered an exception yet,
        # that means this worker was cancelled from something other than the
        # TaskGroup. A non-CancelledError should be raised so the
        # TaskGroup cancels the remaining tasks & propagates an exception
        # up through the connector.
        if not context.first_error:
            msg = format_error_message(e)
            context.first_error = msg

            raise Exception(
                f"Contact list {contact_list.listId} worker was unexpectedly cancelled: {msg}"
            )
        # If a different worker already failed, then this worker task was cancelled by
        # the TaskGroup as part of its exception handling and it's safe to propagate the
        # CancelledError.
        else:
            raise e
    except BaseException as e:
        msg = format_error_message(e)
        if not context.first_error:
            context.first_error = msg

        log.error(
            f"Contact list {contact_list.listId} worker encountered an error.",
            {
                "exception": msg,
            },
        )
        raise e


async def _request_all_contact_list_memberships(
    http: HTTPSession, log: Logger, start: datetime, end: datetime | None = None
) -> AsyncGenerator[ContactListMembership, None]:
    log.info(
        "Fetching contact list memberships",
        {
            "start": start.isoformat(),
            "end": end.isoformat() if end else None,
        },
    )

    control = ContactListMembershipControl()
    all_lists = [
        item
        async for item in _request_contact_lists_in_time_range(
            http,
            log,
            # We only care about listIds, so we do not want to make an extra request
            #  asking for all the additional properties there are
            False,
            start,
            end,
        )
    ]

    async with asyncio.TaskGroup() as tg:

        async def dispatcher() -> None:
            for contact_list in all_lists:
                await control.task_slots.acquire()
                tg.create_task(
                    _list_memberships_worker(
                        http, log, contact_list, control, start, end
                    )
                )

        tg.create_task(dispatcher())

        while True:
            item = await control.results.get()
            if item is None:
                # A worker task has completed
                control.finished_tasks += 1
                control.task_slots.release()

                if control.finished_tasks == len(all_lists):
                    break
            else:
                yield item


async def fetch_contact_list_memberships_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[ContactListMembership | PageCursor, None]:
    assert isinstance(page, int | None)
    assert isinstance(cutoff, datetime)

    next_list = await anext(
        (
            item
            async for item in _request_contact_lists(http, log, False)
            if page is None or item.listId > page
        ),
        None,
    )
    if next_list is None:
        return

    async for membership in _request_contact_list_memberships(
        http, log, next_list, EPOCH_PLUS_ONE_SECOND
    ):
        yield membership

    yield next_list.listId


async def fetch_contact_list_memberships(
    http: HTTPSession, log: Logger, log_cursor: LogCursor
) -> AsyncGenerator[ContactListMembership | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    # We are deliberately not implementing the same delayed stream mechanism
    # used in CRM object APIs. We have no way to filter/sort by `hs_lastmodifieddate`
    # and we don't want to paginate through all items twice
    start_date = log_cursor - delayed_offset
    now = datetime.now(tz=UTC)

    async for item in _request_all_contact_list_memberships(http, log, start_date):
        key = f"{item.listId}:{item.membershipTimestamp}"

        if cache.should_yield(
            Names.contact_list_memberships, key, item.membershipTimestamp
        ):
            yield item

    yield now


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)


def _dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _str_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _dt_to_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _chunk_props(props: list[str], max_bytes: int) -> list[list[str]]:
    result: list[list[str]] = []

    current_chunk: list[str] = []
    current_size = 0

    for p in props:
        sz = len(p.encode("utf-8"))

        if current_size + sz > max_bytes:
            result.append(current_chunk)
            current_chunk = []
            current_size = 0

        current_chunk.append(p)
        current_size += sz

    if current_chunk:
        result.append(current_chunk)

    return result
