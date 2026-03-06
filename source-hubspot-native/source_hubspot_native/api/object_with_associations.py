import asyncio
import itertools
from datetime import datetime
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Iterable,
)

from estuary_cdk.buffer_ordered import buffer_ordered
from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession

from ..models import (
    Association,
    BatchResult,
    CRMObject,
    PageResult,
)
from .properties import fetch_properties
from .shared import (
    chunk_props,
    HUB,
)


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
    is_connector_initiated: bool,
) -> AsyncGenerator[CRMObject | str, None]:

    assert isinstance(cutoff, datetime)

    url = f"{HUB}/crm/v3/objects/{object_name}"
    output: list[CRMObject] = []
    properties = await fetch_properties(log, http, object_name)

    # On connector initiated backfills, only capture calculated properties and rely
    # on the merge reduction strategies to merge in partial documents containing
    # updated calculated properties.
    properties_to_fetch: list[str] = [
        p.name for p in properties.results
        if not is_connector_initiated or p.calculated
    ]

    if not properties_to_fetch:
        return

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
    chunked_properties = chunk_props(
        properties_to_fetch,
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
    property_names = [p.name for p in properties.results]

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
