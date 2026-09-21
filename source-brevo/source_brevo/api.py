from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPError, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import BrevoResource, Contact, Webhook

API = "https://api.brevo.com/v3"

# GET /webhooks filters on `type` and defaults to "transactional", so a bare
# request silently returns only part of the account's webhooks. Every webhook
# carries exactly one type, so the three result sets are disjoint and their
# union needs no de-duplication.
WEBHOOK_TYPES = ("marketing", "transactional", "inbound")


def _filter_timestamp(dt: datetime) -> str:
    """Render a datetime the way Brevo's date filters document it:
    `YYYY-MM-DDTHH:mm:ss.SSSZ`. `datetime.isoformat()` emits microseconds and a
    `+00:00` offset instead, so the format is built explicitly."""
    utc = dt.astimezone(UTC)
    return f"{utc:%Y-%m-%dT%H:%M:%S}.{utc.microsecond // 1000:03d}Z"


async def _fetch_page[T: BrevoResource](
    http: HTTPSession,
    log: Logger,
    model: type[T],
    params: dict[str, Any],
) -> AsyncGenerator[T, None]:
    """Stream one page of `model`'s collection endpoint, yielding each item."""
    _, body = await http.request_stream(log, f"{API}/{model.PATH}", params=params)
    processor = IncrementalJsonProcessor(
        body(),
        f"{model.ITEMS_KEY}.item",
        model,
    )

    async for item in processor:
        yield item


async def _paginate[T: BrevoResource](
    http: HTTPSession,
    log: Logger,
    model: type[T],
    params: dict[str, Any] | None = None,
) -> AsyncGenerator[T, None]:
    """Drain a collection endpoint.

    Completion is the short-page signal: a page holding fewer rows than the
    requested `limit` is the last one. That rule holds even where the response
    reports a `count` total, and several of Brevo's collections omit `count`
    entirely, so it is the one signal available everywhere.

    Endpoints that accept no pagination parameters (`PAGE_SIZE is None`) are
    fetched with a single request """
    if model.PAGE_SIZE is None:
        async for item in _fetch_page(http, log, model, params or {}):
            yield item
        return

    offset = 0
    while True:
        count = 0
        # Pagination keys go last so a caller's `params` can never override them
        # and silently break the walk. `sort=asc` keeps the walk append-only:
        # under Brevo's `desc` default every insert renumbers the whole result
        # set, which for a snapshot stream rewrites every `/_meta/row_id`.
        page = _fetch_page(
            http,
            log,
            model,
            {**(params or {}), "limit": model.PAGE_SIZE, "offset": offset, "sort": "asc"},
        )

        async for item in page:
            yield item
            count += 1

        offset += count
        if count < model.PAGE_SIZE:
            return


async def snapshot_resource[T: BrevoResource](
    http: HTTPSession,
    log: Logger,
    model: type[T],
) -> AsyncGenerator[T, None]:
    """Snapshot a collection that offers no server-side date filter."""
    async for item in _paginate(http, log, model):
        yield item


async def snapshot_webhooks(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[Webhook, None]:
    """Snapshot webhooks across all three types — see `WEBHOOK_TYPES`.

    Brevo reports "this account has no webhooks of that type" as a 400 carrying
    `document_not_found`, not as an empty array. Most accounts use only one or
    two of the three types, so letting that surface as a task failure would
    break the stream for nearly everyone. The match is deliberately narrow —
    status *and* error code, and only before this type has produced a row —
    so a genuine 400 still fails loudly.

    That last condition matters more than it looks: suppressing a 400 raised
    *partway* through a walk would truncate the snapshot, and the CDK tombstones
    every row past a shortened snapshot's end, so a mid-walk error would delete
    live rows from the collection. Unreachable while `/webhooks` is unpaginated,
    but the guard costs nothing and the endpoint documents `sort`, so paging it
    later is plausible."""
    for webhook_type in WEBHOOK_TYPES:
        yielded = False
        try:
            async for webhook in _paginate(
                http, log, Webhook, {"type": webhook_type, "sort": "asc"}
            ):
                yielded = True
                yield webhook
        except HTTPError as err:
            if yielded or err.code != 400 or "document_not_found" not in err.message:
                raise
            log.info(
                "no webhooks configured for this type",
                {"type": webhook_type},
            )


async def fetch_contacts_changes(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Contact | LogCursor, None]:
    """Sweep contacts modified at or after `log_cursor`.

    Brevo can only sort /contacts by *creation* date — there is no
    sort-by-modification option — so the rows a `modifiedSince` filter returns
    arrive in no useful cursor order: a later page may hold an older
    `modifiedAt` than an earlier one. The sweep therefore has to drain every
    matching page before it can checkpoint at all.

    The next cursor is the instant the sweep *started*, not the maximum
    `modifiedAt` it saw. /contacts has no `modifiedBefore`-style parameter, so a
    contact modified mid-sweep at a creation-order position already passed is
    missed by this sweep; anchoring the cursor to the start instant means the
    next one re-reads it. Taking `max(modifiedAt)` instead would advance past
    that modification and drop it permanently.

    The cursor is yielded unconditionally, which the CDK requires of any sweep
    that emitted a document — and this one routinely emits on an otherwise quiet
    poll, because `modifiedSince` is inclusive and so keeps returning the contact
    sitting exactly on the cursor.

    `sweep_start` is floored just above `log_cursor` rather than taken raw from
    the clock. The CDK also requires cursors to strictly increase, and a wall
    clock does not guarantee that: a cursor written by a host running fast, then
    read on a correctly-synced one, would otherwise raise on every poll until
    real time caught up. Flooring errs toward re-reading a window, which only
    duplicates."""
    assert isinstance(log_cursor, datetime)

    sweep_start = max(
        datetime.now(tz=UTC), log_cursor + timedelta(milliseconds=1)
    )

    emitted = 0
    async for contact in _paginate(
        http,
        log,
        Contact,
        {"modifiedSince": _filter_timestamp(log_cursor)},
    ):
        emitted += 1
        yield contact

    # The cursor advances whether or not anything matched, so a systematic empty
    # result — a renamed response envelope, a filter Brevo stops honouring —
    # would otherwise look exactly like a healthy idle connector. Logging the
    # count each sweep is what makes that distinguishable.
    log.info(
        "contacts sweep complete",
        {
            "emitted": emitted,
            "modifiedSince": _filter_timestamp(log_cursor),
            "cursor": sweep_start,
        },
    )

    yield sweep_start


async def backfill_contacts(
    http: HTTPSession,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Contact | PageCursor, None]:
    """Walk the historical contacts in ascending creation order.

    `sort=asc` makes offset paging safe against *inserts*: contacts created
    while the backfill runs are appended past the tail, so they can never shift
    a row into or out of an offset window already read. The descending default
    would push every new contact onto page 1 and shift the whole scan.

    It is not safe against *deletions*, and Brevo offers no `sinceId` to make it
    so (`ids` caps out at 20 values). A contact deleted mid-backfill shifts
    everything after it down one, so the row on the next offset boundary is
    skipped, and an old `modifiedAt` means the incremental sweep will not pick
    it up either. Only a full re-backfill recovers such a row; nothing triggers
    one automatically.

    No date filter is applied — `createdSince` would exclude older contacts that
    the backfill exists to collect. Rows modified at or after `cutoff` are left
    to the incremental sweep so the two halves don't both emit them."""
    assert isinstance(page, int)
    assert isinstance(cutoff, datetime)
    assert Contact.PAGE_SIZE is not None

    count = 0
    async for contact in _fetch_page(
        http,
        log,
        Contact,
        {"limit": Contact.PAGE_SIZE, "offset": page, "sort": "asc"},
    ):
        count += 1
        if contact.modifiedAt < cutoff:
            yield contact

    if count < Contact.PAGE_SIZE:
        return

    yield page + count
