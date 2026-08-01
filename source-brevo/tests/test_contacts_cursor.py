"""Unit tests for the parts of the Brevo fetch layer the snapshot tests cannot
reach.

`tests/test_snapshots.py` drives `flowctl preview` against a live account, so it
only exercises whatever that account happens to contain and can never provoke a
particular cursor or error state. These cover the contracts the CDK imposes:

  - a `FetchChangesFn` that emitted documents must yield a LogCursor
    (`estuary_cdk/capture/common.py`, "yielded documents without a final
    LogCursor"), and each cursor must strictly exceed the last;
  - a `FetchPageFn` must stop by returning without a PageCursor;
  - a snapshot that comes back short makes the CDK tombstone the missing rows,
    so a truncated snapshot silently deletes data.
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncGenerator

import pytest
from estuary_cdk.http import HTTPError
from source_brevo.api import (
    WEBHOOK_TYPES,
    backfill_contacts,
    fetch_contacts_changes,
    snapshot_resource,
    snapshot_webhooks,
)
from source_brevo.models import Contact, ContactAttribute, Webhook

LOG = logging.getLogger(__name__)
CURSOR = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
PAGE_SIZE = Contact.PAGE_SIZE or 0


class StubSession:
    """Stand-in for HTTPSession serving canned JSON bodies.

    The signature mirrors `HTTPSession.request_stream` positionally — `method`
    really is the third parameter — so a caller that stopped passing `params` by
    keyword fails here rather than in production. Serving encoded bytes keeps the
    real IncrementalJsonProcessor and Pydantic parsing inside the test."""

    def __init__(
        self,
        pages: list[list[dict[str, Any]]],
        items_key: str = "contacts",
        errors: dict[int, Exception] | None = None,
    ):
        self.pages = pages
        self.items_key = items_key
        self.errors = errors or {}
        self.requests: list[dict[str, Any]] = []

    async def request_stream(
        self,
        log: logging.Logger,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[dict[str, str], Any]:
        index = len(self.requests)
        self.requests.append(params or {})

        if index in self.errors:
            raise self.errors[index]

        page = self.pages.pop(0) if self.pages else []
        rows = ",".join(_row_json(r) for r in page)
        body = ('{"%s":[%s]}' % (self.items_key, rows)).encode()

        async def gen() -> AsyncGenerator[bytes, None]:
            yield body

        return {}, gen


def _row_json(row: dict[str, Any]) -> str:
    if "modifiedAt" in row:
        return '{"id":%d,"modifiedAt":"%s"}' % (row["id"], row["modifiedAt"])
    return '{"id":%d}' % row["id"]


def _at(offset: timedelta) -> str:
    return (CURSOR + offset).isoformat()


def _contacts(count: int, offset: timedelta = timedelta()) -> list[dict[str, Any]]:
    return [{"id": i, "modifiedAt": _at(offset)} for i in range(count)]


async def _drain(gen: AsyncGenerator[Any, None]) -> list[Any]:
    return [item async for item in gen]


@pytest.mark.asyncio
async def test_boundary_only_row_still_yields_a_cursor():
    """`modifiedSince` is inclusive, so a quiet poll returns the contact sitting
    exactly on the cursor and nothing newer. A document is emitted, so a cursor
    must follow or the CDK kills the task."""
    session = StubSession([[{"id": 1, "modifiedAt": _at(timedelta())}]])

    items = await _drain(fetch_contacts_changes(session, LOG, CURSOR))  # type: ignore[arg-type]

    docs = [i for i in items if isinstance(i, Contact)]
    cursors = [i for i in items if isinstance(i, datetime)]
    assert len(docs) == 1
    assert len(cursors) == 1, "documents were emitted without a following cursor"
    assert items[-1] is cursors[0], "the cursor must come last"
    assert cursors[0] > CURSOR, "cursors must strictly increase"


@pytest.mark.asyncio
async def test_cursor_is_the_sweep_start_instant():
    """The cursor must be when the sweep began — not the newest `modifiedAt`
    seen, which would advance past a mid-sweep modification and lose it, and not
    a token increment over the old cursor, which would never make progress."""
    before = datetime.now(tz=UTC)
    session = StubSession([[{"id": 1, "modifiedAt": _at(timedelta(days=365))}]])

    items = await _drain(fetch_contacts_changes(session, LOG, CURSOR))  # type: ignore[arg-type]
    after = datetime.now(tz=UTC)

    cursor = items[-1]
    assert isinstance(cursor, datetime)
    assert before <= cursor <= after, (
        f"cursor {cursor} is not the sweep-start instant; "
        "max(modifiedAt) would leak future timestamps and lose mid-sweep edits"
    )


@pytest.mark.asyncio
async def test_cursor_still_advances_when_the_clock_runs_backwards():
    """A cursor written by a host with a fast clock, then read on a synced one,
    must not stall the task: the CDK raises if a cursor fails to strictly
    increase. Flooring just above the old cursor errs toward re-reading."""
    future_cursor = datetime.now(tz=UTC) + timedelta(hours=1)
    session = StubSession([[{"id": 1, "modifiedAt": _at(timedelta())}]])

    items = await _drain(fetch_contacts_changes(session, LOG, future_cursor))  # type: ignore[arg-type]

    cursor = items[-1]
    assert isinstance(cursor, datetime)
    assert cursor > future_cursor, "cursor went backwards; the CDK would raise"


@pytest.mark.asyncio
async def test_empty_sweep_still_advances_the_cursor():
    """A poll matching nothing has proven there is nothing left before the sweep
    began, so the window may close — and the cursor must still increase."""
    session = StubSession([[]])

    items = await _drain(fetch_contacts_changes(session, LOG, CURSOR))  # type: ignore[arg-type]

    assert not [i for i in items if isinstance(i, Contact)]
    assert len(items) == 1 and isinstance(items[0], datetime)
    assert items[0] > CURSOR


@pytest.mark.asyncio
async def test_sweep_pages_to_exhaustion_and_filters_on_the_cursor():
    """A full page must trigger another request, and every request must carry
    `modifiedSince` plus ascending creation order."""
    session = StubSession([_contacts(PAGE_SIZE), _contacts(1, timedelta(hours=1))])

    items = await _drain(fetch_contacts_changes(session, LOG, CURSOR))  # type: ignore[arg-type]

    docs = [i for i in items if isinstance(i, Contact)]
    assert len(docs) == PAGE_SIZE + 1
    assert len(session.requests) == 2, "a full page must be followed by another request"
    assert [r["offset"] for r in session.requests] == [0, PAGE_SIZE]
    for request in session.requests:
        assert request["sort"] == "asc"
        assert request["modifiedSince"] == "2026-08-01T12:00:00.000Z"


@pytest.mark.asyncio
async def test_backfill_advances_by_rows_read_not_rows_emitted():
    """The cutoff filter suppresses recent rows, but the next offset must count
    every row the page returned. Counting only emitted rows would slide the
    window backwards and re-read — or, with a full page of suppressed rows,
    stall on the same offset forever."""
    cutoff = CURSOR + timedelta(hours=1)
    # A full page in which every row is newer than the cutoff, so none is emitted.
    session = StubSession([_contacts(PAGE_SIZE, timedelta(hours=2))])

    items = await _drain(backfill_contacts(session, LOG, 0, cutoff))  # type: ignore[arg-type]

    assert not [i for i in items if isinstance(i, Contact)], "cutoff should suppress"
    assert items == [PAGE_SIZE], (
        "the page cursor must advance by rows read, not rows emitted"
    )


@pytest.mark.asyncio
async def test_backfill_stops_on_a_short_page():
    """A short page ends the backfill, signalled by returning without a cursor."""
    session = StubSession([_contacts(3)])

    items = await _drain(backfill_contacts(session, LOG, 0, CURSOR + timedelta(days=1)))  # type: ignore[arg-type]

    assert len([i for i in items if isinstance(i, Contact)]) == 3
    assert not [i for i in items if isinstance(i, int)], (
        "a short page must terminate the backfill, not yield another cursor"
    )


@pytest.mark.asyncio
async def test_unpaginated_snapshot_sends_no_pagination_params():
    """Endpoints with `PAGE_SIZE is None` accept no `limit`/`offset`/`sort`, and
    Brevo rejects surprising parameters outright, so none may be sent."""
    session = StubSession([[{"id": 1}]], items_key=ContactAttribute.ITEMS_KEY)

    await _drain(snapshot_resource(session, LOG, ContactAttribute))  # type: ignore[arg-type]

    assert len(session.requests) == 1
    assert session.requests[0] == {}


@pytest.mark.asyncio
async def test_webhooks_treats_an_opening_document_not_found_as_empty():
    """Brevo reports "no webhooks of this type" as a 400 `document_not_found`
    rather than an empty array, and most accounts leave at least one type
    unused, so that must not fail the stream."""
    session = StubSession(
        [[{"id": 1}], [{"id": 2}]],
        items_key=Webhook.ITEMS_KEY,
        errors={0: HTTPError("... document_not_found ...", 400)},
    )

    items = await _drain(snapshot_webhooks(session, LOG))  # type: ignore[arg-type]

    assert len(session.requests) == len(WEBHOOK_TYPES), "every type must be tried"
    assert len(items) == 2, "the two populated types must still be captured"


@pytest.mark.asyncio
async def test_webhooks_reraises_a_document_not_found_after_rows_were_yielded(
    monkeypatch: pytest.MonkeyPatch,
):
    """Suppressing a 400 raised partway through a walk would hand the CDK a
    short snapshot, and the CDK tombstones every row past a snapshot's end —
    silently deleting live rows. Only an opening 400 may be swallowed.

    `/webhooks` is unpaginated today, so one request per type means every error
    is an opening one and this is unreachable. The guard exists for the day the
    endpoint is paged — it documents `sort`, so that is plausible — and forcing
    a PAGE_SIZE here is what pins it against that change."""
    monkeypatch.setattr(Webhook, "PAGE_SIZE", 2)
    session = StubSession(
        [[{"id": 1}, {"id": 2}]],
        items_key=Webhook.ITEMS_KEY,
        errors={1: HTTPError("... document_not_found ...", 400)},
    )

    with pytest.raises(HTTPError):
        await _drain(snapshot_webhooks(session, LOG))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_webhooks_reraises_an_unrelated_error():
    """The suppression matches on status *and* error code; anything else — a
    revoked key, a malformed request — must still fail the task."""
    session = StubSession(
        [], items_key=Webhook.ITEMS_KEY, errors={0: HTTPError("forbidden", 403)}
    )

    with pytest.raises(HTTPError):
        await _drain(snapshot_webhooks(session, LOG))  # type: ignore[arg-type]
