from base64 import b64decode
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import Headers, HTTPSession, ShouldRetryProtocol
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from ijson import JSONError
from pydantic import AwareDatetime, BaseModel, field_validator

from .models import MetadataResponse, ObjectMetadata, RecordsRemainder, TwentyRecord

# Twenty Cloud sits behind Cloudflare, which refuses some default user agents.
HEADERS = {"User-Agent": "Estuary source-twenty"}

# Twenty stamps updatedAt before commit; lag lets recent writes settle.
LAG = timedelta(minutes=1)
# Record lists silently clamp larger limits to 200; metadata pages are requested at the same size.
PAGE_SIZE = 200
# Validation and discovery reads give up on a persistent server error instead of retrying it forever.
VALIDATION_ATTEMPTS = 3


class _RecordCursor(BaseModel):
    updatedAt: AwareDatetime
    id: str

    @field_validator("updatedAt", mode="before")
    @classmethod
    def parse_updated_at(cls, value: object) -> object:
        return datetime.fromisoformat(value) if isinstance(value, str) else value


def _decode_cursor(cursor: str) -> tuple[datetime, str] | None:
    # Decoding is best-effort so a changed cursor format won't break captures.
    try:
        decoded = _RecordCursor.model_validate_json(
            b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        )
        return decoded.updatedAt, decoded.id
    except ValueError:
        return None


def validation_should_retry(
    status: int, headers: Headers, body: bytes, attempt: int
) -> bool:
    return attempt < VALIDATION_ATTEMPTS


def now() -> datetime:
    return datetime.now(tz=UTC)


def capture_horizon() -> datetime:
    timestamp = now()
    return timestamp.replace(microsecond=timestamp.microsecond // 1000 * 1000) - LAG


def _timestamp(value: datetime) -> str:
    return (
        value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def _next_cursor(cursor: str | None, seen: set[str]) -> str:
    # Twenty's cursors are value-based keysets, so a repeat means paging would loop.
    if cursor is None or cursor in seen:
        raise RuntimeError(f"Twenty's paging did not advance (endCursor {cursor!r}).")
    seen.add(cursor)
    return cursor


async def list_objects(
    log: Logger,
    http: HTTPSession,
    base_url: str,
    should_retry: ShouldRetryProtocol | None = None,
) -> AsyncGenerator[ObjectMetadata, None]:
    url = f"{base_url.rstrip('/')}{ObjectMetadata.PATH}"
    params: dict[str, str | int] = {"limit": PAGE_SIZE}
    seen: set[str] = set()

    while True:
        response = MetadataResponse.model_validate_json(
            await http.request(
                log,
                url,
                params=params,
                headers=HEADERS,
                should_retry=should_retry,
            )
        )
        for obj in response.data:
            yield obj

        if not response.pageInfo.hasNextPage:
            return

        params["starting_after"] = _next_cursor(response.pageInfo.endCursor, seen)


async def probe_object(
    log: Logger, http: HTTPSession, base_url: str, object_name: str
) -> None:
    # Sends the capture's own filter and sort order, so a role refused either fails at publish.
    processor = await _list_records(
        log,
        http,
        base_url,
        object_name,
        capture_horizon(),
        limit=1,
        should_retry=validation_should_retry,
    )
    try:
        async for _ in processor:
            pass
    except JSONError as err:
        raise ValueError(str(err)) from err
    processor.get_remainder().page_info(object_name)


async def _list_records(
    log: Logger,
    http: HTTPSession,
    base_url: str,
    object_name: str,
    before: datetime,
    since: datetime | None = None,
    page: str | None = None,
    limit: int = PAGE_SIZE,
    should_retry: ShouldRetryProtocol | None = None,
) -> IncrementalJsonProcessor[TwentyRecord, RecordsRemainder]:
    url = base_url.rstrip("/") + TwentyRecord.PATH_TEMPLATE.format(
        name_plural=object_name
    )
    filters: list[str] = []
    if since is not None:
        filters.append(f'{TwentyRecord.SINCE_PARAM}:"{_timestamp(since)}"')
    filters.extend(
        [
            f'{TwentyRecord.BEFORE_PARAM}:"{_timestamp(before)}"',
            TwentyRecord.INCLUDE_DELETED_FILTER,
        ]
    )
    # The HTTP layer URL-encodes these parameters, including the filter, once.
    params: dict[str, str | int] = {
        "limit": limit,
        "depth": 0,
        "order_by": TwentyRecord.ORDER_BY,
        "filter": f"and({','.join(filters)})",
    }
    if page is not None:
        params["starting_after"] = page

    headers, body = await http.request_stream(
        log, url, params=params, headers=HEADERS, should_retry=should_retry
    )
    key = TwentyRecord.ITEMS_KEY.format(name_plural=object_name)
    return IncrementalJsonProcessor(
        body(),
        prefix=f"data.{key}.item",
        streamed_item_cls=TwentyRecord,
        remainder_cls=RecordsRemainder,
    )


async def backfill_records(
    http: HTTPSession,
    base_url: str,
    object_name: str,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[TwentyRecord | PageCursor, None]:
    """Capture history before the shared cutoff, one resumable page at a time.

                      cutoff - 1ms   cutoff
    ───────────────────────┼──────────┼─────▶ time (1ms ticks)
                           │          │
    updatedAt[lt] ═════════╪══════════(
    window        ◀════════]──────────┼─────
                           │          └─ first incremental tick (gte)
                           └─ last backfill tick; history has no lower bound

    The upper bound is exclusive; the incremental lower bound is inclusive.
    PageCursor preserves Twenty's value-based (updatedAt, id) endCursor.
    """
    assert isinstance(cutoff, datetime)
    assert page is None or isinstance(page, str)

    processor = await _list_records(log, http, base_url, object_name, cutoff, page=page)
    async for doc in processor:
        yield doc

    page_info = processor.get_remainder().page_info(object_name)
    if page_info.hasNextPage:
        if page is not None and page_info.endCursor is not None:
            current_key = _decode_cursor(page)
            next_key = _decode_cursor(page_info.endCursor)
            # Only a step back in time or an exact repeat fails, so a server that
            # rounds the cursor's timestamp can't trip on ties broken by id.
            if (
                current_key is not None
                and next_key is not None
                and (next_key[0] < current_key[0] or next_key == current_key)
            ):
                raise RuntimeError(
                    f"Twenty's paging did not advance (page cursor {page!r}, "
                    f"endCursor {page_info.endCursor!r})."
                )
        yield _next_cursor(page_info.endCursor, {page} if page else set())


async def fetch_records(
    http: HTTPSession,
    base_url: str,
    object_name: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TwentyRecord | LogCursor, None]:
    """Capture a settled window of live records, soft deletes, and restores.

                       cursor    horizon - 1ms   horizon
    ──────────────────────┼────────────┼────────────┼─────▶ time (1ms ticks)
                          │            │            │
    updatedAt[gte] ───────[════════════╪════════════╪═════▶
    updatedAt[lt]  ═══════╪════════════╪════════════(
    emitted        ───────[════════════]────────────┼─────
                          │            │            └─ floor(now, ms) - LAG;
                          │            │               recent writes wait
                          │            └─ last emitted tick
                          └─ inclusive resume preserves ties across pages

    The lower bound is inclusive and the upper bound is exclusive.
    Page checkpoints may replay their millisecond on restart; the completed
    window checkpoints its horizon only after emitting documents.
    """
    assert isinstance(log_cursor, datetime)
    horizon = capture_horizon()
    if horizon <= log_cursor:
        return

    page: str | None = None
    seen: set[str] = set()
    checkpoint = log_cursor
    emitted = False

    while True:
        processor = await _list_records(
            log, http, base_url, object_name, horizon, since=log_cursor, page=page
        )
        last_record = None
        async for doc in processor:
            yield doc
            emitted = True
            last_record = doc

        page_info = processor.get_remainder().page_info(object_name)
        if not page_info.hasNextPage:
            break

        if last_record is not None and last_record.get_cursor() > checkpoint:
            checkpoint = last_record.get_cursor()
            yield checkpoint

        page = _next_cursor(page_info.endCursor, seen)

    if emitted:
        yield horizon
