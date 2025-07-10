from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from source_monday.graphql import (
    execute_query,
    fetch_activity_logs,
    fetch_boards_by_ids,
    fetch_boards_minimal,
    fetch_items_by_ids,
    fetch_items,
    fetch_boards_with_retry,
    TEAMS,
    USERS,
    TAGS,
)
from source_monday.models import (
    FullRefreshResource,
    Board,
    Item,
)
from source_monday.graphql.items.item_cache import ItemCacheSession
from source_monday.utils import parse_monday_17_digit_timestamp


# Limit incremental sync windows to 2 hours to balance completeness with performance.
# Smaller windows reduce memory usage and improve error recovery, while ensuring
# we don't miss updates due to Monday.com's 10k activity log retention limit per board.
MAX_WINDOW_SIZE = timedelta(hours=2)
FULL_REFRESH_RESOURCES: list[tuple[str, str]] = [
    ("tags", TAGS),
    ("users", USERS),
    ("teams", TEAMS),
]


_boards_backfill_cache: list[str] = []
_item_cache_session: ItemCacheSession | None = None


def dt_to_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


async def _get_or_create_item_cache_session(
    http: HTTPSession, log: Logger, cutoff: datetime
) -> ItemCacheSession:
    global _item_cache_session

    if _item_cache_session is None or _item_cache_session.cutoff != cutoff:
        log.debug(f"Creating new item cache session for cutoff: {cutoff}")
        _item_cache_session = ItemCacheSession(
            http=http,
            log=log,
            cutoff=cutoff,
            boards_batch_size=100,
            items_batch_size=500,
        )
        await _item_cache_session.initialize()

    return _item_cache_session


async def fetch_boards_changes(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Board | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    max_updated_at = log_cursor
    has_updates = False
    board_ids_to_fetch: set[str] = set()
    all_boards: set[str] = set()

    minimal_boards_checked = 0
    async for board in fetch_boards_minimal(http, log):
        all_boards.add(board.id)
        minimal_boards_checked += 1

        if board.updated_at > log_cursor:
            if board.state == "deleted":
                board.meta_ = Board.Meta(op="d")
                has_updates = True
                max_updated_at = max(max_updated_at, board.updated_at)
                yield board
                log.debug(
                    f"Board {board.id} marked as deleted (updated: {board.updated_at})"
                )
            else:
                board_ids_to_fetch.add(board.id)
                max_updated_at = max(max_updated_at, board.updated_at)
                log.debug(
                    f"Board {board.id} added to fetch list (updated: {board.updated_at})"
                )

    activity_logs_processed = 0
    async for activity_log in fetch_activity_logs(
        http,
        log,
        list(all_boards),
        log_cursor.replace(microsecond=0).isoformat(),
    ):
        activity_logs_processed += 1
        created_at = parse_monday_17_digit_timestamp(activity_log.created_at, log)

        if not activity_log.resource_id:
            log.debug(
                f"Skipping activity log with no resource id (event: {activity_log.event})",
                {
                    "activity_log": activity_log,
                },
            )
            continue

        if "delete" in activity_log.event:
            deleted_board = Board(
                id=activity_log.resource_id,
                updated_at=created_at,
                state="deleted",
                workspace=None,
                _meta=Board.Meta(op="d"),
            )
            has_updates = True
            max_updated_at = max(max_updated_at, created_at)
            yield deleted_board
            log.debug(
                f"Board {activity_log.resource_id} marked as deleted in activity logs"
            )
        else:
            board_ids_to_fetch.add(activity_log.resource_id)
            log.debug(
                f"Activity log found for board {activity_log.resource_id} "
                f"(event created_at: {created_at})"
            )

    log.debug(f"Total unique board IDs to fetch: {len(board_ids_to_fetch)}")

    boards_yielded = 0
    if board_ids_to_fetch:
        async for board in fetch_boards_by_ids(http, log, ids=list(board_ids_to_fetch)):
            if board.updated_at > log_cursor:
                if board.state == "deleted":
                    board.meta_ = Board.Meta(op="d")
                has_updates = True
                max_updated_at = max(max_updated_at, board.updated_at)
                yield board
                boards_yielded += 1

    log.debug(
        f"Board sync: {boards_yielded} yielded, {minimal_boards_checked} checked, {activity_logs_processed} activity logs"
    )

    if has_updates:
        new_cursor = max_updated_at + timedelta(seconds=1)
        log.debug(f"Incremental sync complete. New cursor: {new_cursor}")
        yield new_cursor
    else:
        log.debug("Incremental sync complete. No updates found.")


async def fetch_boards_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Board | PageCursor, None]:
    assert page is None or isinstance(page, int)
    assert isinstance(cutoff, datetime)

    if not page:
        page = 1

    limit = 100
    docs_emitted = 0

    if not _boards_backfill_cache:
        log.debug("Fetching initial boards for backfill cache")

        async for board in fetch_boards_minimal(http, log):
            if board.updated_at <= cutoff:
                _boards_backfill_cache.append(board.id)

        log.debug(
            f"Populated backfill cache with {len(_boards_backfill_cache)} boards for cutoff {cutoff}"
        )

    log.debug(
        f"Processing page {page} with limit {limit} from cache of {len(_boards_backfill_cache)} boards"
    )

    boards_in_page = 0
    async for board in fetch_boards_with_retry(
        http,
        log,
        ids=_boards_backfill_cache,
        page=page,
        limit=limit,
    ):
        boards_in_page += 1
        if board.updated_at <= cutoff:
            if board.state != "deleted":
                docs_emitted += 1
                yield board

    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    has_more_pages = end_idx < len(_boards_backfill_cache)

    if has_more_pages:
        log.debug(
            f"Boards backfill completed for page {page}",
            {
                "board_page": page,
                "qualifying_boards": docs_emitted,
                "boards_in_page": boards_in_page,
                "total_cache_size": len(_boards_backfill_cache),
                "next_page": page + 1,
            },
        )
        yield page + 1
    else:
        log.debug(
            f"Boards backfill completed for page {page} - no more pages",
            {
                "board_page": page,
                "qualifying_boards": docs_emitted,
                "boards_in_page": boards_in_page,
                "total_cache_size": len(_boards_backfill_cache),
                "reason": "no_more_pages",
            },
        )
        return


async def fetch_items_changes(
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Item | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    window_start = log_cursor
    now = datetime.now(UTC)
    window_end = min(window_start + MAX_WINDOW_SIZE, now)

    max_updated_at_in_window = window_start
    item_ids_to_fetch: set[str] = set()
    docs_emitted = 0

    board_ids = []
    async for board in fetch_boards_minimal(http, log):
        if board.state != "deleted":
            board_ids.append(board.id)

    async for item in fetch_items(
        http,
        log,
        board_ids=board_ids,
        start=window_start.date().isoformat(),
        # Monday.com API query filters only support daily granularity, not time-based filtering.
        # Adding one day ensures we capture all items updated on the end date, then filter client-side.
        end=(window_end.date() + timedelta(days=1)).isoformat(),
    ):
        # Filter client-side since API query only supports daily granularity
        if window_start <= item.updated_at <= window_end:
            if item.state == "deleted":
                item.meta_ = Item.Meta(op="d")

            max_updated_at_in_window = max(max_updated_at_in_window, item.updated_at)
            docs_emitted += 1
            yield item
        else:
            log.warning(
                f"Item {item.id} updated_at {item.updated_at} is outside the sync window "
                f"({window_start} to {window_end}). This should not happen with the query filter."
            )

    activity_logs_processed = 0
    async for activity_log in fetch_activity_logs(
        http,
        log,
        board_ids,
        window_start.replace(microsecond=0).isoformat(),
        window_end.replace(microsecond=0).isoformat(),
    ):
        activity_logs_processed += 1
        created_at = parse_monday_17_digit_timestamp(activity_log.created_at, log)

        if not activity_log.resource_id:
            log.debug(
                f"Skipping activity log with no resource id (event: {activity_log.event})",
                {
                    "activity_log": activity_log,
                },
            )
            continue

        if "delete" in activity_log.event:
            deleted_item = Item(
                id=activity_log.resource_id,
                updated_at=created_at,
                state="deleted",
                _meta=Item.Meta(op="d"),
            )
            max_updated_at_in_window = max(max_updated_at_in_window, created_at)
            docs_emitted += 1
            yield deleted_item
        else:
            item_ids_to_fetch.add(activity_log.resource_id)

    if item_ids_to_fetch:
        async for item in fetch_items_by_ids(
            http,
            log,
            list(item_ids_to_fetch),
        ):
            if window_start <= item.updated_at <= window_end:
                if item.state == "deleted":
                    item.meta_ = Item.Meta(op="d")

                max_updated_at_in_window = max(
                    max_updated_at_in_window, item.updated_at
                )
                docs_emitted += 1
                yield item

    if docs_emitted > 0:
        yield max_updated_at_in_window + timedelta(seconds=1)
    else:
        # Advance cursor even without updates to prevent missing data due to Monday.com's
        # 10k activity log retention limit per board. Staying at the same cursor risks
        # missing logs that get purged between sync cycles.
        yield window_end


async def fetch_items_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Item | PageCursor, None]:
    assert page is None or isinstance(page, str)
    assert isinstance(cutoff, datetime)

    cache_session = await _get_or_create_item_cache_session(http, log, cutoff)
    items_generator, next_page_cursor = await cache_session.process_page(page)

    async for item in items_generator:
        yield item

    if next_page_cursor:
        yield next_page_cursor


async def snapshot_resource(
    http: HTTPSession,
    name: str,
    query: str,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    async for resource_data in execute_query(
        FullRefreshResource,
        http,
        log,
        f"data.{name}.item",
        query,
    ):
        yield resource_data
