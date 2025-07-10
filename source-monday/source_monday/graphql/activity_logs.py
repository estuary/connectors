from datetime import UTC, datetime
from logging import Logger
from typing import Any, AsyncGenerator

from estuary_cdk.http import HTTPSession

from source_monday.graphql.query_executor import execute_query
from source_monday.models import ActivityLog
from source_monday.utils import parse_monday_17_digit_timestamp

BOARDS_PER_BATCH = 500
ACTIVITY_LOGS_PER_BOARD = 100


async def fetch_activity_logs(
    http: HTTPSession,
    log: Logger,
    board_ids: list[str] | None,
    start: str,
    end: str | None = None,
) -> AsyncGenerator[ActivityLog, None]:
    assert start != "", "Start must not be empty"

    # import here to avoid circular imports
    from source_monday.graphql import fetch_boards_minimal

    if not board_ids:
        board_ids = []
        async for board in fetch_boards_minimal(http, log):
            if board.state != "deleted":
                board_ids.append(board.id)

    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    start = start_dt.isoformat().replace("+00:00", "Z")

    if end is None:
        end_dt = datetime.now(tz=UTC)
        end = end_dt.isoformat().replace("+00:00", "Z")
    else:
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))

    # Process boards in batches with high per-board activity log limits
    # Monday.com's 10k limit log limit is a PER BOARD storage cap, not per request
    # We can potentially get: boards_per_batch × activity_logs_per_board total logs
    boards_per_batch = BOARDS_PER_BATCH
    activity_logs_per_board = ACTIVITY_LOGS_PER_BOARD

    total_logs_processed = 0

    log.debug(
        f"Processing activity logs from {start} to {end} with "
        f"{boards_per_batch} boards per batch, {activity_logs_per_board} logs per board"
    )

    for i in range(0, len(board_ids), boards_per_batch):
        batch_board_ids = board_ids[i : i + boards_per_batch]
        batch_number = (i // boards_per_batch) + 1
        total_batches = (len(board_ids) + boards_per_batch - 1) // boards_per_batch

        log.debug(
            f"Processing board batch {batch_number}/{total_batches} with {len(batch_board_ids)} boards"
        )

        batch_logs_count = 0
        async for activity_log in _fetch_activity_logs_for_board_batch(
            http, log, batch_board_ids, start, end, activity_logs_per_board
        ):
            yield activity_log
            batch_logs_count += 1
            total_logs_processed += 1

        log.debug(
            f"Batch {batch_number}/{total_batches}: retrieved {batch_logs_count} activity logs"
        )

    log.info(f"Total activity logs processed: {total_logs_processed}")


async def _fetch_activity_logs_for_board_batch(
    http: HTTPSession,
    log: Logger,
    board_ids: list[str],
    start_time: str,
    end_time: str,
    activity_logs_per_board: int,
) -> AsyncGenerator[ActivityLog, None]:
    """
    Fetch activity logs for a batch of boards.

    The key insight: limit argument is PER BOARD, not total across request.
    So with 5000 boards and limit:100, we can potentially get 500,000 logs in one request.
    This is why we use the IncrementalJsonProcessor to handle large responses efficiently.
    """
    page = 1

    while True:
        variables: dict[str, Any] = {
            "start": start_time,
            "end": end_time,
            "ids": board_ids,
            "limit": len(board_ids),
            "activity_limit": activity_logs_per_board,
            "page": page,
        }
        logs_in_page = 0

        async for activity_log in execute_query(
            ActivityLog,
            http,
            log,
            "data.boards.activity_logs.item",
            ACTIVITY_LOGS,
            variables,
        ):
            try:
                log_time = parse_monday_17_digit_timestamp(activity_log.created_at, log)
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))

                # Double-check the log is within our time bounds
                if start_dt <= log_time <= end_dt:
                    yield activity_log
                    logs_in_page += 1

            except (ValueError, TypeError) as e:
                if activity_log.created_at is not None:
                    log.warning(
                        f"Failed to parse activity log timestamp {activity_log.created_at}: {e}"
                    )
                continue

        if logs_in_page == 0:
            log.debug(
                f"No activity logs found for page {page} with start {start_time} and end {end_time}"
            )
            break

        page += 1
        log.debug(f"Page {page - 1}: processed {logs_in_page} activity logs")


ACTIVITY_LOGS = """
query ($start: ISO8601DateTime!, $end: ISO8601DateTime!, $ids: [ID!]!, $limit: Int!, $activity_limit: Int!, $page: Int = 1) {
    boards(ids: $ids, limit: $limit, page: $page, order_by: created_at) {
        activity_logs(from: $start, to: $end, limit: $activity_limit) {
            account_id
            created_at
            data
            entity
            event
            id
            user_id
        }
    }
}
"""
