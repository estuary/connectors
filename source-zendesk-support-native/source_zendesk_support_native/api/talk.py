import asyncio
from datetime import datetime, UTC
from logging import Logger
from typing import AsyncGenerator

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from ..models import (
    TimestampedResource,
    TalkIncrementalExportResponse,
)

from .shared import (
    CHECKPOINT_INTERVAL,
    INCREMENTAL_LAG,
    INCREMENTAL_TIME_EXPORT_REQ_PER_MIN_LIMIT,
    url_base,
    dt_to_s,
    s_to_dt,
)

# Talk API incremental exports (calls, legs) have a different response structure than other incremental exports.
# They don't return an end_of_stream field - instead we check if end_time == start_time.
# See: https://developer.zendesk.com/api-reference/voice/talk-api/incremental_exports/
talk_incremental_export_api_lock = asyncio.Lock()


async def _fetch_talk_incremental_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[TalkIncrementalExportResponse],
    start_date: datetime,
    log: Logger,
    horizon: datetime | None = None,
) -> AsyncGenerator[TimestampedResource | datetime, None]:
    # Talk API incremental exports use timestamps for pagination similar to other time-based exports.
    # The end_time returned in the response is the updated_at timestamp of the last record.
    # This means we'll get at least one duplicate when paginating (records with updated_at == end_time).
    # Unlike other incremental exports, Talk API doesn't have end_of_stream - we detect end by
    # checking if end_time == start_time (no newer records exist).
    url = f"{url_base(subdomain)}/channels/voice/stats/incremental/{path}"

    params = {"start_time": dt_to_s(start_date)}

    last_seen_dt = start_date
    count = 0

    while True:
        async with talk_incremental_export_api_lock:
            _, body = await http.request_stream(log, url, params=params)
            processor = IncrementalJsonProcessor(
                body(),
                f"{path}.item",
                TimestampedResource,
                response_model,
            )

            async for resource in processor:
                # Stop at the first record Zendesk may not have finished making visible.
                # Results are ordered ascending by updated_at, so every later record is
                # unsettled too. This stops rather than skips because the loop advances
                # start_time to fetch the next page — skipping would just walk further
                # into the unsettled window.
                if horizon is not None and resource.updated_at >= horizon:
                    if last_seen_dt > start_date:
                        yield last_seen_dt
                    return

                # Ignore duplicate results that were yielded on the previous sweep.
                if resource.updated_at <= start_date:
                    continue

                # Checkpoint previously yielded documents if we see a new updated_at value.
                if (
                    resource.updated_at > last_seen_dt and
                    last_seen_dt != start_date and
                    count >= CHECKPOINT_INTERVAL
                ):
                    yield last_seen_dt
                    count = 0

                yield resource
                count += 1
                last_seen_dt = resource.updated_at

            remainder = processor.get_remainder()

            if remainder.count == 0 or remainder.end_time is None:
                return

            # Error if 1000+ records have the same updated_at value. This stops the stream from
            # looping & making the same request endlessly.
            if params["start_time"] == remainder.end_time and remainder.count >= 1000:
                raise RuntimeError(
                    f"At least 1,000 {name} were updated at {remainder.end_time}, and this stream "
                    f"cannot progress without potentially missing data. Contact Estuary Support for help."
                )

            # Talk API responses don't have an end_of_stream field for detecting the last page,
            # so we detect the last page by checking if end_time == start_time.
            # This means no newer records exist beyond the current page.
            if params["start_time"] == remainder.end_time:
                if last_seen_dt > start_date:
                    yield last_seen_dt
                return

            params["start_time"] = remainder.end_time

            # Sleep to avoid excessively hitting this endpoint's 10 req/min limit.
            await asyncio.sleep(60 / INCREMENTAL_TIME_EXPORT_REQ_PER_MIN_LIMIT)


async def fetch_talk_incremental_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[TalkIncrementalExportResponse],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    generator = _fetch_talk_incremental_export_resources(
        http, subdomain, name, path, response_model, log_cursor, log,
        datetime.now(tz=UTC) - INCREMENTAL_LAG,
    )

    async for result in generator:
        yield result


async def backfill_talk_incremental_export_resources(
    http: HTTPSession,
    subdomain: str,
    name: str,
    path: str,
    response_model: type[TalkIncrementalExportResponse],
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TimestampedResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)

    if page is None:
        start = start_date
    else:
        assert isinstance(page, int)
        start = s_to_dt(page)

    generator = _fetch_talk_incremental_export_resources(http, subdomain, name, path, response_model, start, log)

    async for result in generator:
        if isinstance(result, datetime):
            yield dt_to_s(result)
        elif result.updated_at > cutoff:
            return
        else:
            yield result
