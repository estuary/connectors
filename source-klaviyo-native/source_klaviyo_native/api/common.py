import asyncio
from datetime import UTC, datetime, timedelta
from logging import Logger
import time
from typing import AsyncGenerator, TypeVar
from urllib.parse import parse_qs, urlparse

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from ..models import (
    BaseStream,
    CouponCodes,
    Coupons,
    CursorField,
    IncrementalResponse,
    IncrementalStream,
    LowerBoundOperator,
    SnapshotResponse,
    UpperBoundOperator,
)
from ..shared import dt_to_str, str_to_dt


_StreamT = TypeVar('_StreamT', bound=BaseStream)


BASE_URL = "https://a.klaviyo.com/api"
API_VERSION = "2025-07-15"
REQUIRED_HEADERS = {
    "REVISION": API_VERSION
}
PAGE_CURSOR_PARAMETER = "page[cursor]"
CHECKPOINT_INTERVAL = 1_000
SMALLEST_KLAVIYO_DATETIME_GRAIN = timedelta(seconds=1)
TARGET_FETCH_PAGE_INVOCATION_RUN_TIME = 60 * 5 # 5 minutes


def _extract_page_cursor(next_url: str | None) -> str | None:
    if not next_url:
        return None

    parsed = urlparse(next_url)
    query_params = parse_qs(parsed.query)

    cursor_values = query_params.get(PAGE_CURSOR_PARAMETER)

    if cursor_values:
        return cursor_values[0]

    return None


def _construct_filter_param(
    cursor_field: CursorField,
    lower_bound_operator: LowerBoundOperator,
    lower_bound: datetime,
    upper_bound_operator: UpperBoundOperator | None = None,
    upper_bound: datetime | None = None,
    additional_filters: list[str] | None = None
) -> str: 
    if (
        isinstance(upper_bound_operator, str) and upper_bound is None
    ) or (
        upper_bound_operator is None and isinstance(upper_bound, datetime)
    ):
        raise RuntimeError("upper_bound and upper_bound_operator must both be provided")

    filters = []

    lower_bound_filter = f"{lower_bound_operator}({cursor_field},{lower_bound})"
    filters.append(lower_bound_filter)

    if upper_bound:
        upper_bound_filter = f"{upper_bound_operator}({cursor_field},{upper_bound})"
        filters.append(upper_bound_filter)

    if additional_filters:
        filters.extend(additional_filters)

    return ",".join(filters)


async def _paginate_through_resources(
    http: HTTPSession,
    url: str,
    params: dict[str, str | int],
    model: type[_StreamT],
    log: Logger,
) -> AsyncGenerator[_StreamT, None]:
    if issubclass(model, IncrementalStream):
        response_wrapper = IncrementalResponse[model]
    else:
        response_wrapper = SnapshotResponse[model]

    while True:
        _, body = await http.request_stream(log, url, params=params, headers=REQUIRED_HEADERS)
        processor = IncrementalJsonProcessor(
            body(),
            "data.item",
            model,
            response_wrapper,
        )

        async for doc in processor:
            yield doc

        remainder = processor.get_remainder()
        next_page_cursor = _extract_page_cursor(remainder.links.next)

        if next_page_cursor:
            params[PAGE_CURSOR_PARAMETER] = next_page_cursor
        else:
            break


async def snapshot_resources(
    http: HTTPSession,
    model: type[BaseStream],
    log: Logger,
) -> AsyncGenerator[BaseStream, None]:
    url = f"{BASE_URL}/{model.path}"
    params = {}

    if model.extra_params:
        params.update(model.extra_params)

    async for resource in _paginate_through_resources(
        http, url, params, model, log,
    ):
        yield resource


async def snapshot_coupon_codes(
    http: HTTPSession,
    log: Logger,
) -> AsyncGenerator[BaseStream, None]:
    coupon_ids: list[str] = []

    async for coupon in snapshot_resources(http, Coupons, log):
        coupon_ids.append(coupon.id)

    if len(coupon_ids) == 0:
        return

    url = f"{BASE_URL}/{CouponCodes.path}"
    params = {}

    if CouponCodes.extra_params:
        params.update(CouponCodes.extra_params)

    params["filter"] = f"any(coupon.id,{coupon_ids})".replace(" ", "")

    count = 0

    async for resource in _paginate_through_resources(
        http, url, params, CouponCodes, log,
    ):
        count += 1
        yield resource


async def fetch_incremental_resources(
    http: HTTPSession,
    model: type[IncrementalStream],
    horizon: timedelta | None,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[IncrementalStream | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    now = datetime.now(tz=UTC)
    lower_bound = log_cursor
    upper_bound = now - horizon if horizon else now

    if log_cursor >= now - timedelta(seconds=5):
        # Klaviyo returns an error if we use a datetime that's too close to the present
        # in the filter query parameter. If the log_cursor is less than N seconds in the past,
        # sleep to avoid triggering that error.
        await asyncio.sleep(5)

    url = f"{BASE_URL}/{model.path}"
    params: dict[str, str | int] = {
        "sort": model.cursor_field,
        "filter": _construct_filter_param(
            cursor_field=model.cursor_field,
            lower_bound_operator=LowerBoundOperator.GREATER_THAN,
            lower_bound=lower_bound,
            additional_filters=model.additional_filters,
        )
    }

    if model.extra_params:
        params.update(model.extra_params)

    last_seen_dt = log_cursor
    count = 0

    async for doc in _paginate_through_resources(
        http, url, params, model, log,
    ):
        doc_cursor = doc.get_cursor_value()

        # Sanity check to confirm documents are received in ascending
        # order of their cursor field.
        if doc_cursor < last_seen_dt:
            msg = f"Received documents out of order from the Klaviyo API."
            log.error(msg, {
                "id": doc.id,
                model.cursor_field: doc_cursor,
                f"previous {model.cursor_field}": last_seen_dt,
            })
            raise RuntimeError(msg)

        # If we see a document with a cursor field value on or after
        # the upper bound datetime, stop paginating. This prevents 
        # delayed streams from progressing too close to the present
        # and also prevents real-time streams from moving far into
        # the future when Klaviyo returns records that have a cursor
        # field in the future.
        if doc_cursor >= upper_bound:
            break

        # If we see a document updated more recently than the previous
        # document we emitted, checkpoint the previous documents.
        if (
            count >= CHECKPOINT_INTERVAL and
            doc_cursor > last_seen_dt
        ):
            yield last_seen_dt
            count = 0

        yield doc
        count += 1
        last_seen_dt = doc_cursor

    # Emit a final checkpoint if there are yielded documents that weren't checkpointed.
    if last_seen_dt != log_cursor and count > 0:
        yield last_seen_dt


async def backfill_incremental_resources(
    http: HTTPSession,
    model: type[IncrementalStream],
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[IncrementalStream | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    assert isinstance(page, str)

    start = str_to_dt(page)

    if start >= cutoff:
        return

    url = f"{BASE_URL}/{model.path}"

    if model.supports_less_than_filter:
        filter_param = _construct_filter_param(
            cursor_field=model.cursor_field,
            lower_bound_operator=LowerBoundOperator.GREATER_THAN,
            lower_bound=start,
            upper_bound_operator=UpperBoundOperator.LESS_THAN,
            upper_bound=cutoff + SMALLEST_KLAVIYO_DATETIME_GRAIN,
            additional_filters=model.additional_filters,
        )
    else:
        filter_param = _construct_filter_param(
            cursor_field=model.cursor_field,
            lower_bound_operator=LowerBoundOperator.GREATER_THAN,
            lower_bound=start,
            additional_filters=model.additional_filters,
        )

    params: dict[str, str | int] = {
        "sort": model.cursor_field,
        "filter": filter_param,
    }

    if model.extra_params:
        params.update(model.extra_params)

    last_seen_dt = start
    count = 0

    start_time = time.time()

    async for doc in _paginate_through_resources(
        http, url, params, model, log,
    ):
        doc_cursor = doc.get_cursor_value()

        # If a document is updated after the cutoff, the backfill is done.
        if doc_cursor > cutoff:
            return

        # If we see a document updated more recently than the previous
        # document we emitted, checkpoint the previous documents.
        if (
            count >= CHECKPOINT_INTERVAL and
            doc_cursor > last_seen_dt
        ):
            yield dt_to_str(last_seen_dt)
            count = 0

            # If backfill_incremental_resources has been running for more than TARGET_FETCH_PAGE_INVOCATION_RUN_TIME
            # minutes, then yield control back to the CDK after a checkpoint. This forces backfills to check the
            # stopping event every so often & gracefully exit if it's set.
            if time.time() - start_time >= TARGET_FETCH_PAGE_INVOCATION_RUN_TIME:
                return

        yield doc
        count += 1
        last_seen_dt = doc_cursor
