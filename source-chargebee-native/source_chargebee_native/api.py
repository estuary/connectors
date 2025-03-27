from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator, TypeVar

from estuary_cdk.http import HTTPSession
from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)
from source_chargebee_native.models import (
    APIResponse,
    ChargebeeResource,
    IncrementalChargebeeResource,
)


MAX_PAGE_LIMIT = 100

ChargebeeResourceType = TypeVar("ChargebeeResourceType", bound=ChargebeeResource)


def _dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp())


def _ts_to_dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=UTC)


async def _fetch_resource_data(
    http: HTTPSession,
    log: Logger,
    site: str,
    resource_name: str,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    offset: str | None = None,
    include_deleted: bool = False,
    resource_type: type[ChargebeeResourceType] = ChargebeeResource,
) -> tuple[list[ChargebeeResourceType], str | None]:
    url = f"https://{site}.chargebee.com/api/v2/{resource_name}"

    params: dict[str, int | str | list[int]] = {
        "limit": MAX_PAGE_LIMIT,
    }

    if offset is not None:
        params["offset"] = offset

    if start_date and end_date:
        params["updated_at[between]"] = (
            f"[{int(start_date.timestamp())},{int(end_date.timestamp())}]"
        )
    elif start_date:
        params["updated_at[after]"] = int(start_date.timestamp())
    elif end_date:
        params["updated_at[before]"] = int(end_date.timestamp())

    if include_deleted:
        params["include_deleted"] = "true"

    response = APIResponse[resource_type].model_validate_json(
        await http.request(log, url, params=params)
    )

    if not response.list:
        return [], response.next_offset

    return response.list, response.next_offset


async def snapshot_resource(
    http: HTTPSession,
    site: str,
    resource_name: str,
    log: Logger,
) -> AsyncGenerator[ChargebeeResource, None]:
    offset = None

    while True:
        resource_data, next_offset = await _fetch_resource_data(
            http,
            log,
            site,
            resource_name,
            offset=offset,
        )

        if not resource_data:
            break

        for doc in resource_data:
            yield doc

        if not next_offset:
            break

        offset = next_offset


async def fetch_resource_page(
    http: HTTPSession,
    site: str,
    resource_name: str,
    resource_type: type[ChargebeeResourceType],
    start_date: datetime,
    log: Logger,
    offset: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ChargebeeResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    assert isinstance(offset, str | None)

    resource_data, next_offset = await _fetch_resource_data(
        http,
        log,
        site,
        resource_name,
        start_date,
        cutoff,
        offset,
        False,
        resource_type,
    )

    if not resource_data:
        return

    for doc in resource_data:
        yield doc

    if next_offset:
        yield next_offset


async def fetch_resource_changes(
    http: HTTPSession,
    site: str,
    resource_name: str,
    resource_type: type[IncrementalChargebeeResource],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ChargebeeResource | LogCursor, None]:
    assert isinstance(log_cursor, int)

    max_updated_at = _ts_to_dt(log_cursor)
    has_results = False
    end_date = min(max_updated_at + timedelta(days=30), datetime.now(tz=UTC))
    offset = None

    while True:
        resource_data, next_offset = await _fetch_resource_data(
            http,
            log,
            site,
            resource_name,
            max_updated_at,
            end_date,
            offset,
            True,
            resource_type,
        )

        if not resource_data:
            break

        has_results = True

        for doc in resource_data:
            max_updated_at = max(max_updated_at, _ts_to_dt(doc.updated_at))

            if doc.deleted:
                doc.meta_ = doc.Meta(op="d")

            yield doc

        if not next_offset:
            break

        offset = next_offset

    if has_results:
        yield _dt_to_ts(max_updated_at + timedelta(seconds=1))
