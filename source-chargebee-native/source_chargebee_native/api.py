from datetime import datetime, timedelta, UTC
from logging import Logger
from typing import AsyncGenerator, TypeVar

import asyncio

from estuary_cdk.http import HTTPSession
from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)
from source_chargebee_native.models import (
    APIResponse,
    ChargebeeResource,
    IncrementalChargebeeResource,
    AssociationConfig,
)


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
    limit: int,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    offset: str | None = None,
    include_deleted: bool = False,
    resource_type: type[ChargebeeResourceType] = ChargebeeResource,
    filter_params: dict[str, str] | None = None,
    returns_list: bool = True,
) -> tuple[list[ChargebeeResourceType], str | None]:
    url = f"https://{site}.chargebee.com/api/v2/{resource_name}"

    params: dict[str, int | str | list[int]] = {}

    if returns_list:
        params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

    if issubclass(resource_type, IncrementalChargebeeResource):
        cursor_field = resource_type.CURSOR_FIELD

        if start_date and end_date:
            params[f"{cursor_field}[between]"] = (
                f"[{int(start_date.timestamp())},{int(end_date.timestamp())}]"
            )
        elif start_date:
            params[f"{cursor_field}[after]"] = int(start_date.timestamp())
        elif end_date:
            params[f"{cursor_field}[before]"] = int(end_date.timestamp())

    if start_date and end_date and start_date == end_date:
        raise ValueError(
            "Start and end dates cannot be the same",
            {"start_date": start_date, "end_date": end_date},
        )

    if include_deleted:
        params["include_deleted"] = "true"

    if filter_params:
        params.update(filter_params)

    async def _fetch_data(
        http: HTTPSession,
        log: Logger,
        url: str,
        params: dict[str, int | str | list[int]],
        resource_type: type[ChargebeeResourceType],
    ) -> tuple[list[ChargebeeResourceType], str | None]:
        response = APIResponse[resource_type].model_validate_json(
            await http.request(log, url, params=params)
        )

        if not response.list:
            return [], response.next_offset

        return response.list, response.next_offset

    max_retries = 10
    retry_count = 0

    while retry_count < max_retries:
        request_start_time = datetime.now(tz=UTC)
        try:
            return await _fetch_data(
                http=http,
                log=log,
                url=url,
                params=params,
                resource_type=resource_type,
            )
        except asyncio.TimeoutError as e:
            request_end_time = datetime.now(tz=UTC)
            request_duration = (request_end_time - request_start_time).total_seconds()
            retry_count += 1
            if retry_count >= max_retries:
                raise
            log.warning(
                "Timeout occurred while fetching resource data",
                {
                    "resource": resource_name,
                    "params": params,
                    "retry_count": retry_count,
                    "max_retries": max_retries,
                    "request_duration_seconds": request_duration,
                    "error": str(e),
                },
            )

    return [], None


async def snapshot_resource(
    http: HTTPSession,
    site: str,
    resource_name: str,
    limit: int,
    log: Logger,
    filter_params: dict[str, str] | None = None,
) -> AsyncGenerator[ChargebeeResource, None]:
    offset = None

    while True:
        resource_data, next_offset = await _fetch_resource_data(
            http=http,
            log=log,
            site=site,
            resource_name=resource_name,
            offset=offset,
            limit=limit,
            filter_params=filter_params,
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
    resource_type: type[IncrementalChargebeeResource],
    start_date: datetime,
    limit: int,
    log: Logger,
    offset: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ChargebeeResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    assert isinstance(offset, str | None)

    resource_data, next_offset = await _fetch_resource_data(
        http=http,
        log=log,
        site=site,
        resource_name=resource_name,
        start_date=start_date,
        end_date=cutoff,
        offset=offset,
        limit=limit,
        include_deleted=False,
        resource_type=resource_type,
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
    limit: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ChargebeeResource | LogCursor, None]:
    assert isinstance(log_cursor, int)

    start_date = _ts_to_dt(log_cursor)
    end_date = min(start_date + timedelta(days=30), datetime.now(tz=UTC))
    max_updated_at = start_date
    has_results = False
    offset = None

    while True:
        resource_data, next_offset = await _fetch_resource_data(
            http=http,
            log=log,
            site=site,
            resource_name=resource_name,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=limit,
            include_deleted=True,
            resource_type=resource_type,
        )

        if not resource_data:
            break

        has_results = True

        for doc in resource_data:
            if doc.cursor_value > log_cursor:
                continue

            max_updated_at = max(max_updated_at, _ts_to_dt(doc.cursor_value))

            if doc.deleted:
                doc.meta_ = doc.Meta(op="d")

            yield doc

        if not next_offset:
            break

        offset = next_offset

    if has_results:
        yield _dt_to_ts(max_updated_at + timedelta(seconds=1))


async def _get_parent_ids(
    http: HTTPSession,
    site: str,
    association_config: AssociationConfig,
    limit: int,
    log: Logger,
) -> list[str]:
    parent_ids = []
    async for parent in snapshot_resource(
        http=http,
        site=site,
        resource_name=association_config.parent_resource,
        limit=limit,
        log=log,
        filter_params=association_config.parent_filter_params,
    ):
        parent_data = parent.model_dump()
        parent_ids.append(
            parent_data[association_config.parent_response_key][
                association_config.parent_key_field
            ]
        )

    return parent_ids


async def snapshot_associated_resource(
    http: HTTPSession,
    site: str,
    association_config: AssociationConfig,
    limit: int,
    log: Logger,
) -> AsyncGenerator[ChargebeeResource, None]:
    parent_ids = await _get_parent_ids(
        http=http,
        site=site,
        association_config=association_config,
        limit=limit,
        log=log,
    )

    for parent_id in parent_ids:
        endpoint = association_config.endpoint_pattern.format(
            parent=association_config.parent_resource, id=parent_id
        )

        offset = None
        while True:
            resource_data, next_offset = await _fetch_resource_data(
                http=http,
                log=log,
                site=site,
                resource_name=endpoint,
                offset=offset,
                limit=limit,
                returns_list=association_config.returns_list,
            )

            if not resource_data:
                break

            for doc in resource_data:
                yield doc

            if not next_offset:
                break

            offset = next_offset


async def fetch_associated_resource_page(
    http: HTTPSession,
    site: str,
    association_config: AssociationConfig,
    resource_type: type[IncrementalChargebeeResource],
    start_date: datetime,
    limit: int,
    log: Logger,
    offset: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[ChargebeeResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    assert isinstance(offset, str | None)

    parent_ids = await _get_parent_ids(
        http=http,
        site=site,
        association_config=association_config,
        limit=limit,
        log=log,
    )

    for parent_id in parent_ids:
        endpoint = association_config.endpoint_pattern.format(
            parent=association_config.parent_resource, id=parent_id
        )

        child_data, next_offset = await _fetch_resource_data(
            http=http,
            log=log,
            site=site,
            resource_name=endpoint,
            start_date=start_date,
            end_date=cutoff,
            limit=limit,
            offset=offset,
            include_deleted=False,
            resource_type=resource_type,
        )

        if not child_data:
            continue

        for doc in child_data:
            yield doc

        if next_offset:
            yield next_offset


async def fetch_associated_resource_changes(
    http: HTTPSession,
    site: str,
    association_config: AssociationConfig,
    resource_type: type[IncrementalChargebeeResource],
    limit: int,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ChargebeeResource | LogCursor, None]:
    assert isinstance(log_cursor, int)

    start_date = _ts_to_dt(log_cursor)
    end_date = min(start_date + timedelta(days=30), datetime.now(tz=UTC))
    max_updated_at = start_date
    has_results = False

    parent_ids = await _get_parent_ids(
        http=http,
        site=site,
        association_config=association_config,
        limit=limit,
        log=log,
    )

    for parent_id in parent_ids:
        endpoint = association_config.endpoint_pattern.format(
            parent=association_config.parent_resource, id=parent_id
        )
        offset = None

        while True:
            child_data, next_offset = await _fetch_resource_data(
                http=http,
                log=log,
                site=site,
                resource_name=endpoint,
                start_date=start_date,
                end_date=end_date,
                offset=offset,
                limit=limit,
                include_deleted=True,
                resource_type=resource_type,
            )

            if not child_data:
                break

            has_results = True

            for doc in child_data:
                if doc.cursor_value > log_cursor:
                    continue

                max_updated_at = max(max_updated_at, _ts_to_dt(doc.cursor_value))

                if doc.deleted:
                    doc.meta_ = doc.Meta(op="d")

                yield doc

            if not next_offset:
                break

            offset = next_offset

    if has_results:
        yield _dt_to_ts(max_updated_at + timedelta(seconds=1))
