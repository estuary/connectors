from datetime import datetime, timedelta
from logging import Logger
from typing import Any, AsyncGenerator, Literal, TypeVar

from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)
from source_gainsight_nxt.models import (
    GainsightResource,
    GainsightResourceWithModifiedDate,
    DescribeObjectResponse,
)


MAX_PAGE_LIMIT = 5000

GainsightResourceType = TypeVar("GainsightResourceType", bound=GainsightResource)


def _build_request_body(
    fields: list[str],
    modified_date_field: str | None,
    start_date: datetime,
    end_date: datetime | None,
    offset: int,
    ordering: Literal["asc", "desc"],
) -> dict[str, Any]:
    query = {
        "select": fields,
        "limit": MAX_PAGE_LIMIT,
        "offset": offset,
    }

    if modified_date_field:
        query["orderBy"] = {modified_date_field: ordering}

        if end_date:
            values = [start_date.isoformat(), end_date.isoformat()]
        else:
            values = [start_date.isoformat()]

        query["where"] = {
            "conditions": [
                {
                    "name": modified_date_field,
                    "alias": "A",
                    "value": values,
                    "operator": "BTW" if end_date else "GTE",
                },
            ],
            "expression": "A",
        }

    return query


async def fetch_object_fields(
    http: HTTPSession,
    log: Logger,
    domain: str,
    object_type: type[GainsightResourceType],
) -> list[str]:
    url = f"{domain.rstrip('/')}/v1/meta/services/objects/{object_type.OBJECT_NAME}/describe"
    params = {
        "ic": "false",  # include children
        "cl": 0,  # child level
        "idd": "false",
        "ihc": "false",
        "piec": "false",
    }

    response = DescribeObjectResponse.model_validate_json(
        await http.request(log, url, params=params)
    )

    if not response.data:
        return []

    # We should only see one object in the data list since we set ic=false and cl=0
    if len(response.data) != 1:
        raise ValueError(
            f"Expected 1 object, got {len(response.data)} when retrieving fields for {object_type.OBJECT_NAME}"
        )

    return [field.fieldName for field in response.data[0].fields]


async def _fetch_object_data(
    http: HTTPSession,
    log: Logger,
    domain: str,
    object_type: type[GainsightResourceType],
    request_body: dict[str, Any],
) -> AsyncGenerator[GainsightResourceType, None]:
    url = f"{domain.rstrip('/')}/v1/data/objects/query/{object_type.OBJECT_NAME}"

    _, body = await http.request_stream(log, url, method="POST", json=request_body)

    async for item in IncrementalJsonProcessor(
        body(),
        "data.records.item",
        object_type,
    ):
        yield item


async def snapshot_resource(
    http: HTTPSession,
    fields: list[str],
    domain: str,
    resource: type[GainsightResource],
    start_date: datetime,
    log: Logger,
) -> AsyncGenerator[GainsightResource, None]:
    offset = 0
    end_date = datetime.now(tz=start_date.tzinfo)

    while True:
        request_body = _build_request_body(
            fields=fields,
            modified_date_field=None,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            ordering="desc",
        )

        count = 0
        async for doc in _fetch_object_data(http, log, domain, resource, request_body):
            count += 1
            yield doc

        if count == 0:
            break

        offset += count


async def fetch_resource_page(
    http: HTTPSession,
    fields: list[str],
    domain: str,
    resource: type[GainsightResourceWithModifiedDate],
    start_date: datetime,
    log: Logger,
    offset: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[GainsightResource | PageCursor, None]:
    assert isinstance(cutoff, datetime)
    current_offset = offset if offset is not None else 0
    assert isinstance(current_offset, int)

    request_body = _build_request_body(
        fields=fields,
        modified_date_field=resource.MODIFIED_DATE_FIELD,
        start_date=start_date,
        end_date=cutoff,
        offset=current_offset,
        ordering="desc",
    )

    count = 0
    async for doc in _fetch_object_data(http, log, domain, resource, request_body):
        count += 1
        yield doc

    if count > 0:
        yield current_offset + count


async def fetch_resource_changes(
    http: HTTPSession,
    fields: list[str],
    domain: str,
    resource: type[GainsightResourceWithModifiedDate],
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[GainsightResource | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    max_updated_at = log_cursor
    offset = 0
    end_date = min(log_cursor + timedelta(days=30), datetime.now(tz=log_cursor.tzinfo))

    while True:
        request_body = _build_request_body(
            fields=fields,
            modified_date_field=resource.MODIFIED_DATE_FIELD,
            start_date=log_cursor,
            end_date=end_date,
            offset=offset,
            ordering="desc",
        )

        count = 0
        async for doc in _fetch_object_data(http, log, domain, resource, request_body):
            max_updated_at = max(max_updated_at, doc.cursor_value)
            count += 1
            yield doc

        if count == 0:
            break

        offset += count

    if offset > 0:
        yield max_updated_at + timedelta(seconds=1)
