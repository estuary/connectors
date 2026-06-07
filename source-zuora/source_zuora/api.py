from datetime import datetime, UTC
from logging import Logger
from typing import Any, AsyncGenerator
import xml.etree.ElementTree as ET

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from pydantic import BaseModel

from .models import ZuoraDocument, OBJECT_MODELS

# Default maximum page size for ZOQL queries.
QUERY_BATCH_SIZE = 2000

# Number of documents to emit before issuing a checkpoint.
CHECKPOINT_INTERVAL = 500


class QueryResult(BaseModel, extra="forbid"):
    done: bool
    queryLocator: str | None = None
    records: list[dict[str, Any]] = []
    size: int = 0


async def describe_object(
    tenant_endpoint: str,
    http: HTTPSession,
    log: Logger,
    object_name: str,
) -> list[str]:
    """
    Fetch all selectable field names for a Zuora object via the describe endpoint.
    Returns a list of field names suitable for use in a ZOQL SELECT statement.
    """
    url = f"{tenant_endpoint}/v1/describe/{object_name}"
    response_bytes = await http.request(log, url)

    root = ET.fromstring(response_bytes)
    fields: list[str] = []

    for field_el in root.findall(".//field"):
        name_el = field_el.find("name")
        selectable_el = field_el.find("selectable")
        if (
            name_el is not None
            and name_el.text
            and selectable_el is not None
            and selectable_el.text == "true"
        ):
            fields.append(name_el.text)

    return fields


def _format_cursor(dt: datetime) -> str:
    """Format a datetime for use in a ZOQL WHERE clause."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


async def fetch_changes(
    object_name: str,
    fields: list[str],
    tenant_endpoint: str,
    http: HTTPSession,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[ZuoraDocument | LogCursor, None]:
    """
    Incrementally fetch records updated since log_cursor using ZOQL.
    Yields documents and an updated LogCursor.
    """
    assert isinstance(log_cursor, datetime)

    model_cls = OBJECT_MODELS.get(object_name, ZuoraDocument)
    fields_str = ", ".join(fields)
    cursor_str = _format_cursor(log_cursor)
    query = (
        f"SELECT {fields_str} FROM {object_name} "
        f"WHERE UpdatedDate >= '{cursor_str}'"
    )

    url = f"{tenant_endpoint}/v1/action/query"
    body: dict[str, Any] = {
        "queryString": query,
        "conf": {"queryBatchSize": QUERY_BATCH_SIZE},
    }

    max_updated = log_cursor
    emitted = 0

    while True:
        response_bytes = await http.request(log, url, method="POST", json=body)
        result = QueryResult.model_validate_json(response_bytes)

        for record in result.records:
            doc = model_cls.model_validate(record)

            if doc.UpdatedDate and doc.UpdatedDate > max_updated:
                max_updated = doc.UpdatedDate

            yield doc
            emitted += 1

            if emitted % CHECKPOINT_INTERVAL == 0 and max_updated > log_cursor:
                yield max_updated

        if result.done or not result.queryLocator:
            break

        url = f"{tenant_endpoint}/v1/action/queryMore"
        body = {"queryLocator": result.queryLocator}

    if max_updated > log_cursor:
        yield max_updated


async def fetch_page(
    object_name: str,
    fields: list[str],
    tenant_endpoint: str,
    http: HTTPSession,
    log: Logger,
    page: PageCursor,
) -> AsyncGenerator[ZuoraDocument | PageCursor, None]:
    """
    Fetch a page of records for full backfill using ZOQL.
    page=None starts a fresh query; page=queryLocator continues pagination.
    Yields documents and a PageCursor for the next page (or nothing if done).
    """
    model_cls = OBJECT_MODELS.get(object_name, ZuoraDocument)

    if page is None:
        fields_str = ", ".join(fields)
        query = f"SELECT {fields_str} FROM {object_name}"
        url = f"{tenant_endpoint}/v1/action/query"
        body: dict[str, Any] = {
            "queryString": query,
            "conf": {"queryBatchSize": QUERY_BATCH_SIZE},
        }
    else:
        assert isinstance(page, str)
        url = f"{tenant_endpoint}/v1/action/queryMore"
        body = {"queryLocator": page}

    response_bytes = await http.request(log, url, method="POST", json=body)
    result = QueryResult.model_validate_json(response_bytes)

    for record in result.records:
        doc = model_cls.model_validate(record)
        yield doc

    if not result.done and result.queryLocator:
        yield result.queryLocator
