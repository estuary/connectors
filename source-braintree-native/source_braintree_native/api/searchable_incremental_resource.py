import asyncio
import itertools
from datetime import datetime
from logging import Logger
from typing import Any, AsyncGenerator, TypeVar

from braintree import BraintreeGateway

from estuary_cdk.http import HTTPSession

from .common import (
    HEADERS,
    SEARCH_PAGE_SIZE,
    SEMAPHORE_LIMIT,
    SEARCH_LIMIT,
    braintree_object_to_dict,
    braintree_xml_to_dict,
    search_limit_error_message,
    reduce_window_end,
)
from ..models import (
    IncrementalResource,
    Transaction,
    IncrementalResourceBraintreeClass,
    IdSearchResponse,
    SearchResponse,
)


async def fetch_searchable_resource_ids_by_field_between(
    http: HTTPSession,
    base_url: str,
    path: str,
    field_name: str,
    start: datetime,
    end: datetime,
    log: Logger,
    search_limit: int = SEARCH_LIMIT,
    enforce_limit: bool = False
) -> list[str]:
    url = f"{base_url}/{path}/advanced_search_ids"
    body = {
        "search": {
            field_name.lower(): {
                "min": start.isoformat(),
                "max": end.isoformat(),
            }
        }
    }

    response = IdSearchResponse.model_validate(
        braintree_xml_to_dict(
            await http.request(log, url, "POST", json=body, headers=HEADERS)
        )
    )

    size = len(response.search_results.ids)
    if enforce_limit and size >= search_limit:
        raise RuntimeError(search_limit_error_message(size))

    return response.search_results.ids


async def determine_next_searchable_resource_window_end_by_field(
    http: HTTPSession,
    base_url: str,
    path: str,
    field_name: str,
    start: datetime,
    initial_end: datetime,
    log: Logger,
    search_limit: int = SEARCH_LIMIT,
) -> datetime:
    end = initial_end

    while True:
        ids = await fetch_searchable_resource_ids_by_field_between(
            http,
            base_url,
            path,
            field_name,
            start,
            end,
            log,
            search_limit=search_limit,
        )

        if len(ids) < search_limit:
            break

        end = reduce_window_end(start, end)

    return end


async def _fetch_chunk(
    http: HTTPSession,
    base_url: str,
    path: str,
    response_model: type[SearchResponse],
    ids: list[str],
    semaphore: asyncio.Semaphore,
    log: Logger,
) -> list[dict[str, Any]]:
    assert len(ids) <= SEARCH_PAGE_SIZE

    url = f"{base_url}/{path}/advanced_search"
    body = {
        "search": {
            "ids": ids,
        }
    }

    async with semaphore:
        response = response_model.model_validate(
            braintree_xml_to_dict(
                await http.request(log, url, "POST", json=body, headers=HEADERS)
            )
        )

    if response.resources.resource is None:
        return []

    resources = response.resources.resource
    if isinstance(resources, dict):
        resources = [resources]

    return resources


_IncrementalDocument = TypeVar("_IncrementalDocument", bound=IncrementalResource | Transaction)


async def fetch_by_ids(
    http: HTTPSession,
    base_url: str,
    path: str,
    response_model: type[SearchResponse],
    document_model: type[_IncrementalDocument],
    ids: list[str],
    gateway: BraintreeGateway,
    braintree_class: IncrementalResourceBraintreeClass,
    log: Logger,
) -> AsyncGenerator[_IncrementalDocument, None]:
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    for coro in asyncio.as_completed(
        [
            _fetch_chunk(http, base_url, path, response_model, list(chunk), semaphore, log)
            for chunk in itertools.batched(ids, SEARCH_PAGE_SIZE)
        ]
    ):
        result = await coro
        for resource in result:
            yield document_model.model_validate(
                braintree_object_to_dict(
                    braintree_class(gateway, resource)
                )
            )


async def fetch_searchable_resources_created_between(
    http: HTTPSession,
    base_url: str,
    path: str,
    response_model: type[SearchResponse],
    start: datetime,
    end: datetime,
    gateway: BraintreeGateway,
    braintree_class: IncrementalResourceBraintreeClass,
    log: Logger,
) -> AsyncGenerator[IncrementalResource, None]:
    ids = await fetch_searchable_resource_ids_by_field_between(
        http,
        base_url,
        path,
        "created_at",
        start,
        end,
        log,
        enforce_limit=True
    )

    if len(ids) == 0:
        return

    async for doc in fetch_by_ids(
        http,
        base_url,
        path,
        response_model,
        IncrementalResource,
        ids,
        gateway,
        braintree_class,
        log,
    ):
        yield doc
