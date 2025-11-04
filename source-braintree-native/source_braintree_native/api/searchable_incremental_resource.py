import asyncio
import itertools
from datetime import datetime
from logging import Logger
from typing import Any, AsyncGenerator, Awaitable, TypeVar

from braintree import BraintreeGateway

from estuary_cdk.http import HTTPError, HTTPSession

from .common import (
    HEADERS,
    SEARCH_PAGE_SIZE,
    SEMAPHORE_LIMIT,
    SEARCH_LIMIT,
    braintree_object_to_dict,
    braintree_xml_to_dict,
    process_completed_fetches,
    search_limit_error_message,
    reduce_window_end,
)
from ..models import (
    IncrementalResource,
    IncrementalResourceBraintreeClass,
    IdSearchResponse,
    SearchResponse,
)


def _should_retry(
    status: int,
    headers: dict[str, Any],
    body: bytes,
    attempt: int,
) -> bool:
    # If the response is a 500 status code, that could mean that too much data was requested
    # and the connector should make a new request for less data.
    return status != 500


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


async def _fetch_resource_page(
    http: HTTPSession,
    base_url: str,
    path: str,
    response_model: type[SearchResponse],
    ids: list[str],
    semaphore: asyncio.Semaphore,
    log: Logger,
) -> list[dict[str, Any]]:
    url = f"{base_url}/{path}/advanced_search"
    body = {
        "search": {
            "ids": ids,
        }
    }

    async with semaphore:
        response = response_model.model_validate(
            braintree_xml_to_dict(
                await http.request(log, url, "POST", json=body, headers=HEADERS, should_retry=_should_retry)
            )
        )

    if response.resources.resource is None:
        return []

    resources = response.resources.resource
    if isinstance(resources, dict):
        resources = [resources]

    return resources


async def _fetch_resource_batch(
    http: HTTPSession,
    base_url: str,
    path: str,
    response_model: type[SearchResponse],
    ids: list[str],
    semaphore: asyncio.Semaphore,
    log: Logger,
) -> list[dict[str, Any]]:
    assert len(ids) <= SEARCH_PAGE_SIZE

    try:
        # We try to fetch all resources in a single page.
        return await _fetch_resource_page(
            http,
            base_url,
            path,
            response_model,
            ids,
            semaphore,
            log,
        )
    except HTTPError as err:
        # If Braintree's API server returns a 500 response, then it may be having problems
        # sending all resources in a single response. We try fetching resources individually
        # to make it easier for the API server to respond successfully.
        if err.code != 500:
            raise err

        log.info(f"Received status {err.code} response when fetching {len(ids)} resources. Attempting to fetch resources individually.")

    resources: list[dict[str, Any]] = []

    async for resource in process_completed_fetches(
        [_fetch_resource_page(http, base_url, path, response_model, [id], semaphore, log) for id in ids]
    ):
        resources.append(resource)

    return resources


async def fetch_by_ids(
    http: HTTPSession,
    base_url: str,
    path: str,
    response_model: type[SearchResponse],
    ids: list[str],
    gateway: BraintreeGateway,
    braintree_class: IncrementalResourceBraintreeClass,
    log: Logger,
) -> AsyncGenerator[IncrementalResource, None]:
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async for resource in process_completed_fetches(
        [_fetch_resource_batch(http, base_url, path, response_model, list(chunk), semaphore, log)
         for chunk in itertools.batched(ids, SEARCH_PAGE_SIZE)]
    ):
        yield IncrementalResource.model_validate(
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
        ids,
        gateway,
        braintree_class,
        log,
    ):
        yield doc
