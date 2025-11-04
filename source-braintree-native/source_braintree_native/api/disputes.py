import asyncio
import math
from datetime import datetime
from logging import Logger
from typing import AsyncGenerator, Awaitable, Any

import braintree
from estuary_cdk.http import HTTPSession

from .common import HEADERS, SEARCH_PAGE_SIZE, SEMAPHORE_LIMIT, braintree_object_to_dict, braintree_xml_to_dict, process_completed_fetches
from ..models import IncrementalResource, DisputeSearchField, DisputesSearchResponse

async def _fetch_dispute_page(
    http: HTTPSession,
    base_url: str,
    search_field: DisputeSearchField,
    start: datetime,
    end: datetime,
    page: int,
    semaphore: asyncio.Semaphore,
    log: Logger,
) -> list[dict[str, Any]]:
    url = f"{base_url}/disputes/advanced_search"
    body = {
        "search": {
            search_field: {
                "min": start.isoformat(),
                "max": end.isoformat(),
            }
        }
    }
    params = {
        "page": page,
    }

    async with semaphore:
        response = DisputesSearchResponse.model_validate(
            braintree_xml_to_dict(
                await http.request(log, url, "POST", params, body, headers=HEADERS)
            )
        )

    if response.resources.resource is None:
        return []

    resources = response.resources.resource
    if isinstance(resources, dict):
        resources = [resources]

    return resources


async def _determine_total_pages(
    http: HTTPSession,
    base_url: str,
    search_field: DisputeSearchField,
    start: datetime,
    end: datetime,
    log: Logger,
) -> int:
    url = f"{base_url}/disputes/advanced_search"
    body = {
        "search": {
            search_field: {
                "min": start.isoformat(),
                "max": end.isoformat(),
            }
        }
    }
    params = {
        "page": 1,
    }

    response = DisputesSearchResponse.model_validate(
        braintree_xml_to_dict(
            await http.request(log, url, "POST", params, body, headers=HEADERS)
        )
    )

    if response.resources.resource is None or response.resources.total_items == 0:
        return 0

    total_pages = math.ceil(response.resources.total_items / SEARCH_PAGE_SIZE)
    return total_pages


# Unlike the other incremental resource endpoints, the number of records returned by a single disputes
# search is not limited to 10,000 records.
async def fetch_disputes_between(
    http: HTTPSession,
    base_url: str,
    search_field: DisputeSearchField,
    start: datetime,
    end: datetime,
    log: Logger,
) -> AsyncGenerator[IncrementalResource, None]:
    total_pages = await _determine_total_pages(
        http,
        base_url, 
        search_field,
        start,
        end,
        log,
    )

    if total_pages == 0:
        return

    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async for dispute in process_completed_fetches(
        [_fetch_dispute_page(http, base_url, search_field, start, end, page, semaphore, log)
         for page in range(1, total_pages + 1)]
    ):
        yield IncrementalResource.model_validate(
            braintree_object_to_dict(
                braintree.Dispute(dispute)
            )
        )
