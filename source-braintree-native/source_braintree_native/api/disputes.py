from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

import braintree
from estuary_cdk.http import HTTPSession

from .common import HEADERS, SEARCH_PAGE_SIZE, braintree_object_to_dict, braintree_xml_to_dict
from ..models import IncrementalResource, DisputeSearchField, DisputesSearchResponse

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

    while True:
        response = DisputesSearchResponse.model_validate(
            braintree_xml_to_dict(
                await http.request(log, url, "POST", params, body, headers=HEADERS)
            )
        )

        if isinstance(response.resources.resource, dict):
            response.resources.resource = [response.resources.resource]

        if response.resources.resource is None:
            return

        for dispute in response.resources.resource:
            yield IncrementalResource.model_validate(
                braintree_object_to_dict(
                    braintree.Dispute(dispute)
                )
            )

        if len(response.resources.resource) < SEARCH_PAGE_SIZE:
            return

        params["page"] += 1
