from logging import Logger
from typing import AsyncGenerator

from braintree import BraintreeGateway
from estuary_cdk.http import HTTPSession

from .common import HEADERS, braintree_object_to_dict, braintree_xml_to_dict
from ..models import FullRefreshResource, NonPaginatedSnapshotResponse, NonPaginatedSnapshotBraintreeClass


async def snapshot_non_paginated_resource(
    http: HTTPSession,
    base_url: str,
    path: str,
    response_model: type[NonPaginatedSnapshotResponse],
    gateway: BraintreeGateway,
    braintree_class: NonPaginatedSnapshotBraintreeClass,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{base_url}/{path}"
    response = response_model.model_validate(
        braintree_xml_to_dict(
            await http.request(log, url, headers=HEADERS)
        )
    )

    if response.resources is None:
        return

    for resource in response.resources:
        yield FullRefreshResource.model_validate(
            braintree_object_to_dict(
                braintree_class(gateway, resource)
            )
        )
