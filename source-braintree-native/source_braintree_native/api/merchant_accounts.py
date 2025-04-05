from logging import Logger
from typing import AsyncGenerator

import braintree
from braintree import BraintreeGateway
from braintree.util.xml_util import XmlUtil
from estuary_cdk.http import HTTPSession

from .common import HEADERS, braintree_object_to_dict
from ..models import FullRefreshResource, MerchantAccountsResponse


async def snapshot_merchant_accounts(
    http: HTTPSession,
    base_url: str,
    gateway: BraintreeGateway,
    log: Logger,
) -> AsyncGenerator[FullRefreshResource, None]:
    url = f"{base_url}/merchant_accounts"

    params = {
        "page": 1,
    }

    while True:
        response = MerchantAccountsResponse.model_validate(
            XmlUtil.dict_from_xml(
                await http.request(log, url, params=params, headers=HEADERS)
            )
        )

        if response.merchant_accounts.merchant_account is None:
            return

        # If there's only a single record, it's not within a list. We place it in a list to make later processing consistent.
        if isinstance(response.merchant_accounts.merchant_account, dict):
            response.merchant_accounts.merchant_account = [response.merchant_accounts.merchant_account]

        for merchant_account in response.merchant_accounts.merchant_account:
            yield FullRefreshResource.model_validate(
                braintree_object_to_dict(
                    braintree.MerchantAccount(gateway, merchant_account)
                )
            )

        if len(response.merchant_accounts.merchant_account) < response.merchant_accounts.page_size:
            return

        params["page"] += 1
