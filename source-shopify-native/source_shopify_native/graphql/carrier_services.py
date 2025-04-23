from logging import Logger
from datetime import datetime
from typing import AsyncGenerator

from ..models import ShopifyGraphQlResource


class CarrierServices(ShopifyGraphQlResource):
    QUERY = """
    name
    callbackUrl
    active
    supportsServiceDiscovery
    """

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return CarrierServices.build_query_with_fragment(
            "carrierServices",
            "UPDATED_AT",
            start,
            end,
            query="active:true",
            includeLegacyId=False,
            includeCreatedAt=False,
            includeUpdatedAt=False,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in CarrierServices._process_result(
            log, lines, "gid://shopify/CarrierService/"
        ):
            yield record
