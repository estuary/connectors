from datetime import datetime
from logging import Logger
from typing import AsyncGenerator
from ..metafields import MetafieldsResource

from source_shopify_native.models import SortKey

class CustomerMetafields(MetafieldsResource):
    NAME = "customer_metafields"
    PARENT_ID_KEY = "gid://shopify/Customer/"
    QUERY_ROOT = "customers"
    SORT_KEY = SortKey.UPDATED_AT

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return CustomerMetafields.build_query_with_fragment(
            start,
            end,
        )

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        return MetafieldsResource._process_metafields_result(
            log, lines, CustomerMetafields.PARENT_ID_KEY
        )
