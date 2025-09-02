from datetime import datetime
from logging import Logger
from typing import AsyncGenerator
from ..metafields import MetafieldsResource

from source_shopify_native.models import SortKey


class OrderMetafields(MetafieldsResource):
    NAME = "order_metafields"
    PARENT_ID_KEY = "gid://shopify/Order/"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return OrderMetafields.build_query_with_fragment(
            start,
            end,
        )

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        return MetafieldsResource._process_metafields_result(
            log, lines, OrderMetafields.PARENT_ID_KEY
        )
