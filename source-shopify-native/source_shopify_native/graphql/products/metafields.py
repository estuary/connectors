from datetime import datetime
from logging import Logger
from typing import AsyncGenerator
from ..metafields import MetafieldsResource


class ProductMetafields(MetafieldsResource):
    NAME = "product_metafields"
    PARENT_ID_KEY = "gid://shopify/Product/"

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return ProductMetafields.build_query_with_fragment(
            "products",
            "UPDATED_AT",
            start,
            end,
        )

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        return MetafieldsResource._process_metafields_result(
            log, lines, ProductMetafields.PARENT_ID_KEY
        )
