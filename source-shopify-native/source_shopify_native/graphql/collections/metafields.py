from datetime import datetime
from logging import Logger
from typing import AsyncGenerator
from ..metafields import MetafieldsResource

from source_shopify_native.models import SortKey


class CustomCollectionMetafields(MetafieldsResource):
    NAME = "custom_collection_metafields"
    PARENT_ID_KEY = "gid://shopify/Collection/"
    QUERY_ROOT = "collections"
    SORT_KEY = SortKey.UPDATED_AT

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return CustomCollectionMetafields.build_query_with_fragment(
            start,
            end,
            query="AND collection_type:custom",
            includeCreatedAt=False,
        )

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        return MetafieldsResource._process_metafields_result(
            log, lines, CustomCollectionMetafields.PARENT_ID_KEY
        )


class SmartCollectionMetafields(MetafieldsResource):
    NAME = "smart_collection_metafields"
    PARENT_ID_KEY = "gid://shopify/Collection/"
    QUERY_ROOT = "collections"
    SORT_KEY = SortKey.UPDATED_AT

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return SmartCollectionMetafields.build_query_with_fragment(
            start,
            end,
            query="AND collection_type:smart",
            includeCreatedAt=False,
        )

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        return MetafieldsResource._process_metafields_result(
            log, lines, SmartCollectionMetafields.PARENT_ID_KEY
        )
