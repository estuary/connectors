from datetime import datetime
from ..metafields import MetafieldsResource


class CustomCollectionMetafields(MetafieldsResource):
    PARENT_ID_KEY = "gid://shopify/Collection/"

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return CustomCollectionMetafields.build_query_with_fragment(
            "collections",
            "UPDATED_AT",
            start,
            end,
            query="AND collection_type:custom",
            includeCreatedAt=False,
        )


class SmartCollectionMetafields(MetafieldsResource):
    PARENT_ID_KEY = "gid://shopify/Collection/"

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return SmartCollectionMetafields.build_query_with_fragment(
            "collections",
            "UPDATED_AT",
            start,
            end,
            query="AND collection_type:smart",
            includeCreatedAt=False,
        )
