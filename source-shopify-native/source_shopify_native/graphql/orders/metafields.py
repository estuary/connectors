from datetime import datetime
from ..metafields import MetafieldsResource


class OrderMetafields(MetafieldsResource):
    PARENT_ID_KEY = "gid://shopify/Order/"

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return OrderMetafields.build_query_with_fragment(
            "orders",
            "UPDATED_AT",
            start,
            end,
        )
