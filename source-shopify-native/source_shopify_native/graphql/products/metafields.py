from datetime import datetime
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
