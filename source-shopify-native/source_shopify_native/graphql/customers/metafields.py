from datetime import datetime
from ..metafields import MetafieldsResource


class CustomerMetafields(MetafieldsResource):
    PARENT_ID_KEY = "gid://shopify/Customer/"

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return CustomerMetafields.build_query_with_fragment(
            "customers",
            "UPDATED_AT",
            start,
            end,
        )
