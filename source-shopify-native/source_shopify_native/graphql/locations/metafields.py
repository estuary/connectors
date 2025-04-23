from datetime import datetime
from ..metafields import MetafieldsResource


class LocationMetafields(MetafieldsResource):
    PARENT_ID_KEY = "gid://shopify/Location/"

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return LocationMetafields.build_query_with_fragment(
            "locations",
            None,
            start,
            end,
            includeUpdatedAt=False,
        )
