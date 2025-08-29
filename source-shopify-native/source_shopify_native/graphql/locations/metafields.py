from datetime import datetime
from logging import Logger
from typing import AsyncGenerator
from ..metafields import MetafieldsResource


class LocationMetafields(MetafieldsResource):
    NAME = "location_metafields"
    PARENT_ID_KEY = "gid://shopify/Location/"

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return LocationMetafields.build_query_with_fragment(
            "locations",
            None,
            start,
            end,
        )

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        return MetafieldsResource._process_metafields_result(
            log, lines, LocationMetafields.PARENT_ID_KEY
        )
