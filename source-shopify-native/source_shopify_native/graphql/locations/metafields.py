from datetime import datetime
from logging import Logger
from typing import AsyncGenerator
from ..metafields import MetafieldsResource


class LocationMetafields(MetafieldsResource):
    NAME = "location_metafields"
    PARENT_ID_KEY = "gid://shopify/Location/"
    QUERY_ROOT = "locations"
    QUALIFYING_SCOPES = {"read_locations", "read_inventory", "read_markets_home"}

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return LocationMetafields.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
        )

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        return MetafieldsResource._process_metafields_result(
            log, lines, LocationMetafields.PARENT_ID_KEY
        )
