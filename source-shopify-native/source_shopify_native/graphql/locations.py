from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from ..models import ShopifyGraphQlResource


class Locations(ShopifyGraphQlResource):
    @property
    def name(self) -> str:
        return "locations"

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        query = """
        {
            locations {
                edges {
                    node {
                        id
                        name
                        address {
                            address1
                            address2
                            city
                            country
                            countryCode
                            phone
                            province
                            provinceCode
                            zip
                        }
                        addressVerified
                        deactivatedAt
                        fulfillsOnlineOrders
                        hasActiveInventory
                        isActive
                        shipsInventory
                        fulfillmentService {
                            id
                            handle
                            serviceName
                            type
                        }
                        metafields {
                            edges {
                                node {
                                    id
                                    namespace
                                    key
                                    value
                                    type
                                    createdAt
                                    updatedAt
                                }
                            }
                        }
                    }
                }
            }
        }
        """

        return query

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[Any, None]:
        METAFIELDS_KEY = "metafields"

        current_location = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/Location/" in id:
                if current_location:
                    yield current_location

                current_location = record
                current_location[METAFIELDS_KEY] = []

            elif "gid://shopify/Metafield/" in id:
                if not current_location:
                    log.error("Found a metafield before finding a location.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_location.get("id", ""):
                    log.error(
                        "Metafield's parent id does not match the current location's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "metafield.id": id,
                            "metafield.__parentId": record.get("__parentId"),
                            "current_location.id": current_location.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_location[METAFIELDS_KEY].append(record)

        if current_location:
            yield current_location
