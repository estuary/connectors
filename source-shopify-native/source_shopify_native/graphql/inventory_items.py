from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from .common import dt_to_str, round_to_latest_midnight
from ..models import ShopifyGraphQlResource


class InventoryItems(ShopifyGraphQlResource):
    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        lower_bound = dt_to_str(start)
        upper_bound = dt_to_str(round_to_latest_midnight(end))

        query = f"""
        {{
            inventoryItems(
                query: "updated_at:>={lower_bound} AND updated_at:<={upper_bound}"
            ) {{
                edges {{
                    node {{
                        id
                        createdAt
                        updatedAt
                        countryCodeOfOrigin
                        harmonizedSystemCode
                        provinceCodeOfOrigin
                        sku
                        tracked
                        requiresShipping
                        unitCost {{
                            amount
                            currencyCode
                        }}
                        variant {{
                            id
                            product {{
                                id
                                title
                            }}
                        }}
                        inventoryHistoryUrl
                        inventoryLevels {{
                            edges {{
                                node {{
                                    id
                                    createdAt
                                    updatedAt
                                    canDeactivate
                                    deactivationAlert
                                    location {{
                                        id
                                    }}
                                    quantities(names: ["available", "incoming", "committed", "damaged", "on_hand", "quality_control", "reserved", "safety_stock"]) {{
                                        id
                                        name
                                        quantity
                                        updatedAt
                                    }}
                                    scheduledChanges {{
                                        edges {{
                                            node {{
                                                expectedAt
                                                fromName
                                                toName
                                                ledgerDocumentUri
                                                quantity
                                            }}
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
        """

        return query

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[Any, None]:
        INVENTORY_LEVELS_KEY = "inventoryLevels"
        SCHEDULED_CHANGES_KEY = "scheduledChanges"

        current_inventory_item = None
        current_inventory_level = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/InventoryItem/" in id:
                if current_inventory_item:
                    yield current_inventory_item

                current_inventory_item = record
                current_inventory_item[INVENTORY_LEVELS_KEY] = []

            elif "gid://shopify/InventoryLevel/" in id:
                if not current_inventory_item:
                    log.error(
                        "Found an inventory level before finding an inventory item."
                    )
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_inventory_item.get(
                    "id", ""
                ):
                    log.error(
                        "Inventory level's parent id does not match the current inventory item's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "inventoryLevel.id": id,
                            "inventoryLevel.__parentId": record.get("__parentId"),
                            "current_inventory_item.id": current_inventory_item.get(
                                "id"
                            ),
                        },
                    )
                    raise RuntimeError()

                current_inventory_level = record
                current_inventory_level[SCHEDULED_CHANGES_KEY] = []
                current_inventory_item[INVENTORY_LEVELS_KEY].append(
                    current_inventory_level
                )

            elif "gid://shopify/InventoryLevelScheduledChange/" in id:
                if not current_inventory_level:
                    log.error(
                        "Found an inventory level scheduled change before finding an inventory level."
                    )
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_inventory_level.get(
                    "id", ""
                ):
                    log.error(
                        "Inventory level scheduled change's parent id does not match the current inventory level's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "inventoryLevelScheduledChange.id": id,
                            "inventoryLevelScheduledChange.__parentId": record.get(
                                "__parentId"
                            ),
                            "current_inventory_level.id": current_inventory_level.get(
                                "id"
                            ),
                        },
                    )
                    raise RuntimeError()

                current_inventory_level[SCHEDULED_CHANGES_KEY].append(record)

        if current_inventory_item:
            yield current_inventory_item
