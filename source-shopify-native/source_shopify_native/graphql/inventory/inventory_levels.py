from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from ...models import ShopifyGraphQLResource


class InventoryLevels(ShopifyGraphQLResource):
    NAME = "inventory_levels"
    QUERY_ROOT = "inventoryItems"
    QUALIFYING_SCOPES = {"read_inventory"}
    QUERY = """
    inventoryLevels {
        edges {
            node {
                id
                createdAt
                updatedAt
                canDeactivate
                deactivationAlert
                item {
                    id
                    legacyResourceId
                }
                location {
                    id
                    legacyResourceId
                }
                quantities(names: ["available", "incoming", "committed", "damaged", "on_hand", "quality_control", "reserved", "safety_stock"]) {
                    id
                    name
                    quantity
                    updatedAt
                }
                scheduledChanges {
                    edges {
                        node {
                            expectedAt
                            fromName
                            toName
                            ledgerDocumentUri
                            quantity
                        }
                    }
                }
            }
        }
    }
    """

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return InventoryLevels.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        SCHEDULED_CHANGES_KEY = "scheduledChanges"
        current_inventory_level = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/InventoryLevel/" in id:
                if current_inventory_level:
                    yield current_inventory_level

                current_inventory_level = record
                current_inventory_level[SCHEDULED_CHANGES_KEY] = []

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

            if current_inventory_level:
                yield current_inventory_level
