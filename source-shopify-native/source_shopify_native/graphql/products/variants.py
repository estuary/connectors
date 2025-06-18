from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from source_shopify_native.models import ShopifyGraphQLResource


class ProductVariants(ShopifyGraphQLResource):
    NAME = "product_variants"
    QUERY = """
    variants {
        edges {
            node {
                legacyResourceId
                id
                title
                price
                position
                inventoryPolicy
                compareAtPrice
                createdAt
                updatedAt
                taxable
                barcode
                sku
                inventoryQuantity
                image {
                    id
                }
                selectedOptions {
                    name
                    value
                    optionValue {
                        id
                        name
                    }
                }
                inventoryItem {
                    id
                    legacyResourceId
                }
            }
        }
    }
    """

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return ProductVariants.build_query_with_fragment(
            "products",
            "UPDATED_AT",
            start,
            end,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        VARIANTS_KEY = "variants"
        current_product = None
        current_variant = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/Product/" in id:
                if current_product:
                    yield current_product

                current_product = record

                current_product[VARIANTS_KEY] = []

            elif "gid://shopify/ProductVariant/" in id:
                if not current_product:
                    log.error("Found a variant before finding a product.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_product.get("id", ""):
                    log.error(
                        "Variant's parent id does not match the current product's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "variant.id": id,
                            "variant.__parentId": record.get("__parentId"),
                            "current_product.id": current_product.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_variant = record
                current_product[VARIANTS_KEY].append(current_variant)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_product:
            yield current_product
