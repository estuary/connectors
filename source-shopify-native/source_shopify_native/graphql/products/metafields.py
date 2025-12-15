from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator
from ..metafields import MetafieldsResource

from source_shopify_native.models import SortKey


class ProductMetafields(MetafieldsResource):
    NAME = "product_metafields"
    PARENT_ID_KEY = "gid://shopify/Product/"
    QUERY_ROOT = "products"
    SORT_KEY = SortKey.UPDATED_AT
    QUALIFYING_SCOPES = {"read_products"}

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
    ) -> str:
        return ProductMetafields.build_query_with_fragment(
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
            log, lines, ProductMetafields.PARENT_ID_KEY
        )

class ProductVariantMetafields(MetafieldsResource):
    NAME = "product_variant_metafields"
    PARENT_ID_KEY = "gid://shopify/ProductVariant/"
    QUERY_ROOT = "products"
    SORT_KEY = SortKey.UPDATED_AT
    QUALIFYING_SCOPES = {"read_products"}
    QUERY = """
    variants {
        edges {
            node {
                id
                legacyResourceId
                createdAt
                updatedAt
                metafields {
                    edges {
                        node {
                            id
                            legacyResourceId
                            namespace
                            key
                            value
                            type
                            definition {
                                id
                                description
                                namespace
                                type {
                                    category
                                    name
                                }
                            }
                            ownerType
                            createdAt
                            updatedAt
                        }
                    }
                }
            }
        }
    }
    """

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return ProductVariantMetafields.build_query_with_fragment(
            start,
            end,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        VARIANTS_KEY = "variants"
        METAFIELDS_KEY = "metafields"
        current_product = None
        current_variant = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/Product/" in id:
                if current_product:
                    # Yield any remaining variant from previous product
                    if current_variant:
                        yield current_variant
                        current_variant = None
                
                current_product = record

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
                
                # Yield previous variant if exists
                if current_variant:
                    yield current_variant
                
                current_variant = record
                current_variant[METAFIELDS_KEY] = []

            elif "gid://shopify/Metafield/" in id:
                if not current_variant:
                    log.error("Found a metafield before finding a variant.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_variant.get("id", ""):
                    log.error(
                        "Metafield's parent id does not match the current variant's id.",
                        {
                            "metafield.id": record.get("id"),
                            "metafield.__parentId": record.get("__parentId"),
                            "current_variant.id": current_variant.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_variant[METAFIELDS_KEY].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        # Yield final variant if exists
        if current_variant:
            yield current_variant
