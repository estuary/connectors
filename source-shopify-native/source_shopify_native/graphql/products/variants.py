from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from source_shopify_native.models import ShopifyGraphQLResource, SortKey, StoreCapabilities

# Every type implementing Shopify's `Media` interface, one per `MediaContentType` value
# (IMAGE, VIDEO, EXTERNAL_VIDEO, MODEL_3D). `ProductVariant.media` takes no media_type
# filter, so a variant's media line can be any of these.
MEDIA_GID_PREFIXES = (
    "gid://shopify/MediaImage/",
    "gid://shopify/Video/",
    "gid://shopify/ExternalVideo/",
    "gid://shopify/Model3d/",
)


class ProductVariants(ShopifyGraphQLResource):
    NAME = "product_variants"
    QUERY_ROOT = "products"
    SORT_KEY = SortKey.UPDATED_AT
    QUALIFYING_SCOPES = {"read_products"}
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
                media {
                    edges {
                        node {
                            id
                            alt
                            mediaContentType
                            ... on MediaImage {
                                image {
                                    url
                                    width
                                    height
                                }
                            }
                        }
                    }
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
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        return ProductVariants.build_query_with_fragment(
            start,
            end,
            first=first,
            after=after,
            capabilities=capabilities,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        VARIANTS_KEY = "variants"
        MEDIA_KEY = "media"
        current_product = None
        # Variants of the current product, keyed by id, so media lines can be attached to the
        # variant named by their __parentId. Shopify only guarantees a child appears somewhere
        # after its parent, not immediately after, so media can't be attached positionally.
        current_variants_by_id: dict[str, dict[str, Any]] = {}

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")
            parent_id: str = record.get("__parentId", "")

            if "gid://shopify/Product/" in id:
                if current_product:
                    yield current_product

                current_product = record

                current_product[VARIANTS_KEY] = []
                current_variants_by_id = {}

            elif "gid://shopify/ProductVariant/" in id:
                if not current_product:
                    log.error("Found a variant before finding a product.")
                    raise RuntimeError()
                elif parent_id != current_product.get("id", ""):
                    log.error(
                        "Variant's parent id does not match the current product's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "variant.id": id,
                            "variant.__parentId": parent_id,
                            "current_product.id": current_product.get("id"),
                        },
                    )
                    raise RuntimeError()

                record[MEDIA_KEY] = []
                current_product[VARIANTS_KEY].append(record)
                current_variants_by_id[id] = record

            elif id.startswith(MEDIA_GID_PREFIXES):
                # Classify on the gid prefix rather than "parented to a variant", so a
                # connection added under variants later without updating this function falls
                # through to the unidentified-line error instead of landing in `media`.
                if parent_id not in current_variants_by_id:
                    log.error(
                        "Media's parent is not a variant of the current product. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "media.id": id,
                            "media.__parentId": parent_id,
                            "current_product.id": current_product.get("id")
                            if current_product
                            else None,
                        },
                    )
                    raise RuntimeError()

                current_variants_by_id[parent_id][MEDIA_KEY].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_product:
            yield current_product
