from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from source_shopify_native.models import ShopifyGraphQLResource, SortKey


class ProductMedia(ShopifyGraphQLResource):
    NAME = "product_media"
    QUERY_ROOT = "products"
    SORT_KEY = SortKey.UPDATED_AT
    QUERY = """
    media(query:"media_type:IMAGE") {
        edges {
            node {
                ... on MediaImage {
                    id
                    alt
                    image {
                        url
                        width
                        height
                    }
                }
            }
        }
    }
    """

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return ProductMedia.build_query_with_fragment(
            start,
            end,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[Any, None]:
        MEDIA_KEY = "media"
        current_product = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/Product/" in id:
                if current_product:
                    yield current_product

                current_product = record

                current_product[MEDIA_KEY] = []

            elif "gid://shopify/MediaImage/" in id:
                if not current_product:
                    log.error("Found media before finding a product.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_product.get("id", ""):
                    log.error(
                        "Media's parent id does not match the current product's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "media.id": id,
                            "media.__parentId": record.get("__parentId"),
                            "current_product.id": current_product.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_product[MEDIA_KEY].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_product:
            yield current_product
