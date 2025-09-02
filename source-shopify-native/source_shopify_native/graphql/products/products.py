from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from source_shopify_native.models import ShopifyGraphQLResource, SortKey


class Products(ShopifyGraphQLResource):
    NAME = "products"
    QUERY_ROOT = "products"
    SORT_KEY = SortKey.UPDATED_AT
    QUERY = """
    title
    bodyHtml
    vendor
    productType
    handle
    publishedAt
    templateSuffix
    description
    descriptionHtml
    isGiftCard
    giftCardTemplateSuffix
    hasOnlyDefaultVariant
    hasOutOfStockVariants
    hasVariantsThatRequiresComponents
    requiresSellingPlan
    onlineStorePreviewUrl
    onlineStoreUrl
    tags
    totalInventory
    tracksInventory
    status
    seo {
        description
        title
    }
    priceRangeV2 {
        maxVariantPrice {
            amount
            currencyCode
        }
        minVariantPrice {
            amount
            currencyCode
        }
    }
    mediaCount {
        count
        precision
    }
    options {
        id
        name
        position
        values
    }
    resourcePublications {
        edges {
            node {
                isPublished
                publication {
                    id
                    name
                    catalog {
                        id
                        title
                        status
                    }
                }
            }
        }
    }
    featuredMedia {
        id
        alt
        mediaContentType
        status
        mediaErrors {
            code
            details
            message
        }
        mediaWarnings {
            code
            message
        }
        preview {
            image {
                altText
                height
                id
                url
                height
                width
            }
            status
        }
    }
    feedback {
        details {
            app {
                id
                description
            }
            feedbackGeneratedAt
            link {
                label
                url
            }
            messages {
                field
                message
            }
            state
        }
        summary
    }
    variantsCount {
        count
        precision
    }
    """

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return Products.build_query_with_fragment(
            start,
            end,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        PUBLICATIONS_KEY = "resourcePublications"
        current_product = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/Product/" in id:
                if current_product:
                    yield current_product

                current_product = record
                current_product[PUBLICATIONS_KEY] = []

            elif not id and "gid://shopify/Publication/" in record.get(
                "publication", {}
            ).get("id", ""):
                if not current_product:
                    log.error("Found a publication before finding a product.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_product.get("id", ""):
                    log.error(
                        "Publication's parent id does not match the current product's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "publication.id": record.get("publication", {}).get("id"),
                            "publication.__parentId": record.get("__parentId"),
                            "current_product.id": current_product.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_product[PUBLICATIONS_KEY].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_product:
            yield current_product
