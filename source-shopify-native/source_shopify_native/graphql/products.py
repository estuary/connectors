from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from source_shopify_native.graphql.common import dt_to_str, round_to_latest_midnight
from source_shopify_native.models import ShopifyGraphQlResource


class Products(ShopifyGraphQlResource):
    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        lower_bound = dt_to_str(start)
        upper_bound = dt_to_str(round_to_latest_midnight(end))

        query = f"""
        {{
            products(
                query: "updated_at:>={lower_bound} AND updated_at:<={upper_bound}"
                sortKey: UPDATED_AT
            ) {{
                edges {{
                    node {{
                        updatedAt
                        legacyResourceId
                        id
                        title
                        bodyHtml
                        vendor
                        productType
                        createdAt
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
                        seo {{
                            description
                            title
                        }}
                        priceRangeV2 {{
                            maxVariantPrice {{
                                amount
                                currencyCode
                            }}
                            minVariantPrice {{
                                amount
                                currencyCode
                            }}
                        }}
                        mediaCount {{
                            count
                            precision
                        }}
                        options {{
                            id
                            name
                            position
                            values
                        }}
                        resourcePublications {{
                            edges {{
                                node {{
                                    isPublished
                                    publication {{
                                        id
                                        name
                                        catalog {{
                                            id
                                            title
                                            status
                                        }}
                                    }}
                                }}
                            }}
                        }}
                        media(query:"media_type:IMAGE") {{
                            edges {{
                                node {{
                                    ... on MediaImage {{
                                        id
                                        alt
                                        image {{
                                            url
                                            width
                                            height
                                        }}
                                    }}
                                }}
                            }}
                        }}
                        featuredMedia {{
                            id
                            alt
                            mediaContentType
                            status
                            mediaErrors {{
                                code
                                details
                                message
                            }}
                            mediaWarnings {{
                                code
                                message
                            }}
                            preview {{
                                image {{
                                    altText
                                    height
                                    id
                                    url
                                    height
                                    width
                                }}
                                status
                            }}
                        }}
                        feedback {{
                            details {{
                                app {{
                                    id
                                    description
                                }}
                                feedbackGeneratedAt
                                link {{
                                    label
                                    url
                                }}
                                messages {{
                                    field
                                    message
                                }}
                                state
                            }}
                            summary
                        }}
                        variantsCount {{
                            count
                            precision
                        }}
                        variants {{
                            edges {{
                                node {{
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
                                    image {{
                                        id
                                    }}
                                    selectedOptions {{
                                        name
                                        value
                                        optionValue {{
                                            id
                                            name
                                        }}
                                    }}
                                    inventoryItem {{
                                        id
                                        legacyResourceId
                                        sku
                                        requiresShipping
                                        tracked
                                        measurement {{
                                            id
                                            weight {{
                                                value
                                                unit
                                            }}
                                        }}
                                        inventoryLevels {{
                                            edges {{
                                                node {{
                                                    id
                                                    location {{
                                                        id
                                                        name
                                                        fulfillmentService {{
                                                            id
                                                            handle
                                                            serviceName
                                                            type
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
                }}
            }}
        }}
        """

        return query

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[Any, None]:
        # Key names were aligned with the corresponding field name returned by the non-bulk GraphQL API.
        VARIANTS_KEY = "variants"
        PUBLICATIONS_KEY = "resourcePublications"
        MEDIA_KEY = "media"
        INVENTORY_LEVELS_KEY = "inventoryLevels"

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
                current_product[PUBLICATIONS_KEY] = []
                current_product[MEDIA_KEY] = []

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
                current_variant[INVENTORY_LEVELS_KEY] = []
                current_product[VARIANTS_KEY].append(current_variant)

            elif "gid://shopify/InventoryLevel/" in id:
                if not current_variant:
                    log.error("Found an inventory level before finding a variant.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_variant.get("id", ""):
                    log.error(
                        "Inventory level's parent id does not match the current variant's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "inventoryLevel.id": id,
                            "inventoryLevel.__parentId": record.get("__parentId"),
                            "current_variant.id": current_variant.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_variant[INVENTORY_LEVELS_KEY].append(record)

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

        yield current_product
