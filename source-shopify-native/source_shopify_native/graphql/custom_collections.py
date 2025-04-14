from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from .common import dt_to_str, round_to_latest_midnight
from ..models import ShopifyGraphQlResource


class CustomCollections(ShopifyGraphQlResource):
    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        lower_bound = dt_to_str(start)
        upper_bound = dt_to_str(round_to_latest_midnight(end))

        query = f"""
        {{
            collections(
                query: "updated_at:>={lower_bound} AND updated_at:<={upper_bound} AND collection_type:custom"
                sortKey: UPDATED_AT
            ) {{
                edges {{
                    node {{
                        id
                        title
                        handle
                        description
                        descriptionHtml
                        updatedAt
                        templateSuffix
                        sortOrder
                        productsCount {{
                            count
                            precision
                        }}
                        ruleSet {{
                            appliedDisjunctively
                            rules {{
                                column
                                condition
                                relation
                            }}
                        }}
                        products {{
                            edges {{
                                node {{
                                    id
                                }}
                            }}
                        }}
                        image {{
                            id
                            altText
                            url
                            width
                            height
                        }}
                        publications {{
                            edges {{
                                node {{
                                    __typename
                                    publishDate
                                    publication {{
                                        id
                                        name
                                    }}
                                }}
                            }}
                        }}
                        metafields {{
                            edges {{
                                node {{
                                    id
                                    namespace
                                    key
                                    value
                                    type
                                    createdAt
                                    updatedAt
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
    ) -> AsyncGenerator[dict, None]:
        PRODUCTS_KEY = "products"
        METAFIELDS_KEY = "metafields"
        PUBLICATIONS_KEY = "publications"

        current_collection = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")
            parent_id = record.get("__parentId", "")
            typename = record.get("__typename", "")

            if "gid://shopify/Collection/" in id:
                if current_collection:
                    yield current_collection

                current_collection = record
                current_collection[PRODUCTS_KEY] = []
                current_collection[METAFIELDS_KEY] = []
                current_collection[PUBLICATIONS_KEY] = []

            elif "gid://shopify/Product/" in id:
                if not current_collection:
                    log.error("Found a product before finding a collection.")
                    raise RuntimeError()
                if parent_id != current_collection.get("id", ""):
                    log.error(
                        "Product's parent id does not match the current collection's id.",
                        {
                            "product.id": id,
                            "product.__parentId": parent_id,
                            "current_collection.id": current_collection.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_collection[PRODUCTS_KEY].append(record)

            elif "gid://shopify/Metafield/" in id:
                if not current_collection:
                    log.error("Found a metafield before finding a collection.")
                    raise RuntimeError()
                if parent_id != current_collection.get("id", ""):
                    log.error(
                        "Metafield's parent id does not match the current collection's id.",
                        {
                            "metafield.__parentId": parent_id,
                            "current_collection.id": current_collection.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_collection[METAFIELDS_KEY].append(record)

            elif typename == "CollectionPublication":
                if not current_collection:
                    log.error("Found a publication before finding a collection.")
                    raise RuntimeError()
                if parent_id != current_collection.get("id", ""):
                    log.error(
                        "Publication's parent id does not match the current collection's id.",
                        {
                            "publication.__parentId": parent_id,
                            "current_collection.id": current_collection.get("id"),
                        },
                    )
                    raise RuntimeError()

                record.pop("__typename", None)
                current_collection[PUBLICATIONS_KEY].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_collection:
            yield current_collection
