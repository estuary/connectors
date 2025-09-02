from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator, ClassVar, Literal

from ...models import ShopifyGraphQLResource, SortKey


class _Collections(ShopifyGraphQLResource):
    COLLECTION_TYPE: ClassVar[str] = ""
    QUERY_ROOT = "collections"
    SORT_KEY = SortKey.UPDATED_AT
    QUERY = """
    title
    handle
    description
    descriptionHtml
    templateSuffix
    sortOrder
    productsCount {
        count
        precision
    }
    products {
        edges {
            node {
                id
                legacyResourceId
            }
        }
    }
    publications {
        edges {
            node {
                __typename
                publishDate
                publication {
                    id
                    name
                }
            }
        }
    }
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.COLLECTION_TYPE:
            raise NotImplementedError("Subclasses must set COLLECTION_TYPE")

    @staticmethod
    def build_query_with_type(
        collection_type: Literal["custom", "smart"], start: datetime, end: datetime
    ) -> str:
        return _Collections.build_query_with_fragment(
            start,
            end,
            query=f"collection_type:{collection_type}",
            includeCreatedAt=False,
        )

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        raise NotImplementedError("build_query method must be implemented")

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        PRODUCTS_KEY = "products"
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


class CustomCollections(_Collections):
    NAME = "custom_collections"
    COLLECTION_TYPE = "custom"

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return _Collections.build_query_with_type("custom", start, end)

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        return _Collections.process_result(log, lines)


class SmartCollections(_Collections):
    NAME = "smart_collections"
    COLLECTION_TYPE = "smart"

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return _Collections.build_query_with_type("smart", start, end)

    @staticmethod
    def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        return _Collections.process_result(log, lines)
