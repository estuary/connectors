from logging import Logger
import json
from datetime import datetime
from typing import Any, AsyncGenerator, ClassVar

from ..models import ShopifyGraphQLResource


class MetafieldsResource(ShopifyGraphQLResource):
    QUERY = """
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
    """

    PARENT_ID_KEY: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.PARENT_ID_KEY:
            raise NotImplementedError("Subclasses must set PARENT_ID_KEY")

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        # This method should be implemented by subclasses
        # pylint: disable=unused-argument
        raise NotImplementedError("Subclasses must implement build_query")

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        METAFIELDS_KEY = "metafields"
        current_parent = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if MetafieldsResource.PARENT_ID_KEY in id:
                if current_parent:
                    yield current_parent

                current_parent = record
                current_parent[METAFIELDS_KEY] = []

            elif "gid://shopify/Metafield/" in id:
                if not current_parent:
                    log.error("Found a metafield before finding a parent record.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_parent.get("id", ""):
                    log.error(
                        "Metafield's parent id does not match the current parent record's id.",
                        {
                            "metafield.id": record.get("id"),
                            "metafield.__parentId": record.get("__parentId"),
                            "current_parent.id": current_parent.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_parent[METAFIELDS_KEY].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_parent:
            yield current_parent
