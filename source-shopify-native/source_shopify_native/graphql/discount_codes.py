from logging import Logger
import json
from datetime import datetime
from typing import Any, AsyncGenerator

from ..models import ShopifyGraphQlResource


class DiscountCodes(ShopifyGraphQlResource):
    QUERY = """
    codeDiscount {
        ... on DiscountCodeBasic {
            updatedAt
            createdAt
            title
            asyncUsageCount
            codes {
                edges {
                    node {
                        id
                        code
                    }
                }
            }
        }
        ... on DiscountCodeBxgy {
            updatedAt
            createdAt
            title
            asyncUsageCount
            codes {
                edges {
                    node {
                        id
                        code
                    }
                }
            }
        }
        ... on DiscountCodeFreeShipping {
            updatedAt
            createdAt
            title
            asyncUsageCount
            codes {
                edges {
                    node {
                        id
                        code
                    }
                }
            }
        }
    }
    """

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return DiscountCodes.build_query_with_fragment(
            "codeDiscountNodes",
            "UPDATED_AT",
            start,
            end,
            includeLegacyId=False,
            includeCreatedAt=False,
            includeUpdatedAt=False,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        CODES_KEY = "codes"
        current_discount_code = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")
            typename: str = record.get("__typename", "")

            if "gid://shopify/DiscountCodeNode/" in id:
                if current_discount_code:
                    yield current_discount_code

                current_discount_code = record
                if (
                    current_discount_code.get("codeDiscount")
                    and CODES_KEY not in current_discount_code["codeDiscount"]
                ):
                    current_discount_code["codeDiscount"][CODES_KEY] = []

            elif (
                typename == "DiscountRedeemCode"
                or "gid://shopify/DiscountRedeemCode/" in id
            ):
                if not current_discount_code:
                    log.error(
                        "Found a redeem code before finding a discount code node."
                    )
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_discount_code.get(
                    "id", ""
                ):
                    log.error(
                        "Redeem code's parent id does not match the current discount code's id.",
                        {
                            "redeem_code.id": record.get("id"),
                            "redeem_code.__parentId": record.get("__parentId"),
                            "current_discount_code.id": current_discount_code.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_discount_code["codeDiscount"][CODES_KEY].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_discount_code:
            yield current_discount_code
