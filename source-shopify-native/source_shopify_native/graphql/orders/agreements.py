import json
from datetime import datetime
from logging import Logger
from typing import AsyncGenerator, Any

from ..common import money_bag_fragment
from ...models import ShopifyGraphQlResource


class OrderAgreements(ShopifyGraphQlResource):
    QUERY = """
    agreements {
        edges {
            node {
                id
                happenedAt
                reason
                sales {
                    edges {
                        node {
                            quantity
                            id
                            actionType
                            lineType
                            totalAmount {
                                ..._MoneyBagFields
                            }
                            totalDiscountAmountAfterTaxes {
                                ..._MoneyBagFields
                            }
                            totalDiscountAmountBeforeTaxes {
                                ..._MoneyBagFields
                            }
                            totalTaxAmount {
                                ..._MoneyBagFields
                            }
                            ... on ProductSale {
                                id
                                lineItem {
                                    id
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """
    FRAGMENTS = [money_bag_fragment]

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return OrderAgreements.build_query_with_fragment(
            "orders",
            "UPDATED_AT",
            start,
            end,
        )

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        AGREEMENTS_KEY = "agreements"
        SALES_KEY = "sales"

        current_order = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)

            if "gid://shopify/Order/" in record.get("id", ""):
                if current_order:
                    yield current_order

                current_order = record
                current_order[AGREEMENTS_KEY] = []

            elif "gid://shopify/SalesAgreement/" in record.get("id", ""):
                if not current_order:
                    log.error("Found an agreement before finding an order.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_order.get("id", ""):
                    log.error(
                        "Agreement's parent id does not match the current order's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "agreement.id": record.get("id"),
                            "agreement.__parentId": record.get("__parentId"),
                            "current_order.id": current_order.get("id"),
                        },
                    )
                    raise RuntimeError()

                record[SALES_KEY] = []
                current_order[AGREEMENTS_KEY].append(record)

            elif "gid://shopify/Sale/" in record.get("id", ""):
                if not current_order:
                    log.error("Found a sale before finding an order.")
                    raise RuntimeError()

                parent_id = record.get("__parentId", "")
                parent_agreement = None

                for agreement in current_order[AGREEMENTS_KEY]:
                    if agreement.get("id", "") == parent_id:
                        parent_agreement = agreement
                        break

                if not parent_agreement:
                    log.error(
                        "Could not find parent agreement for sale.",
                        {
                            "sale.id": record.get("id"),
                            "sale.__parentId": parent_id,
                            "current_order.id": current_order.get("id"),
                        },
                    )
                    raise RuntimeError()

                parent_agreement[SALES_KEY].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_order:
            yield current_order
