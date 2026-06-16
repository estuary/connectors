import json
from datetime import datetime
from logging import Logger
from typing import AsyncGenerator, Any

from ...models import (
    ConditionalField,
    ShopifyGraphQLResource,
    SortKey,
    StoreCapabilities,
)


class OrderReturns(ShopifyGraphQLResource):
    NAME = "order_returns"
    QUERY_ROOT = "orders"
    SORT_KEY = SortKey.UPDATED_AT
    QUALIFYING_SCOPES = {"read_returns", "read_marketplace_returns"}
    QUERY = """
    returns {
        edges {
            node {
                id
                name
                status
                totalQuantity
                closedAt
                createdAt
                decline {
                    note
                    reason
                }
                # {{ staffMember }}
                returnLineItems {
                    edges {
                        node {
                            id
                            customerNote
                            quantity
                            refundedQuantity
                            returnReasonNote
                            returnReasonDefinition {
                                id
                                deleted
                                handle
                                name
                            }
                        }
                    }
                }
                exchangeLineItems {
                    edges {
                        node {
                            id
                            quantity
                            processableQuantity
                            processedQuantity
                            variantId
                        }
                    }
                }
                refunds {
                    edges {
                        node {
                            id
                        }
                    }
                }
            }
        }
    }
    """
    CONDITIONAL_FIELDS = [
        # staffMember requires the read_users scope, which is only grantable on
        # Shopify Plus / Advanced stores or finance embedded apps.
        ConditionalField(
            placeholder="# {{ staffMember }}",
            fields="""staffMember {
                    id
                }""",
            is_available=lambda caps: "read_users" in caps.scopes
            and caps.is_plus_or_advanced,
        ),
    ]

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        return OrderReturns.build_query_with_fragment(
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
        RETURNS_KEY = "returns"
        RETURN_LINE_ITEMS_KEY = "returnLineItems"
        EXCHANGE_LINE_ITEMS_KEY = "exchangeLineItems"
        REFUNDS_KEY = "refunds"

        current_order = None

        def find_parent_return(parent_id: str) -> dict[str, Any] | None:
            if not current_order:
                return None
            for ret in current_order[RETURNS_KEY]:
                if ret.get("id", "") == parent_id:
                    return ret
            return None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/Order/" in id:
                if current_order:
                    yield current_order

                current_order = record
                current_order[RETURNS_KEY] = []

            elif (
                "gid://shopify/ReturnLineItem/" in id
                or "gid://shopify/UnverifiedReturnLineItem/" in id
            ):
                parent_id = record.get("__parentId", "")
                parent_return = find_parent_return(parent_id)
                if not parent_return:
                    log.error(
                        "Could not find parent return for return line item.",
                        {
                            "returnLineItem.id": id,
                            "returnLineItem.__parentId": parent_id,
                            "current_order.id": current_order.get("id")
                            if current_order
                            else None,
                        },
                    )
                    raise RuntimeError()
                parent_return[RETURN_LINE_ITEMS_KEY].append(record)

            elif "gid://shopify/ExchangeLineItem/" in id:
                parent_id = record.get("__parentId", "")
                parent_return = find_parent_return(parent_id)
                if not parent_return:
                    log.error(
                        "Could not find parent return for exchange line item.",
                        {
                            "exchangeLineItem.id": id,
                            "exchangeLineItem.__parentId": parent_id,
                            "current_order.id": current_order.get("id")
                            if current_order
                            else None,
                        },
                    )
                    raise RuntimeError()
                parent_return[EXCHANGE_LINE_ITEMS_KEY].append(record)

            elif "gid://shopify/Refund/" in id:
                parent_id = record.get("__parentId", "")
                parent_return = find_parent_return(parent_id)
                if not parent_return:
                    log.error(
                        "Could not find parent return for refund.",
                        {
                            "refund.id": id,
                            "refund.__parentId": parent_id,
                            "current_order.id": current_order.get("id")
                            if current_order
                            else None,
                        },
                    )
                    raise RuntimeError()
                parent_return[REFUNDS_KEY].append(record)

            elif "gid://shopify/Return/" in id:
                if not current_order:
                    log.error("Found a return before finding an order.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_order.get("id", ""):
                    log.error(
                        "Return's parent id does not match the current order's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "return.id": id,
                            "return.__parentId": record.get("__parentId"),
                            "current_order.id": current_order.get("id"),
                        },
                    )
                    raise RuntimeError()

                record[RETURN_LINE_ITEMS_KEY] = []
                record[EXCHANGE_LINE_ITEMS_KEY] = []
                record[REFUNDS_KEY] = []
                current_order[RETURNS_KEY].append(record)

            else:
                log.error("Unidentified line in JSONL response.", record)
                raise RuntimeError()

        if current_order:
            yield current_order
