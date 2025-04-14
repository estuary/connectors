from datetime import datetime
from logging import Logger
import json
from typing import Any, AsyncGenerator

from .common import dt_to_str, round_to_latest_midnight
from ..models import ShopifyGraphQlResource


class Customers(ShopifyGraphQlResource):
    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        lower_bound = dt_to_str(start)
        upper_bound = dt_to_str(round_to_latest_midnight(end))

        query = f"""
        {{
            customers(
                query: "updated_at:>={lower_bound} AND updated_at:<={upper_bound}"
                sortKey: UPDATED_AT
            ) {{
                edges {{
                    node {{
                        id
                        updatedAt
                        createdAt
                        displayName
                        email
                        firstName
                        lastName
                        phone
                        note
                        verifiedEmail
                        state
                        taxExempt
                        taxExemptions
                        locale
                        defaultAddress {{
                            id
                            address1
                            address2
                            city
                            company
                            country
                            countryCodeV2
                            firstName
                            lastName
                            name
                            phone
                            province
                            provinceCode
                            zip
                        }}
                        addresses {{
                            address1
                            address2
                            city
                            company
                            country
                            countryCodeV2
                            firstName
                            id
                            lastName
                            name
                            phone
                            province
                            provinceCode
                            zip
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
                        statistics {{
                            predictedSpendTier
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
        METAFIELDS_KEY = "metafields"

        current_customer = None

        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            id: str = record.get("id", "")

            if "gid://shopify/Customer/" in id:
                if current_customer:
                    yield current_customer

                current_customer = record
                current_customer[METAFIELDS_KEY] = []
                # Convert addresses from object to list if it's not already a list
                if current_customer.get("addresses") and not isinstance(
                    current_customer.get("addresses"), list
                ):
                    current_customer["addresses"] = [current_customer["addresses"]]
            elif "gid://shopify/Metafield/" in id:
                if not current_customer:
                    log.error("Found a metafield before finding a customer.")
                    raise RuntimeError()
                elif record.get("__parentId", "") != current_customer.get("id", ""):
                    log.error(
                        "Metafield's parent id does not match the current customer's id. Check if the JSONL response from Shopify is not ordered correctly.",
                        {
                            "metafield.id": id,
                            "metafield.__parentId": record.get("__parentId"),
                            "current_customer.id": current_customer.get("id"),
                        },
                    )
                    raise RuntimeError()

                current_customer[METAFIELDS_KEY].append(record)

        if current_customer:
            yield current_customer
