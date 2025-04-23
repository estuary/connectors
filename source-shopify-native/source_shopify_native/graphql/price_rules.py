from logging import Logger
import json
from datetime import datetime
from typing import Any, AsyncGenerator

from ..models import ShopifyGraphQlResource


customer_selection_fields = """
fragment CustomerSelectionFields on DiscountCustomerSelection {
    ... on DiscountCustomerAll {
        allCustomers
    }
    ... on DiscountCustomerSegments {
        segments {
            id
        }
    }
    ... on DiscountCustomers {
        customers {
            id
        }
    }
}
"""

customer_gets_fiels = """
fragment CustomerGetsFields on DiscountCustomerGets {
    value {
        ... on DiscountAmount {
            amount {
                amount
                currencyCode
            }
        }
        ... on DiscountPercentage {
            percentage
        }
    }
    items {
        ...ItemsSelectionFields
    }
    appliesOnOneTimePurchase
    appliesOnSubscription
}
"""

codes_fields = """
fragment CodesFields on DiscountRedeemCodeConnection {
    edges {
        node {
            id
            code
        }
    }
}
"""

items_selection_fields = """
fragment ItemsSelectionFields on DiscountItems {
  ... on DiscountCollections {
    collections {
      edges {
        node {
          id
        }
      }
    }
  }
  ... on DiscountProducts {
    products {
      edges {
        node {
          id
        }
      }
    }
    productVariants {
      edges {
        node {
          id
        }
      }
    }
  }
  ... on AllDiscountItems {
    allItems
  }
}
"""


class PriceRules(ShopifyGraphQlResource):
    QUERY = """
    discount {
        __typename
        ... on DiscountCodeBasic {
            title
            summary
            status
            startsAt
            endsAt
            usageLimit
            appliesOncePerCustomer
            customerSelection {
                ...CustomerSelectionFields
            }
            customerGets {
                ...CustomerGetsFields
            }
            combinesWith {
                orderDiscounts
                productDiscounts
                shippingDiscounts
            }
        }

        ... on DiscountCodeBxgy {
            title
            summary
            status
            startsAt
            endsAt
            usageLimit
            appliesOncePerCustomer
            customerSelection {
                ...CustomerSelectionFields
            }
            customerGets {
                ...CustomerGetsFields
            }
        }

        ... on DiscountCodeFreeShipping {
            title
            summary
            status
            startsAt
            endsAt
            usageLimit
            appliesOncePerCustomer
            customerSelection {
                ...CustomerSelectionFields
            }
            destinationSelection {
                ... on DiscountCountries {
                    countries
                    includeRestOfWorld
                }
                ... on DiscountCountryAll {
                    allCountries
                }
            }
        }

        ... on DiscountCodeApp {
            title
            status
            appDiscountType {
                functionId
            }
        }
    }
    """
    FRAGMENTS = [
        customer_selection_fields,
        customer_gets_fiels,
        items_selection_fields,
    ]

    @staticmethod
    def build_query(start: datetime, end: datetime) -> str:
        return PriceRules.build_query_with_fragment(
            "discountNodes",
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
        async for line in lines:
            record: dict[str, Any] = json.loads(line)
            yield record
