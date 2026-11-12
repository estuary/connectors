from pydantic import BaseModel

from ...models import ShopifyGraphQLResource

# Streams read through shopifyPaymentsAccount need one of these besides their own scope.
SHOPIFY_PAYMENTS_ACCOUNT_SCOPES = {"read_shopify_payments_accounts", "read_shopify_payments"}


class BusinessEntities(BaseModel, extra="allow"):
    """The shop's business entities, each with its own Shopify Payments account if it has
    one. Shopify doesn't say whose account the shop-level shopifyPaymentsAccount is when
    there are several.
    """

    class BusinessEntity(BaseModel, extra="forbid"):
        class ShopifyPaymentsAccount(BaseModel, extra="forbid"):
            id: str

        id: str
        shopifyPaymentsAccount: ShopifyPaymentsAccount | None

    businessEntities: list[BusinessEntity]

    @staticmethod
    def query() -> str:
        return """
        {
            businessEntities {
                id
                shopifyPaymentsAccount {
                    id
                }
            }
        }
        """


def build_account_query(
    model: type[ShopifyGraphQLResource],
    search: str,
    first: int | None,
    after: str | None,
) -> str:
    """Build a query for one page of `model`'s connection under shopifyPaymentsAccount.

    These connections aren't top-level and aren't filtered on updated_at, so
    build_query_with_fragment doesn't fit them.
    """
    query = f"""
    {{
        {model.QUERY_ROOT_PARENT} {{
            {model.QUERY_ROOT}(
                query: "{search}"
                {f"first: {first}" if first else ""}
                {f'after: "{after}"' if after else ""}
            ) {{
                edges {{
                    node {{
                        {model.QUERY}
                    }}
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
    }}
    """

    for fragment in model.FRAGMENTS:
        query += fragment

    return query
