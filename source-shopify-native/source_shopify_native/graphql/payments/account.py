from ...models import ShopifyGraphQLResource

# Streams read through shopifyPaymentsAccount need one of these besides their own scope.
SHOPIFY_PAYMENTS_ACCOUNT_SCOPES = {"read_shopify_payments_accounts", "read_shopify_payments"}


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
