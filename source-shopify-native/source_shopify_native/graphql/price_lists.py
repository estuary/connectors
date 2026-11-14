from datetime import datetime

from ..models import ShopifyGraphQLResource, StoreCapabilities


class PriceLists(ShopifyGraphQLResource):
    """Price lists of a Shopify store, each with its currency, its adjustment of the store's
    prices, and the catalog it prices.

    PriceList has no `updatedAt` and the `priceLists` connection cannot filter by date, so there
    is no usable incremental cursor and the full set is re-fetched each snapshot.

    A price list's catalog is a market, company location or app catalog. The connector captures
    none of them in a stream of its own, so the catalog is embedded rather than referenced by id.
    A price list can have no catalog.
    """

    NAME = "price_lists"
    QUERY_ROOT = "priceLists"
    SHOULD_USE_BULK_QUERIES = False
    QUALIFYING_SCOPES = {"read_products"}
    QUERY = """
    id
    name
    currency
    fixedPricesCount
    parent {
        adjustment {
            type
            value
        }
        settings {
            compareAtMode
        }
    }
    catalog {
        __typename
        id
        title
        status
    }
    """

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        # An unfiltered, cursor-paginated query in id order: PriceList has no updated_at and the
        # priceLists connection cannot filter by date, so `start`/`end` are unused.
        return f"""
        {{
            priceLists(
                sortKey: ID
                {f"first: {first}" if first else ""}
                {f'after: "{after}"' if after else ""}
            ) {{
                edges {{
                    node {{
                        {PriceLists.QUERY}
                    }}
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
        """
