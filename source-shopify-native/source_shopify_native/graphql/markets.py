from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ..models import NestedConnection, ShopifyGraphQLResource, StoreCapabilities

# Selection for each MarketCatalog under a Market. `markets` is deliberately left out: it points
# back at the Market carrying this catalog, and each catalog's own `markets` connection would
# multiply the query cost for data the parent document already holds.
_MARKET_CATALOG_SELECTION = """
id
title
status
publication {
    id
}
operations {
    id
    processedRowCount
    rowCount {
        count
        exceedsMax
    }
    status
}
"""


class Markets(ShopifyGraphQLResource):
    """Markets of a Shopify store, each carrying its market catalogs inline.

    Market has no `createdAt`/`updatedAt` and the `markets` connection cannot filter or
    sort by date, so there is no usable incremental cursor and the full set is re-fetched
    each snapshot.

    MarketCatalog has no query root of its own that the connector captures, so catalogs are
    embedded in their Market rather than referenced by id.

    Two connections are deliberately left out of the field set for now:
    - `webPresences`, which would need a second inline connection and force `OUTER_PAGE_SIZE`
      down to stay under Shopify's per-query cost ceiling.
    - `conditions.regionsCondition.regions`, because `RegionsCondition` is not a `Node`. The
      nested resolver drains overflow with `node(id:)` re-queries, so a market with more regions
      than the inline page would have no id to page from.
    """

    NAME = "markets"
    QUERY_ROOT = "markets"
    SHOULD_USE_BULK_QUERIES = False
    QUALIFYING_SCOPES = {"read_markets"}
    QUERY = """
    id
    name
    handle
    status
    type
    catalogsCount {
        count
        precision
    }
    currencySettings {
        baseCurrency {
            currencyCode
            currencyName
            enabled
            manualRate
            rateUpdatedAt
        }
        localCurrencies
        roundingEnabled
    }
    priceInclusions {
        inclusiveDutiesPricingStrategy
        inclusiveTaxPricingStrategy
    }
    conditions {
        conditionTypes
    }
    # {{ catalogs }}
    """
    NESTED_CONNECTIONS = [
        NestedConnection(
            parent_path=[],
            parent_typename="Market",
            field_name="catalogs",
            node_selection=_MARKET_CATALOG_SELECTION,
            page_size=5,
            overflow_page_size=100,
        ),
    ]
    # 25 markets x (~5 object points + 5 catalogs x ~4) lands near 625, leaving headroom under
    # Shopify's 1000 point per-query ceiling. MAX_COST_EXCEEDED is fatal, so the margin is
    # deliberate.
    OUTER_PAGE_SIZE = 25

    @staticmethod
    def build_query(
        start: datetime,
        end: datetime,
        first: int | None = None,
        after: str | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> str:
        # An unfiltered, cursor-paginated query: Market has no updated_at and the markets
        # connection cannot filter or sort by date, so `start`/`end` are unused.
        return f"""
        {{
            markets(
                {f"first: {first}" if first else ""}
                {f'after: "{after}"' if after else ""}
            ) {{
                edges {{
                    node {{
                        {Markets._resolve_nested_connections(Markets.QUERY)}
                    }}
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
        """

    @staticmethod
    async def process_result(
        log: Logger, lines: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[dict, None]:
        async for record in Markets._process_result(
            log, lines, "gid://shopify/Market/"
        ):
            yield record
