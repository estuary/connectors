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

# Selection for each region of a Market. Every region is a country: MarketRegionCountry is
# MarketRegion's only type.
_MARKET_REGION_SELECTION = """
id
name
... on MarketRegionCountry {
    code
}
"""


class Markets(ShopifyGraphQLResource):
    """Markets of a Shopify store, each carrying its market catalogs and the countries it covers
    inline.

    Market has no `createdAt`/`updatedAt` and the `markets` connection cannot filter or
    sort by date, so there is no usable incremental cursor and the full set is re-fetched
    each snapshot.

    MarketCatalog has no query root of its own that the connector captures, so catalogs are
    embedded in their Market rather than referenced by id.

    `webPresences` is deliberately left out of the field set for now: it would need another
    inline connection and force `OUTER_PAGE_SIZE` down to stay under Shopify's per-query cost
    ceiling.
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
        regionsCondition {
            # {{ regions }}
        }
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
        NestedConnection(
            parent_path=["conditions", "regionsCondition"],
            parent_typename="RegionsCondition",
            field_name="regions",
            node_selection=_MARKET_REGION_SELECTION,
            # Every region is a country and Shopify has 245 country codes, so one page holds all
            # of a market's regions. That matters because RegionsCondition is not a Node: an
            # overflow would have no id to page from, and the resolver raises instead.
            page_size=250,
        ),
    ]
    # Shopify requests 260 points for a page of 25 markets with both connections inline, leaving
    # headroom under its 1000 point per-query ceiling. MAX_COST_EXCEEDED is fatal, so the margin
    # is deliberate.
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
