from datetime import datetime
from logging import Logger
from typing import AsyncGenerator

from ..models import ShopifyGraphQLResource, StoreCapabilities


class StaffMembers(ShopifyGraphQLResource):
    """Staff members of a Shopify store.

    StaffMember has no `updatedAt` and the `staffMembers` connection cannot filter
    or sort by date, so there is no usable incremental cursor.

    The `staffMembers` query requires the `read_users` scope and is only available
    on Shopify Plus / Advanced stores or finance embedded apps.
    """

    NAME = "staff_members"
    QUERY_ROOT = "staffMembers"
    SHOULD_USE_BULK_QUERIES = False
    QUALIFYING_SCOPES = {"read_users"}
    REQUIRES_PLUS_OR_ADVANCED_PLAN = True
    QUERY = """
    id
    active
    accountType
    email
    exists
    firstName
    lastName
    name
    initials
    isShopOwner
    locale
    phone
    privateData {
        accountSettingsUrl
        createdAt
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
        # An unfiltered, cursor-paginated query: StaffMember has no updated_at and
        # the staffMembers connection cannot filter or sort by date, so `start`/
        # `end` are unused and the full set is re-fetched each snapshot.
        return f"""
        {{
            staffMembers(
                {f"first: {first}" if first else ""}
                {f'after: "{after}"' if after else ""}
            ) {{
                edges {{
                    node {{
                        {StaffMembers.QUERY}
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
        async for record in StaffMembers._process_result(
            log, lines, "gid://shopify/StaffMember/"
        ):
            yield record
