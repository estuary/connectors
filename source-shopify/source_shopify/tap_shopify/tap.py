"""tap_shopify tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# Import stream types
from tap_shopify.streams import (
    AbandonedCheckouts,
    CollectStream,
    CustomCollections,
    CustomersStream,
    InventoryItemsStream,
    InventoryLevelsStream,
    LocationsStream,
    MetafieldsStream,
    OrdersStream,
    ProductsStream,
    TransactionsStream,
    UsersStream,
)

STREAM_TYPES = [
    AbandonedCheckouts,
    CollectStream,
    CustomCollections,
    CustomersStream,
    InventoryItemsStream,
    InventoryLevelsStream,
    LocationsStream,
    MetafieldsStream,
    OrdersStream,
    ProductsStream,
    TransactionsStream,
]


class Tap_Shopify(Tap):
    """tap_shopify tap class."""

    name = "tap-shopify"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            description="The access token to authenticate with the Shopify API",
        ),
        th.Property(
            "store",
            th.StringType,
            required=True,
            description=(
                "Shopify store id, use the prefix of your admin url "
                + "e.g. https://[your store].myshopify.com/admin"
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "admin_url",
            th.StringType,
            description=(
                "The Admin url for your Shopify store " + "(overrides 'store' property)"
            ),
        ),
        th.Property(
            "is_plus_account",
            th.BooleanType,
            description=("Enabled Shopify plus account endpoints.)"),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        if self.config.get("is_plus_account"):
            STREAM_TYPES.append(UsersStream)

        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
