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
    RefundsStream,
    DiscountCode,
    PriceRuleStream,
    CarrierServicesStream,
    OrderRiskStream,
)

# Commented stream types are gated behind our shopify app being granted protected data permissions
STREAM_TYPES = [
    # AbandonedCheckouts,
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
    RefundsStream,
    DiscountCode,
    PriceRuleStream,
    CarrierServicesStream,
    OrderRiskStream,
]


class Tap_Shopify(Tap):
    """tap_shopify tap class."""

    name = "tap-shopify"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "credentials",
            th.CustomType(
                {
                    "type": "object",
                    "description": "Credentials for connecting to the Shopify API",
                    "discriminator": {
                        "propertyName": "auth_type"
                    },
                    "oneOf": [
                        {
                            "type": "object",
                            "title": "Shopify OAuth",
                            "x-oauth2-provider": "shopify",
                            "properties": {
                                "auth_type": {
                                    "type": "string",
                                    "const": "oauth",
                                    "description": "Discriminator for object of type 'oauth'."
                                },
                                "client_id": {
                                    "type": "string",
                                    "secret": True
                                },
                                "client_secret": {
                                    "type": "string",
                                    "secret": True
                                },
                                "access_token": {
                                    "type": "string",
                                    "secret": True
                                }
                            },
                            "required": [
                                "auth_type",
                                "client_id",
                                "client_secret",
                                "access_token"
                            ]
                        },
                        {
                            "type": "object",
                            "title": "Access Token",
                            "properties": {
                                "auth_type": {
                                    "type": "string",
                                    "const": "access_token",
                                    "description": "Discriminator for object of type 'access_token'."
                                },
                                "access_token": {
                                    "type": ["string"],
                                    "description": "The access token to authenticate with the Shopify API",
                                    "secret": True
                                }
                            },
                            "required": [
                                "auth_type",
                                "access_token"
                            ]
                        }
                    ]
                },
            ),
            required=True
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
                "The Admin url for your Shopify store " +
                "(overrides 'store' property)"
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
