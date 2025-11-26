from typing import Any

API_VERSION = "v23.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"


# Sourced schema for AdAccount - matches source-facebook-marketing/schemas/ad_account.json
AD_ACCOUNT_SOURCED_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "account_id": {"type": "string"},
        "amount_spent": {"type": "string"},
        "balance": {"type": "string"},
        "business_zip": {"type": "string"},
        "business": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "id": {"type": "string"},
            },
        },
        "disable_reason": {"type": "number"},
        "end_advertiser": {"type": "number"},
        "fb_entity": {"type": "number"},
        "funding_source": {"type": "number"},
        "id": {"type": "string"},
        "is_personal": {"type": "number"},
        "min_campaign_group_spend_cap": {"type": "number"},
        "min_daily_budget": {"type": "number"},
        "owner": {"type": "number"},
        "spend_cap": {"type": "string"},
        "tax_id_status": {"type": "number"},
        "tax_id_type": {"type": "string"},
        "timezone_id": {"type": "number"},
        "timezone_offset_hours_utc": {"type": "number"},
    },
    "required": ["id"],
}


# Sourced schema for AdCreative - matches source-facebook-marketing/schemas/ad_creatives.json
AD_CREATIVE_SOURCED_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "account_id": {"type": "string"},
        "actor_id": {"type": "string"},
        "asset_feed_spec": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "optimization_type": {
                    "oneOf": [
                        {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        {
                            "type": "string",
                        },
                    ]
                },
            },
        },
        "id": {"type": "string"},
        "object_id": {"type": "string"},
        "object_story_spec": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "page_id": {"type": "string"},
                "link_data": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "call_to_action": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "value": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "properties": {
                                        "application": {"type": "string"},
                                    },
                                },
                            },
                        },
                    },
                },
                "video_data": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "call_to_action": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "value": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "properties": {
                                        "application": {"type": "string"},
                                    },
                                },
                            },
                        },
                        "video_id": {"type": "string"},
                    },
                },
            },
        },
        "product_set_id": {"type": "string"},
        "template_url_spec": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "config": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "app_id": {"type": "string"},
                    },
                },
            },
        },
        "video_id": {"type": "string"},
    },
    "required": ["id"],
}
