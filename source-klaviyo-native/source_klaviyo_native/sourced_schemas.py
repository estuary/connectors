from typing import Any

# The union of each schema into the binding's inferred schema keeps a field's
# `required` and `format` only when both have them. Each schema declares just the
# fields every document has: the JSON:API envelope plus `attributes` and
# `relationships`. Fields beneath those are left to inference so the schemas
# don't need updating when Klaviyo's API changes. The cost is that those fields
# aren't required.
#
# The CDK's with_sourced_schema_defaults fills in `_meta`, `type: object`,
# `additionalProperties: False`, `required`, and bounds.

_LINKS: dict[str, Any] = {
    "properties": {
        "self": {"type": "string"},
    },
}

SOURCED_SCHEMAS: dict[str, dict[str, Any]] = {
    "campaigns": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "coupon_codes": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "coupons": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "links": _LINKS,
        },
    },
    "events": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {
                "properties": {
                    # Keys differ by metric, and `$event_id` is the only one every
                    # event has.
                    "event_properties": {
                        "properties": {
                            "$event_id": {"type": "string"},
                        },
                    },
                },
            },
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "flows": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "forms": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "images": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string", "format": "integer"},
            "attributes": {"type": "object"},
            "links": _LINKS,
        },
    },
    "lists": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "metrics": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "profiles": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "push_tokens": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "segments": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "tag_groups": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string", "format": "uuid"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "tags": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string", "format": "uuid"},
            "attributes": {"type": "object"},
            "relationships": {"type": "object"},
            "links": _LINKS,
        },
    },
    "templates": {
        "properties": {
            "type": {"type": "string"},
            "id": {"type": "string"},
            "attributes": {"type": "object"},
            "links": _LINKS,
        },
    },
}
