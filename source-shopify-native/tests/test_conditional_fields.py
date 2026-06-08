"""
Tests for the scope/plan-gated conditional-fields mechanism: fields that are spliced
into a resource's GraphQL query only when the store's capabilities allow it.
"""

from datetime import datetime, UTC

import pytest

from source_shopify_native.graphql.orders.orders import Orders
from source_shopify_native.models import (
    ConditionalField,
    ShopifyGraphQLResource,
    SortKey,
    StoreCapabilities,
    requires_any_scope,
)

START = datetime(2024, 1, 1, tzinfo=UTC)
END = datetime(2024, 2, 1, tzinfo=UTC)


def test_placeholder_substituted_at_any_depth():
    """The mechanism splices at the placeholder's position, not only the query root."""

    class _Nested(ShopifyGraphQLResource):
        NAME = "nested_test"
        QUERY_ROOT = "things"
        SORT_KEY = SortKey.UPDATED_AT
        QUERY = """
        lineItems {
            edges {
                node {
                    id
                    # {{ scopedField }}
                }
            }
        }
        """
        CONDITIONAL_FIELDS = [
            ConditionalField(
                placeholder="# {{ scopedField }}",
                fields="scopedField { id }",
                is_available=requires_any_scope("some_scope"),
            ),
        ]

        @staticmethod
        def build_query(start, end, first=None, after=None, capabilities=None):
            return _Nested.build_query_with_fragment(
                start, end, first=first, after=after, capabilities=capabilities
            )

    granted = _Nested.build_query(
        START, END, capabilities=StoreCapabilities(scopes=frozenset({"some_scope"}))
    )
    denied = _Nested.build_query(
        START, END, capabilities=StoreCapabilities(scopes=frozenset())
    )

    # When granted, the field appears nested inside lineItems' node, not at the root.
    node_block = granted[granted.index("lineItems") :]
    assert "scopedField { id }" in node_block
    assert "scopedField" not in denied


def test_placeholder_absent_from_query_is_rejected_at_definition():
    """A marker missing from QUERY fails loudly at class-definition time."""
    with pytest.raises(ValueError, match="not present in its QUERY"):

        class _Broken(ShopifyGraphQLResource):
            NAME = "broken_test"
            QUERY_ROOT = "things"
            SORT_KEY = SortKey.UPDATED_AT
            # QUERY deliberately omits the `# {{ ghost }}` marker.
            QUERY = "id"
            CONDITIONAL_FIELDS = [
                ConditionalField(
                    placeholder="# {{ ghost }}",
                    fields="ghost { id }",
                    is_available=requires_any_scope("some_scope"),
                ),
            ]


def test_non_comment_placeholder_is_rejected():
    """A placeholder that isn't a comment can't degrade to a no-op, so it's rejected."""
    with pytest.raises(ValueError, match="must be a GraphQL comment"):
        ConditionalField(
            placeholder="retailLocation",
            fields="retailLocation { id }",
            is_available=requires_any_scope("read_locations"),
        )


def test_predicate_can_gate_on_plan_tier():
    """is_available is an arbitrary predicate, so plan-tier gating works (e.g. #4227)."""
    cond = ConditionalField(
        placeholder="# {{ staffMember }}",
        fields="staffMember { id }",
        is_available=lambda caps: "read_users" in caps.scopes
        and caps.is_plus_or_advanced,
    )

    plus_with_scope = StoreCapabilities(
        scopes=frozenset({"read_users"}), is_plus_or_advanced=True
    )
    basic_with_scope = StoreCapabilities(
        scopes=frozenset({"read_users"}), is_plus_or_advanced=False
    )
    plus_without_scope = StoreCapabilities(
        scopes=frozenset(), is_plus_or_advanced=True
    )

    assert cond.is_available(plus_with_scope)
    assert not cond.is_available(basic_with_scope)
    assert not cond.is_available(plus_without_scope)
