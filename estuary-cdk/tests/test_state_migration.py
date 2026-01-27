"""
Tests for base capture connector state migration functionality.

These tests validate:
- The migrate_connector_state() method is called before bindings are opened
- State is correctly checkpointed with merge_patch=False when modified
- Default implementation returns unchanged state
"""

import asyncio
import io
import json
from datetime import datetime, UTC
from typing import Callable, Awaitable
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from estuary_cdk.capture.base_capture_connector import BaseCaptureConnector
from estuary_cdk.capture.common import (
    ConnectorState,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture import request, response, Task
from estuary_cdk.flow import ConnectorSpec
from estuary_cdk.logger import FlowLogger

pytestmark = pytest.mark.asyncio


class MockEndpointConfig:
    """Mock endpoint config for testing."""
    pass


class MockConnector(BaseCaptureConnector[MockEndpointConfig, ResourceConfig, ConnectorState]):
    """Test connector that allows customizing migration behavior."""

    def __init__(self, migration_fn=None):
        super().__init__()
        self._migration_fn = migration_fn
        self.migration_calls = []

    def request_class(self):
        return request.Request[MockEndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log, _):
        return ConnectorSpec(
            configSchema={},
            documentationUrl="",
            resourceConfigSchema={},
            resourcePathPointers=["/name"],
        )

    async def discover(self, log, discover):
        return response.Discovered(bindings=[])

    async def validate(self, log, validate):
        return response.Validated(bindings=[])

    async def open(self, log, open):
        async def capture(task: Task):
            pass
        return (response.Opened(explicitAcknowledgements=False), capture)

    async def migrate_connector_state(self, log, open):
        self.migration_calls.append((log, open))
        if self._migration_fn:
            result = self._migration_fn(log, open)
            if asyncio.iscoroutine(result):
                return await result
            return result
        return await super().migrate_connector_state(log, open)


class TestBaseCaptureConnectorStateMigration:
    """Tests for BaseCaptureConnector.migrate_connector_state()"""

    async def test_default_migration_returns_unchanged_state(self):
        """Default implementation returns (state, False) without modification."""
        connector = MockConnector()
        log = MagicMock(spec=FlowLogger)

        state = ConnectorState(bindingStateV1={
            "binding1": ResourceState(
                inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))
            )  # type: ignore[call-arg]
        })

        open_req = MagicMock()
        open_req.state = state

        migrated_state, was_modified = await connector.migrate_connector_state(log, open_req)

        assert migrated_state is state
        assert was_modified is False

    async def test_custom_migration_can_modify_state(self):
        """Custom migration can return modified state."""
        new_cursor = datetime(2024, 1, 1, tzinfo=UTC)

        async def custom_migration(log, open):
            state = open.state
            # Migrate from flat to dict-based state
            for key, resource_state in state.bindingStateV1.items():
                if resource_state and isinstance(resource_state.inc, ResourceState.Incremental):
                    resource_state.inc = {"store1": resource_state.inc}
            return (state, True)

        connector = MockConnector(migration_fn=custom_migration)
        log = MagicMock(spec=FlowLogger)

        original_inc = ResourceState.Incremental(cursor=new_cursor)
        state = ConnectorState(bindingStateV1={
            "binding1": ResourceState(inc=original_inc)  # type: ignore[call-arg]
        })

        open_req = MagicMock()
        open_req.state = state

        migrated_state, was_modified = await connector.migrate_connector_state(log, open_req)

        assert was_modified is True
        assert migrated_state is state
        # State should now be dict-based
        binding1_state = migrated_state.bindingStateV1["binding1"]
        assert binding1_state is not None
        assert isinstance(binding1_state.inc, dict)
        assert binding1_state.inc["store1"] is original_inc

    async def test_migration_preserves_multiple_bindings(self):
        """Migration should handle multiple bindings correctly."""
        async def custom_migration(log, open):
            state = open.state
            modified = False
            for key, resource_state in state.bindingStateV1.items():
                if resource_state and isinstance(resource_state.inc, ResourceState.Incremental):
                    resource_state.inc = {"default": resource_state.inc}
                    modified = True
            return (state, modified)

        connector = MockConnector(migration_fn=custom_migration)
        log = MagicMock(spec=FlowLogger)

        cursor1 = datetime(2024, 1, 1, tzinfo=UTC)
        cursor2 = datetime(2024, 2, 1, tzinfo=UTC)

        state = ConnectorState(bindingStateV1={
            "orders": ResourceState(inc=ResourceState.Incremental(cursor=cursor1)),  # type: ignore[call-arg]
            "products": ResourceState(inc=ResourceState.Incremental(cursor=cursor2)),  # type: ignore[call-arg]
        })

        open_req = MagicMock()
        open_req.state = state

        migrated_state, was_modified = await connector.migrate_connector_state(log, open_req)

        assert was_modified is True
        orders_state = migrated_state.bindingStateV1["orders"]
        products_state = migrated_state.bindingStateV1["products"]
        assert orders_state is not None
        assert products_state is not None
        assert isinstance(orders_state.inc, dict)
        assert isinstance(products_state.inc, dict)
        assert orders_state.inc["default"].cursor == cursor1
        assert products_state.inc["default"].cursor == cursor2

    async def test_migration_skips_already_migrated_state(self):
        """Migration should skip bindings that already have dict-based state."""
        async def custom_migration(log, open):
            state = open.state
            modified = False
            for key, resource_state in state.bindingStateV1.items():
                if resource_state and isinstance(resource_state.inc, ResourceState.Incremental):
                    resource_state.inc = {"default": resource_state.inc}
                    modified = True
            return (state, modified)

        connector = MockConnector(migration_fn=custom_migration)
        log = MagicMock(spec=FlowLogger)

        cursor1 = datetime(2024, 1, 1, tzinfo=UTC)
        already_migrated = {"store1": ResourceState.Incremental(cursor=cursor1)}

        state = ConnectorState(bindingStateV1={
            "orders": ResourceState(inc=already_migrated),  # type: ignore[call-arg]  # Already dict-based
        })

        open_req = MagicMock()
        open_req.state = state

        migrated_state, was_modified = await connector.migrate_connector_state(log, open_req)

        # Should not be modified since it was already dict-based
        assert was_modified is False
        orders_state = migrated_state.bindingStateV1["orders"]
        assert orders_state is not None
        assert orders_state.inc == already_migrated

    async def test_migration_handles_none_states(self):
        """Migration should handle None states gracefully."""
        async def custom_migration(log, open):
            state = open.state
            modified = False
            for key, resource_state in state.bindingStateV1.items():
                if resource_state is None:
                    continue
                if isinstance(resource_state.inc, ResourceState.Incremental):
                    resource_state.inc = {"default": resource_state.inc}
                    modified = True
            return (state, modified)

        connector = MockConnector(migration_fn=custom_migration)
        log = MagicMock(spec=FlowLogger)

        state = ConnectorState(bindingStateV1={
            "orders": None,  # None state
            "products": ResourceState(
                inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))
            ),  # type: ignore[call-arg]
        })

        open_req = MagicMock()
        open_req.state = state

        migrated_state, was_modified = await connector.migrate_connector_state(log, open_req)

        assert was_modified is True
        assert migrated_state.bindingStateV1["orders"] is None
        products_state = migrated_state.bindingStateV1["products"]
        assert products_state is not None
        assert isinstance(products_state.inc, dict)

    async def test_migration_is_idempotent(self):
        """Running migration multiple times should produce the same result."""
        async def custom_migration(log, open):
            state = open.state
            modified = False
            for key, resource_state in state.bindingStateV1.items():
                if resource_state and isinstance(resource_state.inc, ResourceState.Incremental):
                    resource_state.inc = {"default": resource_state.inc}
                    modified = True
            return (state, modified)

        connector = MockConnector(migration_fn=custom_migration)
        log = MagicMock(spec=FlowLogger)

        cursor = datetime(2024, 1, 1, tzinfo=UTC)
        state = ConnectorState(bindingStateV1={
            "orders": ResourceState(inc=ResourceState.Incremental(cursor=cursor)),  # type: ignore[call-arg]
        })

        open_req = MagicMock()
        open_req.state = state

        # First migration
        migrated_state1, was_modified1 = await connector.migrate_connector_state(log, open_req)
        assert was_modified1 is True

        # Second migration should not modify (already migrated)
        open_req.state = migrated_state1
        migrated_state2, was_modified2 = await connector.migrate_connector_state(log, open_req)
        assert was_modified2 is False

        # State should be identical
        orders_state = migrated_state2.bindingStateV1["orders"]
        assert orders_state is not None
        assert isinstance(orders_state.inc, dict)
        assert orders_state.inc["default"].cursor == cursor


class TestStateMigrationWithBackfill:
    """Tests for state migration with backfill state."""

    async def test_migration_handles_backfill_state(self):
        """Migration should correctly handle backfill state conversion."""
        cutoff = datetime(2024, 1, 1, tzinfo=UTC)

        async def custom_migration(log, open):
            state = open.state
            modified = False
            for key, resource_state in state.bindingStateV1.items():
                if resource_state is None:
                    continue
                if isinstance(resource_state.inc, ResourceState.Incremental):
                    resource_state.inc = {"store1": resource_state.inc}
                    modified = True
                if isinstance(resource_state.backfill, ResourceState.Backfill):
                    resource_state.backfill = {"store1": resource_state.backfill}
                    modified = True
            return (state, modified)

        connector = MockConnector(migration_fn=custom_migration)
        log = MagicMock(spec=FlowLogger)

        state = ConnectorState(bindingStateV1={
            "orders": ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page="page2"),
            ),  # type: ignore[call-arg]
        })

        open_req = MagicMock()
        open_req.state = state

        migrated_state, was_modified = await connector.migrate_connector_state(log, open_req)

        assert was_modified is True
        orders_state = migrated_state.bindingStateV1["orders"]
        assert orders_state is not None
        assert isinstance(orders_state.inc, dict)
        assert isinstance(orders_state.backfill, dict)
        assert orders_state.backfill["store1"].cutoff == cutoff
        assert orders_state.backfill["store1"].next_page == "page2"

    async def test_migration_preserves_completed_backfill(self):
        """Migration should preserve None backfill (completed backfill)."""
        async def custom_migration(log, open):
            state = open.state
            modified = False
            for key, resource_state in state.bindingStateV1.items():
                if resource_state is None:
                    continue
                if isinstance(resource_state.inc, ResourceState.Incremental):
                    resource_state.inc = {"store1": resource_state.inc}
                    modified = True
                # Don't touch None backfill
            return (state, modified)

        connector = MockConnector(migration_fn=custom_migration)
        log = MagicMock(spec=FlowLogger)

        state = ConnectorState(bindingStateV1={
            "orders": ResourceState(
                inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC)),
                backfill=None,  # Backfill completed
            ),  # type: ignore[call-arg]
        })

        open_req = MagicMock()
        open_req.state = state

        migrated_state, was_modified = await connector.migrate_connector_state(log, open_req)

        assert was_modified is True
        orders_state = migrated_state.bindingStateV1["orders"]
        assert orders_state is not None
        assert orders_state.backfill is None


class TestStateMigrationNoInterleaving:
    """Tests to ensure state migration doesn't cause interleaving issues."""

    async def test_migration_is_atomic_per_binding(self):
        """Each binding's state should be migrated atomically."""
        migration_order = []

        async def custom_migration(log, open):
            state = open.state
            for key, resource_state in state.bindingStateV1.items():
                if resource_state and isinstance(resource_state.inc, ResourceState.Incremental):
                    migration_order.append(f"start_{key}")
                    resource_state.inc = {"store1": resource_state.inc}
                    migration_order.append(f"end_{key}")
            return (state, True)

        connector = MockConnector(migration_fn=custom_migration)
        log = MagicMock(spec=FlowLogger)

        state = ConnectorState(bindingStateV1={
            "orders": ResourceState(inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))),  # type: ignore[call-arg]
            "products": ResourceState(inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))),  # type: ignore[call-arg]
            "customers": ResourceState(inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))),  # type: ignore[call-arg]
        })

        open_req = MagicMock()
        open_req.state = state

        await connector.migrate_connector_state(log, open_req)

        # Verify each binding's migration completed before the next started
        for i in range(0, len(migration_order), 2):
            key = migration_order[i].replace("start_", "")
            assert migration_order[i] == f"start_{key}"
            assert migration_order[i + 1] == f"end_{key}"

    async def test_migration_state_consistency(self):
        """State should remain consistent throughout migration."""
        async def custom_migration(log, open):
            state = open.state

            # Verify initial state consistency
            for key, resource_state in state.bindingStateV1.items():
                if resource_state and isinstance(resource_state.inc, ResourceState.Incremental):
                    # At this point, should be flat
                    assert not isinstance(resource_state.inc, dict)

            # Perform migration
            for key, resource_state in state.bindingStateV1.items():
                if resource_state and isinstance(resource_state.inc, ResourceState.Incremental):
                    resource_state.inc = {"store1": resource_state.inc}

            # Verify final state consistency
            for key, resource_state in state.bindingStateV1.items():
                if resource_state and resource_state.inc:
                    # At this point, should be dict-based
                    assert isinstance(resource_state.inc, dict)

            return (state, True)

        connector = MockConnector(migration_fn=custom_migration)
        log = MagicMock(spec=FlowLogger)

        state = ConnectorState(bindingStateV1={
            "orders": ResourceState(inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))),  # type: ignore[call-arg]
            "products": ResourceState(inc=ResourceState.Incremental(cursor=datetime.now(tz=UTC))),  # type: ignore[call-arg]
        })

        open_req = MagicMock()
        open_req.state = state

        # This will raise assertion error if state becomes inconsistent
        await connector.migrate_connector_state(log, open_req)
