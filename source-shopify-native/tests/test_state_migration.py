"""
Tests for multi-store support: state migration, key validation, config migration,
and store context injection.
"""

import pytest
from datetime import datetime, timedelta, UTC
from logging import Logger
from unittest.mock import MagicMock, AsyncMock

from estuary_cdk.capture.common import ResourceState, ResourceConfig, CaptureBinding
from estuary_cdk.capture import Task, request
from estuary_cdk.flow import CollectionSpec, ValidationError

from source_shopify_native.models import ConnectorState, EndpointConfig, ShopifyGraphQLResource, StoreValidationContext
from source_shopify_native.resources import (
    _reconcile_connector_state,
    _migrate_flat_to_dict_state,
    _add_missing_store_entries,
)
from source_shopify_native import _validate_binding_keys, _should_use_store_in_key


CURSOR = datetime(2024, 6, 15, tzinfo=UTC)
CUTOFF = datetime(2024, 6, 1, tzinfo=UTC)


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


def _make_binding(state_key: str = "orders") -> CaptureBinding[ResourceConfig]:
    binding = MagicMock(spec=CaptureBinding)
    binding.stateKey = state_key
    return binding


def _make_task() -> Task:
    task = MagicMock(spec=Task)
    task.log = MagicMock()
    task.checkpoint = AsyncMock()
    return task


def _make_initial_state(store_ids: list[str], use_backfill: bool = False) -> ResourceState:
    cutoff = datetime.now(tz=UTC)
    start_date = cutoff - timedelta(days=30)
    if use_backfill:
        return ResourceState(
            inc={sid: ResourceState.Incremental(cursor=cutoff) for sid in store_ids},
            backfill={sid: ResourceState.Backfill(next_page=start_date.isoformat(), cutoff=cutoff) for sid in store_ids},
        )
    return ResourceState(
        inc={sid: ResourceState.Incremental(cursor=start_date) for sid in store_ids},
    )


async def _reconcile(state, store_ids, initial_state, legacy_store_id, state_key="orders"):
    binding = _make_binding(state_key)
    task = _make_task()
    await _reconcile_connector_state(store_ids, binding, state, initial_state, task, legacy_store_id=legacy_store_id)
    return task


class TestReconcileConnectorState:
    pytestmark = pytest.mark.asyncio

    async def test_migrate_flat_incremental(self):
        state = ResourceState(inc=ResourceState.Incremental(cursor=CURSOR))
        task = await _reconcile(state, ["my-store"], _make_initial_state(["my-store"]), "my-store")

        assert isinstance(state.inc, dict)
        assert state.inc["my-store"].cursor == CURSOR
        task.checkpoint.assert_called_once()

    async def test_migrate_flat_with_backfill(self):
        state = ResourceState(
            inc=ResourceState.Incremental(cursor=CURSOR),
            backfill=ResourceState.Backfill(cutoff=CUTOFF, next_page="cursor_abc"),
        )
        task = await _reconcile(state, ["s1"], _make_initial_state(["s1"], use_backfill=True), "s1", state_key="products")

        assert state.inc["s1"].cursor == CURSOR
        assert state.backfill["s1"].cutoff == CUTOFF
        assert state.backfill["s1"].next_page == "cursor_abc"

    async def test_dict_state_unchanged(self):
        state = ResourceState(inc={"my-store": ResourceState.Incremental(cursor=CURSOR)})
        task = await _reconcile(state, ["my-store"], _make_initial_state(["my-store"]), "my-store")

        assert state.inc["my-store"].cursor == CURSOR
        task.checkpoint.assert_not_called()

    async def test_uses_explicit_legacy_store_id(self):
        state = ResourceState(inc=ResourceState.Incremental(cursor=CURSOR))
        store_ids = ["new-primary", "legacy-store", "third-store"]

        await _reconcile(state, store_ids, _make_initial_state(store_ids), "legacy-store")

        assert state.inc["legacy-store"].cursor == CURSOR
        assert "new-primary" in state.inc
        assert "third-store" in state.inc

    async def test_idempotent(self):
        state = ResourceState(inc=ResourceState.Incremental(cursor=CURSOR))

        task1 = await _reconcile(state, ["s1"], _make_initial_state(["s1"]), "s1")
        assert task1.checkpoint.call_count == 1

        task2 = await _reconcile(state, ["s1"], _make_initial_state(["s1"]), "s1")
        task2.checkpoint.assert_not_called()
        assert state.inc["s1"].cursor == CURSOR

    async def test_completed_backfill_converted_to_dict(self):
        state = ResourceState(inc=ResourceState.Incremental(cursor=CURSOR), backfill=None)

        await _reconcile(state, ["s1"], _make_initial_state(["s1"]), "s1")

        assert isinstance(state.backfill, dict)
        assert state.backfill["s1"] is None

    async def test_add_new_store(self):
        state = ResourceState(inc={"existing": ResourceState.Incremental(cursor=CURSOR)})
        store_ids = ["existing", "new-store"]

        task = await _reconcile(state, store_ids, _make_initial_state(store_ids), "existing")

        assert state.inc["existing"].cursor == CURSOR
        assert "new-store" in state.inc
        task.checkpoint.assert_called_once()

    async def test_add_store_with_backfill(self):
        state = ResourceState(
            inc={"store-a": ResourceState.Incremental(cursor=CURSOR)},
            backfill={"store-a": None},
        )
        store_ids = ["store-a", "store-b"]
        task = await _reconcile(state, store_ids, _make_initial_state(store_ids, use_backfill=True), "store-a")

        assert "store-b" in state.inc
        assert state.backfill["store-b"] is not None
        task.checkpoint.assert_called_once()

    async def test_independent_backfill_addition(self):
        """Backfill is added for a store even when its inc already exists."""
        state = ResourceState(
            inc={"a": ResourceState.Incremental(cursor=CURSOR), "b": ResourceState.Incremental(cursor=CURSOR)},
            backfill={"a": ResourceState.Backfill(cutoff=CURSOR, next_page="p1")},
        )

        task = await _reconcile(state, ["a", "b"], _make_initial_state(["a", "b"], use_backfill=True), "a")

        assert state.backfill["b"] is not None
        assert state.backfill["a"].next_page == "p1"
        task.checkpoint.assert_called_once()

    async def test_removed_store_state_preserved(self):
        cursor_b = datetime(2024, 7, 1, tzinfo=UTC)
        state = ResourceState(inc={
            "store-a": ResourceState.Incremental(cursor=CURSOR),
            "store-b": ResourceState.Incremental(cursor=cursor_b),
        })

        task = await _reconcile(state, ["store-a"], _make_initial_state(["store-a"]), "store-a")

        assert state.inc["store-b"].cursor == cursor_b
        task.checkpoint.assert_not_called()

    async def test_error_on_invalid_initial_state(self):
        state = ResourceState(inc=ResourceState.Incremental(cursor=CURSOR))
        invalid = ResourceState(inc=ResourceState.Incremental(cursor=CURSOR))

        with pytest.raises(RuntimeError, match="Invalid initial_state.*orders"):
            binding = _make_binding("orders")
            task = _make_task()
            await _reconcile_connector_state(["s1"], binding, state, invalid, task, legacy_store_id="s1")

    async def test_error_on_none_inc_state(self):
        state = ResourceState(inc=None)

        with pytest.raises(RuntimeError, match="not dict-based"):
            binding = _make_binding("orders")
            task = _make_task()
            await _reconcile_connector_state(["s1"], binding, state, _make_initial_state(["s1"]), task, legacy_store_id="s1")


class TestMigrateFlatToDictState:
    def test_migrates_incremental(self, log):
        state = ResourceState(inc=ResourceState.Incremental(cursor=CURSOR))
        assert _migrate_flat_to_dict_state(state, "s1", log) is True
        assert state.inc["s1"].cursor == CURSOR

    def test_migrates_backfill(self, log):
        state = ResourceState(
            inc=ResourceState.Incremental(cursor=CURSOR),
            backfill=ResourceState.Backfill(cutoff=CUTOFF, next_page="p1"),
        )
        assert _migrate_flat_to_dict_state(state, "s1", log) is True
        assert state.backfill["s1"].cutoff == CUTOFF

    def test_migrates_completed_backfill(self, log):
        state = ResourceState(inc=ResourceState.Incremental(cursor=CURSOR), backfill=None)
        assert _migrate_flat_to_dict_state(state, "s1", log) is True
        assert state.backfill["s1"] is None

    @pytest.mark.parametrize("inc", [
        {"s1": ResourceState.Incremental(cursor=CURSOR)},
        None,
    ], ids=["already_dict", "none"])
    def test_skips_non_flat(self, inc, log):
        state = ResourceState(inc=inc)
        assert _migrate_flat_to_dict_state(state, "s1", log) is False


class TestAddMissingStoreEntries:
    def test_adds_missing(self, log):
        state = ResourceState(inc={"a": ResourceState.Incremental(cursor=CURSOR)})
        assert _add_missing_store_entries(state, ["a", "b"], _make_initial_state(["a", "b"]), log) is True
        assert "b" in state.inc

    def test_noop_when_complete(self, log):
        state = ResourceState(inc={
            "a": ResourceState.Incremental(cursor=CURSOR),
            "b": ResourceState.Incremental(cursor=CURSOR),
        })
        assert _add_missing_store_entries(state, ["a", "b"], _make_initial_state(["a", "b"]), log) is False

    def test_error_on_non_dict_state(self, log):
        state = ResourceState(inc=ResourceState.Incremental(cursor=CURSOR))
        with pytest.raises(RuntimeError, match="not dict-based.*bug in state migration"):
            _add_missing_store_entries(state, ["s1"], _make_initial_state(["s1"]), log)

    def test_adds_backfill_independently(self, log):
        state = ResourceState(
            inc={"a": ResourceState.Incremental(cursor=CURSOR), "b": ResourceState.Incremental(cursor=CURSOR)},
            backfill={"a": ResourceState.Backfill(cutoff=CURSOR, next_page="p1")},
        )
        assert _add_missing_store_entries(state, ["a", "b"], _make_initial_state(["a", "b"], use_backfill=True), log) is True
        assert state.backfill["b"] is not None

    def test_initializes_backfill_dict_from_none(self, log):
        state = ResourceState(inc={"a": ResourceState.Incremental(cursor=CURSOR)}, backfill=None)
        assert _add_missing_store_entries(state, ["a", "b"], _make_initial_state(["a", "b"], use_backfill=True), log) is True
        assert isinstance(state.backfill, dict)
        assert "b" in state.backfill


class TestStoreValidationContext:
    def test_store_survives_exclude_unset(self):
        doc = ShopifyGraphQLResource.model_validate(
            {"id": "gid://shopify/Order/123"}, context=StoreValidationContext(store="my-store"),
        )
        dumped = doc.model_dump(exclude_unset=True, by_alias=True)
        assert dumped["_meta"]["store"] == "my-store"

    def test_store_in_fields_set(self):
        doc = ShopifyGraphQLResource.model_validate(
            {"id": "gid://shopify/Product/456"}, context=StoreValidationContext(store="test-store"),
        )
        assert doc.meta_.store == "test-store"
        assert "meta_" in doc.model_fields_set

    def test_no_context_leaves_store_none(self):
        assert ShopifyGraphQLResource(id="gid://shopify/Order/789").meta_.store is None

    def test_works_with_model_validate_json(self):
        doc = ShopifyGraphQLResource.model_validate_json(
            '{"id": "gid://shopify/Order/100"}', context=StoreValidationContext(store="json-store"),
        )
        assert doc.meta_.store == "json-store"
        assert doc.model_dump(exclude_unset=True, by_alias=True)["_meta"]["store"] == "json-store"

    def test_existing_meta_preserved(self):
        doc = ShopifyGraphQLResource.model_validate(
            {"id": "gid://shopify/Order/123", "_meta": {"op": "c"}},
            context=StoreValidationContext(store="my-store"),
        )
        assert doc.meta_.store == "my-store"
        assert doc.meta_.op == "c"


def _make_validate_binding(name: str, key: list[str], backfill: int = 0) -> request.ValidateBinding:
    binding = MagicMock(spec=request.ValidateBinding)
    binding.resourceConfig = MagicMock()
    binding.resourceConfig.name = name
    binding.collection = MagicMock(spec=CollectionSpec)
    binding.collection.key = key
    binding.backfill = backfill
    return binding


def _make_validate_request(
    stores: list[str],
    bindings: list[request.ValidateBinding],
    last_bindings: list[request.ValidateBinding] | None = None,
) -> request.Validate:
    config_data = {
        "stores": [{"store": s, "credentials": {"access_token": "tok"}} for s in stores],
    }
    config = EndpointConfig.model_validate(config_data)

    req = MagicMock(spec=request.Validate)
    req.config = config
    req.bindings = bindings

    if last_bindings is not None:
        req.lastCapture = MagicMock()
        req.lastCapture.bindings = last_bindings
    else:
        req.lastCapture = None

    return req


class TestValidateBindingKeys:
    def test_noop_single_store(self):
        req = _make_validate_request(
            ["store-a"],
            [_make_validate_binding("orders", ["/id"])],
            [_make_validate_binding("orders", ["/id"])],
        )
        _validate_binding_keys(req)  # should not raise

    def test_noop_no_last_capture(self):
        req = _make_validate_request(["s1", "s2"], [_make_validate_binding("orders", ["/_meta/store", "/id"])])
        _validate_binding_keys(req)  # should not raise

    def test_noop_already_composite_keys(self):
        prev = [_make_validate_binding("orders", ["/_meta/store", "/id"])]
        req = _make_validate_request(
            ["s1", "s2"],
            [_make_validate_binding("orders", ["/_meta/store", "/id"])],
            prev,
        )
        _validate_binding_keys(req)  # should not raise

    def test_requires_backfill_on_multi_store_transition(self):
        prev = [_make_validate_binding("orders", ["/id"]), _make_validate_binding("products", ["/id"])]
        curr = [_make_validate_binding("orders", ["/id"]), _make_validate_binding("products", ["/id"])]
        req = _make_validate_request(["s1", "s2"], curr, prev)

        with pytest.raises(ValidationError) as exc_info:
            _validate_binding_keys(req)

        assert "orders" in exc_info.value.errors[0]
        assert "products" in exc_info.value.errors[0]

    def test_backfill_acknowledgment_clears_error(self):
        prev = [_make_validate_binding("orders", ["/id"], backfill=0)]
        curr = [_make_validate_binding("orders", ["/id"], backfill=1)]
        req = _make_validate_request(["s1", "s2"], curr, prev)

        _validate_binding_keys(req)  # should not raise

    def test_new_binding_not_flagged(self):
        prev = [_make_validate_binding("orders", ["/id"])]
        curr = [
            _make_validate_binding("orders", ["/id"]),
            _make_validate_binding("new-resource", ["/_meta/store", "/id"]),
        ]
        req = _make_validate_request(["s1", "s2"], curr, prev)

        with pytest.raises(ValidationError) as exc_info:
            _validate_binding_keys(req)

        errors_text = exc_info.value.errors[0]
        assert "orders" in errors_text
        assert "new-resource" not in errors_text


class TestShouldUseStoreInKey:
    def test_true_when_bindings_have_store_key(self):
        bindings = [_make_validate_binding("orders", ["/_meta/store", "/id"])]
        assert _should_use_store_in_key(bindings, 1) is True

    def test_true_when_multi_store(self):
        bindings = [_make_validate_binding("orders", ["/id"])]
        assert _should_use_store_in_key(bindings, 2) is True

    def test_false_for_legacy_single_store(self):
        bindings = [_make_validate_binding("orders", ["/id"])]
        assert _should_use_store_in_key(bindings, 1) is False

    def test_false_when_no_bindings(self):
        assert _should_use_store_in_key(None, 1) is False

    def test_true_prevents_key_regression(self):
        """Going from multi-store back to single should keep composite keys."""
        bindings = [_make_validate_binding("orders", ["/_meta/store", "/id"])]
        assert _should_use_store_in_key(bindings, 1) is True


class TestEndpointConfigMigration:
    def test_legacy_config_migrated(self):
        config = EndpointConfig.model_validate({
            "store": "my-shop",
            "credentials": {"access_token": "tok"},
        })
        assert len(config.stores) == 1
        assert config.stores[0].store == "my-shop"
        assert config._was_migrated is True
        assert config._legacy_store == "my-shop"

    def test_multi_store_config_not_flagged(self):
        config = EndpointConfig.model_validate({
            "stores": [{"store": "s1", "credentials": {"access_token": "t1"}}],
        })
        assert config._was_migrated is False
        assert config._legacy_store is None

    def test_duplicate_stores_rejected(self):
        with pytest.raises(ValueError, match="Duplicate store names"):
            EndpointConfig.model_validate({
                "stores": [
                    {"store": "dup", "credentials": {"access_token": "t1"}},
                    {"store": "dup", "credentials": {"access_token": "t2"}},
                ],
            })
