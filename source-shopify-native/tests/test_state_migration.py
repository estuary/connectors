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

from source_shopify_native.models import ConnectorState, EndpointConfig, ShopifyClientCredentials, ShopifyGraphQLResource, StoreValidationContext
from source_shopify_native.resources import (
    _reconcile_connector_state,
    _migrate_flat_to_dict_state,
)
from source_shopify_native import _validate_config


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

    def test_no_context_leaves_store_empty(self):
        assert ShopifyGraphQLResource(id="gid://shopify/Order/789").meta_.store == ""

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
    should_use_composite_key: bool = True,
    prev_should_use_composite_key: bool | None = None,
) -> request.Validate:
    config_data = {
        "stores": [{"store": s, "credentials": {"credentials_title": "Private App Credentials", "access_token": "tok"}} for s in stores],
        "advanced": {"should_use_composite_key": should_use_composite_key},
    }
    config = EndpointConfig.model_validate(config_data)

    req = MagicMock(spec=request.Validate)
    req.config = config
    req.bindings = bindings

    if last_bindings is not None:
        # Default: previous flag matches current (no toggle) unless explicitly set.
        prev_flag = prev_should_use_composite_key if prev_should_use_composite_key is not None else should_use_composite_key
        prev_config_data = {
            "stores": [{"store": s, "credentials": {"credentials_title": "Private App Credentials", "access_token": "tok"}} for s in stores],
            "advanced": {"should_use_composite_key": prev_flag},
        }
        req.lastCapture = MagicMock()
        req.lastCapture.bindings = last_bindings
        req.lastCapture.config.config = prev_config_data
    else:
        req.lastCapture = None

    return req


class TestValidateConfig:
    def test_single_store_flag_false_ok(self):
        """Legacy capture with one store and composite keys disabled — no error."""
        req = _make_validate_request(
            ["store-a"],
            [_make_validate_binding("orders", ["/id"])],
            [_make_validate_binding("orders", ["/id"])],
            should_use_composite_key=False,
        )
        _validate_config(req)  # should not raise

    def test_multi_store_flag_true_ok(self):
        """New capture with multiple stores and composite keys enabled — no error."""
        req = _make_validate_request(
            ["s1", "s2"],
            [_make_validate_binding("orders", ["/_meta/store", "/id"])],
        )
        _validate_config(req)  # should not raise

    def test_multi_store_flag_false_raises(self):
        """Legacy capture trying to add stores without enabling composite keys — error."""
        req = _make_validate_request(
            ["s1", "s2"],
            [_make_validate_binding("orders", ["/id"])],
            should_use_composite_key=False,
        )
        with pytest.raises(ValidationError) as exc_info:
            _validate_config(req)

        assert "composite keys" in exc_info.value.errors[0].lower()

    def test_single_store_flag_true_ok(self):
        """Single store with composite keys enabled (new capture default) — no error."""
        req = _make_validate_request(
            ["store-a"],
            [_make_validate_binding("orders", ["/_meta/store", "/id"])],
        )
        _validate_config(req)  # should not raise

    def test_enabling_composite_keys_requires_backfill(self):
        """Toggling composite keys on with legacy non-composite bindings requires dataflow reset."""
        prev = [_make_validate_binding("orders", ["/id"]), _make_validate_binding("products", ["/id"])]
        curr = [_make_validate_binding("orders", ["/id"]), _make_validate_binding("products", ["/id"])]
        req = _make_validate_request(["s1"], curr, prev, should_use_composite_key=True)

        with pytest.raises(ValidationError) as exc_info:
            _validate_config(req)

        assert "orders" in exc_info.value.errors[0]
        assert "products" in exc_info.value.errors[0]

    def test_enabling_composite_keys_with_backfill_ok(self):
        """Toggling composite keys on with backfill incremented — no error."""
        prev = [_make_validate_binding("orders", ["/id"], backfill=0)]
        curr = [_make_validate_binding("orders", ["/id"], backfill=1)]
        req = _make_validate_request(["s1"], curr, prev, should_use_composite_key=True)

        _validate_config(req)  # should not raise

    def test_new_binding_not_flagged_for_backfill(self):
        """New bindings that weren't in lastCapture don't need backfill."""
        prev = [_make_validate_binding("orders", ["/id"])]
        curr = [
            _make_validate_binding("orders", ["/id"]),
            _make_validate_binding("new-resource", ["/_meta/store", "/id"]),
        ]
        req = _make_validate_request(["s1"], curr, prev, should_use_composite_key=True)

        with pytest.raises(ValidationError) as exc_info:
            _validate_config(req)

        errors_text = exc_info.value.errors[0]
        assert "orders" in errors_text
        assert "new-resource" not in errors_text

    def test_already_composite_bindings_no_backfill_needed(self):
        """When all previous bindings already have composite keys, no backfill needed."""
        prev = [_make_validate_binding("orders", ["/_meta/store", "/id"])]
        curr = [_make_validate_binding("orders", ["/_meta/store", "/id"])]
        req = _make_validate_request(["s1", "s2"], curr, prev, should_use_composite_key=True)

        _validate_config(req)  # should not raise

    def test_toggling_flag_requires_rediscovery(self):
        """Toggling the composite key flag requires re-discovery before publishing."""
        prev = [_make_validate_binding("orders", ["/id"])]
        curr = [_make_validate_binding("orders", ["/id"])]
        req = _make_validate_request(
            ["s1"], curr, prev,
            should_use_composite_key=True,
            prev_should_use_composite_key=False,
        )

        with pytest.raises(ValidationError) as exc_info:
            _validate_config(req)

        assert "discover" in exc_info.value.errors[0].lower()

    def test_toggling_flag_off_requires_rediscovery(self):
        """Toggling composite keys off also requires re-discovery."""
        prev = [_make_validate_binding("orders", ["/_meta/store", "/id"])]
        curr = [_make_validate_binding("orders", ["/_meta/store", "/id"])]
        req = _make_validate_request(
            ["s1"], curr, prev,
            should_use_composite_key=False,
            prev_should_use_composite_key=True,
        )

        with pytest.raises(ValidationError) as exc_info:
            _validate_config(req)

        assert "discover" in exc_info.value.errors[0].lower()

    def test_toggling_flag_on_after_rediscovery_requires_backfill(self):
        """Flag toggled on and user already re-discovered, but backfill not incremented."""
        prev = [_make_validate_binding("orders", ["/id"])]
        curr = [_make_validate_binding("orders", ["/_meta/store", "/id"])]
        req = _make_validate_request(
            ["s1"], curr, prev,
            should_use_composite_key=True,
            prev_should_use_composite_key=False,
        )

        with pytest.raises(ValidationError) as exc_info:
            _validate_config(req)

        assert "backfill" in exc_info.value.errors[0].lower()
        assert "orders" in exc_info.value.errors[0]

    def test_toggling_flag_on_after_rediscovery_with_backfill_ok(self):
        """Flag toggled on, user re-discovered, and backfill incremented — no error."""
        prev = [_make_validate_binding("orders", ["/id"], backfill=0)]
        curr = [_make_validate_binding("orders", ["/_meta/store", "/id"], backfill=1)]
        req = _make_validate_request(
            ["s1"], curr, prev,
            should_use_composite_key=True,
            prev_should_use_composite_key=False,
        )

        _validate_config(req)  # should not raise

    def test_toggling_flag_off_after_rediscovery_ok(self):
        """Flag toggled off and user already re-discovered — no error."""
        prev = [_make_validate_binding("orders", ["/_meta/store", "/id"])]
        curr = [_make_validate_binding("orders", ["/id"])]
        req = _make_validate_request(
            ["s1"], curr, prev,
            should_use_composite_key=False,
            prev_should_use_composite_key=True,
        )

        _validate_config(req)  # should not raise

    def test_prev_config_with_sops_encrypted_credentials(self):
        """Previous config may have SOPS-encrypted fields (client_id_sops instead of client_id).
        _validate_config should not fail when parsing the previous config."""
        req = MagicMock(spec=request.Validate)
        req.config = EndpointConfig.model_validate({
            "stores": [{"store": "s1", "credentials": {"credentials_title": "Private App Credentials", "access_token": "tok"}}],
            "advanced": {"should_use_composite_key": True},
        })
        req.bindings = [_make_validate_binding("orders", ["/_meta/store", "/id"])]

        req.lastCapture = MagicMock()
        req.lastCapture.bindings = [_make_validate_binding("orders", ["/_meta/store", "/id"])]
        # Simulate SOPS-encrypted previous config: client_id_sops instead of client_id.
        req.lastCapture.config.config = {
            "stores": [{
                "store": "s1",
                "credentials": {
                    "credentials_title": "Client Credentials",
                    "client_id_sops": "ENC[AES256_GCM,...]",
                    "client_secret_sops": "ENC[AES256_GCM,...]",
                },
            }],
            "advanced": {"should_use_composite_key": True},
        }

        _validate_config(req)  # should not raise

    def test_prev_config_legacy_flat_with_sops(self):
        """Legacy flat config with SOPS-encrypted credentials should be treated as composite=False."""
        req = MagicMock(spec=request.Validate)
        req.config = EndpointConfig.model_validate({
            "stores": [{"store": "s1", "credentials": {"credentials_title": "Private App Credentials", "access_token": "tok"}}],
            "advanced": {"should_use_composite_key": False},
        })
        req.bindings = [_make_validate_binding("orders", ["/id"])]

        req.lastCapture = MagicMock()
        req.lastCapture.bindings = [_make_validate_binding("orders", ["/id"])]
        # Legacy flat config with SOPS fields — should infer composite=False.
        req.lastCapture.config.config = {
            "store": "s1",
            "credentials": {
                "credentials_title": "Client Credentials",
                "client_id_sops": "ENC[AES256_GCM,...]",
                "client_secret_sops": "ENC[AES256_GCM,...]",
            },
        }

        _validate_config(req)  # should not raise (no toggle, both False)


class TestEndpointConfigMigration:
    def test_legacy_config_migrated(self):
        config = EndpointConfig.model_validate({
            "store": "my-shop",
            "credentials": {"credentials_title": "Private App Credentials", "access_token": "tok"},
        })
        assert len(config.stores) == 1
        assert config.stores[0].store == "my-shop"
        assert config._was_migrated is True
        assert config._legacy_store == "my-shop"
        assert config.advanced.should_use_composite_key is False

    def test_multi_store_config_not_flagged(self):
        config = EndpointConfig.model_validate({
            "stores": [{"store": "s1", "credentials": {"credentials_title": "Private App Credentials", "access_token": "t1"}}],
        })
        assert config._was_migrated is False
        assert config._legacy_store is None
        assert config.advanced.should_use_composite_key is False

    def test_duplicate_stores_rejected(self):
        with pytest.raises(ValueError, match="Duplicate store names"):
            EndpointConfig.model_validate({
                "stores": [
                    {"store": "dup", "credentials": {"credentials_title": "Private App Credentials", "access_token": "t1"}},
                    {"store": "dup", "credentials": {"credentials_title": "Private App Credentials", "access_token": "t2"}},
                ],
            })


class TestClientCredentialsConfig:
    def test_client_credentials_parsed(self):
        config = EndpointConfig.model_validate({
            "stores": [{
                "store": "my-shop",
                "credentials": {
                    "credentials_title": "Client Credentials",
                    "client_id": "app-id",
                    "client_secret": "app-secret",
                },
            }],
        })
        assert isinstance(config.stores[0].credentials, ShopifyClientCredentials)
        assert config.stores[0].credentials.client_id == "app-id"
        assert config.stores[0].credentials.client_secret == "app-secret"

    def test_access_token_still_works(self):
        config = EndpointConfig.model_validate({
            "stores": [{
                "store": "my-shop",
                "credentials": {"credentials_title": "Private App Credentials", "access_token": "shpat_xxx"},
            }],
        })
        from estuary_cdk.flow import AccessToken
        assert isinstance(config.stores[0].credentials, AccessToken)

    def test_legacy_flat_config_with_client_credentials(self):
        config = EndpointConfig.model_validate({
            "store": "my-shop",
            "credentials": {
                "credentials_title": "Client Credentials",
                "client_id": "app-id",
                "client_secret": "app-secret",
            },
        })
        assert len(config.stores) == 1
        assert isinstance(config.stores[0].credentials, ShopifyClientCredentials)
        assert config._was_migrated is True
        assert config.advanced.should_use_composite_key is False
