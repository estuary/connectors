"""
Tests for skipping unavailable stores: a frozen (402), locked (423), or closed (404) store is
skipped at initialization, while any other failure, or every store being unavailable, still
raises. Validation tolerates frozen or locked stores but rejects a 404, since a closed store
and a misspelled store name look the same.
"""

from logging import Logger
from unittest.mock import MagicMock, patch

import pytest
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError

import source_shopify_native.graphql as gql
from source_shopify_native.models import EndpointConfig, StoreCapabilities
from source_shopify_native.resources import (
    StoreContext,
    StoreInitError,
    discovered_resources,
    validate_credentials,
)


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


def _config(*stores: str) -> EndpointConfig:
    return EndpointConfig.model_validate(
        {
            "stores": [
                {
                    "store": store,
                    "credentials": {
                        "credentials_title": "Private App Credentials",
                        "access_token": "tok",
                    },
                }
                for store in stores
            ],
        }
    )


def _store_context(*resources) -> StoreContext:
    return StoreContext(
        http=MagicMock(),
        client=MagicMock(),
        bulk_job_manager=MagicMock(),
        capabilities=StoreCapabilities(scopes=frozenset()),
        available_resources=set(resources),
    )


def _patch_store_init(outcomes: dict[str, StoreContext | BaseException]):
    """Patch per-store initialization to return a context or raise, keyed by store name."""

    async def fake_create_store_context(log, http, store_config, should_cancel_ongoing_job):
        outcome = outcomes[store_config.store]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    return patch(
        "source_shopify_native.resources._create_store_context",
        side_effect=fake_create_store_context,
    )


def _patch_connectivity(codes: dict[str, int]):
    """Patch the connectivity probe to fail with the given HTTP status per store name."""

    async def fake_check_connectivity(self):
        code = codes.get(self.client.store)
        if code is not None:
            raise HTTPError(f"HTTP {code}", code)

    return patch(
        "source_shopify_native.resources.BulkJobManager.check_connectivity",
        new=fake_check_connectivity,
    )


pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("code", [402, 423, 404])
async def test_unavailable_store_is_skipped(log, code):
    outcomes = {
        "healthy": _store_context(gql.Orders),
        "down": HTTPError("Unavailable Shop", code),
    }
    with _patch_store_init(outcomes):
        resources = await discovered_resources(log, MagicMock(), _config("healthy", "down"))

    assert {r.name for r in resources} == {gql.Orders.NAME}
    assert set(resources[0].initial_state.inc.keys()) == {"healthy"}

    log.warning.assert_called_once()
    assert "down" in log.warning.call_args.args[0]


async def test_other_failures_still_raise(log):
    outcomes = {
        "bad-token": HTTPError("Invalid API key", 401),
        "down": HTTPError("Unavailable Shop", 402),
        "healthy": _store_context(gql.Orders),
    }
    with _patch_store_init(outcomes), pytest.raises(StoreInitError) as exc_info:
        await discovered_resources(log, MagicMock(), _config("bad-token", "down", "healthy"))

    assert set(exc_info.value.failed_stores) == {"bad-token"}
    assert exc_info.value.initialized_stores == ["healthy"]


async def test_all_stores_unavailable_raises(log):
    outcomes = {
        "a": HTTPError("Unavailable Shop", 402),
        "b": HTTPError("Not Found", 404),
    }
    with _patch_store_init(outcomes), pytest.raises(StoreInitError) as exc_info:
        await discovered_resources(log, MagicMock(), _config("a", "b"))

    assert set(exc_info.value.failed_stores) == {"a", "b"}
    assert exc_info.value.initialized_stores == []


@pytest.mark.parametrize("code", [402, 423])
async def test_validation_tolerates_a_frozen_or_locked_store(log, code):
    with _patch_connectivity({"down": code}):
        await validate_credentials(log, MagicMock(), _config("healthy", "down"))

    log.warning.assert_called_once()
    assert "down" in log.warning.call_args.args[0]


@pytest.mark.parametrize(
    "code, expected", [(401, "Invalid credentials"), (404, "Store not found")]
)
async def test_validation_rejects_other_failures(log, code, expected):
    with _patch_connectivity({"bad": code}), pytest.raises(ValidationError) as exc_info:
        await validate_credentials(log, MagicMock(), _config("healthy", "bad"))

    assert len(exc_info.value.errors) == 1
    assert "bad" in exc_info.value.errors[0]
    assert expected in exc_info.value.errors[0]
