"""Unit tests for resources.py: describe error handling, incremental-vs-snapshot
dispatch, resource shapes, and the all_resources/enabled_resources split.
"""

import logging
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from estuary_cdk.capture import common
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError
from estuary_cdk.incremental_csv_processor import BaseCSVRow

from source_zuora import resources
from source_zuora.models import (
    EndpointConfig,
    TransactionDateDocument,
    UpdatedDateDocument,
)

_LOG = logging.getLogger(__name__)


def _config() -> EndpointConfig:
    return EndpointConfig.model_validate(
        {
            "credentials": {"client_id": "x", "client_secret": "y"},
            "base_url": "https://rest.zuora.com",
            "start_date": "2020-01-01T00:00:00Z",
        }
    )


def _binding(name: str) -> SimpleNamespace:
    return SimpleNamespace(resourceConfig=SimpleNamespace(name=name))


# --- _describe_one -------------------------------------------------------------


@pytest.mark.asyncio
async def test_describe_one_success_returns_described_object():
    async def fake(base_url, http, log, name):
        return ["Id", "UpdatedDate"]

    with patch("source_zuora.resources.fetch_object_fields", fake):
        result = await resources._describe_object(
            "Account", "u", AsyncMock(), _LOG
        )
    assert result is not None
    assert result.name == "Account" and result.fields == ["Id", "UpdatedDate"]


@pytest.mark.asyncio
async def test_describe_one_http_error_is_skipped():
    async def fake(base_url, http, log, name):
        raise HTTPError("nope", 500)

    with patch("source_zuora.resources.fetch_object_fields", fake):
        assert await resources._describe_object(
            "Account", "u", AsyncMock(), _LOG
        ) is None


@pytest.mark.asyncio
async def test_describe_one_generic_error_is_skipped():
    async def fake(base_url, http, log, name):
        raise ValueError("bad xml")

    with patch("source_zuora.resources.fetch_object_fields", fake):
        assert await resources._describe_object(
            "Account", "u", AsyncMock(), _LOG
        ) is None


@pytest.mark.asyncio
async def test_describe_one_no_exportable_fields_is_skipped():
    async def fake(base_url, http, log, name):
        return []

    with patch("source_zuora.resources.fetch_object_fields", fake):
        assert await resources._describe_object(
            "Account", "u", AsyncMock(), _LOG
        ) is None


# --- resource builders ---------------------------------------------------------


def test_incremental_resource_shape_and_boundary_ownership():
    start = datetime(2020, 1, 1, tzinfo=UTC)
    cutoff = datetime(2024, 1, 1, tzinfo=UTC)
    r = resources._incremental_resource(
        "Account", ["Id", "UpdatedDate"], UpdatedDateDocument, object(), start, cutoff
    )
    assert r.key == ["/Id"]
    assert r.model is UpdatedDateDocument
    assert r.schema_inference is True
    # Backfill covers [start, cutoff); incremental resumes exactly at cutoff, so
    # the boundary instant is owned by exactly one side (no gap / no overlap).
    assert r.initial_state.backfill.cutoff == cutoff
    assert r.initial_state.backfill.next_page is None
    assert r.initial_state.inc.cursor == cutoff


def test_snapshot_resource_shape():
    r = resources._snapshot_resource("Product", ["Id", "Name"], object())
    assert isinstance(r, common.SnapshotResource)
    assert r.key == ["/_meta/row_id"]
    assert r.model is BaseCSVRow
    assert r.schema_inference is True


# --- all_resources / enabled_resources dispatch --------------------------------


@pytest.mark.asyncio
async def test_all_resources_enumerates_catalog_and_dispatches_by_updated_date():
    async def fake_describe(base_url, http, log, name):
        # Account is incremental (has UpdatedDate); Product is a snapshot.
        return ["Id", "UpdatedDate"] if name == "Account" else ["Id"]

    async def fake_catalog(base_url, http, log):
        return ["Account", "Product"]

    with patch("source_zuora.resources.fetch_object_fields", fake_describe), patch(
        "source_zuora.resources.discover_object_names", fake_catalog
    ), patch.object(resources, "_attach_token_source"):
        res = await resources.all_resources(_LOG, AsyncMock(), _config())

    by_name = {r.name: r for r in res}
    assert by_name["Account"].key == ["/Id"]  # incremental
    assert by_name["Product"].key == ["/_meta/row_id"]  # snapshot


@pytest.mark.asyncio
async def test_all_resources_classifies_by_cursor_field_priority():
    # An object with only TransactionDate cursors on it (incremental); one with
    # neither UpdatedDate nor TransactionDate is a snapshot; and when both cursor
    # fields are present UpdatedDate wins (it's the true update timestamp).
    fields_by_object = {
        "PaymentTransactionLog": ["Id", "TransactionDate"],
        "Product": ["Id", "Name"],
        "Invoice": ["Id", "UpdatedDate", "TransactionDate"],
    }

    async def fake_describe(base_url, http, log, name):
        return fields_by_object[name]

    async def fake_catalog(base_url, http, log):
        return list(fields_by_object)

    with patch("source_zuora.resources.fetch_object_fields", fake_describe), patch(
        "source_zuora.resources.discover_object_names", fake_catalog
    ), patch.object(resources, "_attach_token_source"):
        res = await resources.all_resources(_LOG, AsyncMock(), _config())

    by_name = {r.name: r for r in res}
    assert by_name["PaymentTransactionLog"].model is TransactionDateDocument
    assert by_name["Product"].key == ["/_meta/row_id"]  # snapshot
    assert by_name["Invoice"].model is UpdatedDateDocument  # UpdatedDate wins


@pytest.mark.asyncio
async def test_enabled_resources_describes_only_bound_objects():
    described: list[str] = []
    catalog_calls = {"n": 0}

    async def fake_describe(base_url, http, log, name):
        described.append(name)
        return ["Id", "UpdatedDate"]

    async def fake_catalog(base_url, http, log):
        catalog_calls["n"] += 1
        return ["A", "B", "C"]

    with patch("source_zuora.resources.fetch_object_fields", fake_describe), patch(
        "source_zuora.resources.discover_object_names", fake_catalog
    ), patch.object(resources, "_attach_token_source"):
        await resources.enabled_resources(
            _LOG, AsyncMock(), _config(), [_binding("A"), _binding("C")]
        )

    assert sorted(described) == ["A", "C"]
    assert catalog_calls["n"] == 0  # catalog is skipped when bindings are known


# --- validate_credentials error mapping ----------------------------------------


@pytest.mark.asyncio
async def test_validate_credentials_success():
    async def ok(base_url, http, log):
        return ["Account"]

    with patch("source_zuora.resources.discover_object_names", ok), patch.object(
        resources, "_attach_token_source"
    ):
        await resources.validate_credentials(_LOG, AsyncMock(), _config())  # no raise


@pytest.mark.asyncio
async def test_validate_credentials_401_reports_auth_failure():
    async def unauthorized(base_url, http, log):
        raise HTTPError("unauthorized", 401)

    with patch("source_zuora.resources.discover_object_names", unauthorized), patch.object(
        resources, "_attach_token_source"
    ):
        with pytest.raises(ValidationError) as exc:
            await resources.validate_credentials(_LOG, AsyncMock(), _config())
    assert any("Authentication failed" in m for m in exc.value.args[0])


@pytest.mark.asyncio
async def test_validate_credentials_other_http_error_is_generic():
    async def server_error(base_url, http, log):
        raise HTTPError("boom", 500)

    with patch("source_zuora.resources.discover_object_names", server_error), patch.object(
        resources, "_attach_token_source"
    ):
        with pytest.raises(ValidationError) as exc:
            await resources.validate_credentials(_LOG, AsyncMock(), _config())
    assert any("Failed to connect to Zuora" in m for m in exc.value.args[0])


@pytest.mark.asyncio
async def test_validate_credentials_non_http_error_is_generic():
    async def broken(base_url, http, log):
        raise ValueError("bad xml")

    with patch("source_zuora.resources.discover_object_names", broken), patch.object(
        resources, "_attach_token_source"
    ):
        with pytest.raises(ValidationError) as exc:
            await resources.validate_credentials(_LOG, AsyncMock(), _config())
    assert any("Failed to connect to Zuora" in m for m in exc.value.args[0])


# --- _setup_http_auth ----------------------------------------------------------


def test_setup_http_auth_configures_oauth_token_source():
    http = SimpleNamespace()
    resources._attach_token_source(http, _config())
    assert http.token_source is not None
    assert (
        http.token_source.oauth_spec.accessTokenUrlTemplate
        == "https://rest.zuora.com/oauth/token"
    )
