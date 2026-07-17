import logging
from datetime import timedelta
from typing import AsyncGenerator, Callable

import orjson
import pytest

from estuary_cdk.http import HTTPError
from source_dynamics_365_finance_and_operations.adls_gen2_client import ADLSPathMetadata
from source_dynamics_365_finance_and_operations.api import (
    SETTLE_DELAY,
    TRICKLE_FEED_SERVICE_DIR,
    TableSchemaUnavailableError,
    TransformedRow,
    get_table_metadata,
    should_wait_for_finalization,
    stream_folder_rows,
    transform_row,
)
from source_dynamics_365_finance_and_operations.shared import str_to_dt


CSV_NAME = "2026-01-01T00:00:00.000Z/Table/data.csv"


def make_row(
    row_id: str,
    versionnumber: str,
    is_delete: bool = False,
    sink_modified_on: str = "1/1/2026 12:00:00 AM",
    **extra,
) -> dict:
    """Build a row in the shape produced by transform_row."""
    return {
        "Id": row_id,
        "IsDelete": is_delete,
        "versionnumber": versionnumber,
        "SinkModifiedOn": sink_modified_on,
        "_meta": {"op": "d" if is_delete else "u", "source_file": CSV_NAME},
        **extra,
    }


async def as_async_gen(items: list[dict]) -> AsyncGenerator[dict, None]:
    for item in items:
        yield item


async def collect(gen: AsyncGenerator[dict, None]) -> list[dict]:
    return [row async for row in gen]


class TestTransformRow:
    """Tests for the transform_row helper function."""

    def test_converts_boolean_field_true(self):
        """Boolean field with 'true' string should become True."""
        row: dict[str, str | None] = {"IsActive": "true", "Name": "Test"}
        boolean_fields = frozenset({"IsActive"})

        result = transform_row(row, boolean_fields, CSV_NAME)

        assert result["IsActive"] is True
        assert result["Name"] == "Test"

    def test_converts_boolean_field_false(self):
        """Boolean field with 'false' string should become False."""
        row: dict[str, str | None] = {"IsActive": "false", "Name": "Test"}
        boolean_fields = frozenset({"IsActive"})

        result = transform_row(row, boolean_fields, CSV_NAME)

        assert result["IsActive"] is False

    def test_converts_boolean_field_case_insensitive(self):
        """Boolean conversion should be case-insensitive."""
        row: dict[str, str | None] = {"IsActive": "TRUE", "IsEnabled": "False", "IsValid": "TrUe"}
        boolean_fields = frozenset({"IsActive", "IsEnabled", "IsValid"})

        result = transform_row(row, boolean_fields, CSV_NAME)

        assert result["IsActive"] is True
        assert result["IsEnabled"] is False
        assert result["IsValid"] is True

    def test_boolean_field_none_becomes_false(self):
        """Boolean field with None value should become False."""
        row: dict[str, str | None] = {"IsActive": None, "Name": "Test"}
        boolean_fields = frozenset({"IsActive"})

        result = transform_row(row, boolean_fields, CSV_NAME)

        assert result["IsActive"] is False

    def test_multiple_boolean_fields(self):
        """Multiple boolean fields should all be converted."""
        row: dict[str, str | None] = {"IsActive": "true", "IsDeleted": "false", "IsEnabled": "true"}
        boolean_fields = frozenset({"IsActive", "IsDeleted", "IsEnabled"})

        result = transform_row(row, boolean_fields, CSV_NAME)

        assert result["IsActive"] is True
        assert result["IsDeleted"] is False
        assert result["IsEnabled"] is True

    def test_meta_op_delete_when_isdelete_true(self):
        """_meta.op should be 'd' when IsDelete is True."""
        row: dict[str, str | None] = {"IsDelete": "True", "Name": "Test"}
        boolean_fields = frozenset()

        result = transform_row(row, boolean_fields, CSV_NAME)

        assert result["_meta"] == {"op": "d", "source_file": CSV_NAME}

    def test_meta_op_update_when_isdelete_empty(self):
        """_meta.op should be 'u' when IsDelete is empty string."""
        row: dict[str, str | None] = {"IsDelete": "", "Name": "Test"}
        boolean_fields = frozenset()

        result = transform_row(row, boolean_fields, CSV_NAME)

        assert result["_meta"] == {"op": "u", "source_file": CSV_NAME}

    def test_meta_op_update_when_isdelete_false(self):
        """_meta.op should be 'u' when IsDelete is 'False' and converted to bool."""
        row: dict[str, str | None] = {"IsDelete": "False", "Name": "Test"}
        boolean_fields = frozenset({"IsDelete"})

        result = transform_row(row, boolean_fields, CSV_NAME)

        assert result["IsDelete"] is False
        assert result["_meta"] == {"op": "u", "source_file": CSV_NAME}

    def test_mutates_row_in_place(self):
        """transform_row should mutate the row in place and return it."""
        row: dict[str, str | None] = {"IsActive": "true"}
        boolean_fields = frozenset({"IsActive"})

        result = transform_row(row, boolean_fields, CSV_NAME)

        assert result is row
        assert row["IsActive"] is True
        assert "_meta" in row


def fake_csv(name: str) -> ADLSPathMetadata:
    """Build an ADLSPathMetadata."""
    return ADLSPathMetadata(
        name=name,
        lastModified="Wed, 24 Sep 2025 14:24:24 GMT",
        etag="x",
        isDirectory=None,
        contentLength=None,
        group=None,
        owner=None,
        permissions=None,
        creationTime=None,
    )


def make_factory(
    rows_by_csv: dict[str, list[TransformedRow]],
) -> Callable[[ADLSPathMetadata], AsyncGenerator[TransformedRow, None]]:
    def open_csv(csv: ADLSPathMetadata) -> AsyncGenerator[TransformedRow, None]:
        async def gen() -> AsyncGenerator[TransformedRow, None]:
            for row in rows_by_csv[csv.name]:
                yield row
        return gen()
    return open_csv


class TestStreamFolderRows:
    """Tests for the per-folder, file-op-aware streaming state machine."""

    @pytest.mark.asyncio
    async def test_in_order_passthrough(self):
        """All-upsert single file: rows yield in order."""
        csvs = [fake_csv("upserts.csv")]
        factory = make_factory({
            "upserts.csv": [
                make_row("A", "10"),
                make_row("B", "20"),
                make_row("A", "30"),
            ],
        })
        result = await collect(stream_folder_rows(csvs, factory))
        assert [(r["Id"], r["versionnumber"]) for r in result] == [
            ("A", "10"),
            ("B", "20"),
            ("A", "30"),
        ]

    @pytest.mark.asyncio
    async def test_delete_files_emitted_after_upsert_files(self):
        """All upsert files are read before any delete file."""
        csvs = [fake_csv("upserts.csv"), fake_csv("deletes.csv")]
        factory = make_factory({
            "upserts.csv": [make_row("A", "10"), make_row("B", "30")],
            "deletes.csv": [make_row("A", "20", is_delete=True)],
        })
        result = await collect(stream_folder_rows(csvs, factory))
        assert [(r["Id"], r["versionnumber"], r["IsDelete"]) for r in result] == [
            ("A", "10", False),
            ("B", "30", False),
            ("A", "20", True),
        ]

    @pytest.mark.asyncio
    async def test_mtime_ordering_does_not_invert_passes(self):
        """A delete file with earlier mtime than an upsert file is still
        deferred until after all upsert files."""
        csvs = [fake_csv("deletes.csv"), fake_csv("upserts.csv")]
        factory = make_factory({
            "deletes.csv": [make_row("A", "10", is_delete=True)],
            "upserts.csv": [make_row("A", "20")],
        })
        result = await collect(stream_folder_rows(csvs, factory))
        assert [(r["Id"], r["versionnumber"], r["IsDelete"]) for r in result] == [
            ("A", "20", False),
            ("A", "10", True),
        ]

    @pytest.mark.asyncio
    async def test_homogeneity_violation_in_upsert_file(self):
        """An upsert file with a delete row mid-stream raises RuntimeError."""
        csvs = [fake_csv("mixed.csv")]
        factory = make_factory({
            "mixed.csv": [
                make_row("A", "10"),
                make_row("B", "20", is_delete=True),
            ],
        })
        with pytest.raises(RuntimeError, match="mixed.csv"):
            await collect(stream_folder_rows(csvs, factory))

    @pytest.mark.asyncio
    async def test_homogeneity_violation_in_delete_file(self):
        """A delete file with a non-delete row mid-stream raises RuntimeError."""
        csvs = [fake_csv("mixed.csv")]
        factory = make_factory({
            "mixed.csv": [
                make_row("A", "10", is_delete=True),
                make_row("B", "20"),
            ],
        })
        with pytest.raises(RuntimeError, match="mixed.csv"):
            await collect(stream_folder_rows(csvs, factory))

    @pytest.mark.asyncio
    async def test_empty_csv_skipped(self):
        """An empty CSV is skipped without affecting subsequent CSVs."""
        csvs = [fake_csv("empty.csv"), fake_csv("upserts.csv")]
        factory = make_factory({
            "empty.csv": [],
            "upserts.csv": [make_row("A", "10")],
        })
        result = await collect(stream_folder_rows(csvs, factory))
        assert [(r["Id"], r["versionnumber"]) for r in result] == [("A", "10")]


def entity(name: str) -> dict:
    """A model.json entity with the standard metadata columns plus one boolean."""
    return {
        "$type": "LocalEntity",
        "name": name,
        "description": "",
        "attributes": [
            {"name": "Id", "dataType": "guid"},
            {"name": "IsDelete", "dataType": "string"},
            {"name": "IsActive", "dataType": "boolean"},
        ],
    }


def model_json(table_names: list[str]) -> bytes:
    return orjson.dumps({"name": "cdm", "entities": [entity(n) for n in table_names]})


class FakeADLSClient:
    """Serves file bytes by path and raises a 404 HTTPError for anything else,
    matching how ADLSGen2Client surfaces missing files."""

    def __init__(self, files: dict[str, bytes]):
        self._files = files
        self.log = logging.getLogger("test-d365-api")

    async def read_file(self, path: str) -> bytes:
        if path not in self._files:
            raise HTTPError("The specified path does not exist.", 404)
        return self._files[path]


class TestGetTableMetadata:
    """Tests for schema resolution, including the per-table model.json fallback
    used when a folder-level model.json was written but truncated."""

    TIMESTAMP = "2025-04-11T06.24.48Z"
    TABLE = "whsworktrans"

    def per_table_path(self) -> str:
        return f"{self.TIMESTAMP}/{TRICKLE_FEED_SERVICE_DIR}/{self.TABLE}-model.json"

    @pytest.mark.asyncio
    async def test_uses_folder_level_model_json(self):
        client = FakeADLSClient({
            f"{self.TIMESTAMP}/model.json": model_json(["customers", self.TABLE]),
        })
        metadata = await get_table_metadata(self.TIMESTAMP, self.TABLE, client, client.log)
        assert metadata.name == self.TABLE
        assert metadata.field_names == ["Id", "IsDelete", "IsActive"]
        assert metadata.boolean_fields == frozenset({"IsActive"})

    @pytest.mark.asyncio
    async def test_falls_back_to_per_table_model_json_when_truncated(self):
        """A truncated folder-level model.json (lists earlier tables but not this
        one) falls back to the authoritative per-table model.json."""
        client = FakeADLSClient({
            f"{self.TIMESTAMP}/model.json": model_json(["customers", "vendinvoicetrans"]),
            self.per_table_path(): model_json([self.TABLE]),
        })
        metadata = await get_table_metadata(self.TIMESTAMP, self.TABLE, client, client.log)
        assert metadata.name == self.TABLE
        assert metadata.field_names == ["Id", "IsDelete", "IsActive"]

    @pytest.mark.asyncio
    async def test_raises_when_truncated_and_no_per_table_model_json(self):
        client = FakeADLSClient({
            f"{self.TIMESTAMP}/model.json": model_json(["customers", "vendinvoicetrans"]),
        })
        with pytest.raises(TableSchemaUnavailableError, match="no per-table model.json"):
            await get_table_metadata(self.TIMESTAMP, self.TABLE, client, client.log)

    @pytest.mark.asyncio
    async def test_does_not_fall_back_when_folder_model_json_has_no_entities(self):
        """An empty folder-level model.json means the folder hasn't finalized;
        we must not trust a per-table model.json even if one exists."""
        client = FakeADLSClient({
            f"{self.TIMESTAMP}/model.json": model_json([]),
            self.per_table_path(): model_json([self.TABLE]),
        })
        with pytest.raises(TableSchemaUnavailableError, match="lists no entities"):
            await get_table_metadata(self.TIMESTAMP, self.TABLE, client, client.log)

    @pytest.mark.asyncio
    async def test_raises_when_per_table_model_json_lacks_table(self):
        client = FakeADLSClient({
            f"{self.TIMESTAMP}/model.json": model_json(["customers", "vendinvoicetrans"]),
            self.per_table_path(): model_json(["a_different_table"]),
        })
        with pytest.raises(TableSchemaUnavailableError, match="does not describe the table"):
            await get_table_metadata(self.TIMESTAMP, self.TABLE, client, client.log)


class TestShouldWaitForFinalization:
    """Tests for the settle-window decision used when a timestamp folder has
    table data but no model.json."""

    SUCCESSOR = "2024-09-11T16.29.10Z"

    def test_ancient_successor_is_not_waited_on(self):
        """A folder whose successor is long past is treated as incomplete and
        not waited on."""
        now = str_to_dt(self.SUCCESSOR) + timedelta(days=730)
        assert should_wait_for_finalization(self.SUCCESSOR, now) is False

    def test_recent_successor_is_waited_on(self):
        """A folder whose successor is younger than SETTLE_DELAY may still be
        finalizing, so we keep waiting."""
        now = str_to_dt(self.SUCCESSOR) + (SETTLE_DELAY - timedelta(minutes=1))
        assert should_wait_for_finalization(self.SUCCESSOR, now) is True

    def test_settle_delay_boundary_is_not_waited_on(self):
        """At exactly SETTLE_DELAY the folder has had enough time to finalize."""
        now = str_to_dt(self.SUCCESSOR) + SETTLE_DELAY
        assert should_wait_for_finalization(self.SUCCESSOR, now) is False

    def test_just_under_settle_delay_is_waited_on(self):
        now = str_to_dt(self.SUCCESSOR) + SETTLE_DELAY - timedelta(seconds=1)
        assert should_wait_for_finalization(self.SUCCESSOR, now) is True

