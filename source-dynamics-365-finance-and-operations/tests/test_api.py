import logging
from datetime import timedelta
from typing import AsyncGenerator, Callable

import orjson
import pytest

from estuary_cdk.http import HTTPError
from estuary_cdk.incremental_csv_processor import IncrementalCSVRowProcessor
from source_dynamics_365_finance_and_operations.adls_gen2_client import ADLSPathMetadata
from source_dynamics_365_finance_and_operations.api import (
    SETTLE_DELAY,
    TRICKLE_FEED_SERVICE_DIR,
    RowSchemaMismatchError,
    TableSchemaUnavailableError,
    TransformedRow,
    _table_metadata_from_entity,
    bind_row,
    get_table_metadata,
    read_csv_rows,
    should_wait_for_finalization,
    stream_folder_rows,
    transform_row,
)
from source_dynamics_365_finance_and_operations.models import TableMetadata
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


class TestBindRow:
    """Tests for binding a headerless CSV row's cells to field names.

    Synapse Link appends columns added to a table after its initial export to
    the end of every row, past IsDelete. A CSV written across such an append
    holds rows of both widths, so rows can be narrower than the folder's
    model.json declares."""

    # Three appended columns, so a row can legitimately fall short by more
    # than one - the shape real tables have (prodtable spans 110..118
    # columns, salesline 267..291).
    _FIELD_NAMES = ["Id", "SinkModifiedOn", "IsDelete", "app_a", "app_b", "app_c"]
    METADATA = TableMetadata(
        name="prodtable",
        field_names=_FIELD_NAMES,
        boolean_fields=frozenset({"IsDelete"}),
        # Derived the way _table_metadata_from_entity derives it, so the
        # fixture cannot drift from what the connector would compute.
        min_row_width=_FIELD_NAMES.index("IsDelete") + 1,
    )

    def test_full_width_row(self):
        row = bind_row(["abc", "ts", "", "1", "2", "3"], self.METADATA, CSV_NAME, 1)

        assert row == {
            "Id": "abc", "SinkModifiedOn": "ts", "IsDelete": None,
            "app_a": "1", "app_b": "2", "app_c": "3",
        }

    @pytest.mark.parametrize("absent", [1, 2, 3])
    def test_rows_predating_appended_columns_omit_them(self, absent: int):
        """A row written before the last `absent` columns were added binds its
        cells and leaves those columns absent rather than null."""
        width = len(self.METADATA.field_names) - absent
        row = bind_row(["abc", "ts", ""] + ["x"] * (width - 3), self.METADATA, CSV_NAME, 1)

        assert len(row) == width
        assert [f for f in self.METADATA.field_names if f not in row] == \
            self.METADATA.field_names[width:]

    def test_narrowest_legal_row_is_min_row_width(self):
        row = bind_row(["abc", "ts", "True"], self.METADATA, CSV_NAME, 1)

        assert row == {"Id": "abc", "SinkModifiedOn": "ts", "IsDelete": "True"}

    def test_empty_cells_become_none(self):
        row = bind_row(["", "", "True", "", "", ""], self.METADATA, CSV_NAME, 1)

        assert row == {
            "Id": None, "SinkModifiedOn": None, "IsDelete": "True",
            "app_a": None, "app_b": None, "app_c": None,
        }

    def test_row_narrower_than_min_row_width_raises(self):
        """Truncation reaching past IsDelete means the cells no longer line up,
        so it must not be read as a row predating an append."""
        with pytest.raises(RowSchemaMismatchError, match="at least 3 columns wide"):
            bind_row(["abc", "ts"], self.METADATA, CSV_NAME, 1)

    def test_row_wider_than_schema_raises(self):
        """Extra cells have no field name to bind to."""
        with pytest.raises(RowSchemaMismatchError, match="no field name to bind to"):
            bind_row(["abc", "ts", "", "1", "2", "3", "surplus"], self.METADATA, CSV_NAME, 1)

    def test_error_names_the_csv(self):
        with pytest.raises(RowSchemaMismatchError, match=CSV_NAME):
            bind_row(["abc", "ts"], self.METADATA, CSV_NAME, 1)

    def test_transform_row_leaves_absent_boolean_absent(self):
        """An appended boolean column absent from an older row must not be
        fabricated as False."""
        metadata = TableMetadata(
            name="prodtable",
            field_names=["Id", "IsDelete", "appended_flag"],
            boolean_fields=frozenset({"IsDelete", "appended_flag"}),
            min_row_width=2,
        )

        row = transform_row(
            bind_row(["abc", ""], metadata, CSV_NAME, 1), metadata.boolean_fields, CSV_NAME
        )

        assert "appended_flag" not in row
        assert row["IsDelete"] is False
        assert row["_meta"] == {"op": "u", "source_file": CSV_NAME}


class TestReadCsvRows:
    """Tests for the invariants that hold across a file rather than within a
    single row."""

    METADATA = TestBindRow.METADATA

    async def as_values(self, rows: list[list[str]]) -> AsyncGenerator[list[str], None]:
        for row in rows:
            yield row

    async def read(self, rows: list[list[str]]) -> list[TransformedRow]:
        log = logging.getLogger("test-d365-api")
        return [
            row async for row in
            read_csv_rows(self.as_values(rows), self.METADATA, CSV_NAME, log)
        ]

    @pytest.mark.asyncio
    async def test_widening_rows_are_read(self):
        """The observed shape: narrow rows predating an append, then wider
        rows once the column exists."""
        rows = await self.read([
            ["a", "ts", ""],
            ["b", "ts", ""],
            ["c", "ts", "", "1", "2", "3"],
        ])

        assert [r["Id"] for r in rows] == ["a", "b", "c"]
        assert "app_c" not in rows[0]
        assert rows[2]["app_c"] == "3"

    @pytest.mark.asyncio
    async def test_rows_are_transformed(self):
        """Rows arrive transformed, with booleans converted and _meta set."""
        rows = await self.read([["a", "ts", "True"], ["b", "ts", "True"]])

        assert rows[0]["IsDelete"] is True
        assert rows[0]["_meta"] == {"op": "d", "source_file": CSV_NAME}

    @pytest.mark.asyncio
    async def test_empty_file_yields_nothing(self):
        assert await self.read([]) == []

    @pytest.mark.asyncio
    async def test_every_row_narrow_is_read(self):
        """A file written entirely before an append is uniformly narrow, which
        is legal - widths never decrease."""
        rows = await self.read([["a", "ts", ""], ["b", "ts", ""], ["c", "ts", ""]])

        assert [r["Id"] for r in rows] == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_first_row_narrower_than_min_row_width_raises(self):
        with pytest.raises(RowSchemaMismatchError, match="at least 3 columns wide"):
            await self.read([["a", "ts"]])

    @pytest.mark.asyncio
    async def test_error_reports_the_row_number(self):
        """The row number is what makes a failure findable in a large CSV."""
        with pytest.raises(RowSchemaMismatchError, match="row 3 of") as excinfo:
            await self.read([
                ["a", "ts", ""],
                ["b", "ts", ""],
                ["c", "ts", "", "1", "2", "3", "surplus"],
            ])

        assert excinfo.value.row_number == 3



class TestCsvBytesToDocuments:
    """Covers the seam between the CDK's positional parser and the connector's
    binding: raw bytes in, capture documents out."""

    METADATA = TestBindRow.METADATA

    async def chunks(self, data: str, size: int = 7) -> AsyncGenerator[bytes, None]:
        """Small chunks, so rows straddle chunk boundaries as they do over HTTP."""
        raw = data.encode("utf-8")
        for i in range(0, len(raw), size):
            yield raw[i:i + size]

    async def read(self, data: str) -> list[TransformedRow]:
        log = logging.getLogger("test-d365-api")
        return [
            row async for row in read_csv_rows(
                IncrementalCSVRowProcessor(self.chunks(data)),
                self.METADATA,
                CSV_NAME,
                log,
            )
        ]

    @pytest.mark.asyncio
    async def test_mixed_width_file(self):
        """The shape of a variable width CSV: narrow rows written before a column
        was appended, then wider rows once it existed."""
        rows = await self.read(
            "a,ts,\r\n"
            "b,ts,True\r\n"
            "c,ts,,1,2,3\r\n"
        )

        assert [r["Id"] for r in rows] == ["a", "b", "c"]
        assert "app_c" not in rows[0] and "app_c" not in rows[1]
        assert rows[1]["_meta"]["op"] == "d"
        assert rows[2]["app_c"] == "3"

    @pytest.mark.asyncio
    async def test_several_columns_appended_during_one_interval(self):
        """Columns can be added at different points within a single export
        interval, so one file can hold rows of three or more widths. Each row
        binds the prefix of the schema that existed when it was written."""
        rows = await self.read(
            "a,ts,\r\n"
            "b,ts,,1\r\n"
            "c,ts,,1,2\r\n"
            "d,ts,,1,2,3\r\n"
        )

        absent = [
            [f for f in self.METADATA.field_names if f not in r] for r in rows
        ]
        assert absent == [
            ["app_a", "app_b", "app_c"],
            ["app_b", "app_c"],
            ["app_c"],
            [],
        ]
        assert [r["Id"] for r in rows] == ["a", "b", "c", "d"]

    @pytest.mark.asyncio
    async def test_quoted_values_spanning_chunks(self):
        rows = await self.read('"a,1",ts,,"quoted, value",2,3\r\n')

        assert rows[0]["Id"] == "a,1"
        assert rows[0]["app_a"] == "quoted, value"



class TestTableMetadataFromEntity:
    """Tests for locating how narrow a row may be from model.json."""

    def test_min_row_width_ends_at_is_delete(self):
        metadata = _table_metadata_from_entity({
            "name": "prodtable",
            "attributes": [
                {"name": "Id", "dataType": "guid"},
                {"name": "IsDelete", "dataType": "boolean"},
                {"name": "delta_custom", "dataType": "int64"},
            ],
        })

        assert metadata.field_names == ["Id", "IsDelete", "delta_custom"]
        assert metadata.min_row_width == 2
        assert metadata.boolean_fields == frozenset({"IsDelete"})

    def test_entity_with_non_boolean_is_delete_raises(self):
        """A string IsDelete would stay out of boolean_fields, leaving every
        delete row reading as an upsert with no error at all."""
        with pytest.raises(ValueError, match="rather than 'boolean'"):
            _table_metadata_from_entity({
                "name": "odd",
                "attributes": [
                    {"name": "Id", "dataType": "guid"},
                    {"name": "IsDelete", "dataType": "string"},
                ],
            })

    def test_entity_without_is_delete_raises(self):
        """IsDelete determines a row's operation and orders upserts ahead of
        deletes, so a table lacking it cannot be read at all. Fail here with an
        explanation rather than downstream on a KeyError."""
        with pytest.raises(ValueError, match="no IsDelete column"):
            _table_metadata_from_entity({
                "name": "odd",
                "attributes": [
                    {"name": "Id", "dataType": "guid"},
                    {"name": "other", "dataType": "string"},
                ],
            })


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

    @pytest.mark.asyncio
    async def test_narrow_delete_row_is_still_deferred(self):
        """A delete predating an appended column is narrower than the schema.
        IsDelete is bound in every legal row width, so such a row is still
        recognized as a delete and deferred to pass 2."""
        metadata = TestBindRow.METADATA

        def build(values: list[str]) -> TransformedRow:
            return transform_row(
                bind_row(values, metadata, CSV_NAME, 1), metadata.boolean_fields, CSV_NAME
            )

        csvs = [fake_csv("deletes.csv"), fake_csv("upserts.csv")]
        factory = make_factory({
            "deletes.csv": [build(["A", "ts", "True"])],
            "upserts.csv": [build(["A", "ts", "", "1", "2", "3"])],
        })

        result = await collect(stream_folder_rows(csvs, factory))

        assert [(r["Id"], r["IsDelete"]) for r in result] == [("A", False), ("A", True)]
        assert "app_c" not in result[1]


def entity(name: str) -> dict:
    """A model.json entity with the standard metadata columns plus one boolean."""
    return {
        "$type": "LocalEntity",
        "name": name,
        "description": "",
        "attributes": [
            {"name": "Id", "dataType": "guid"},
            {"name": "IsDelete", "dataType": "boolean"},
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
        assert metadata.boolean_fields == frozenset({"IsActive", "IsDelete"})

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
