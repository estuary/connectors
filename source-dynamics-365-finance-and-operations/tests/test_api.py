from typing import AsyncGenerator

import pytest

from source_dynamics_365_finance_and_operations.api import (
    order_key,
    defer_deletes,
    transform_row,
)


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


class TestOrderKey:
    def test_versionnumber_drives_comparison(self):
        a = make_row("X", "10")
        b = make_row("X", "20")
        assert order_key(a) < order_key(b)

    def test_sink_modified_on_breaks_ties(self):
        a = make_row("X", "10", sink_modified_on="4/29/2026 9:01:26 PM")
        b = make_row("X", "10", sink_modified_on="4/29/2026 9:01:27 PM")
        assert order_key(a) < order_key(b)

    @pytest.mark.parametrize("missing_field", ["versionnumber", "SinkModifiedOn"])
    def test_missing_required_field_raises(self, missing_field: str):
        row = make_row("X", "10")
        row[missing_field] = None
        with pytest.raises(ValueError, match="missing required ordering fields"):
            order_key(row)


class TestDeferDeletes:
    """Tests for the per-folder delete-deferral state machine."""

    @pytest.mark.asyncio
    async def test_in_order_passthrough(self):
        rows = [
            make_row("A", "10"),
            make_row("B", "20"),
            make_row("A", "30"),
        ]
        result = await collect(defer_deletes(as_async_gen(rows)))
        assert [(r["Id"], r["versionnumber"]) for r in result] == [
            ("A", "10"),
            ("B", "20"),
            ("A", "30"),
        ]

    @pytest.mark.asyncio
    async def test_delete_emitted_at_end(self):
        """A delete is buffered and only emitted after all upserts."""
        rows = [
            make_row("A", "10"),
            make_row("A", "20", is_delete=True),
            make_row("B", "30"),
        ]
        result = await collect(defer_deletes(as_async_gen(rows)))
        assert [(r["Id"], r["versionnumber"], r["IsDelete"]) for r in result] == [
            ("A", "10", False),
            ("B", "30", False),
            ("A", "20", True),
        ]

    @pytest.mark.asyncio
    async def test_delete_then_recreate_coalesces_delete(self):
        rows = [
            make_row("A", "10", is_delete=True),
            make_row("A", "20"),
        ]
        result = await collect(defer_deletes(as_async_gen(rows)))
        assert [(r["Id"], r["versionnumber"], r["IsDelete"]) for r in result] == [
            ("A", "20", False),
        ]

    @pytest.mark.asyncio
    async def test_recreate_then_stale_delete_arrives_first(self):
        rows = [
            make_row("A", "20"),
            make_row("A", "10", is_delete=True),
        ]
        result = await collect(defer_deletes(as_async_gen(rows)))
        assert [(r["Id"], r["versionnumber"], r["IsDelete"]) for r in result] == [
            ("A", "20", False),
        ]

    @pytest.mark.asyncio
    async def test_insert_delete_recreate_same_id(self):
        """insert@10, delete@20, recreate@30 — recreate wins, delete dropped."""
        rows = [
            make_row("A", "10"),
            make_row("A", "20", is_delete=True),
            make_row("A", "30"),
        ]
        result = await collect(defer_deletes(as_async_gen(rows)))
        assert [(r["Id"], r["versionnumber"], r["IsDelete"]) for r in result] == [
            ("A", "10", False),
            ("A", "30", False),
        ]

    @pytest.mark.asyncio
    async def test_multiple_deletes_keep_highest(self):
        """When multiple deletes for one Id arrive, only the highest survives."""
        rows = [
            make_row("A", "10", is_delete=True),
            make_row("A", "30", is_delete=True),
            make_row("A", "20", is_delete=True),
        ]
        result = await collect(defer_deletes(as_async_gen(rows)))
        assert [(r["Id"], r["versionnumber"], r["IsDelete"]) for r in result] == [
            ("A", "30", True),
        ]

    @pytest.mark.asyncio
    async def test_state_resets_between_folders(self):
        """A new call has fresh state; deletes are not silently suppressed."""
        # First call: emit insert@10
        first = await collect(defer_deletes(
            as_async_gen([make_row("A", "10")]),
        ))
        assert len(first) == 1

        # Second call (next folder): delete@20 should emit since state is
        # not carried across calls.
        second = await collect(defer_deletes(
            as_async_gen([make_row("A", "20", is_delete=True)]),
        ))
        assert [(r["Id"], r["IsDelete"]) for r in second] == [("A", True)]

    @pytest.mark.asyncio
    async def test_equal_versionnumber_tiebreaker(self):
        """Equal versionnumber falls back to SinkModifiedOn."""
        rows = [
            make_row("A", "10", sink_modified_on="4/29/2026 9:01:26 PM"),
            make_row(
                "A", "10",
                is_delete=True,
                sink_modified_on="4/29/2026 9:01:27 PM",
            ),
        ]
        result = await collect(defer_deletes(as_async_gen(rows)))
        # Delete has higher SinkModifiedOn, so it wins.
        assert [(r["Id"], r["IsDelete"]) for r in result] == [
            ("A", False),
            ("A", True),
        ]

    @pytest.mark.asyncio
    async def test_stale_upsert_dropped(self):
        """
        An upsert with a lower versionnumber than one already
        emitted for the same Id should be dropped.
        """
        rows = [
            make_row("A", "30"),
            make_row("A", "10"),
        ]
        result = await collect(defer_deletes(as_async_gen(rows)))
        assert [(r["Id"], r["versionnumber"]) for r in result] == [
            ("A", "30"),
        ]

