import pytest

from source_dynamics_365_finance_and_operations.api import transform_row


class TestTransformRow:
    """Tests for the transform_row helper function."""

    def test_converts_boolean_field_true(self):
        """Boolean field with 'true' string should become True."""
        row = {"IsActive": "true", "Name": "Test"}
        boolean_fields = frozenset({"IsActive"})

        result = transform_row(row, boolean_fields)

        assert result["IsActive"] is True
        assert result["Name"] == "Test"

    def test_converts_boolean_field_false(self):
        """Boolean field with 'false' string should become False."""
        row = {"IsActive": "false", "Name": "Test"}
        boolean_fields = frozenset({"IsActive"})

        result = transform_row(row, boolean_fields)

        assert result["IsActive"] is False

    def test_converts_boolean_field_case_insensitive(self):
        """Boolean conversion should be case-insensitive."""
        row = {"IsActive": "TRUE", "IsEnabled": "False", "IsValid": "TrUe"}
        boolean_fields = frozenset({"IsActive", "IsEnabled", "IsValid"})

        result = transform_row(row, boolean_fields)

        assert result["IsActive"] is True
        assert result["IsEnabled"] is False
        assert result["IsValid"] is True

    def test_boolean_field_none_becomes_false(self):
        """Boolean field with None value should become False."""
        row = {"IsActive": None, "Name": "Test"}
        boolean_fields = frozenset({"IsActive"})

        result = transform_row(row, boolean_fields)

        assert result["IsActive"] is False

    def test_boolean_field_missing_becomes_false(self):
        """Missing boolean field should be set to False."""
        row = {"Name": "Test"}
        boolean_fields = frozenset({"IsActive"})

        result = transform_row(row, boolean_fields)

        assert result["IsActive"] is False
        assert result["Name"] == "Test"

    def test_multiple_boolean_fields(self):
        """Multiple boolean fields should all be converted."""
        row = {"IsActive": "true", "IsDeleted": "false", "IsEnabled": "true"}
        boolean_fields = frozenset({"IsActive", "IsDeleted", "IsEnabled"})

        result = transform_row(row, boolean_fields)

        assert result["IsActive"] is True
        assert result["IsDeleted"] is False
        assert result["IsEnabled"] is True

    def test_empty_boolean_fields(self):
        """Empty boolean_fields set should only add _meta."""
        row = {"Name": "Test", "Value": "123"}
        boolean_fields = frozenset()

        result = transform_row(row, boolean_fields)

        assert result["Name"] == "Test"
        assert result["Value"] == "123"
        assert "_meta" in result

    def test_meta_op_delete_when_isdelete_true(self):
        """_meta.op should be 'd' when IsDelete is True."""
        row = {"IsDelete": True, "Name": "Test"}
        boolean_fields = frozenset()

        result = transform_row(row, boolean_fields)

        assert result["_meta"] == {"op": "d"}

    def test_meta_op_update_when_isdelete_false(self):
        """_meta.op should be 'u' when IsDelete is False."""
        row = {"IsDelete": False, "Name": "Test"}
        boolean_fields = frozenset()

        result = transform_row(row, boolean_fields)

        assert result["_meta"] == {"op": "u"}

    def test_meta_op_update_when_isdelete_none(self):
        """_meta.op should be 'u' when IsDelete is None."""
        row = {"IsDelete": None, "Name": "Test"}
        boolean_fields = frozenset()

        result = transform_row(row, boolean_fields)

        assert result["_meta"] == {"op": "u"}

    def test_meta_op_update_when_isdelete_missing(self):
        """_meta.op should be 'u' when IsDelete field is missing."""
        row = {"Name": "Test"}
        boolean_fields = frozenset()

        result = transform_row(row, boolean_fields)

        assert result["_meta"] == {"op": "u"}

    def test_isdelete_boolean_conversion_and_meta(self):
        """IsDelete should be converted to bool and used for _meta.op."""
        row = {"IsDelete": "true", "Name": "Test"}
        boolean_fields = frozenset({"IsDelete"})

        result = transform_row(row, boolean_fields)

        # IsDelete should be converted to True (bool)
        assert result["IsDelete"] is True
        # _meta.op should be 'd' because IsDelete is truthy
        assert result["_meta"] == {"op": "d"}

    def test_isdelete_none_becomes_false_with_update_meta(self):
        """IsDelete None should become False and _meta.op should be 'u'."""
        row = {"IsDelete": None, "Name": "Test"}
        boolean_fields = frozenset({"IsDelete"})

        result = transform_row(row, boolean_fields)

        # IsDelete should be converted to False (bool)
        assert result["IsDelete"] is False
        # _meta.op should be 'u' because IsDelete is falsy
        assert result["_meta"] == {"op": "u"}

    def test_mutates_row_in_place(self):
        """transform_row should mutate the row in place and return it."""
        row = {"IsActive": "true"}
        boolean_fields = frozenset({"IsActive"})

        result = transform_row(row, boolean_fields)

        assert result is row
        assert row["IsActive"] is True
        assert "_meta" in row
