import pytest

from estuary_cdk.utils import compare_capture_records, compare_values


class TestCompareValues:
    """Tests for compare_values function."""

    @pytest.mark.parametrize("value", ["hello", 42, True, None])
    def test_identical_scalars(self, value):
        assert compare_values(value, value, "path") == []

    @pytest.mark.parametrize("actual,expected", [
        ("hello", "world"),
        (42, 100),
        (True, False),
        ("value", None),
    ])
    def test_different_scalars(self, actual, expected):
        errors = compare_values(actual, expected, "path")
        assert len(errors) == 1
        assert errors[0].startswith("path: expected ")

    def test_identical_dicts(self):
        assert compare_values({"a": 1, "b": 2}, {"a": 1, "b": 2}, "path") == []

    def test_empty_dicts(self):
        assert compare_values({}, {}, "path") == []

    def test_dict_with_extra_keys_in_actual(self):
        actual = {"a": 1, "b": 2, "c": 3}
        expected = {"a": 1, "b": 2}
        assert compare_values(actual, expected, "path") == []

    def test_dict_with_missing_key(self):
        errors = compare_values({"a": 1}, {"a": 1, "b": 2}, "path")
        assert errors == ["path: key 'b' missing from actual document"]

    def test_dict_with_multiple_missing_keys(self):
        errors = compare_values({"a": 1}, {"a": 1, "b": 2, "c": 3}, "path")
        assert len(errors) == 2
        assert "path: key 'b' missing from actual document" in errors
        assert "path: key 'c' missing from actual document" in errors

    def test_dict_with_changed_value(self):
        errors = compare_values({"a": 1, "b": 999}, {"a": 1, "b": 2}, "path")
        assert errors == ["path.b: expected 2, got 999"]

    def test_nested_dicts_with_extra_key(self):
        actual = {"a": {"b": 1, "c": 2}}
        expected = {"a": {"b": 1}}
        assert compare_values(actual, expected, "path") == []

    def test_nested_dicts_with_missing_key(self):
        errors = compare_values({"a": {"b": 1}}, {"a": {"b": 1, "c": 2}}, "path")
        assert errors == ["path.a: key 'c' missing from actual document"]

    def test_nested_dicts_with_changed_value(self):
        errors = compare_values({"a": {"b": {"c": 999}}}, {"a": {"b": {"c": 1}}}, "path")
        assert errors == ["path.a.b.c: expected 1, got 999"]

    def test_identical_primitive_lists(self):
        assert compare_values([1, 2, 3], [1, 2, 3], "path") == []

    def test_empty_lists(self):
        assert compare_values([], [], "path") == []

    def test_different_primitive_lists(self):
        errors = compare_values([1, 2, 999], [1, 2, 3], "path")
        assert errors == ["path: expected [1, 2, 3], got [1, 2, 999]"]

    def test_primitive_lists_different_length(self):
        errors = compare_values([1, 2], [1, 2, 3], "path")
        assert errors == ["path: list length 2, expected 3"]

    def test_lists_of_dicts_with_extra_keys(self):
        actual = [{"a": 1, "extra": "field"}, {"b": 2}]
        expected = [{"a": 1}, {"b": 2}]
        assert compare_values(actual, expected, "path") == []

    def test_lists_of_dicts_with_missing_key(self):
        errors = compare_values([{"a": 1}, {}], [{"a": 1}, {"b": 2}], "path")
        assert errors == ["path[1]: key 'b' missing from actual document"]

    def test_lists_of_dicts_with_changed_value(self):
        errors = compare_values([{"a": 999}], [{"a": 1}], "path")
        assert errors == ["path[0].a: expected 1, got 999"]

    def test_nested_lists(self):
        errors = compare_values([[1, 2], [3, 999]], [[1, 2], [3, 4]], "path")
        assert errors == ["path[1]: expected [3, 4], got [3, 999]"]

    @pytest.mark.parametrize("actual,expected,expected_type", [
        ([1, 2], {"a": 1}, "dict"),
        ("string", {"a": 1}, "dict"),
        ({"a": 1}, [1, 2], "list"),
        ("string", [1, 2], "list"),
    ])
    def test_type_mismatch(self, actual, expected, expected_type):
        errors = compare_values(actual, expected, "path")
        assert len(errors) == 1
        assert f"expected {expected_type}" in errors[0]

    def test_multiple_errors_in_dict(self):
        errors = compare_values({"a": 999}, {"a": 1, "b": 2, "c": 3}, "path")
        assert len(errors) == 3
        assert "path.a: expected 1, got 999" in errors
        assert "path: key 'b' missing from actual document" in errors
        assert "path: key 'c' missing from actual document" in errors


class TestCompareCaptureRecords:
    """Tests for compare_capture_records function."""

    def test_identical_records(self):
        actual = [["binding1", {"id": 1, "name": "foo"}], ["binding2", {"id": 2}]]
        expected = [["binding1", {"id": 1, "name": "foo"}], ["binding2", {"id": 2}]]
        assert compare_capture_records(actual, expected) == []

    def test_empty_records(self):
        assert compare_capture_records([], []) == []

    def test_different_record_counts(self):
        actual = [["binding1", {"id": 1}], ["binding2", {"id": 2}]]
        expected = [["binding1", {"id": 1}]]
        errors = compare_capture_records(actual, expected)
        assert errors == ["Record count: got 2, expected 1"]

    def test_extra_fields_allowed(self):
        actual = [["binding1", {"id": 1, "extra": "field", "another": 123}]]
        expected = [["binding1", {"id": 1}]]
        assert compare_capture_records(actual, expected) == []

    def test_missing_field_with_binding_name_in_path(self):
        actual = [["users", {"id": 1}], ["orders", {"id": 2}]]
        expected = [["users", {"id": 1}], ["orders", {"id": 2, "status": "pending"}]]
        errors = compare_capture_records(actual, expected)
        assert errors == ["orders: key 'status' missing from actual document"]

    def test_changed_value_with_binding_name_in_path(self):
        actual = [["users", {"id": 1}], ["orders", {"id": 2, "status": "wrong"}]]
        expected = [["users", {"id": 1}], ["orders", {"id": 2, "status": "pending"}]]
        errors = compare_capture_records(actual, expected)
        assert errors == ["orders.status: expected 'pending', got 'wrong'"]

    def test_nested_document_missing_field(self):
        actual = [["binding1", {"id": 1, "meta": {}}]]
        expected = [["binding1", {"id": 1, "meta": {"created": "2024-01-01"}}]]
        errors = compare_capture_records(actual, expected)
        assert errors == ["binding1.meta: key 'created' missing from actual document"]

    def test_fallback_to_index_when_record_not_list(self):
        actual = [{"id": 1}]
        expected = [{"id": 1, "name": "foo"}]
        errors = compare_capture_records(actual, expected)
        assert errors == ["record[0]: key 'name' missing from actual document"]

    def test_malformed_actual_record_single_element(self):
        actual = [["binding1"]]
        expected = [["binding1", {"id": 1}]]
        errors = compare_capture_records(actual, expected)
        assert errors == ["binding1: expected dict, got list"]

    def test_multiple_errors_across_records(self):
        actual = [["binding1", {"id": 999}], ["binding2", {}]]
        expected = [["binding1", {"id": 1}], ["binding2", {"id": 2}]]
        errors = compare_capture_records(actual, expected)
        assert len(errors) == 2
        assert "binding1.id: expected 1, got 999" in errors
        assert "binding2: key 'id' missing from actual document" in errors
