import pytest

from source_airtable_native.models import (
    AirtableField,
    AirtableRecord,
    AirtableRecordFields,
    FieldPresenceContext,
    _empty_value_for_field_type,
    create_incremental_record_model,
    expected_field_defaults,
)


def _field(name: str, type: str) -> AirtableField:
    return AirtableField(id=f"fld{name}", name=name, type=type)


# _empty_value_for_field_type


def test_empty_value_checkbox_is_false():
    assert _empty_value_for_field_type("checkbox") is False


@pytest.mark.parametrize("field_type", ["singleLineText", "multilineText", "richText", "email", "url", "phoneNumber"])
def test_empty_value_text_types_are_empty_string(field_type):
    assert _empty_value_for_field_type(field_type) == ""


@pytest.mark.parametrize("field_type", [
    "multipleSelects", "multipleRecordLinks", "multipleAttachments", "multipleCollaborators",
    "multipleLookupValues",  # cell format is unconditionally array-shaped regardless of linked field type
])
def test_empty_value_array_types_are_empty_array(field_type):
    assert _empty_value_for_field_type(field_type) == []


@pytest.mark.parametrize("field_type", [
    "number", "currency", "percent", "duration", "rating", "count", "autoNumber",  # no empty-value convention at all
    "date", "dateTime",  # same
    "singleSelect",  # constrained option set, "" isn't a documented "no selection" value
    "formula", "rollup",  # shape varies per-record via a nested `result` type
    "singleCollaborator", "createdBy", "lastModifiedBy",  # object-shaped, no empty-object convention
    "somethingAirtableAddsLater",  # unknown types must fail safe
])
def test_empty_value_everything_else_is_none(field_type):
    assert _empty_value_for_field_type(field_type) is None


# expected_field_defaults


def test_expected_field_defaults_maps_each_name_to_its_type_default():
    fields = [_field("On Hold", "multipleSelects"), _field("Score", "number")]
    assert expected_field_defaults(fields) == {"On Hold": [], "Score": None}


def test_expected_field_defaults_omits_the_excluded_name():
    fields = [_field("On Hold", "multipleSelects"), _field("Last Modified", "lastModifiedTime")]
    assert expected_field_defaults(fields, exclude="Last Modified") == {"On Hold": []}


def test_expected_field_defaults_exclude_not_present_is_a_noop():
    fields = [_field("On Hold", "multipleSelects"), _field("Notes", "singleLineText")]
    assert expected_field_defaults(fields, exclude="Nonexistent") == {"On Hold": [], "Notes": ""}


def test_expected_field_defaults_empty_fields_list():
    assert expected_field_defaults([]) == {}


# AirtableRecordFields.normalize_fields


def test_normalize_fields_requires_a_field_presence_context():
    with pytest.raises(RuntimeError):
        AirtableRecordFields.model_validate({"Notes": "hi"})


def test_normalize_fields_fills_absent_with_its_type_default_and_leaves_present_untouched():
    # Mirrors the reported bug: "On Hold" (a multi-select) was set once and is
    # now absent from Airtable's response because it was cleared. An empty
    # multi-select's only possible value is [], so that's injected instead of
    # None - which also means no schema widening is needed for this field.
    fields_obj = AirtableRecordFields.model_validate(
        {"On Hold": ["Borrower Delay"]},
        context=FieldPresenceContext(expected_field_defaults([
            _field("On Hold", "multipleSelects"),
            _field("Notes", "singleLineText"),
        ])),
    )

    assert fields_obj.model_extra == {"On Hold": ["Borrower Delay"], "Notes": ""}


def test_normalize_fields_falls_back_to_none_for_a_type_with_no_safe_empty_value():
    fields_obj = AirtableRecordFields.model_validate(
        {},
        context=FieldPresenceContext(expected_field_defaults([_field("Score", "number")])),
    )

    assert fields_obj.model_extra == {"Score": None}


def test_normalize_fields_all_present_is_a_noop():
    fields_obj = AirtableRecordFields.model_validate(
        {"Notes": "hello"},
        context=FieldPresenceContext({"Notes": ""}),
    )

    assert fields_obj.model_extra == {"Notes": "hello"}


def test_normalize_fields_leaves_a_stripped_formula_error_field_omitted():
    # A formula field in an error state is a transient computation hiccup
    # (a dependent field mid-edit, recompute lag), not evidence the field's
    # real value is now empty. It's stripped by _is_formula_error but must
    # NOT be filled with a default - it should stay omitted so `merge` reads
    # it as "no change" and keeps whatever value was last known-good, rather
    # than the error blip overwriting a real value with null.
    fields_obj = AirtableRecordFields.model_validate(
        {"Score": {"error": "#ERROR!"}, "Other": "y"},
        context=FieldPresenceContext(expected_field_defaults([
            _field("Score", "formula"),
            _field("Other", "singleLineText"),
        ])),
    )

    assert fields_obj.model_extra == {"Other": "y"}
    assert "Score" not in fields_obj.model_extra


def test_normalize_fields_never_touches_the_cursor_field():
    record_cls = create_incremental_record_model("Last Modified")
    expected = expected_field_defaults(
        [_field("Notes", "singleLineText"), _field("Last Modified", "lastModifiedTime")],
        exclude="Last Modified",
    )
    assert expected == {"Notes": ""}

    record = record_cls.model_validate(
        {
            "id": "rec1",
            "createdTime": "2024-01-01T00:00:00.000Z",
            "fields": {"Last Modified": "2024-01-01T00:00:00.000Z", "Notes": "hi"},
        },
        context=FieldPresenceContext(expected),
    )

    # The cursor value is tracked through its own aliased field, not model_extra,
    # and must not gain a colliding "Last Modified" entry there.
    assert "Last Modified" not in record.fields.model_extra
    assert record.fields.model_extra == {"Notes": "hi"}
    assert record.cursor_value.isoformat() == "2024-01-01T00:00:00+00:00"


def test_normalize_fields_survives_document_emission_serialization():
    # This is the exact call estuary_cdk's task.py `_captured()` makes when
    # emitting a document: model_dump_json(by_alias=True, exclude_unset=True).
    # Covers both a type-aware fill (On Hold -> []) and a None fallback
    # (Score -> null) surviving the same serialization path.
    record = AirtableRecord.model_validate(
        {
            "id": "recmsClyQuc4xhNk7",
            "createdTime": "2024-01-01T00:00:00.000Z",
            "fields": {"Notes": "hi"},
        },
        context=FieldPresenceContext(expected_field_defaults([
            _field("On Hold", "multipleSelects"),
            _field("Score", "number"),
            _field("Notes", "singleLineText"),
        ])),
    )

    dumped = record.model_dump_json(by_alias=True, exclude_unset=True)
    assert '"On Hold":[]' in dumped
    assert '"Score":null' in dumped
