"""Tests for the describe-type -> JSON-schema mapping used to build SourcedSchemas."""

import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import pytest

from estuary_cdk.capture.task import with_sourced_schema_defaults

from source_zuora.models import (
    CONVERTED_TYPES,
    ZUORA_TYPE_SCHEMAS,
    DescribeField,
    DescribeObject,
    normalize_datetime,
    parse_boolean,
    sourced_schema,
    ValidationContext,
    UpdatedDateDocument,
    ZuoraRow,
    ZuoraType,
    UnknownZuoraTypeError,
    _column_to_field,
)


EXPORT = ["soap", "export"]


def test_every_type_zuora_declares_is_a_known_member():
    declared = {
        "text", "decimal", "datetime", "picklist", "date",
        "boolean", "integer", "ZOQL", "longtext", "number", "timestamp",
    }
    assert {ZuoraType.parse(t, "f") for t in declared} == set(ZuoraType)


def test_every_member_has_a_schema():
    # Also asserted at import; restated here so the failure names this contract.
    assert ZUORA_TYPE_SCHEMAS.keys() == set(ZuoraType)


@pytest.mark.parametrize(
    "raw", ["someTypeZuoraAddedLater", None, ""], ids=["unknown", "absent", "empty"]
)
def test_parse_rejects_a_type_it_does_not_recognize(raw):
    # Degrading to an untyped string would claim we know the field's shape when we do
    # not, and would strip whatever format inference had established for the column.
    with pytest.raises(UnknownZuoraTypeError, match="Balance"):
        ZuoraType.parse(raw, "Balance")


def test_every_converted_type_has_a_converter_branch():
    # convert_typed_cells raises if these disagree, which would only surface on a
    # tenant that has such a field. Pin it here instead.
    row = {"f": "unconvertible"}
    for zuora_type in CONVERTED_TYPES:
        with pytest.raises(Exception) as caught:
            ZuoraRow.model_validate(row, context=ValidationContext(object_name="Thing", field_types={"f": zuora_type}))
        assert "no converter" not in str(caught.value), (
            f"{zuora_type} is in CONVERTED_TYPES but convert_typed_cells has no branch"
        )


def test_numerics_are_formatted_strings_not_json_numbers():
    # A JSON number would lose precision on Zuora's money decimals; materialize-sql
    # turns a format-annotated string into a numeric column anyway.
    for zuora_type in (ZuoraType.DECIMAL, ZuoraType.NUMBER):
        assert ZUORA_TYPE_SCHEMAS[zuora_type] == {"type": "string", "format": "number"}
    assert ZUORA_TYPE_SCHEMAS[ZuoraType.INTEGER] == {"type": "string", "format": "integer"}


def test_booleans_are_declared_as_real_booleans():
    # The one type with no string-format equivalent, hence the one that needs its
    # values converted.
    assert ZUORA_TYPE_SCHEMAS[ZuoraType.BOOLEAN] == {"type": "boolean"}


def test_dates_are_format_annotated():
    assert ZUORA_TYPE_SCHEMAS[ZuoraType.DATE] == {"type": "string", "format": "date"}
    assert ZUORA_TYPE_SCHEMAS[ZuoraType.DATETIME] == {
        "type": "string",
        "format": "date-time",
    }


def test_types_needing_value_conversion_are_declared_as_such():
    # Declaring a format the raw cell does not satisfy is worse than declaring none, so
    # these two must never be annotated without their converter being applied.
    assert CONVERTED_TYPES == {ZuoraType.BOOLEAN, ZuoraType.DATETIME}


# How AQuA renders a datetime: an ISO 8601 basic offset, which RFC3339 rejects for
# want of a colon.
ZUORA_DATETIME = "2026-08-11T13:08:57+0000"


def test_normalize_datetime_fixes_the_shape_zuora_actually_emits():
    assert normalize_datetime("CreatedDate", ZUORA_DATETIME) == "2026-08-11T13:08:57+00:00"


def test_normalize_datetime_is_idempotent_and_passes_compliant_values_through():
    once = normalize_datetime("CreatedDate", ZUORA_DATETIME)
    assert normalize_datetime("CreatedDate", once) == once
    for compliant in (
        "2026-08-11T13:08:57Z",
        "2026-08-11T13:08:57+00:00",
        "2026-08-11T13:08:57.123456+00:00",
    ):
        assert normalize_datetime("CreatedDate", compliant) == compliant


def test_normalize_datetime_preserves_a_non_utc_offset():
    # dateTimeUtc pins the offset to UTC today; preserving it costs nothing and avoids
    # silently relabelling a local time as UTC if that ever changes.
    assert (
        normalize_datetime("CreatedDate", "2026-08-11T13:08:57-0530")
        == "2026-08-11T13:08:57-05:30"
    )


def test_normalize_datetime_accepts_fractional_seconds():
    assert (
        normalize_datetime("CreatedDate", "2026-08-11T13:08:57.500+0000")
        == "2026-08-11T13:08:57.500+00:00"
    )


@pytest.mark.parametrize(
    "raw",
    [
        "2026-08-11 13:08:57+0000",  # space separator, no T
        "2026-08-11T13:08:57",  # no offset at all
        "2026-08-11",  # a date, not a datetime
        "",
        "not a date",
    ],
)
def test_normalize_datetime_rejects_shapes_we_have_not_seen(raw: str):
    # A value we cannot make compliant must fail here rather than reach a destination
    # that would reject it more confusingly.
    with pytest.raises(ValueError, match="CreatedDate"):
        normalize_datetime("CreatedDate", raw)


def test_no_declaration_mentions_null():
    assert not any(
        "null" in schema["type"] for schema in ZUORA_TYPE_SCHEMAS.values()
    )


def test_an_unrecognized_type_names_the_field_and_what_to_do():
    with pytest.raises(UnknownZuoraTypeError) as caught:
        ZuoraType.parse("geolocation", "ShipToAddress")
    message = str(caught.value)
    assert "ShipToAddress" in message and "geolocation" in message
    assert "ZUORA_TYPE_SCHEMAS" in message


def test_sourced_schema_is_closed():
    schema = sourced_schema({"Id": ZuoraType.TEXT, "Balance": ZuoraType.DECIMAL})
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False


def test_meta_and_required_are_filled_in_by_the_cdk():
    # The connector deliberately does not hand-roll these; assert on what the runtime
    # actually receives, so this keeps holding if the CDK's defaults change.
    complete = with_sourced_schema_defaults(
        sourced_schema({"Id": ZuoraType.TEXT, "Balance": ZuoraType.DECIMAL})
    )
    properties = complete["properties"]
    assert "_meta" in properties
    assert set(complete["required"]) == {"Id", "Balance", "_meta"}
    assert complete["additionalProperties"] is False


def test_sourced_schema_types_each_field_from_its_zuora_type():
    schema = sourced_schema({"AutoPay": ZuoraType.BOOLEAN, "Name": ZuoraType.TEXT, "Mrr": ZuoraType.DECIMAL})
    properties = schema["properties"]
    assert isinstance(properties, dict)
    assert properties["AutoPay"] == {"type": "boolean"}
    assert properties["Name"] == {"type": "string"}
    assert properties["Mrr"] == {"type": "string", "format": "number"}


def test_query_field_types_keys_joined_columns_by_their_document_name():
    # A joined Contact.Id selection arrives in the document as ContactId, and describe
    # says nothing about its type because it is a relationship, not a field.
    described = DescribeObject(
        name="Invoice",
        fields=[
            DescribeField(name="Id", selectable=True, contexts=EXPORT, type="text"),
            DescribeField(name="Amount", selectable=True, contexts=EXPORT, type="decimal"),
            DescribeField(name="Skipped", selectable=False, contexts=["soap"], type="text"),
        ],
        related_object_names=["Account"],
    )
    assert described.query_field_types == {
        "Id": ZuoraType.TEXT,
        "Amount": ZuoraType.DECIMAL,
        "AccountId": ZuoraType.TEXT,
    }


def test_query_field_types_covers_exactly_the_selected_columns():
    described = DescribeObject(
        name="Invoice",
        fields=[
            DescribeField(name="Id", selectable=True, contexts=EXPORT, type="text"),
            DescribeField(name="Amount", selectable=True, contexts=EXPORT, type="decimal"),
        ],
        related_object_names=["Account"],
    )
    # query_field_names selects "Account.Id"; the document carries "AccountId".
    selected = {
        (name.split(".")[0] + "Id") if "." in name else name
        for name in described.query_field_names
    }
    assert selected == described.query_field_types.keys()


def test_a_field_with_no_declared_type_fails_the_object():
    # Discovery fails rather than the object silently losing a column's type.
    described = DescribeObject(
        name="Thing",
        fields=[DescribeField(name="Id", selectable=True, contexts=EXPORT)],
    )
    with pytest.raises(UnknownZuoraTypeError, match="Id"):
        described.query_field_types


@pytest.mark.parametrize(
    "raw, expected",
    [("true", True), ("false", False), ("TRUE", True), ("False", False), (" true ", True)],
)
def test_parse_boolean_accepts_zuoras_spellings(raw: str, expected: bool):
    assert parse_boolean("AutoPay", raw) is expected


@pytest.mark.parametrize("raw", ["1", "0", "yes", "no", "", "maybe", "t"])
def test_parse_boolean_rejects_everything_else_loudly(raw: str):
    # Silently accepting a wider vocabulary would hide a field that is not really a
    # boolean; the failure is how we find out.
    with pytest.raises(ValueError, match="AutoPay"):
        parse_boolean("AutoPay", raw)


# --- cell conversion -----------------------------------------------------------
#
# A declared type the document does not satisfy is worse than no declaration, so these
# pin the two conversions the sourced schema depends on.

ACCOUNT_TYPES = {
    "Id": ZuoraType.TEXT,
    "AutoPay": ZuoraType.BOOLEAN,
    "CreatedDate": ZuoraType.DATETIME,
    "UpdatedDate": ZuoraType.DATETIME,
    "Balance": ZuoraType.DECIMAL,
    "Status": ZuoraType.PICKLIST,
}


def _row(**overrides: str | None) -> dict[str, str | None]:
    row: dict[str, str | None] = {
        "Id": "8a2880fb96384b0a019643313fe536fb",
        "AutoPay": "true",
        "CreatedDate": "2026-08-11T13:08:57+0000",
        "UpdatedDate": "2026-08-11T13:08:57+0000",
        "Balance": "1234.56",
        "Status": "Active",
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    "column, object_name, expected",
    [
        # Own columns drop the prefix so they match their describe names.
        ("Invoice.Id", "Invoice", "Id"),
        ("Invoice.AccountId", "Invoice", "AccountId"),
        # Joined columns keep the related object as a flattened name.
        ("Account.Id", "Invoice", "AccountId"),
        ("AccountReceivableAccountingCode.Id", "InvoiceItem",
         "AccountReceivableAccountingCodeId"),
        # An object whose name is a prefix of another's must not be confused for
        # it: an InvoiceItem export's own columns say "InvoiceItem.", while
        # "Invoice." is the join.
        ("InvoiceItem.Id", "InvoiceItem", "Id"),
        ("Invoice.Id", "InvoiceItem", "InvoiceId"),
        # A header without a prefix is passed through rather than mangled.
        ("Id", "Account", "Id"),
    ],
)
def test_column_to_field(column, object_name, expected):
    assert _column_to_field(column, object_name) == expected


def test_the_validator_renames_columns_before_converting():
    # field_types is keyed by the document's names, so the rename has to come first or
    # nothing would match and no cell would convert.
    doc = ZuoraRow.model_validate(
        {"Invoice.Id": "inv1", "Account.Id": "acc1", "Invoice.Posted": "true"},
        context=ValidationContext(
            object_name="Invoice",
            field_types={
                "Id": ZuoraType.TEXT,
                "AccountId": ZuoraType.TEXT,
                "Posted": ZuoraType.BOOLEAN,
            },
        ),
    )
    assert doc.Id == "inv1"
    assert doc.AccountId == "acc1"
    assert doc.Posted is True


def test_renaming_is_idempotent_so_revalidation_is_safe():
    # A document's own field names carry no prefix, so a second pass leaves them be.
    assert _column_to_field("AccountId", "Invoice") == "AccountId"
    assert _column_to_field("Id", "Invoice") == "Id"


def test_boolean_cells_become_real_booleans():
    doc = ZuoraRow.model_validate(_row(), context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES))
    assert doc.AutoPay is True
    assert ZuoraRow.model_validate(
        _row(AutoPay="false"), context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES)
    ).AutoPay is False


def test_datetime_cells_become_rfc3339():
    doc = ZuoraRow.model_validate(_row(), context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES))
    assert doc.CreatedDate == "2026-08-11T13:08:57+00:00"


def test_untyped_cells_are_left_exactly_as_they_arrived():
    # Numerics stay strings on purpose: the schema annotates them with format: number,
    # which materializes as numeric without risking a float round-trip.
    doc = ZuoraRow.model_validate(_row(), context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES))
    assert doc.Balance == "1234.56"
    assert doc.Status == "Active"
    assert doc.Id == "8a2880fb96384b0a019643313fe536fb"


def test_empty_cells_stay_null_rather_than_converting():
    doc = ZuoraRow.model_validate(
        _row(AutoPay="", CreatedDate=""), context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES)
    )
    assert doc.AutoPay is None
    assert doc.CreatedDate is None


@pytest.mark.parametrize(
    "context",
    [None, {"object_name": "Account", "field_types": ACCOUNT_TYPES}],
    ids=["absent", "bare dict"],
)
def test_validating_without_a_validation_context_is_an_error(context):
    # Silently skipping conversion would emit cells that contradict the schema the
    # binding declares, so anything but a ValidationContext fails loudly -- including
    # the bare dict this used to accept.
    with pytest.raises(Exception, match="ValidationContext"):
        ZuoraRow.model_validate(_row(), context=context)


def test_an_unexpected_boolean_token_fails_the_row():
    with pytest.raises(Exception, match="AutoPay"):
        ZuoraRow.model_validate(
            _row(AutoPay="Y"), context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES)
        )


def test_the_cursor_field_still_parses_after_normalization():
    # UpdatedDate is both converted here and declared AwareDatetime on the model, so
    # this is the one field where the two have to agree.
    doc = UpdatedDateDocument.model_validate(
        _row(), context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES)
    )
    assert doc.get_cursor() == datetime(2026, 8, 11, 13, 8, 57, tzinfo=UTC)


def test_validating_an_already_converted_document_is_a_no_op():
    # Re-validation must not try to parse a real bool as a string.
    once = ZuoraRow.model_validate(_row(), context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES))
    twice = ZuoraRow.model_validate(
        once.model_dump(by_alias=True), context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES)
    )
    assert twice.AutoPay is True
    assert twice.CreatedDate == "2026-08-11T13:08:57+00:00"


# --- the closing of the loop ---------------------------------------------------
#
# The whole point of the converters is that a document satisfies the schema declared
# for it. If those two disagree, the mismatch surfaces at materialization time in a
# destination, which is the worst place to find it.
#
# Scope note: these validate against the connector's own declaration, not the
# CDK-completed one, and exclude _meta. A SourcedSchema is a widening hint the runtime
# unions into the inferred schema, not a validator of connector output -- its _meta
# block requires the `uuid` the runtime adds afterwards, and bounds `row_id` to values
# a connector deliberately violates with -1 for "not known".


class DeclarationViolation(AssertionError):
    """A value does not satisfy the schema declared for its column."""


def _check_declared_columns(doc: ZuoraRow, field_types: dict[str, ZuoraType]) -> None:
    """Assert every column's value satisfies the schema declared for it.

    Hand-rolled rather than handed to jsonschema, because jsonschema does not enforce
    `format` unless a checker is installed for it -- and it ships no `date-time` checker
    at all, which is precisely the declaration most worth checking here.
    """
    document = doc.model_dump(by_alias=True, mode="json")
    document.pop("_meta", None)
    for name, zuora_type in field_types.items():
        declared = ZUORA_TYPE_SCHEMAS[zuora_type]
        value = document[name]
        if declared["type"] == "boolean":
            if not isinstance(value, bool):
                raise DeclarationViolation(f"{name}: declared boolean, got {value!r}")
            continue
        if not isinstance(value, str):
            raise DeclarationViolation(f"{name}: declared string, got {value!r}")
        match declared.get("format"):
            case "date-time":
                if not re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})",
                    value,
                ):
                    raise DeclarationViolation(f"{name}: not RFC3339: {value!r}")
            case "date":
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                    raise DeclarationViolation(f"{name}: not a date: {value!r}")
            case "integer":
                if not re.fullmatch(r"-?\d+", value):
                    raise DeclarationViolation(f"{name}: not an integer: {value!r}")
            case "number":
                try:
                    Decimal(value)
                except InvalidOperation:
                    raise DeclarationViolation(f"{name}: not a number: {value!r}")


def test_a_converted_document_satisfies_every_column_it_declares():
    _check_declared_columns(
        ZuoraRow.model_validate(
            _row(),
            context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES),
        ),
        ACCOUNT_TYPES,
    )


def test_an_all_null_row_is_not_expected_to_satisfy_the_declaration():
    # Deliberate: the declaration does not mention null, so an all-null row violates it.
    # That is not a contradiction -- the runtime unions this schema into the inferred
    # one, which picks null up from the documents. A schema a document must satisfy is
    # the wrong mental model for a sourced schema.
    empty = {name: "" for name in ACCOUNT_TYPES}
    doc = ZuoraRow.model_validate(
        empty, context=ValidationContext(object_name="Account", field_types=ACCOUNT_TYPES)
    )
    with pytest.raises(DeclarationViolation):
        _check_declared_columns(doc, ACCOUNT_TYPES)


def test_the_raw_export_row_would_not():
    # Why the converters are mandatory rather than cosmetic: straight off the wire the
    # row holds "true" where the schema declares a boolean, and an offset RFC3339
    # rejects where it declares date-time.
    raw = ZuoraRow.model_construct(**_row())
    with pytest.raises(DeclarationViolation, match="AutoPay"):
        _check_declared_columns(raw, ACCOUNT_TYPES)
    # And the datetime specifically -- the half jsonschema never checked.
    with pytest.raises(DeclarationViolation, match="not RFC3339"):
        _check_declared_columns(
            raw, {"CreatedDate": ZuoraType.DATETIME}
        )
