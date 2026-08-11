"""Tests for the describe-type -> JSON-schema mapping used to build SourcedSchemas."""

import pytest

from source_zuora.models import (
    ZUORA_TYPE_SCHEMAS,
    DescribeField,
    DescribeObject,
    sourced_schema,
    ValidationContext,
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


def test_renaming_is_idempotent_so_revalidation_is_safe():
    # A document's own field names carry no prefix, so a second pass leaves them be.
    assert _column_to_field("AccountId", "Invoice") == "AccountId"
    assert _column_to_field("Id", "Invoice") == "Id"
