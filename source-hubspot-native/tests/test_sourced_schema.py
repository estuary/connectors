import json
from pathlib import Path

from jsonschema import Draft7Validator

from source_hubspot_native.models import BaseCRMObject, Properties


def test_sourced_schema_is_valid_json_schema():
    """Verify that sourced_schema produces a valid JSON Schema."""
    example_response_path = Path(__file__).parent / "example_contacts_properties_response.json"
    with open(example_response_path) as f:
        raw_response = json.load(f)

    properties = Properties.model_validate(raw_response)
    schema = BaseCRMObject.sourced_schema(properties.results)

    # Raises SchemaError if the schema is invalid.
    Draft7Validator.check_schema(schema)


def test_sourced_schema_structure():
    """Verify the sourced schema has the expected structure."""
    example_response_path = Path(__file__).parent / "example_contacts_properties_response.json"
    with open(example_response_path) as f:
        raw_response = json.load(f)

    properties = Properties.model_validate(raw_response)
    schema = BaseCRMObject.sourced_schema(properties.results)

    # Top-level structure
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert "properties" in schema

    # The schema should have a "properties" key that contains the schema for the
    # CRM object's properties field.
    properties_field_schema = schema["properties"]["properties"]
    assert properties_field_schema["type"] == "object"
    assert properties_field_schema["additionalProperties"] is False
    assert "properties" in properties_field_schema
