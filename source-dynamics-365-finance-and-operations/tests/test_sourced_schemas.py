import logging

from source_dynamics_365_finance_and_operations.models import (
    ModelDotJson,
    model_from_entity,
)


log = logging.getLogger("test")


def make_attribute(name: str, dataType: str, maxLength: int = -1) -> ModelDotJson.Entity.Attribute:
    return ModelDotJson.Entity.Attribute(name=name, dataType=dataType, maxLength=maxLength)


def make_entity(name: str, attributes: list[ModelDotJson.Entity.Attribute]) -> ModelDotJson.Entity:
    return ModelDotJson.Entity(**{"$type": "LocalEntity", "name": name, "description": name, "attributes": attributes})


class TestSourcedSchema:
    def test_string_with_max_length(self):
        entity = make_entity("test", [make_attribute("Name", "string", maxLength=50)])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        assert schema["properties"]["Name"] == {"type": "string", "minLength": 50, "maxLength": 50}

    def test_string_without_max_length(self):
        entity = make_entity("test", [make_attribute("Name", "string", maxLength=-1)])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        assert schema["properties"]["Name"] == {"type": "string", "minLength": 1, "maxLength": 1}

    def test_int64(self):
        entity = make_entity("test", [make_attribute("Count", "int64")])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        assert schema["properties"]["Count"] == {"type": "string", "format": "integer", "minLength": 1, "maxLength": 1}

    def test_decimal(self):
        entity = make_entity("test", [make_attribute("Amount", "decimal")])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        assert schema["properties"]["Amount"] == {"type": "string", "format": "number", "minLength": 1, "maxLength": 1}

    def test_datetime(self):
        entity = make_entity("test", [make_attribute("CreatedDate", "dateTime")])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        assert schema["properties"]["CreatedDate"] == {"type": "string", "format": "date-time", "minLength": 28, "maxLength": 28}

    def test_datetime_sink_fields(self):
        entity = make_entity("test", [
            make_attribute("SinkCreatedOn", "dateTime"),
            make_attribute("SinkModifiedOn", "dateTime"),
        ])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        expected = {"type": "string", "minLength": 19, "maxLength": 22}
        assert schema["properties"]["SinkCreatedOn"] == expected
        assert schema["properties"]["SinkModifiedOn"] == expected

    def test_datetime_offset(self):
        entity = make_entity("test", [make_attribute("CreatedOn", "dateTimeOffset")])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        assert schema["properties"]["CreatedOn"] == {"type": "string", "format": "date-time", "minLength": 33, "maxLength": 33}

    def test_guid(self):
        entity = make_entity("test", [make_attribute("Id", "guid")])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        assert schema["properties"]["Id"] == {"type": "string", "format": "uuid", "minLength": 36, "maxLength": 36}

    def test_boolean(self):
        entity = make_entity("test", [make_attribute("IsDelete", "boolean")])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        assert schema["properties"]["IsDelete"] == {"type": "boolean"}

    def test_unknown_type_skipped(self, caplog):
        entity = make_entity("test", [
            make_attribute("Name", "string", maxLength=10),
            make_attribute("Unknown", "complexType"),
        ])
        model = model_from_entity(entity)

        with caplog.at_level(logging.WARNING):
            schema = model.sourced_schema(log)

        assert "Unknown" not in schema["properties"]
        assert "Unknown" not in schema["required"]
        assert "Name" in schema["properties"]
        assert "Name" in schema["required"]
        assert "Omitting attribute 'Unknown' from sourced schema for entity 'test' due to unknown dataType 'complexType'" in caplog.text

    def test_schema_structure(self):
        entity = make_entity("test", [make_attribute("Name", "string", maxLength=10)])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        assert schema["additionalProperties"] is False
        assert schema["type"] == "object"
        assert "properties" in schema
        assert schema["required"] == ["Name"]

    def test_all_types_together(self):
        entity = make_entity("test", [
            make_attribute("Id", "guid"),
            make_attribute("SinkCreatedOn", "dateTime"),
            make_attribute("SinkModifiedOn", "dateTime"),
            make_attribute("Name", "string", maxLength=20),
            make_attribute("Count", "int64"),
            make_attribute("Amount", "decimal"),
            make_attribute("CreatedDate", "dateTime"),
            make_attribute("CreatedOn", "dateTimeOffset"),
            make_attribute("IsActive", "boolean"),
        ])
        model = model_from_entity(entity)

        schema = model.sourced_schema(log)

        assert len(schema["properties"]) == 9
        assert len(schema["required"]) == 9
        assert schema["properties"]["Id"]["format"] == "uuid"
        assert schema["properties"]["SinkCreatedOn"] == {"type": "string", "minLength": 19, "maxLength": 22}
        assert schema["properties"]["SinkModifiedOn"] == {"type": "string", "minLength": 19, "maxLength": 22}
        assert schema["properties"]["Name"] == {"type": "string", "minLength": 20, "maxLength": 20}
        assert schema["properties"]["Count"]["format"] == "integer"
        assert schema["properties"]["Amount"]["format"] == "number"
        assert schema["properties"]["CreatedDate"]["format"] == "date-time"
        assert schema["properties"]["CreatedOn"]["format"] == "date-time"
        assert schema["properties"]["IsActive"] == {"type": "boolean"}
