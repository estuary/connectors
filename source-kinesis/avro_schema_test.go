package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAvroToJSONSchema_PrimitiveTypes(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "intField", "type": "int"},
			{"name": "longField", "type": "long"},
			{"name": "floatField", "type": "float"},
			{"name": "doubleField", "type": "double"},
			{"name": "stringField", "type": "string"},
			{"name": "booleanField", "type": "boolean"},
			{"name": "bytesField", "type": "bytes"}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	assert.Equal(t, "object", result["type"])

	properties := result["properties"].(map[string]interface{})

	// int and long should map to integer
	assert.Equal(t, map[string]interface{}{"type": "integer"}, properties["intField"])
	assert.Equal(t, map[string]interface{}{"type": "integer"}, properties["longField"])

	// float and double should map to number
	assert.Equal(t, map[string]interface{}{"type": "number"}, properties["floatField"])
	assert.Equal(t, map[string]interface{}{"type": "number"}, properties["doubleField"])

	// string and boolean should keep their type names
	assert.Equal(t, map[string]interface{}{"type": "string"}, properties["stringField"])
	assert.Equal(t, map[string]interface{}{"type": "boolean"}, properties["booleanField"])

	// bytes should be string with base64 encoding (matching source-kafka)
	assert.Equal(t, map[string]interface{}{"type": "string", "contentEncoding": "base64"}, properties["bytesField"])

	// All primitive fields should be required
	required := result["required"].([]string)
	assert.Len(t, required, 7)
	assert.Contains(t, required, "intField")
	assert.Contains(t, required, "longField")
	assert.Contains(t, required, "floatField")
	assert.Contains(t, required, "doubleField")
	assert.Contains(t, required, "stringField")
	assert.Contains(t, required, "booleanField")
	assert.Contains(t, required, "bytesField")
}

func TestAvroToJSONSchema_NullableTypes(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "requiredString", "type": "string"},
			{"name": "nullableString", "type": ["null", "string"]},
			{"name": "nullableInt", "type": ["null", "int"]},
			{"name": "nullableDouble", "type": ["null", "double"]}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	properties := result["properties"].(map[string]interface{})

	// Required field
	assert.Equal(t, map[string]interface{}{"type": "string"}, properties["requiredString"])

	// Nullable fields should have the inner type (without null)
	assert.Equal(t, map[string]interface{}{"type": "string"}, properties["nullableString"])
	assert.Equal(t, map[string]interface{}{"type": "integer"}, properties["nullableInt"])
	assert.Equal(t, map[string]interface{}{"type": "number"}, properties["nullableDouble"])

	// Only requiredString should be in required array
	required := result["required"].([]string)
	assert.Len(t, required, 1)
	assert.Contains(t, required, "requiredString")
	assert.NotContains(t, required, "nullableString")
	assert.NotContains(t, required, "nullableInt")
	assert.NotContains(t, required, "nullableDouble")
}

func TestAvroToJSONSchema_ArrayTypes(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "stringArray", "type": {"type": "array", "items": "string"}},
			{"name": "intArray", "type": {"type": "array", "items": "int"}},
			{"name": "doubleArray", "type": {"type": "array", "items": "double"}}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	properties := result["properties"].(map[string]interface{})

	// Array of strings
	stringArray := properties["stringArray"].(map[string]interface{})
	assert.Equal(t, "array", stringArray["type"])
	assert.Equal(t, map[string]interface{}{"type": "string"}, stringArray["items"])

	// Array of ints
	intArray := properties["intArray"].(map[string]interface{})
	assert.Equal(t, "array", intArray["type"])
	assert.Equal(t, map[string]interface{}{"type": "integer"}, intArray["items"])

	// Array of doubles
	doubleArray := properties["doubleArray"].(map[string]interface{})
	assert.Equal(t, "array", doubleArray["type"])
	assert.Equal(t, map[string]interface{}{"type": "number"}, doubleArray["items"])

	// All array fields should be required
	required := result["required"].([]string)
	assert.Len(t, required, 3)
}

func TestAvroToJSONSchema_NullableArray(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "nullableArray", "type": ["null", {"type": "array", "items": "string"}]}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	properties := result["properties"].(map[string]interface{})

	// Nullable array should have the array type
	nullableArray := properties["nullableArray"].(map[string]interface{})
	assert.Equal(t, "array", nullableArray["type"])
	assert.Equal(t, map[string]interface{}{"type": "string"}, nullableArray["items"])

	// Should not be required
	required := result["required"].([]string)
	assert.Len(t, required, 0)
}

func TestAvroToJSONSchema_EmptyRecord(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "EmptyRecord",
		"fields": []
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	assert.Equal(t, "object", result["type"])
	properties := result["properties"].(map[string]interface{})
	assert.Len(t, properties, 0)
	required := result["required"].([]string)
	assert.Len(t, required, 0)
}

func TestAvroToJSONSchema_MixedRequiredAndOptional(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "MixedRecord",
		"fields": [
			{"name": "id", "type": "long"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": ["null", "string"]},
			{"name": "age", "type": ["null", "int"]},
			{"name": "score", "type": "double"}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	required := result["required"].([]string)
	assert.Len(t, required, 3)
	assert.Contains(t, required, "id")
	assert.Contains(t, required, "name")
	assert.Contains(t, required, "score")
	assert.NotContains(t, required, "email")
	assert.NotContains(t, required, "age")
}

func TestAvroToJSONSchema_InvalidJSON(t *testing.T) {
	avroSchema := `{invalid json}`

	_, err := avroToJSONSchema(avroSchema)
	assert.Error(t, err)
}

func TestAvroToJSONSchema_ComplexSchema(t *testing.T) {
	// A more realistic schema similar to what Glue might return
	avroSchema := `{
		"type": "record",
		"name": "UserEvent",
		"namespace": "com.example.events",
		"fields": [
			{"name": "event_id", "type": "string"},
			{"name": "user_id", "type": "long"},
			{"name": "timestamp", "type": "long"},
			{"name": "event_type", "type": "string"},
			{"name": "payload", "type": ["null", "string"]},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "metrics", "type": ["null", {"type": "array", "items": "double"}]}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	// Verify structure
	assert.Equal(t, "object", result["type"])

	properties := result["properties"].(map[string]interface{})
	assert.Len(t, properties, 7)

	// Check specific fields
	assert.Equal(t, map[string]interface{}{"type": "string"}, properties["event_id"])
	assert.Equal(t, map[string]interface{}{"type": "integer"}, properties["user_id"])
	assert.Equal(t, map[string]interface{}{"type": "integer"}, properties["timestamp"])
	assert.Equal(t, map[string]interface{}{"type": "string"}, properties["payload"])

	tags := properties["tags"].(map[string]interface{})
	assert.Equal(t, "array", tags["type"])

	metrics := properties["metrics"].(map[string]interface{})
	assert.Equal(t, "array", metrics["type"])
	assert.Equal(t, map[string]interface{}{"type": "number"}, metrics["items"])

	// Check required fields
	required := result["required"].([]string)
	assert.Len(t, required, 5)
	assert.Contains(t, required, "event_id")
	assert.Contains(t, required, "user_id")
	assert.Contains(t, required, "timestamp")
	assert.Contains(t, required, "event_type")
	assert.Contains(t, required, "tags")
}

func TestConvertAvroType_UnknownType(t *testing.T) {
	// Unknown complex types should fall back to string
	result, nullable := convertAvroType(map[string]interface{}{
		"type": "unknown_type",
	})

	assert.Equal(t, map[string]interface{}{"type": "string"}, result)
	assert.False(t, nullable) // Unknown types in maps are not automatically nullable
}

func TestConvertAvroType_NestedArray(t *testing.T) {
	// Array of arrays (nested)
	result, nullable := convertAvroType(map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type":  "array",
			"items": "int",
		},
	})

	assert.False(t, nullable)
	assert.Equal(t, "array", result["type"])

	items := result["items"].(map[string]interface{})
	assert.Equal(t, "array", items["type"])
	assert.Equal(t, map[string]interface{}{"type": "integer"}, items["items"])
}

func TestAvroToJSONSchema_OutputIsValidJSON(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": ["null", "string"]}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	// Verify the result can be marshaled to JSON
	jsonBytes, err := json.Marshal(result)
	require.NoError(t, err)
	assert.NotEmpty(t, jsonBytes)

	// Verify it can be unmarshaled back
	var unmarshaled map[string]interface{}
	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)
	assert.Equal(t, "object", unmarshaled["type"])
}

func TestAvroToJSONSchema_LogicalTypes(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "dateField", "type": {"type": "int", "logicalType": "date"}},
			{"name": "timeMillisField", "type": {"type": "int", "logicalType": "time-millis"}},
			{"name": "timeMicrosField", "type": {"type": "long", "logicalType": "time-micros"}},
			{"name": "timestampMillisField", "type": {"type": "long", "logicalType": "timestamp-millis"}},
			{"name": "timestampMicrosField", "type": {"type": "long", "logicalType": "timestamp-micros"}},
			{"name": "uuidField", "type": {"type": "string", "logicalType": "uuid"}},
			{"name": "decimalField", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	properties := result["properties"].(map[string]interface{})

	// Date should have format: date
	assert.Equal(t, map[string]interface{}{"type": "string", "format": "date"}, properties["dateField"])

	// Time should have format: time
	assert.Equal(t, map[string]interface{}{"type": "string", "format": "time"}, properties["timeMillisField"])
	assert.Equal(t, map[string]interface{}{"type": "string", "format": "time"}, properties["timeMicrosField"])

	// Timestamp should have format: date-time
	assert.Equal(t, map[string]interface{}{"type": "string", "format": "date-time"}, properties["timestampMillisField"])
	assert.Equal(t, map[string]interface{}{"type": "string", "format": "date-time"}, properties["timestampMicrosField"])

	// UUID should have format: uuid
	assert.Equal(t, map[string]interface{}{"type": "string", "format": "uuid"}, properties["uuidField"])

	// Decimal should have format: number
	assert.Equal(t, map[string]interface{}{"type": "string", "format": "number"}, properties["decimalField"])
}

func TestAvroToJSONSchema_MapType(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "stringMap", "type": {"type": "map", "values": "string"}},
			{"name": "intMap", "type": {"type": "map", "values": "int"}}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	properties := result["properties"].(map[string]interface{})

	// Map should become object with additionalProperties
	stringMap := properties["stringMap"].(map[string]interface{})
	assert.Equal(t, "object", stringMap["type"])
	assert.Equal(t, map[string]interface{}{"type": "string"}, stringMap["additionalProperties"])

	intMap := properties["intMap"].(map[string]interface{})
	assert.Equal(t, "object", intMap["type"])
	assert.Equal(t, map[string]interface{}{"type": "integer"}, intMap["additionalProperties"])
}

func TestAvroToJSONSchema_EnumType(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["PENDING", "ACTIVE", "COMPLETED"]}}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	properties := result["properties"].(map[string]interface{})

	// Enum should become string with enum values
	status := properties["status"].(map[string]interface{})
	assert.Equal(t, "string", status["type"])
	assert.Equal(t, []string{"PENDING", "ACTIVE", "COMPLETED"}, status["enum"])
}

func TestAvroToJSONSchema_NestedRecord(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "OuterRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "nested", "type": {
				"type": "record",
				"name": "InnerRecord",
				"fields": [
					{"name": "innerField1", "type": "string"},
					{"name": "innerField2", "type": "int"}
				]
			}}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	properties := result["properties"].(map[string]interface{})

	// Check outer fields
	assert.Equal(t, map[string]interface{}{"type": "integer"}, properties["id"])

	// Check nested record
	nested := properties["nested"].(map[string]interface{})
	assert.Equal(t, "object", nested["type"])

	nestedProps := nested["properties"].(map[string]interface{})
	assert.Equal(t, map[string]interface{}{"type": "string"}, nestedProps["innerField1"])
	assert.Equal(t, map[string]interface{}{"type": "integer"}, nestedProps["innerField2"])

	nestedRequired := nested["required"].([]string)
	assert.Contains(t, nestedRequired, "innerField1")
	assert.Contains(t, nestedRequired, "innerField2")
}

func TestAvroToJSONSchema_FixedType(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "fixedField", "type": {"type": "fixed", "name": "md5", "size": 16}}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	properties := result["properties"].(map[string]interface{})

	// Fixed should become string with base64 encoding
	assert.Equal(t, map[string]interface{}{"type": "string", "contentEncoding": "base64"}, properties["fixedField"])
}

func TestAvroToJSONSchema_DefaultValues(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "requiredField", "type": "string"},
			{"name": "fieldWithDefault", "type": "string", "default": "defaultValue"},
			{"name": "intWithDefault", "type": "int", "default": 0}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	required := result["required"].([]string)

	// Only the field without a default should be required
	assert.Len(t, required, 1)
	assert.Contains(t, required, "requiredField")
	assert.NotContains(t, required, "fieldWithDefault")
	assert.NotContains(t, required, "intWithDefault")
}

func TestAvroToJSONSchema_NullableMap(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "nullableMap", "type": ["null", {"type": "map", "values": "string"}]}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	properties := result["properties"].(map[string]interface{})

	// Nullable map should still have the map structure
	nullableMap := properties["nullableMap"].(map[string]interface{})
	assert.Equal(t, "object", nullableMap["type"])
	assert.Equal(t, map[string]interface{}{"type": "string"}, nullableMap["additionalProperties"])

	// Should not be required
	required := result["required"].([]string)
	assert.Len(t, required, 0)
}

func TestAvroToJSONSchema_ArrayOfRecords(t *testing.T) {
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "items", "type": {"type": "array", "items": {
				"type": "record",
				"name": "Item",
				"fields": [
					{"name": "name", "type": "string"},
					{"name": "quantity", "type": "int"}
				]
			}}}
		]
	}`

	result, err := avroToJSONSchema(avroSchema)
	require.NoError(t, err)

	properties := result["properties"].(map[string]interface{})

	items := properties["items"].(map[string]interface{})
	assert.Equal(t, "array", items["type"])

	itemsSchema := items["items"].(map[string]interface{})
	assert.Equal(t, "object", itemsSchema["type"])

	itemProps := itemsSchema["properties"].(map[string]interface{})
	assert.Equal(t, map[string]interface{}{"type": "string"}, itemProps["name"])
	assert.Equal(t, map[string]interface{}{"type": "integer"}, itemProps["quantity"])
}

func TestAvroTypeToJSONSchema(t *testing.T) {
	tests := []struct {
		avroType    string
		logicalType string
		expected    map[string]interface{}
	}{
		{"int", "", map[string]interface{}{"type": "integer"}},
		{"long", "", map[string]interface{}{"type": "integer"}},
		{"float", "", map[string]interface{}{"type": "number"}},
		{"double", "", map[string]interface{}{"type": "number"}},
		{"string", "", map[string]interface{}{"type": "string"}},
		{"boolean", "", map[string]interface{}{"type": "boolean"}},
		{"bytes", "", map[string]interface{}{"type": "string", "contentEncoding": "base64"}},
		{"null", "", map[string]interface{}{"type": "null"}},
		{"int", "date", map[string]interface{}{"type": "string", "format": "date"}},
		{"int", "time-millis", map[string]interface{}{"type": "string", "format": "time"}},
		{"long", "time-micros", map[string]interface{}{"type": "string", "format": "time"}},
		{"long", "timestamp-millis", map[string]interface{}{"type": "string", "format": "date-time"}},
		{"long", "timestamp-micros", map[string]interface{}{"type": "string", "format": "date-time"}},
		{"string", "uuid", map[string]interface{}{"type": "string", "format": "uuid"}},
		{"bytes", "decimal", map[string]interface{}{"type": "string", "format": "number"}},
	}

	for _, tt := range tests {
		name := tt.avroType
		if tt.logicalType != "" {
			name = tt.avroType + "/" + tt.logicalType
		}
		t.Run(name, func(t *testing.T) {
			result := avroTypeToJSONSchema(tt.avroType, tt.logicalType)
			assert.Equal(t, tt.expected, result)
		})
	}
}
