package schemabuilder

import (
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

const schemaURI = "test://example/int-string-len.schema"
const schemaJSON = `
	  { "$defs": {
                "__flowInline1":{
                    "$defs":{
                        "anAnchor": {
                            "$anchor": "AnAnchor",
                            "properties": {
                                "one":{"type": "string"},
                                "two":{"type": "integer"}
                            },
                            "required":["one"],
                            "type":"object"
                        }
                    },
                    "$id": "test://example/int-string.schema",
                    "properties": {
                        "bit": { "type": "boolean" },
                        "int": { "type": "integer" },
                        "str": { "type": "string" }
                    },
                    "required": ["int", "str", "bit"], "type": "object"
                }
            },
            "$id": "test://example/int-string-len.schema",
            "$ref": "test://example/int-string.schema",
            "properties": {
                "arr":{
                    "items":{"$ref": "int-string.schema#AnAnchor"},
                    "type":"array"
                },
                "len":{"type": "integer"}
            },
            "required":["len"]
        }
`

func TestRunSchemaBuilder_NoOverrides(t *testing.T) {
	result, e := RunSchemaBuilder(schemaURI, json.RawMessage(schemaJSON), []FieldOverride{})
	require.NoError(t, e)
	cupaloy.SnapshotT(t, string(result))
}

func TestRunSchemaBuilder_WithOverrides(t *testing.T) {
	var overrides = []FieldOverride{
		{Pointer: "/arr/one", EsType: ElasticFieldType{
			FieldType: "date", DateSpec: DateSpec{Format: "test_format"}}},
		{Pointer: "/bit", EsType: ElasticFieldType{
			FieldType: "keyword", KeywordSpec: KeywordSpec{IgnoreAbove: 500, DualText: true}}},
	}

	result, e := RunSchemaBuilder(schemaURI, json.RawMessage(schemaJSON), overrides)
	require.NoError(t, e)
	cupaloy.SnapshotT(t, string(result))
}

func TestRunSchemaBuilder_Errors(t *testing.T) {
	_, e := RunSchemaBuilder("", json.RawMessage("{}"), []FieldOverride{})
	require.Contains(t, e.Error(), "Failed parsing schema_uri")

	var corruptedSchema = "corrupted schema"
	_, e = RunSchemaBuilder(schemaURI, json.RawMessage(corruptedSchema), []FieldOverride{})
	require.Contains(t, e.Error(), "Failed generating elastic search schema based on input")

	var badOverrides = []FieldOverride{
		{Pointer: "/arr/nonexisting_field", EsType: ElasticFieldType{FieldType: "text"}},
	}
	_, e = RunSchemaBuilder(schemaURI, json.RawMessage(schemaJSON), badOverrides)
	require.Contains(t, e.Error(), "pointer of a non-existing field")
}
