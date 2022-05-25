package schemagen

import (
	"encoding/json"
	"reflect"

	"github.com/alecthomas/jsonschema"
)

func GenerateSchema(title string, configObject interface{}) *jsonschema.Schema {
	// By default, the library generates schemas with a top-level $ref that references a definition.
	// That breaks UI code that tries to generate forms from the schemas, and is just weird and
	// silly anyway. While we're at it, we just disable references altogether, since they tend to
	// hurt readability more than they help for these schemas.
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var schema = reflector.ReflectFromType(reflect.TypeOf(configObject))
	schema.AdditionalProperties = json.RawMessage("true")
	schema.Title = title
	return schema
}
