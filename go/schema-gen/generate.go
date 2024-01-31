package schemagen

import (
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"

	"github.com/invopop/jsonschema"
)

/// Note: This file is also duplicated in the flow repo for now. If you make
/// modifications to this file, you'll want to duplicate those edits. If you
/// don't, some materialization connectors will not receive those updates.

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
	schema.AdditionalProperties = nil // Unset means additional properties are permitted on the root object, as they should be
	schema.Definitions = nil          // Since no references are used, these definitions are just noise
	schema.Title = title
	walkSchema(
		schema,
		createOneOf,
		fixSchemaFlagBools(schema, "secret", "advanced", "multiline", "x-collection-name"),
		fixSchemaOrderingStrings,
	)

	return schema
}

// walkSchema invokes visit on every property of the root schema, and then traverses each of these
// sub-schemas recursively. The visit function should modify the provided schema in-place to
// accomplish the desired transformation.
func walkSchema(root *jsonschema.Schema, visits ...func(t *jsonschema.Schema)) {
	if root.Properties != nil {
		for pair := root.Properties.Oldest(); pair != nil; pair = pair.Next() {
			for _, visit := range visits {
				visit(pair.Value)
			}

			walkSchema(pair.Value, visits...)
		}
	}
}

func createOneOf(t *jsonschema.Schema) {
	for key, _ := range t.Extras {
		os.Stderr.WriteString(fmt.Sprintf("%v\n", key))
		if key == "oneOf" {
			os.Stderr.WriteString(fmt.Sprintf("oneOf found %v!\n", t.Properties.Len()))
			delete(t.Extras, "oneOf")
			var cleanup []string

			var groups = make(map[int]*jsonschema.Schema)
			var discriminator string
			for pair := t.Properties.Oldest(); pair != nil; pair = pair.Next() {
				var pkey = pair.Key
				var p = pair.Value
				os.Stderr.WriteString(fmt.Sprintf("%v=%v\n", pkey, p))
				var indices []int

				for k, v := range p.Extras {
					if k == "oneOf_group" {
						os.Stderr.WriteString(fmt.Sprintf("oneOf_group found!\n"))
						var idxStrings []string
						if str, ok := v.(string); ok {
							idxStrings = []string{str}
						} else if sli, ok := v.([]string); ok {
							idxStrings = sli
						}
						for _, i := range idxStrings {
							var index, err = strconv.Atoi(i)
							if err != nil {
								continue
							}
							indices = append(indices, index)
						}
						os.Stderr.WriteString(fmt.Sprintf("indices %v!\n", indices))
						delete(p.Extras, k)
					}

					if k == "oneOf_discriminator" {
						discriminator = pkey
						delete(p.Extras, k)
					}
				}
				var isRequiredIndex = slices.Index(t.Required, pkey)
				if len(indices) > 0 {
					t.Required = slices.Delete(t.Required, isRequiredIndex, 1)
				}

				for _, index := range indices {
					if _, ok := groups[index]; !ok {
						groups[index] = &jsonschema.Schema{}
						groups[index].Properties = jsonschema.NewProperties()
					}
					var group = groups[index]

					group.Properties.Set(pkey, p)
					if isRequiredIndex > -1 {
						group.Required = append(group.Required, pkey)
					}
					cleanup = append(cleanup, pkey)
				}
			}

			var groupSlice []*jsonschema.Schema
			for _, v := range groups {
				groupSlice = append(groupSlice, v)
			}
			t.OneOf = groupSlice

			if discriminator != "" {
				t.Extras["discriminator"] = map[string]string{
					"propertyName": discriminator,
				}
			}

			for _, k := range cleanup {
				t.Properties.Delete(k)
			}
		}
	}
}

func fixSchemaFlagBools(t *jsonschema.Schema, flagKeys ...string) func(t *jsonschema.Schema) {
	return func(t *jsonschema.Schema) {
		for key, val := range t.Extras {
			for _, flag := range flagKeys {
				if key != flag {
					continue
				} else if val == "true" {
					t.Extras[key] = true
				} else if val == "false" {
					t.Extras[key] = false
				}
			}
		}
	}
}

func fixSchemaOrderingStrings(t *jsonschema.Schema) {
	for key, val := range t.Extras {
		if key == "order" {
			if str, ok := val.(string); ok {
				converted, err := strconv.Atoi(str)
				if err != nil {
					// Don't try to convert strings that don't look like integers.
					continue
				}
				t.Extras[key] = converted
			}
		}
	}
}
