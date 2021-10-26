package testcat

import (
	"reflect"
	"strings"
	"time"
)

// CustomSchema is an interface that allows a struct to build it's own schema
// rather than use the default schema builder.
type CustomBuildSchema interface {
	BuildSchema() (TestSchema, []string, error)
}

// BuildSchema parses a struct and builds the flow.yaml schema representation along with it's keys.
// It can handle just about any struct. To determine the name of the fields in the schema, you can use the
// flowsim tag to set the name. You can also use the 'key' tag option to set the struct field as a key and
// 'required' to set it as a required field. (keys are automatically required) See testdata.Basic for
// and example struct.
func BuildSchema(in interface{}) (TestSchema, []string, error) {

	// If it supports the CustomBuildSchema interface, use it.
	if cbs, ok := in.(CustomBuildSchema); ok {
		return cbs.BuildSchema()
	}

	var schema TestSchema
	var keys []string
	var err error

	// Handle any special built-in types such as time.Time.
	switch in.(type) {
	case time.Time:
		schema.Type = []string{"string"}
		schema.Format = "date-time"
		return schema, keys, err
	}

	value := reflect.ValueOf(in)
	valueKind := value.Kind()

	// Dereference if a pointer.
	if (valueKind == reflect.Ptr && !value.IsNil()) || valueKind == reflect.Interface {
		value = value.Elem()
		valueKind = value.Kind()
	}

	switch valueKind {
	// Support maps, but it won't have any keys.
	case reflect.Map:
		schema.Type = []string{"object"}
		schema.Properties = make(map[string]TestSchema)
		for _, mapKey := range value.MapKeys() {
			if schema.Properties[mapKey.String()], _, err = BuildSchema(value.MapIndex(mapKey).Interface()); err != nil {
				return schema, keys, err
			}
		}
	// For structs, parse all the fields.
	case reflect.Struct:
		schema.Type = []string{"object"}
		schema.Properties = make(map[string]TestSchema)
		for i := 0; i < value.NumField(); i++ {
			structKey := value.Type().Field(i)

			// Non exported fields should be skipped.
			if structKey.PkgPath != "" {
				continue
			}

			// Parse the flowsim tags to find required fields and keys.
			fieldName := structKey.Tag.Get("flowsim")
			tagOptions := []string{}
			if fieldName == "" {
				fieldName = structKey.Name
			} else {
				// Get the field name and tagOptions.
				if idx := strings.Index(fieldName, ","); idx != -1 {
					tagOptions = strings.Split(fieldName[idx+1:], ",")
					fieldName = fieldName[:idx]
				}
				// Skipped field.
				if fieldName == "-" {
					continue
				}
			}
			// If it's a key, mark it as key and as required.
			if optionsHas(tagOptions, "key") {
				keys = append(keys, "/"+fieldName)
				schema.Required = append(schema.Required, fieldName)
			} else if optionsHas(tagOptions, "required") {
				// Mark as required
				schema.Required = append(schema.Required, fieldName)
			}
			// Parse the field properties (as well as child objects).
			var fieldKeys []string
			if schema.Properties[fieldName], fieldKeys, err = BuildSchema(value.Field(i).Interface()); err != nil {
				return schema, keys, err
			}
			// Any child keys need to be appended to the keys slice.
			for _, key := range fieldKeys {
				keys = append(keys, "/"+fieldName+key)
			}
		}
	default:
		// All other types, use reflect.Kind.
		schema.Type = SchemaType(valueKind)
	}

	return schema, keys, nil

}

// optionHas is a helper to parse struct tags and see if it has an option.
func optionsHas(options []string, value string) bool {
	for _, s := range options {
		if s == value {
			return true
		}
	}
	return false
}

// SchemaType returns the json type for a field.
func SchemaType(t reflect.Kind) []string {

	switch t {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return []string{"integer"}
	case reflect.Array, reflect.Slice:
		return []string{"array"}
	case reflect.Float32, reflect.Float64:
		return []string{"number"}
	case reflect.Bool:
		return []string{"boolean"}
	case reflect.String:
		return []string{"string"}
	}
	return []string{"unknown"}

}

// BuildCollection is a helper to automatically build a schema, parse keys and return a Collection.
func BuildCollection(in interface{}) (TestCollection, error) {
	var c TestCollection
	var err error
	if c.Schema, c.Keys, err = BuildSchema(in); err != nil {
		return c, err
	}
	return c, err
}
