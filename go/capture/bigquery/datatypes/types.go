package datatypes

import "github.com/invopop/jsonschema"

// ColumnType describes a BigQuery column type for the purposes of generating a
// JSON Schema fragment.
type ColumnType interface {
	IsNullable() bool
	JSONSchema() *jsonschema.Schema
}

// BasicColumnType is the default ColumnType implementation for BigQuery types
// which can be expressed as a fixed JSON Schema fragment.
type BasicColumnType struct {
	JSONTypes       []string
	ContentEncoding string
	Format          string
	Nullable        bool
	Description     string
	MinLength       *uint64
	MaxLength       *uint64
}

func (ct *BasicColumnType) IsNullable() bool {
	return ct.Nullable
}

func (ct *BasicColumnType) JSONSchema() *jsonschema.Schema {
	var sch = &jsonschema.Schema{
		Format:      ct.Format,
		Extras:      make(map[string]any),
		Description: ct.Description,
	}

	if ct.ContentEncoding != "" {
		sch.Extras["contentEncoding"] = ct.ContentEncoding // New in 2019-09.
	}

	if ct.JSONTypes != nil {
		var types = append([]string(nil), ct.JSONTypes...)
		if ct.Nullable {
			types = append(types, "null")
		}
		if len(types) == 1 {
			sch.Type = types[0]
		} else {
			sch.Extras["type"] = types
		}
	}

	sch.MinLength = ct.MinLength
	sch.MaxLength = ct.MaxLength

	return sch
}

// LookupType returns the default BasicColumnType for the given BigQuery type
// name (e.g. "INT64", "STRING", "STRUCT"), or false if the type is unknown.
// The returned value is a copy; mutating it does not affect subsequent lookups.
func LookupType(typeName string) (BasicColumnType, bool) {
	t, ok := databaseTypeToJSON[typeName]
	return t, ok
}

var databaseTypeToJSON = map[string]BasicColumnType{
	"BOOL": {JSONTypes: []string{"boolean"}},

	"INT64": {JSONTypes: []string{"integer"}},

	"NUMERIC":    {JSONTypes: []string{"string"}, Format: "number"},
	"BIGNUMERIC": {JSONTypes: []string{"string"}, Format: "number"},
	"FLOAT64":    {JSONTypes: []string{"number", "string"}, Format: "number"},

	"STRING": {JSONTypes: []string{"string"}},

	"BYTES": {JSONTypes: []string{"string"}, ContentEncoding: "base64"},

	"DATE":      {JSONTypes: []string{"string"}, Format: "date"},
	"DATETIME":  {JSONTypes: []string{"string"}}, // Not RFC3339 date-time because it lacks a timezone suffix
	"TIME":      {JSONTypes: []string{"string"}}, // Not RFC3339 full-time because it lacks a timezone suffix
	"TIMESTAMP": {JSONTypes: []string{"string"}, Format: "date-time"},

	"JSON": {},

	"STRUCT": {JSONTypes: []string{"object"}},
	"ARRAY":  {JSONTypes: []string{"array"}},
}
