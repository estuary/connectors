package main

import (
	"fmt"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/invopop/jsonschema"
)

// CockroachDB reports column types through pg_catalog using PostgreSQL type names (INT8 →
// "int8", STRING → "text", DECIMAL → "numeric", etc.). This mapping is only used for discovery:
// captured values are never decoded, since the changefeed and `to_jsonb` both deliver
// CockroachDB-native JSON directly (see change.go).

type columnSchema struct {
	contentEncoding string
	format          string
	nullable        bool
	jsonTypes       []string
}

func (s columnSchema) toType() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format: s.format,
		Extras: make(map[string]any),
	}
	if s.contentEncoding != "" {
		out.Extras["contentEncoding"] = s.contentEncoding // New in 2019-09.
	}
	if s.jsonTypes != nil {
		var types = append([]string(nil), s.jsonTypes...)
		if s.nullable {
			types = append(types, "null")
		}
		if len(types) == 1 {
			out.Type = types[0]
		} else {
			out.Extras["type"] = types
		}
	}
	return out
}

// lookupColumnSchema maps a type name to its JSON schema, describing the value as CockroachDB
// actually emits it (no primary-key-specific stringification, unlike the PostgreSQL connector).
func lookupColumnSchema(typeName string) (columnSchema, error) {
	if s, ok := cockroachdbTypeToJSON[typeName]; ok {
		return s, nil
	}
	return columnSchema{}, fmt.Errorf("unable to translate CockroachDB type %q into JSON schema", typeName)
}

var cockroachdbTypeToJSON = map[string]columnSchema{
	"bool": {jsonTypes: []string{"boolean"}},

	"int2": {jsonTypes: []string{"integer"}},
	"int4": {jsonTypes: []string{"integer"}},
	"int8": {jsonTypes: []string{"integer"}},
	"oid":  {jsonTypes: []string{"integer"}},

	// CockroachDB encodes DECIMAL and FLOAT values as JSON numbers in both `to_jsonb` and the
	// changefeed, so we accept "number" while also permitting "string" for the non-finite and
	// out-of-range cases that JSON numbers can't represent.
	"numeric": {jsonTypes: []string{"number", "string"}, format: "number"},
	"float4":  {jsonTypes: []string{"number", "string"}, format: "number"},
	"float8":  {jsonTypes: []string{"number", "string"}, format: "number"},

	"varchar": {jsonTypes: []string{"string"}},
	"bpchar":  {jsonTypes: []string{"string"}},
	"text":    {jsonTypes: []string{"string"}},
	"bytea":   {jsonTypes: []string{"string"}},
	"bit":     {jsonTypes: []string{"string"}},
	"varbit":  {jsonTypes: []string{"string"}},

	"json":  {},
	"jsonb": {},

	// TIMESTAMP and TIME (without time zone) deliberately skip the "date-time"/"time" formats:
	// those require a timezone offset, and CockroachDB's naive-type JSON rendering has none
	// (e.g. "2026-01-01T00:00:00"), which would fail collection-schema validation.
	"date":        {jsonTypes: []string{"string"}, format: "date"},
	"timestamp":   {jsonTypes: []string{"string"}},
	"timestamptz": {jsonTypes: []string{"string"}, format: "date-time"},
	"time":        {jsonTypes: []string{"string"}},
	"timetz":      {jsonTypes: []string{"string"}, format: "time"},
	"interval":    {jsonTypes: []string{"string"}},
	"inet":        {jsonTypes: []string{"string"}},
	"uuid":        {jsonTypes: []string{"string"}, format: "uuid"},
}

// cockroachdbTypeDescription describes a discovered column type for JSON schema generation.
type cockroachdbTypeDescription interface {
	TypeName() string
	String() string
	JSONSchema(isColumnNullable, isPrimaryKey bool) (*jsonschema.Schema, error)
}

type cockroachdbBasicType struct {
	Name string
}

func (t cockroachdbBasicType) String() string   { return t.Name }
func (t cockroachdbBasicType) TypeName() string { return t.Name }
func (t cockroachdbBasicType) JSONSchema(isColumnNullable, isPrimaryKey bool) (*jsonschema.Schema, error) {
	var colSchema, err = lookupColumnSchema(t.Name)
	if err != nil {
		return nil, err
	}
	colSchema.nullable = isColumnNullable
	return colSchema.toType(), nil
}

type cockroachdbEnumType struct{}

func (t cockroachdbEnumType) String() string   { return "enum" }
func (t cockroachdbEnumType) TypeName() string { return "enum" }
func (t cockroachdbEnumType) JSONSchema(isColumnNullable, isPrimaryKey bool) (*jsonschema.Schema, error) {
	return columnSchema{jsonTypes: []string{"string"}, nullable: isColumnNullable}.toType(), nil
}

type cockroachdbArrayType struct {
	Items cockroachdbTypeDescription
}

func (t cockroachdbArrayType) String() string   { return "_" + t.Items.String() }
func (t cockroachdbArrayType) TypeName() string { return "_" + t.Items.TypeName() }
func (t cockroachdbArrayType) JSONSchema(isColumnNullable, isPrimaryKey bool) (*jsonschema.Schema, error) {
	// CockroachDB array values are emitted as flat JSON arrays. Elements are always nullable
	// inside an array regardless of the column's own nullability.
	var elemSchema, err = t.Items.JSONSchema(true, isPrimaryKey)
	if err != nil {
		return nil, err
	}
	var jsonType = &jsonschema.Schema{
		Items:  elemSchema,
		Extras: map[string]any{"type": "array"},
	}
	if isColumnNullable {
		jsonType.Extras["type"] = []string{"array", "null"}
	}
	return jsonType, nil
}

// encodeKeyFDB lowers a backfill key-column value into an FDB tuple element. Values arrive as
// CockroachDB's canonical text rendering (or nil), per the `::STRING` projection in
// buildScanQuery, which round-trips losslessly as a resume-predicate bind argument. Because this
// connector always uses unfiltered backfill streaming, the encoded key only needs to round-trip;
// its byte ordering never has to reproduce the database's sort order.
func encodeKeyFDB(v any) (tuple.TupleElement, error) {
	switch x := v.(type) {
	case nil, string:
		return x, nil
	default:
		return nil, fmt.Errorf("internal error: backfill key value %v has unexpected type %T", v, v)
	}
}

// decodeKeyFDB reverses encodeKeyFDB. The FDB tuple package already decodes elements into their
// natural Go types (int64, string, []byte, etc.), so we pass them through unchanged for binding.
func decodeKeyFDB(t tuple.TupleElement) (any, error) {
	return t, nil
}
