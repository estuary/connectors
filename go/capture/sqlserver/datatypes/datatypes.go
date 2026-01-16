package datatypes

import (
	"fmt"
	"slices"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/google/uuid"
	"github.com/invopop/jsonschema"
)

// Config holds configuration for SQL Server datatype handling.
type Config struct {
	// DatetimeLocation is the timezone used to interpret DATETIME column values.
	// SQL Server DATETIME columns are not timezone-aware, so we need to know
	// what timezone to assume when converting them to RFC3339 timestamps.
	DatetimeLocation *time.Location

	// RowversionAsAny controls whether TIMESTAMP/ROWVERSION columns are
	// discovered with the catch-all type {} instead of {type: string, contentEncoding: base64}.
	//
	// This exists for backwards compatibility with older CDC captures that predate
	// proper rowversion support. New connectors should leave this as false.
	RowversionAsAny bool
}

// TextColumnType describes a text/character column with collation information.
// Used to determine sort ordering behavior for primary keys.
type TextColumnType struct {
	Type      string // Basic type: char / varchar / nchar / nvarchar / text / ntext
	Collation string // Collation name used by the column
	FullType  string // Full type string like "varchar(32)"
	MaxLength int    // Maximum length of the column
}

func (t TextColumnType) String() string {
	return t.FullType
}

// BinaryColumnType describes a binary column.
type BinaryColumnType struct {
	Type      string // Basic type: binary / varbinary
	MaxLength int    // Maximum length in bytes
}

func (t BinaryColumnType) String() string {
	return fmt.Sprintf("%s(%d)", t.Type, t.MaxLength)
}

// columnSchema describes the JSON schema for a database column type.
type columnSchema struct {
	jsonType        string
	format          string
	contentEncoding string
	description     string
	nullable        bool
	minLength       *uint64
	maxLength       *uint64
}

func (s columnSchema) toSchema() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format:      s.format,
		Description: s.description,
		Extras:      make(map[string]any),
		MinLength:   s.minLength,
		MaxLength:   s.maxLength,
	}

	if s.contentEncoding != "" {
		out.Extras["contentEncoding"] = s.contentEncoding
	}

	if s.jsonType == "" {
		// No type constraint.
	} else if s.nullable {
		out.Extras["type"] = []string{s.jsonType, "null"}
	} else {
		out.Type = s.jsonType
	}
	return out
}

const (
	// SortableRFC3339Nano is a time format that produces strings which sort
	// correctly as bytes. The standard library time.RFC3339Nano is wrong for
	// historical reasons - this format always uses 9-digit fractional seconds.
	SortableRFC3339Nano = "2006-01-02T15:04:05.000000000Z07:00"

	// DatetimeKeyEncoding is the format for datetime values in row keys.
	// In order to be round-tripped successfully as a backfill key, datetimes
	// must be serialized with three decimal digits of precision and no timezone.
	DatetimeKeyEncoding = "2006-01-02T15:04:05.000"

	// TruncateColumnThreshold is the maximum size of a single column's value.
	// Values larger than this will be truncated. Set at 8MiB.
	TruncateColumnThreshold = 8 * 1024 * 1024

	// uniqueIdentifierTag is the FDB tuple tag for UUID values that have been
	// byte-shuffled to match SQL Server's sorting order.
	uniqueIdentifierTag = "uuid"

	// collatedTextTag is the FDB tuple tag for text values that include a
	// collation-aware sort key.
	collatedTextTag = "ctext"
)

// columnSchemaMap maps SQL Server type names to their JSON schema representation.
var columnSchemaMap = map[string]columnSchema{
	"bigint":   {jsonType: "integer"},
	"int":      {jsonType: "integer"},
	"smallint": {jsonType: "integer"},
	"tinyint":  {jsonType: "integer"},

	"numeric":    {jsonType: "string", format: "number"},
	"decimal":    {jsonType: "string", format: "number"},
	"money":      {jsonType: "string", format: "number"},
	"smallmoney": {jsonType: "string", format: "number"},

	"bit": {jsonType: "boolean"},

	"float": {jsonType: "number"},
	"real":  {jsonType: "number"},

	"char":     {jsonType: "string"},
	"varchar":  {jsonType: "string"},
	"text":     {jsonType: "string"},
	"nchar":    {jsonType: "string"},
	"nvarchar": {jsonType: "string"},
	"ntext":    {jsonType: "string"},

	"binary":    {jsonType: "string", contentEncoding: "base64"},
	"varbinary": {jsonType: "string", contentEncoding: "base64"},
	"image":     {jsonType: "string", contentEncoding: "base64"},

	// A 'timestamp' in SQL Server is not a timestamp as it's usually meant, it's
	// actually a monotonic integer ID and is also called 'rowversion'. These are
	// 8-byte binary values which we capture as base64-encoded strings.
	"timestamp": {jsonType: "string", contentEncoding: "base64"},

	"date":           {jsonType: "string", format: "date"},
	"datetimeoffset": {jsonType: "string", format: "date-time"},

	// The 'time' format in JSON schemas means the RFC3339 'full-time' grammar rule,
	// which includes a numeric timezone offset. The TIME column in SQL Server has
	// no associated timezone data, and it's not possible to unambiguously assign a
	// numeric timezone offset to these HH:MM:SS time values using the configured
	// datetime location (handwaving at one reason: how do we know if DST applies?).
	//
	// So we don't do that, and that's why TIME columns just get turned into strings
	// without a specific format guarantee here.
	"time": {jsonType: "string"},

	"uniqueidentifier": {jsonType: "string", format: "uuid"},

	"xml": {jsonType: "string"},

	"datetime":      {jsonType: "string", format: "date-time"},
	"datetime2":     {jsonType: "string", format: "date-time"},
	"smalldatetime": {jsonType: "string", format: "date-time"},

	"hierarchyid": {jsonType: "string", contentEncoding: "base64"},
}

// typeNameToColumnSchema returns the JSON schema for a SQL Server type name.
// Returns the schema and true if the type is known, or zero value and false otherwise.
func typeNameToColumnSchema(cfg *Config, typeName string) (columnSchema, bool) {
	// Handle TIMESTAMP/ROWVERSION specially for backwards compatibility
	if typeName == "timestamp" && cfg.RowversionAsAny {
		return columnSchema{}, false
	}

	schema, ok := columnSchemaMap[typeName]
	return schema, ok
}

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func TranslateDBToJSONType(cfg *Config, column sqlcapture.ColumnInfo) (*jsonschema.Schema, error) {
	var schema columnSchema
	var ok bool

	switch dt := column.DataType.(type) {
	case *TextColumnType:
		if schema, ok = typeNameToColumnSchema(cfg, dt.Type); !ok {
			return nil, fmt.Errorf("unhandled SQL Server type %q (found on column %q of table %q)", dt.Type, column.Name, column.TableName)
		}
		// NOTE: In theory we might want to discover a minimum length for fixed-length
		// CHAR(n) columns, but in practice we have observed this minimum being violated
		// and there's no real benefit to having it right now, so we don't.
		if dt.MaxLength > 0 && slices.Contains([]string{"char", "nchar", "varchar", "nvarchar"}, dt.Type) {
			var length = uint64(dt.MaxLength)
			schema.maxLength = &length
		}
	case *BinaryColumnType:
		if schema, ok = typeNameToColumnSchema(cfg, dt.Type); !ok {
			return nil, fmt.Errorf("unhandled SQL Server type %q (found on column %q of table %q)", dt.Type, column.Name, column.TableName)
		}
		// Binary data is base64 encoded: every 3 bytes becomes 4 characters.
		//
		// NOTE: As with CHAR(n) we could theoretically discover a minimum here, and
		// we have not observed that minimum being violated in the real world for a
		// BINARY(n) column, but since there's no real benefit we choose not to at
		// this time.
		if dt.MaxLength > 0 && slices.Contains([]string{"binary", "varbinary"}, dt.Type) {
			var base64Length = uint64((dt.MaxLength + 2) / 3 * 4)
			schema.maxLength = &base64Length
		}
	case string:
		if schema, ok = typeNameToColumnSchema(cfg, dt); !ok {
			return nil, fmt.Errorf("unhandled SQL Server type %q (found on column %q of table %q)", dt, column.Name, column.TableName)
		}
	default:
		return nil, fmt.Errorf("unhandled SQL Server type %#v (found on column %q of table %q)", column.DataType, column.Name, column.TableName)
	}

	// Pass-through the column nullability and description.
	schema.nullable = column.IsNullable
	if column.Description != nil {
		schema.description = *column.Description
	}
	return schema.toSchema(), nil
}

// TranslateColumnValue converts a SQL Server driver value to a JSON-serializable value.
// It handles type-specific transformations like UUID byte-swapping, datetime timezone
// interpretation, and value truncation.
func TranslateColumnValue(columnType any, val any, cfg *Config) (any, error) {
	// Fast path for strings which avoids an extra `any` allocation in the common case of no truncation.
	if str, ok := val.(string); ok {
		if len(str) > TruncateColumnThreshold {
			return str[:TruncateColumnThreshold], nil
		}
		return val, nil
	}

	switch val := val.(type) {
	case []byte:
		switch columnType {
		case "numeric", "decimal", "money", "smallmoney":
			return string(val), nil
		case "uniqueidentifier":
			// Words cannot describe how much this infuriates me. Byte-swap
			// the first eight bytes of the UUID so that values will actually
			// round-trip correctly from the '00112233-4455-6677-8899-AABBCCDDEEFF'
			// textual input format to UUIDs serialized as JSON.
			val[0], val[1], val[2], val[3] = val[3], val[2], val[1], val[0]
			val[4], val[5] = val[5], val[4]
			val[6], val[7] = val[7], val[6]
			u, err := uuid.FromBytes(val)
			if err != nil {
				return nil, err
			}
			return u.String(), nil
		}
		if len(val) > TruncateColumnThreshold {
			val = val[:TruncateColumnThreshold]
		}
		return val, nil
	case time.Time:
		switch columnType {
		case "date":
			// Date columns aren't timezone aware and shouldn't pretend to be valid
			// timestamps, so we format them back to a simple YYYY-MM-DD string here.
			return val.Format("2006-01-02"), nil
		case "time":
			return val.Format("15:04:05.9999999"), nil
		case "datetime", "datetime2", "smalldatetime":
			// The SQL Server client library translates DATETIME columns into Go time.Time values
			// in the UTC location. We need to reinterpret the same YYYY-MM-DD HH:MM:SS.NNN values
			// in the actual user-specified location instead.
			return time.Date(val.Year(), val.Month(), val.Day(), val.Hour(), val.Minute(), val.Second(), val.Nanosecond(), cfg.DatetimeLocation), nil
		}
	}
	return val, nil
}

// EncodeKeyFDB encodes a value for use in an FDB tuple row key.
// It handles special cases like UUID byte-shuffling to match SQL Server sort order,
// and collation-aware text encoding.
func EncodeKeyFDB(key, ktype any) (tuple.TupleElement, error) {
	switch key := key.(type) {
	case time.Time:
		switch ktype {
		case "datetime":
			return key.Format(DatetimeKeyEncoding), nil
		default:
			return key.Format(SortableRFC3339Nano), nil
		}
	case []byte:
		switch ktype {
		case "uniqueidentifier":
			// SQL Server sorts `uniqueidentifier` columns in a bizarre order, but
			// by shuffling the bytes here and reversing it when decoding we can get
			// the serialized form to sort lexicographically. See TestUUIDCaptureOrder
			// for the test that ensures the encoding works correctly.
			var enc = make([]byte, 16)
			copy(enc[0:6], key[10:16])
			copy(enc[6:8], key[8:10])
			copy(enc[8:10], key[6:8])
			copy(enc[10:12], key[4:6])
			copy(enc[12:16], key[0:4])
			return tuple.Tuple{uniqueIdentifierTag, enc}, nil
		case "numeric", "decimal", "money", "smallmoney":
			return string(key), nil
		}
	case string:
		if textInfo, ok := ktype.(*TextColumnType); ok && PredictableCollation(textInfo) {
			var textSortingKey, err = EncodeCollationSortKey(textInfo, key)
			if err != nil {
				return nil, fmt.Errorf("error serializing collated text in key: %w", err)
			}
			return tuple.Tuple{collatedTextTag, textSortingKey, key}, nil
		}
	}
	return key, nil
}

// DecodeKeyFDB decodes a value from an FDB tuple row key.
// It reverses the special encoding done by EncodeKeyFDB.
func DecodeKeyFDB(t tuple.TupleElement) (any, error) {
	switch t := t.(type) {
	case tuple.Tuple:
		if len(t) == 0 {
			return nil, fmt.Errorf("internal error: malformed row key contains empty tuple")
		}
		switch t[0] {
		case uniqueIdentifierTag:
			// Reverse the shuffling done by EncodeKeyFDB
			var enc, key = t[1].([]byte), make([]byte, 16)
			copy(key[10:16], enc[0:6])
			copy(key[8:10], enc[6:8])
			copy(key[6:8], enc[8:10])
			copy(key[4:6], enc[10:12])
			copy(key[0:4], enc[12:16])
			return key, nil
		case collatedTextTag:
			return t[2], nil
		default:
			return nil, fmt.Errorf("internal error: unknown tuple tag %q", t[0])
		}
	default:
		return t, nil
	}
}
