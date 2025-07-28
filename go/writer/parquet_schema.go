package writer

import (
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// ParquetSchema consists of ParquetSchemaElement which represent the column name, if the column is
// required, and what are representation of the data type should be.
type ParquetSchema []ParquetSchemaElement

type ParquetSchemaElement struct {
	Name     string
	DataType ParquetDataType
	Required bool
	FieldId  *int32
	Scale    int32 // only applicable to LogicalTypeDecimal
}

// ParquetDataType provides a mapping for the JSON types we support to an appropriate parquet data
// type. We make use a both primitive and logical types for this.
//
// Primitive types are the basic data types of Parquet. We don't have a use for the INT32 or FLOAT32
// primitive types as-is, and we don't use INT96 which has been deprecated, although it would be
// nice to store very large integers otherwise. Logical types extend primitive types with metadata
// annotations for indicating different kinds of values that are represented by the underlying
// primitive type.
//
// Time and Timestamp logical types annotate INT64 primitive types to represent microseconds since
// midnight and the Unix epoch, respectively. We use microseconds instead of nanoseconds to maximize
// compatibility, since nanosecond resolution is new to the parquet specification and not widely
// supported.
//
// UUIDs are the binary representation of a 16 byte UUID, an annotate a FIXED_LEN_BYTE_ARRAY of the
// requisite length.
//
// Decimals use a FIXED_LEN_BYTE_ARRAY of length 16. They always have a precision of 38, and the
// scale is configurable. Values should be provided using the decimal128 type exported by the
// arrow-go package. This data type is intended to allow storing large exact-precision numeric values,
// often integers that would overflow an int64, and for that case would use a scale of 0.
//
// Intervals use a FIXED_LEN_BYTE_ARRAY of length 12 to store as three little-endian unsigned
// integers that represent durations at different granularities of time. The first stores a number
// in months, the second stores a number in days, and the third stores a number in milliseconds.
//
// Values for integers and numbers may be provided as strings, as long as those strings can be
// parsed into their numeric values, as with strings with numeric format annotations in their JSON
// schemas. Similarly, values for binary columns must be provided as base64-encoded strings. Date,
// time, timestamp, UUID, and interval should must be provided as strings in their respective
// formats.
type ParquetDataType int

const (
	PrimitiveTypeInteger ParquetDataType = iota // INT64 primitive type
	PrimitiveTypeNumber                         // DOUBLE primitive type, which is a 64-bit float
	PrimitiveTypeBoolean                        // BOOLEAN primitive type
	PrimitiveTypeBinary                         // BYTE_ARRAY primitive type
	LogicalTypeString                           // Extends BYTE_ARRAY
	LogicalTypeJson                             // Extends BYTE_ARRAY
	LogicalTypeDate                             // Extends BYTE_ARRAY
	LogicalTypeTime                             // Extends INT64
	LogicalTypeTimestamp                        // Extends INT64
	LogicalTypeUuid                             // Extends FIXED_LEN_BYTE_ARRAY, with a length of 16 bytes
	LogicalTypeDecimal                          // Extends FIXED_LEN_BYTE_ARRAY, with a length of 16 bytes
	LogicalTypeInterval                         // Extends FIXED_LEN_BYTE_ARRAY, with a length of 12 bytes
	LogicalTypeUnknown                          // Must always be nil
)

// makeNode translates a ParquetSchemaElement into an actual parquet schema node.
func makeNode(e ParquetSchemaElement) schema.Node {
	repetition := parquet.Repetitions.Required
	if !e.Required {
		repetition = parquet.Repetitions.Optional
	}
	fieldId := int32(-1)
	if e.FieldId != nil {
		fieldId = *e.FieldId
	}

	switch e.DataType {
	case PrimitiveTypeInteger:
		return schema.NewInt64Node(e.Name, repetition, fieldId)
	case PrimitiveTypeNumber:
		return schema.NewFloat64Node(e.Name, repetition, fieldId)
	case PrimitiveTypeBoolean:
		return schema.NewBooleanNode(e.Name, repetition, fieldId)
	case PrimitiveTypeBinary:
		return schema.NewByteArrayNode(e.Name, repetition, fieldId)
	case LogicalTypeString:
		return schema.Must(schema.NewPrimitiveNodeLogical(
			e.Name,
			repetition,
			schema.StringLogicalType{},
			parquet.Types.ByteArray,
			-1,
			fieldId,
		))
	case LogicalTypeUuid:
		return schema.Must(schema.NewPrimitiveNodeLogical(
			e.Name,
			repetition,
			schema.UUIDLogicalType{},
			parquet.Types.FixedLenByteArray,
			16,
			fieldId,
		))
	case LogicalTypeJson:
		return schema.Must(schema.NewPrimitiveNodeLogical(
			e.Name,
			repetition,
			schema.JSONLogicalType{},
			parquet.Types.ByteArray,
			-1,
			fieldId,
		))
	case LogicalTypeDate:
		return schema.Must(schema.NewPrimitiveNodeLogical(
			e.Name,
			repetition,
			schema.DateLogicalType{},
			parquet.Types.Int32,
			-1,
			fieldId,
		))
	case LogicalTypeTime:
		return schema.Must(schema.NewPrimitiveNodeLogical(
			e.Name,
			repetition,
			schema.NewTimeLogicalType(true, schema.TimeUnitMicros),
			parquet.Types.Int64,
			-1,
			-1,
		))
	case LogicalTypeTimestamp:
		return schema.Must(schema.NewPrimitiveNodeLogical(
			e.Name,
			repetition,
			schema.NewTimestampLogicalType(true, schema.TimeUnitMicros),
			parquet.Types.Int64,
			-1,
			fieldId,
		))
	case LogicalTypeInterval:
		return schema.Must(schema.NewPrimitiveNodeLogical(
			e.Name,
			repetition,
			schema.IntervalLogicalType{},
			parquet.Types.FixedLenByteArray,
			12,
			fieldId,
		))
	case LogicalTypeUnknown:
		return schema.Must(schema.NewPrimitiveNodeLogical(
			e.Name,
			repetition,
			schema.UnknownLogicalType{},
			parquet.Types.Undefined,
			-1,
			fieldId,
		))
	case LogicalTypeDecimal:
		return schema.Must(schema.NewPrimitiveNodeLogical(
			e.Name,
			repetition,
			schema.NewDecimalLogicalType(38, e.Scale),
			parquet.Types.FixedLenByteArray,
			16,
			fieldId,
		))
	default:
		panic(fmt.Sprintf("makeNode unknown type: %d", e.DataType))
	}
}

type parquetSchemaConfig struct {
	durationAsString bool
	arrayAsString    bool
	objectAsString   bool
	timeAsString     bool
	uuidAsString     bool
}

type ParquetSchemaOption func(*parquetSchemaConfig)

func WithParquetSchemaDurationAsString() ParquetSchemaOption {
	return func(cfg *parquetSchemaConfig) {
		cfg.durationAsString = true
	}
}

func WithParquetSchemaArrayAsString() ParquetSchemaOption {
	return func(cfg *parquetSchemaConfig) {
		cfg.arrayAsString = true
	}
}

func WithParquetSchemaObjectAsString() ParquetSchemaOption {
	return func(cfg *parquetSchemaConfig) {
		cfg.objectAsString = true
	}
}

func WithParquetTimeAsString() ParquetSchemaOption {
	return func(cfg *parquetSchemaConfig) {
		cfg.timeAsString = true
	}
}

func WithParquetUUIDAsString() ParquetSchemaOption {
	return func(cfg *parquetSchemaConfig) {
		cfg.uuidAsString = true
	}
}

func ProjectionToParquetSchemaElement(p pf.Projection, castToString bool, opts ...ParquetSchemaOption) ParquetSchemaElement {
	cfg := parquetSchemaConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	out := ParquetSchemaElement{
		Name:     p.Field,
		Required: !slices.Contains(p.Inference.Types, "null") && (p.Inference.Exists == pf.Inference_MUST || p.Inference.DefaultJson != nil),
	}

	if castToString {
		out.DataType = LogicalTypeString
		return out
	}

	if numFormat, ok := boilerplate.AsFormattedNumeric(&p); ok {
		if numFormat == boilerplate.StringFormatInteger {
			out.DataType = PrimitiveTypeInteger
		} else {
			out.DataType = PrimitiveTypeNumber
		}

		return out
	}

	hadType := false
	for _, t := range p.Inference.Types {
		if t == "null" {
			continue
		}

		if hadType {
			out.DataType = typeOrString(LogicalTypeJson, cfg.objectAsString)
			break
		}

		hadType = true

		switch t {
		case "array":
			out.DataType = typeOrString(LogicalTypeJson, cfg.arrayAsString)
		case "object":
			out.DataType = typeOrString(LogicalTypeJson, cfg.objectAsString)
		case "boolean":
			out.DataType = PrimitiveTypeBoolean
		case "integer":
			out.DataType = PrimitiveTypeInteger
		case "number":
			out.DataType = PrimitiveTypeNumber
		case "string":
			if p.Inference.String_.ContentEncoding == "base64" {
				out.DataType = PrimitiveTypeBinary
				continue
			}

			switch p.Inference.String_.Format {
			case "date":
				out.DataType = LogicalTypeDate
			case "date-time":
				out.DataType = LogicalTypeTimestamp
			case "duration":
				out.DataType = typeOrString(LogicalTypeInterval, cfg.durationAsString)
			case "time":
				out.DataType = typeOrString(LogicalTypeTime, cfg.timeAsString)
			case "uuid":
				out.DataType = typeOrString(LogicalTypeUuid, cfg.uuidAsString)
			default:
				out.DataType = LogicalTypeString
			}
		}
	}

	if !hadType {
		out.DataType = LogicalTypeUnknown
	}

	return out
}

func typeOrString[T ParquetDataType](base T, shouldString bool) T {
	if shouldString {
		return T(LogicalTypeString)
	}
	return base
}
