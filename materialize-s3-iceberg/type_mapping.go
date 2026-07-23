package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/estuary/connectors/go/writer"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
)

var schemaOptions = []writer.ParquetSchemaOption{
	// Iceberg does not support column types corresponding to Parquet's JSON or INTERVAL types. Because
	// of this, these fields will be materialized as strings.
	writer.WithParquetSchemaArrayAsString(),
	writer.WithParquetSchemaObjectAsString(),
	writer.WithParquetSchemaDurationAsString(),
	// Spark can't read Iceberg tables with "time" column types, see
	// https://github.com/apache/iceberg/issues/9006. Prioritizing support for Spark seems important
	// enough to force materializing these as strings.
	writer.WithParquetTimeAsString(),
	// Many commonly used versions of Spark also can't read Iceberg tables with "UUID" column types.
	writer.WithParquetUUIDAsString(),
}

// The option-flavored variants are precomputed rather than appended per call:
// appending to schemaOptions at a call site would alias its backing array if
// it ever gained spare capacity.
var nsSchemaOptions = append(slices.Clip(schemaOptions), writer.WithParquetTimestampAsNanoseconds())
var variantSchemaOptions = append(slices.Clip(schemaOptions), writer.WithParquetSchemaJSONAsVariant())
var nsVariantSchemaOptions = append(slices.Clip(nsSchemaOptions), writer.WithParquetSchemaJSONAsVariant())

type fieldConfig struct {
	// IgnoreStringFormat can be set to true to indicate that the field should
	// be materialized as a string, disregarding any format annotations.
	IgnoreStringFormat bool `json:"ignoreStringFormat"`
}

func (fc fieldConfig) Validate() error {
	return nil
}

func (fc fieldConfig) CastToString() bool {
	return fc.IgnoreStringFormat
}

func parquetSchema(fields []string, collection pf.CollectionSpec, fieldConfigJsonMap map[string]json.RawMessage, nanoseconds bool, variants bool) (writer.ParquetSchema, error) {
	out := make(writer.ParquetSchema, 0, len(fields))

	for _, f := range fields {
		var fc fieldConfig
		if rawFieldConfig, ok := fieldConfigJsonMap[f]; ok {
			if err := json.Unmarshal(rawFieldConfig, &fc); err != nil {
				return nil, fmt.Errorf("unmarshaling field config for %q: %w", f, err)
			} else if err := fc.Validate(); err != nil {
				return nil, fmt.Errorf("validating field config for %q: %w", f, err)
			}
		}

		s, err := projectionToParquetSchemaElement(*collection.GetProjection(f), fc, nanoseconds, variants)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}

	return out, nil
}

func projectionToParquetSchemaElement(p pf.Projection, fc fieldConfig, nanoseconds bool, variants bool) (writer.ParquetSchemaElement, error) {
	if fc.IgnoreStringFormat {
		if p.Inference.String_ == nil {
			return writer.ParquetSchemaElement{}, fmt.Errorf("cannot set ignoreStringFormat on non-string field %q", p.Field)
		}
		p.Inference.String_.Format = ""
	}

	// Collection key fields keep their conservative (string) mapping even with
	// variant_columns enabled: Iceberg forbids variant as an identifier field,
	// and keys are meant to be joined and filtered on across engines.
	// ignoreStringFormat likewise remains an escape hatch forcing a JSON
	// string column.
	useVariant := variants && !p.IsPrimaryKey && !fc.IgnoreStringFormat

	var opts []writer.ParquetSchemaOption
	switch {
	case nanoseconds && useVariant:
		opts = nsVariantSchemaOptions
	case nanoseconds:
		opts = nsSchemaOptions
	case useVariant:
		opts = variantSchemaOptions
	default:
		opts = schemaOptions
	}
	return writer.ProjectionToParquetSchemaElement(p, false, opts...), nil
}

func parquetTypeToIcebergType(pqt writer.ParquetDataType) iceberg.Type {
	switch pqt {
	case writer.PrimitiveTypeInteger:
		return iceberg.PrimitiveTypes.Int64
	case writer.PrimitiveTypeNumber:
		return iceberg.PrimitiveTypes.Float64
	case writer.PrimitiveTypeBoolean:
		return iceberg.PrimitiveTypes.Bool
	case writer.PrimitiveTypeBinary:
		return iceberg.PrimitiveTypes.Binary
	case writer.LogicalTypeString:
		return iceberg.PrimitiveTypes.String
	case writer.LogicalTypeDate:
		return iceberg.PrimitiveTypes.Date
	case writer.LogicalTypeTimestamp:
		return iceberg.PrimitiveTypes.TimestampTz
	case writer.LogicalTypeTimestampNanos:
		return iceberg.PrimitiveTypes.TimestampTzNs
	case writer.LogicalTypeUuid:
		return iceberg.PrimitiveTypes.UUID
	case writer.LogicalTypeVariant:
		return iceberg.VariantType{}
	default:
		panic(fmt.Sprintf("unhandled parquet data type: %T (%#v)", pqt, pqt))
	}
}

func isVariant(typ iceberg.Type) bool {
	return typ.Equals(iceberg.VariantType{})
}

type mappedType struct {
	icebergType iceberg.Type
}

func (mt mappedType) String() string {
	return mt.icebergType.String()
}

func (mt mappedType) Compatible(existing boilerplate.ExistingField) bool {
	return strings.EqualFold(existing.Type, mt.icebergType.String())
}

// CanMigrate permits the microsecond↔nanosecond timestamp pair in both
// directions, any non-variant type to variant (the variant_columns flip,
// including column types a JSON-shaped field had previously evolved to), and
// variant back to string (disabling variant_columns or applying castToString).
// Iceberg forbids changing a column's type in place, so UpdateResource
// migrates by dropping and re-adding the column under a new field ID: the new
// type applies to data going forward, and prior rows read as null for the
// column.
func (mt mappedType) CanMigrate(existing boilerplate.ExistingField) bool {
	from, to := strings.ToLower(existing.Type), mt.icebergType.String()
	switch {
	case from == "timestamptz" && to == "timestamptz_ns":
		return true
	case from == "timestamptz_ns" && to == "timestamptz":
		return true
	case to == "variant":
		return from != "variant"
	case from == "variant":
		return to == "string"
	default:
		return false
	}
}
