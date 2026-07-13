package main

import (
	"encoding/json"
	"fmt"
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

func parquetSchema(fields []string, collection pf.CollectionSpec, fieldConfigJsonMap map[string]json.RawMessage) (writer.ParquetSchema, error) {
	out := []writer.ParquetSchemaElement{}

	for _, f := range fields {
		var fc fieldConfig
		if rawFieldConfig, ok := fieldConfigJsonMap[f]; ok {
			if err := json.Unmarshal(rawFieldConfig, &fc); err != nil {
				return nil, fmt.Errorf("unmarshaling field config for %q: %w", f, err)
			} else if err := fc.Validate(); err != nil {
				return nil, fmt.Errorf("validating field config for %q: %w", f, err)
			}
		}

		s, err := projectionToParquetSchemaElement(*collection.GetProjection(f), fc)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}

	return out, nil
}

func projectionToParquetSchemaElement(p pf.Projection, fc fieldConfig) (writer.ParquetSchemaElement, error) {
	if fc.IgnoreStringFormat {
		if p.Inference.String_ == nil {
			return writer.ParquetSchemaElement{}, fmt.Errorf("cannot set ignoreStringFormat on non-string field %q", p.Field)
		}
		p.Inference.String_.Format = ""
	}

	return writer.ProjectionToParquetSchemaElement(p, false, schemaOptions...), nil
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
	case writer.LogicalTypeUuid:
		return iceberg.PrimitiveTypes.UUID
	default:
		panic(fmt.Sprintf("unhandled parquet data type: %T (%#v)", pqt, pqt))
	}
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

func (mt mappedType) CanMigrate(existing boilerplate.ExistingField) bool {
	return false
}
