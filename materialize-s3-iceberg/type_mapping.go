package main

import (
	"encoding/json"
	"fmt"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
	pf "github.com/estuary/flow/go/protocols/flow"
)

type icebergType string

const (
	icebergTypeBoolean     icebergType = "boolean"
	icebergTypeString      icebergType = "string"
	icebergTypeLong        icebergType = "long"   // 64 bit integer
	icebergTypeDouble      icebergType = "double" // 64 bit float
	icebergTypeTimestamptz icebergType = "timestamptz"
	icebergTypeDate        icebergType = "date"
	icebergTypeUuid        icebergType = "uuid"
	icebergTypeBinary      icebergType = "binary"
)

var schemaOptions = []enc.ParquetSchemaOption{
	// Iceberg does not support column types corresponding to Parquet's JSON or INTERVAL types. Because
	// of this, these fields will be materialized as strings.
	enc.WithParquetSchemaArrayAsString(),
	enc.WithParquetSchemaObjectAsString(),
	enc.WithParquetSchemaDurationAsString(),
	// Spark can't read Iceberg tables with "time" column types, see
	// https://github.com/apache/iceberg/issues/9006. Prioritizing support for Spark seems important
	// enough to force materializing these as strings.
	enc.WithParquetTimeAsString(),
	// Many commonly used versions of Spark also can't read Iceberg tables with "UUID" column types.
	enc.WithParquetUUIDAsString(),
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

func parquetSchema(fields []string, collection pf.CollectionSpec, fieldConfigJsonMap map[string]json.RawMessage) (enc.ParquetSchema, error) {
	out := []enc.ParquetSchemaElement{}

	for _, f := range fields {
		s, err := projectionToParquetSchemaElement(*collection.GetProjection(f), fieldConfigJsonMap[f])
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}

	return out, nil
}

func projectionToParquetSchemaElement(p pf.Projection, rawFieldConfig json.RawMessage) (enc.ParquetSchemaElement, error) {
	if rawFieldConfig != nil {
		var parsedFieldConfig fieldConfig
		if err := json.Unmarshal(rawFieldConfig, &parsedFieldConfig); err != nil {
			return enc.ParquetSchemaElement{}, err
		}

		if parsedFieldConfig.IgnoreStringFormat {
			if p.Inference.String_ == nil {
				return enc.ParquetSchemaElement{}, fmt.Errorf("cannot set ignoreStringFormat on non-string field %q", p.Field)
			}
			p.Inference.String_.Format = ""
		}
	}

	return enc.ProjectionToParquetSchemaElement(p, schemaOptions...), nil
}

func parquetTypeToIcebergType(pqt enc.ParquetDataType) icebergType {
	switch pqt {
	case enc.PrimitiveTypeInteger:
		return icebergTypeLong
	case enc.PrimitiveTypeNumber:
		return icebergTypeDouble
	case enc.PrimitiveTypeBoolean:
		return icebergTypeBoolean
	case enc.PrimitiveTypeBinary:
		return icebergTypeBinary
	case enc.LogicalTypeString:
		return icebergTypeString
	case enc.LogicalTypeDate:
		return icebergTypeDate
	case enc.LogicalTypeTimestamp:
		return icebergTypeTimestamptz
	case enc.LogicalTypeUuid:
		return icebergTypeUuid
	default:
		panic(fmt.Sprintf("unhandled parquet data type: %T (%#v)", pqt, pqt))
	}
}

type mappedType struct {
	icebergType icebergType
}

func (mt mappedType) String() string {
	return string(mt.icebergType)
}

func (mt mappedType) Compatible(existing boilerplate.ExistingField) bool {
	return strings.EqualFold(existing.Type, string(mt.icebergType))
}

func (mt mappedType) CanMigrate(existing boilerplate.ExistingField) bool {
	return false
}
