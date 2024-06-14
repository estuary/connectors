package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
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

func schemaWithOptions(fields []string, collection pf.CollectionSpec) enc.ParquetSchema {
	return enc.FieldsToParquetSchema(fields, collection, schemaOptions...)
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

type icebergConstrainter struct{}

func (icebergConstrainter) NewConstraints(p *pf.Projection, deltaUpdates bool) *pm.Response_Validated_Constraint {
	_, isNumeric := boilerplate.AsFormattedNumeric(p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case len(p.Inference.Types) == 0:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field with no types"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field where the only possible type is 'null'"
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "object"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Object fields may be materialized"
	default:
		// Any other case is one where the field is an array or has multiple types.
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "This field is able to be materialized"
	}

	return &constraint
}

func (icebergConstrainter) Compatible(existing boilerplate.EndpointField, proposed *pf.Projection, fc json.RawMessage) (bool, error) {
	s := enc.ProjectionToParquetSchemaElement(*proposed, schemaOptions...)
	t := parquetTypeToIcebergType(s.DataType)

	return strings.EqualFold(existing.Type, string(t)), nil
}

func (icebergConstrainter) DescriptionForType(p *pf.Projection, fc json.RawMessage) (string, error) {
	s := enc.ProjectionToParquetSchemaElement(*p, schemaOptions...)
	t := parquetTypeToIcebergType(s.DataType)

	return string(t), nil
}
