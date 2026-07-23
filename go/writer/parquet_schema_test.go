package writer

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func ptrTo[T any](v T) *T { return &v }

func TestMakeNode(t *testing.T) {
	const fid = int32(42)

	tests := []struct {
		name         string
		elem         ParquetSchemaElement
		wantPhysical parquet.Type
		// wantTypeLen of -1 means "do not assert" (only meaningful for FixedLenByteArray).
		wantTypeLen  int
		checkLogical func(t *testing.T, lt schema.LogicalType)
	}{
		{
			name:         "PrimitiveTypeInteger",
			elem:         ParquetSchemaElement{Name: "intField", DataType: PrimitiveTypeInteger, Required: true, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.Int64,
			wantTypeLen:  -1,
		},
		{
			name:         "PrimitiveTypeNumber",
			elem:         ParquetSchemaElement{Name: "numField", DataType: PrimitiveTypeNumber, Required: false, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.Double,
			wantTypeLen:  -1,
		},
		{
			name:         "PrimitiveTypeBoolean",
			elem:         ParquetSchemaElement{Name: "boolField", DataType: PrimitiveTypeBoolean, Required: true, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.Boolean,
			wantTypeLen:  -1,
		},
		{
			name:         "PrimitiveTypeBinary",
			elem:         ParquetSchemaElement{Name: "binField", DataType: PrimitiveTypeBinary, Required: false, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.ByteArray,
			wantTypeLen:  -1,
		},
		{
			name:         "LogicalTypeString",
			elem:         ParquetSchemaElement{Name: "strField", DataType: LogicalTypeString, Required: true, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.ByteArray,
			wantTypeLen:  -1,
			checkLogical: func(t *testing.T, lt schema.LogicalType) {
				_, ok := lt.(schema.StringLogicalType)
				require.True(t, ok, "expected StringLogicalType, got %T", lt)
			},
		},
		{
			name:         "LogicalTypeJson",
			elem:         ParquetSchemaElement{Name: "jsonField", DataType: LogicalTypeJson, Required: false, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.ByteArray,
			wantTypeLen:  -1,
			checkLogical: func(t *testing.T, lt schema.LogicalType) {
				_, ok := lt.(schema.JSONLogicalType)
				require.True(t, ok, "expected JSONLogicalType, got %T", lt)
			},
		},
		{
			name:         "LogicalTypeDate",
			elem:         ParquetSchemaElement{Name: "dateField", DataType: LogicalTypeDate, Required: true, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.Int32,
			wantTypeLen:  -1,
			checkLogical: func(t *testing.T, lt schema.LogicalType) {
				_, ok := lt.(schema.DateLogicalType)
				require.True(t, ok, "expected DateLogicalType, got %T", lt)
			},
		},
		{
			name:         "LogicalTypeTime",
			elem:         ParquetSchemaElement{Name: "timeField", DataType: LogicalTypeTime, Required: true, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.Int64,
			wantTypeLen:  -1,
			checkLogical: func(t *testing.T, lt schema.LogicalType) {
				tl, ok := lt.(schema.TimeLogicalType)
				require.True(t, ok, "expected TimeLogicalType, got %T", lt)
				require.True(t, tl.IsAdjustedToUTC())
				require.Equal(t, schema.TimeUnitMicros, tl.TimeUnit())
			},
		},
		{
			name:         "LogicalTypeTimestamp",
			elem:         ParquetSchemaElement{Name: "tsField", DataType: LogicalTypeTimestamp, Required: false, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.Int64,
			wantTypeLen:  -1,
			checkLogical: func(t *testing.T, lt schema.LogicalType) {
				tl, ok := lt.(schema.TimestampLogicalType)
				require.True(t, ok, "expected TimestampLogicalType, got %T", lt)
				require.True(t, tl.IsAdjustedToUTC())
				require.Equal(t, schema.TimeUnitMicros, tl.TimeUnit())
			},
		},
		{
			name:         "LogicalTypeTimestampNanos",
			elem:         ParquetSchemaElement{Name: "tsNanosField", DataType: LogicalTypeTimestampNanos, Required: false, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.Int64,
			wantTypeLen:  -1,
			checkLogical: func(t *testing.T, lt schema.LogicalType) {
				tl, ok := lt.(schema.TimestampLogicalType)
				require.True(t, ok, "expected TimestampLogicalType, got %T", lt)
				require.True(t, tl.IsAdjustedToUTC())
				require.Equal(t, schema.TimeUnitNanos, tl.TimeUnit())
			},
		},
		{
			name:         "LogicalTypeUuid",
			elem:         ParquetSchemaElement{Name: "uuidField", DataType: LogicalTypeUuid, Required: true, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.FixedLenByteArray,
			wantTypeLen:  16,
			checkLogical: func(t *testing.T, lt schema.LogicalType) {
				_, ok := lt.(schema.UUIDLogicalType)
				require.True(t, ok, "expected UUIDLogicalType, got %T", lt)
			},
		},
		{
			name:         "LogicalTypeDecimal",
			elem:         ParquetSchemaElement{Name: "decField", DataType: LogicalTypeDecimal, Required: true, FieldId: ptrTo(fid), Scale: 5},
			wantPhysical: parquet.Types.FixedLenByteArray,
			wantTypeLen:  16,
			checkLogical: func(t *testing.T, lt schema.LogicalType) {
				dl, ok := lt.(schema.DecimalLogicalType)
				require.True(t, ok, "expected DecimalLogicalType, got %T", lt)
				require.EqualValues(t, 38, dl.Precision())
				require.EqualValues(t, 5, dl.Scale())
			},
		},
		{
			name:         "LogicalTypeInterval",
			elem:         ParquetSchemaElement{Name: "intervalField", DataType: LogicalTypeInterval, Required: false, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.FixedLenByteArray,
			wantTypeLen:  12,
			checkLogical: func(t *testing.T, lt schema.LogicalType) {
				_, ok := lt.(schema.IntervalLogicalType)
				require.True(t, ok, "expected IntervalLogicalType, got %T", lt)
			},
		},
		{
			name:         "LogicalTypeUnknown",
			elem:         ParquetSchemaElement{Name: "unkField", DataType: LogicalTypeUnknown, Required: false, FieldId: ptrTo(fid)},
			wantPhysical: parquet.Types.Undefined,
			wantTypeLen:  -1,
			checkLogical: func(t *testing.T, lt schema.LogicalType) {
				_, ok := lt.(schema.UnknownLogicalType)
				require.True(t, ok, "expected UnknownLogicalType, got %T", lt)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := makeNode(tt.elem)

			require.Equal(t, tt.elem.Name, node.Name())
			require.Equal(t, fid, node.FieldID(), "FieldId must be propagated to the parquet node")

			wantRep := parquet.Repetitions.Optional
			if tt.elem.Required {
				wantRep = parquet.Repetitions.Required
			}
			require.Equal(t, wantRep, node.RepetitionType())

			pn, ok := node.(*schema.PrimitiveNode)
			require.True(t, ok, "expected *schema.PrimitiveNode, got %T", node)
			require.Equal(t, tt.wantPhysical, pn.PhysicalType())

			if tt.wantTypeLen >= 0 {
				require.Equal(t, tt.wantTypeLen, pn.TypeLength())
			}

			if tt.checkLogical != nil {
				tt.checkLogical(t, pn.LogicalType())
			}
		})
	}
}

func TestMakeNodeVariant(t *testing.T) {
	const fid = int32(42)

	for _, required := range []bool{true, false} {
		t.Run(fmt.Sprintf("required=%v", required), func(t *testing.T) {
			node := makeNode(ParquetSchemaElement{Name: "variantField", DataType: LogicalTypeVariant, Required: required, FieldId: ptrTo(fid)})

			require.Equal(t, "variantField", node.Name())
			require.Equal(t, fid, node.FieldID(), "FieldId must be propagated to the variant group node")

			wantRep := parquet.Repetitions.Optional
			if required {
				wantRep = parquet.Repetitions.Required
			}
			require.Equal(t, wantRep, node.RepetitionType())

			gn, ok := node.(*schema.GroupNode)
			require.True(t, ok, "expected *schema.GroupNode, got %T", node)
			_, ok = gn.LogicalType().(schema.VariantLogicalType)
			require.True(t, ok, "expected VariantLogicalType, got %T", gn.LogicalType())

			require.Equal(t, 2, gn.NumFields())
			for idx, name := range []string{"metadata", "value"} {
				leaf, ok := gn.Field(idx).(*schema.PrimitiveNode)
				require.True(t, ok, "expected *schema.PrimitiveNode for %q, got %T", name, gn.Field(idx))
				require.Equal(t, name, leaf.Name())
				require.Equal(t, parquet.Repetitions.Required, leaf.RepetitionType())
				require.Equal(t, parquet.Types.ByteArray, leaf.PhysicalType())
				require.EqualValues(t, -1, leaf.FieldID(), "variant sub-fields are not columns of the table schema and must not carry field IDs")
			}
		})
	}
}

func TestMakeNodeNilFieldId(t *testing.T) {
	// Every supported data type must produce a node with FieldID() == -1
	// when the schema element does not specify a FieldId.
	dataTypes := []ParquetDataType{
		PrimitiveTypeInteger,
		PrimitiveTypeNumber,
		PrimitiveTypeBoolean,
		PrimitiveTypeBinary,
		LogicalTypeString,
		LogicalTypeJson,
		LogicalTypeDate,
		LogicalTypeTime,
		LogicalTypeTimestamp,
		LogicalTypeTimestampNanos,
		LogicalTypeUuid,
		LogicalTypeDecimal,
		LogicalTypeInterval,
		LogicalTypeVariant,
		LogicalTypeUnknown,
	}
	for _, dt := range dataTypes {
		t.Run(fmt.Sprintf("dataType=%d", dt), func(t *testing.T) {
			node := makeNode(ParquetSchemaElement{Name: "f", DataType: dt, Required: true, Scale: 0})
			require.EqualValues(t, -1, node.FieldID())
		})
	}
}

func TestMakeNodeUnknownDataTypePanics(t *testing.T) {
	require.PanicsWithValue(t, "makeNode unknown type: 999", func() {
		makeNode(ParquetSchemaElement{Name: "f", DataType: ParquetDataType(999)})
	})
}

func TestProjectionToParquetSchemaElement(t *testing.T) {
	tests := []struct {
		name         string
		projection   pf.Projection
		castToString bool
		opts         []ParquetSchemaOption
		wantType     ParquetDataType
		wantRequired bool
	}{
		{
			name: "castToString short-circuits all inference",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"integer"},
				},
			},
			castToString: true,
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
		{
			name: "string format integer becomes integer",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "integer"},
				},
			},
			wantType:     PrimitiveTypeInteger,
			wantRequired: true,
		},
		{
			name: "nullable string format number becomes number",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MAY,
					Types:   []string{"null", "number", "string"},
					String_: &pf.Inference_String{Format: "number"},
				},
			},
			wantType:     PrimitiveTypeNumber,
			wantRequired: false,
		},
		{
			name: "primary key string format integer falls through to string",
			projection: pf.Projection{
				Field:        "f",
				IsPrimaryKey: true,
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "integer"},
				},
			},
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
		{
			name: "single type array becomes json",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"array"},
				},
			},
			wantType:     LogicalTypeJson,
			wantRequired: true,
		},
		{
			name: "single type object becomes json",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"object"},
				},
			},
			wantType:     LogicalTypeJson,
			wantRequired: true,
		},
		{
			name: "boolean",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"boolean"},
				},
			},
			wantType:     PrimitiveTypeBoolean,
			wantRequired: true,
		},
		{
			name: "integer",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"integer"},
				},
			},
			wantType:     PrimitiveTypeInteger,
			wantRequired: true,
		},
		{
			name: "number",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"number"},
				},
			},
			wantType:     PrimitiveTypeNumber,
			wantRequired: true,
		},
		{
			name: "string base64 becomes binary",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{ContentEncoding: "base64"},
				},
			},
			wantType:     PrimitiveTypeBinary,
			wantRequired: true,
		},
		{
			name: "string format date",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "date"},
				},
			},
			wantType:     LogicalTypeDate,
			wantRequired: true,
		},
		{
			name: "string format date-time",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "date-time"},
				},
			},
			wantType:     LogicalTypeTimestamp,
			wantRequired: true,
		},
		{
			name: "string format duration",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "duration"},
				},
			},
			wantType:     LogicalTypeInterval,
			wantRequired: true,
		},
		{
			name: "string format time",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "time"},
				},
			},
			wantType:     LogicalTypeTime,
			wantRequired: true,
		},
		{
			name: "string format uuid",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "uuid"},
				},
			},
			wantType:     LogicalTypeUuid,
			wantRequired: true,
		},
		{
			name: "string with no format and no inference is plain string",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{},
				},
			},
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
		{
			name: "string with unknown format is plain string",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "ipv4"},
				},
			},
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
		{
			name: "multi-type non-null becomes json",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"integer", "boolean"},
				},
			},
			wantType:     LogicalTypeJson,
			wantRequired: true,
		},
		{
			name: "no types becomes unknown",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MAY,
					Types:  nil,
				},
			},
			wantType:     LogicalTypeUnknown,
			wantRequired: false,
		},
		{
			name: "only null type becomes unknown",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MAY,
					Types:  []string{"null"},
				},
			},
			wantType:     LogicalTypeUnknown,
			wantRequired: false,
		},
		{
			name: "MUST with null in types is not required",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"null", "integer"},
				},
			},
			wantType:     PrimitiveTypeInteger,
			wantRequired: false,
		},
		{
			name: "MAY with default and no null becomes required",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:      pf.Inference_MAY,
					Types:       []string{"integer"},
					DefaultJson: json.RawMessage(`0`),
				},
			},
			wantType:     PrimitiveTypeInteger,
			wantRequired: true,
		},
		{
			name: "MAY with no default is optional",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MAY,
					Types:  []string{"integer"},
				},
			},
			wantType:     PrimitiveTypeInteger,
			wantRequired: false,
		},
		{
			name: "WithParquetSchemaArrayAsString flips array to string",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"array"},
				},
			},
			opts:         []ParquetSchemaOption{WithParquetSchemaArrayAsString()},
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
		{
			name: "without WithParquetSchemaArrayAsString array stays json",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"array"},
				},
			},
			wantType:     LogicalTypeJson,
			wantRequired: true,
		},
		{
			name: "WithParquetSchemaObjectAsString flips object to string",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"object"},
				},
			},
			opts:         []ParquetSchemaOption{WithParquetSchemaObjectAsString()},
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
		{
			name: "WithParquetSchemaObjectAsString flips multi-type fallback to string",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"integer", "boolean"},
				},
			},
			opts:         []ParquetSchemaOption{WithParquetSchemaObjectAsString()},
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
		{
			name: "WithParquetSchemaDurationAsString flips duration to string",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "duration"},
				},
			},
			opts:         []ParquetSchemaOption{WithParquetSchemaDurationAsString()},
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
		{
			name: "WithParquetTimeAsString flips time to string",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "time"},
				},
			},
			opts:         []ParquetSchemaOption{WithParquetTimeAsString()},
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
		{
			name: "WithParquetUUIDAsString flips uuid to string",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "uuid"},
				},
			},
			opts:         []ParquetSchemaOption{WithParquetUUIDAsString()},
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
		{
			name: "WithParquetTimestampAsNanoseconds flips date-time to nanos",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists:  pf.Inference_MUST,
					Types:   []string{"string"},
					String_: &pf.Inference_String{Format: "date-time"},
				},
			},
			opts:         []ParquetSchemaOption{WithParquetTimestampAsNanoseconds()},
			wantType:     LogicalTypeTimestampNanos,
			wantRequired: true,
		},
		{
			name: "WithParquetSchemaJSONAsVariant flips object to variant",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"object"},
				},
			},
			opts:         []ParquetSchemaOption{WithParquetSchemaJSONAsVariant()},
			wantType:     LogicalTypeVariant,
			wantRequired: true,
		},
		{
			name: "WithParquetSchemaJSONAsVariant flips array to variant",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"array"},
				},
			},
			opts:         []ParquetSchemaOption{WithParquetSchemaJSONAsVariant()},
			wantType:     LogicalTypeVariant,
			wantRequired: true,
		},
		{
			name: "WithParquetSchemaJSONAsVariant flips multi-type fallback to variant",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"integer", "boolean"},
				},
			},
			opts:         []ParquetSchemaOption{WithParquetSchemaJSONAsVariant()},
			wantType:     LogicalTypeVariant,
			wantRequired: true,
		},
		{
			name: "WithParquetSchemaJSONAsVariant takes precedence over as-string options",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"object"},
				},
			},
			opts: []ParquetSchemaOption{
				WithParquetSchemaArrayAsString(),
				WithParquetSchemaObjectAsString(),
				WithParquetSchemaJSONAsVariant(),
			},
			wantType:     LogicalTypeVariant,
			wantRequired: true,
		},
		{
			name: "castToString overrides WithParquetSchemaJSONAsVariant",
			projection: pf.Projection{
				Field: "f",
				Inference: pf.Inference{
					Exists: pf.Inference_MUST,
					Types:  []string{"object"},
				},
			},
			castToString: true,
			opts:         []ParquetSchemaOption{WithParquetSchemaJSONAsVariant()},
			wantType:     LogicalTypeString,
			wantRequired: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ProjectionToParquetSchemaElement(tt.projection, tt.castToString, tt.opts...)
			require.Equal(t, tt.projection.Field, got.Name)
			require.Equal(t, tt.wantType, got.DataType)
			require.Equal(t, tt.wantRequired, got.Required)
		})
	}
}
