package connector

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/bradleyjkemp/cupaloy"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestAllowedMigrations(t *testing.T) {
	for _, test := range []struct {
		from string
		to   iceberg.Type
	}{
		{"long", iceberg.DecimalTypeOf(38, 0)},
		{"long", iceberg.Float64Type{}},
		{"long", iceberg.StringType{}},
		{"decimal(38, 0)", iceberg.Float64Type{}},
		{"decimal(38, 0)", iceberg.StringType{}},
		{"double", iceberg.StringType{}},
		{"boolean", iceberg.StringType{}},
		{"binary", iceberg.StringType{}},
		{"date", iceberg.StringType{}},
		{"timestamptz", iceberg.StringType{}},
	} {
		t.Run(fmt.Sprintf("%s->%s", test.from, test.to.String()), func(t *testing.T) {
			require.True(t, allowedMigrations.CanMigrate(test.from, test.to))
		})
	}
}

func TestComputeSchemas(t *testing.T) {
	mappedProjection := func(field string, mustExist bool, type_ iceberg.Type) boilerplate.MappedProjection[mapped] {
		return boilerplate.MappedProjection[mapped]{
			Projection: boilerplate.Projection{
				Projection: pf.Projection{
					Field: field,
				},
				MustExist: mustExist,
			},
			Mapped: mapped{type_: type_, Name: field},
		}
	}

	originalFields := []iceberg.NestedField{
		{ID: 1, Name: "firstKey", Required: true, Type: iceberg.Int64Type{}},
		{ID: 2, Name: "secondKey", Required: true, Type: iceberg.StringType{}},
		{ID: 3, Name: "val1", Required: true, Type: iceberg.StringType{}},
		{ID: 4, Name: "val2", Required: false, Type: iceberg.BooleanType{}},
		{ID: 5, Name: "dateToStr", Required: true, Type: iceberg.DateType{}},
		{ID: 6, Name: "intToDecimal", Required: false, Type: iceberg.Int64Type{}},
		{ID: 7, Name: "decimalToFloat", Required: true, Type: iceberg.DecimalTypeOf(38, 0)},
		{ID: 8, Name: "timestampToStr", Required: true, Type: iceberg.TimestampTzType{}},
		// A temporary migration column that still exists from a prior failed
		// migration, with an incompatible type for the upcoming migration.
		{ID: 10, Name: "dateToStr" + migrateFieldSuffix, Required: true, Type: &iceberg.Float64Type{}},
	}

	update := boilerplate.BindingUpdate[config, resource, mapped]{
		NewProjections: []boilerplate.MappedProjection[mapped]{
			mappedProjection("new", false, iceberg.StringType{}),
			mappedProjection("newRequired", true, iceberg.StringType{}),
		},
		NewlyNullableFields: []boilerplate.ExistingField{{Name: "dateToStr"}, {Name: "dateToStr" + migrateFieldSuffix}},
		FieldsToMigrate: []boilerplate.MigrateField[mapped]{
			{
				From: boilerplate.ExistingField{Name: "dateToStr"},
				To:   mappedProjection("dateToStr", false, iceberg.StringType{}),
			},
			{
				From: boilerplate.ExistingField{Name: "intToDecimal"},
				To:   mappedProjection("intToDecimal", false, iceberg.DecimalTypeOf(38, 0)),
			},
			{
				From: boilerplate.ExistingField{Name: "decimalToFloat"},
				To:   mappedProjection("decimalToFloat", true, iceberg.Float64Type{}),
			},
			{
				From: boilerplate.ExistingField{Name: "timestampToStr"},
				To:   mappedProjection("timestampToStr", false, iceberg.StringType{}),
			},
		},
	}

	originalSchema := iceberg.NewSchemaWithIdentifiers(1, []int{1, 2}, originalFields...)
	nextSchema := computeSchemaForUpdatedTable(12, originalSchema, update)
	afterMigrateSchema := computeSchemaForCompletedMigrations(nextSchema, update.FieldsToMigrate)

	var snap strings.Builder

	snap.WriteString("--- Original Schema ---\n")
	snap.WriteString(originalSchema.String())
	snap.WriteString("\n\n")

	snap.WriteString("--- Next Schema ---\n")
	snap.WriteString(nextSchema.String())
	snap.WriteString("\n\n")

	snap.WriteString("--- After Migrate Schema ---\n")
	snap.WriteString(afterMigrateSchema.String())

	cupaloy.SnapshotT(t, snap.String())
}

func TestMapProjectionVariant(t *testing.T) {
	// Builds a boilerplate.Projection the way the validator does, from the
	// projection's inference and field config.
	proj := func(field string, types []string, isKey bool, str *pf.Inference_String, castToString bool) boilerplate.Projection {
		p := pf.Projection{
			Field:        field,
			IsPrimaryKey: isKey,
			Inference: pf.Inference{
				Types:   types,
				Exists:  pf.Inference_MUST,
				String_: str,
			},
		}
		if slices.Contains(types, "integer") || slices.Contains(types, "number") {
			p.Inference.Numeric = &pf.Inference_Numeric{}
		}
		return boilerplate.MapProjection(p, fieldConfig{CastToString_: castToString})
	}
	identity := func(f string) string { return f }

	for _, tt := range []struct {
		name       string
		p          boilerplate.Projection
		variants   bool
		wantType   iceberg.Type
		wantFormat string
	}{
		// Flag off: byte-for-byte the historical mapping.
		{"off/object", proj("f", []string{"object"}, false, nil, false), false, iceberg.StringType{}, ""},
		{"off/array", proj("f", []string{"array"}, false, nil, false), false, iceberg.StringType{}, ""},
		{"off/multiple", proj("f", []string{"string", "object"}, false, nil, false), false, iceberg.StringType{}, ""},

		// Flag on: JSON-shaped fields become variant.
		{"on/object", proj("f", []string{"object"}, false, nil, false), true, iceberg.VariantType{}, ""},
		{"on/array", proj("f", []string{"array"}, false, nil, false), true, iceberg.VariantType{}, ""},
		{"on/multiple", proj("f", []string{"string", "object"}, false, nil, false), true, iceberg.VariantType{}, ""},
		{"on/multiple-with-int", proj("f", []string{"integer", "object"}, false, nil, false), true, iceberg.VariantType{}, ""},

		// Format hints are taken from the string inference of a multi-type
		// projection only.
		{"on/multiple-date-time", proj("f", []string{"string", "object"}, false, &pf.Inference_String{Format: "date-time"}, false), true, iceberg.VariantType{}, "date-time"},
		{"on/multiple-date", proj("f", []string{"string", "integer"}, false, &pf.Inference_String{Format: "date"}, false), true, iceberg.VariantType{}, "date"},
		{"on/multiple-binary", proj("f", []string{"string", "object"}, false, &pf.Inference_String{ContentEncoding: "base64"}, false), true, iceberg.VariantType{}, "binary"},
		{"on/multiple-binary-octet-stream", proj("f", []string{"string", "object"}, false, &pf.Inference_String{ContentEncoding: "base64", ContentType: boilerplate.BinaryContentMediaType}, false), true, iceberg.VariantType{}, "binary"},
		{"on/multiple-base64-other-media-type", proj("f", []string{"string", "object"}, false, &pf.Inference_String{ContentEncoding: "base64", ContentType: "application/x-protobuf"}, false), true, iceberg.VariantType{}, ""},
		{"on/multiple-other-format", proj("f", []string{"string", "object"}, false, &pf.Inference_String{Format: "uuid"}, false), true, iceberg.VariantType{}, ""},

		// Keys, castToString, string-encoded numbers, and scalars are
		// unaffected by the flag.
		{"on/key-multiple", proj("f", []string{"string", "integer"}, true, nil, false), true, iceberg.StringType{}, ""},
		{"on/castToString-object", proj("f", []string{"object"}, false, nil, true), true, iceberg.StringType{}, ""},
		{"on/string-format-integer", proj("f", []string{"string", "integer"}, false, &pf.Inference_String{Format: "integer"}, false), true, iceberg.DecimalTypeOf(38, 0), ""},
		{"on/string-format-number", proj("f", []string{"string", "number"}, false, &pf.Inference_String{Format: "number"}, false), true, iceberg.Float64Type{}, ""},
		{"on/string", proj("f", []string{"string"}, false, nil, false), true, iceberg.StringType{}, ""},
		{"on/date-time", proj("f", []string{"string"}, false, &pf.Inference_String{Format: "date-time"}, false), true, iceberg.TimestampTzType{}, ""},
		{"on/integer", proj("f", []string{"integer"}, false, nil, false), true, iceberg.Int64Type{}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := mapProjection(tt.p, identity, tt.variants)
			require.True(t, tt.wantType.Equals(m.type_), "got %s, want %s", m.type_, tt.wantType)
			require.Equal(t, tt.wantFormat, m.VariantFormat)
		})
	}
}

func TestVariantConverter(t *testing.T) {
	for _, tt := range []struct {
		in   any
		want any
	}{
		{"a string", []byte(`"a string"`)},
		{`"quoted"`, []byte(`"\"quoted\""`)},
		{json.RawMessage(`{"a":1}`), json.RawMessage(`{"a":1}`)},
		{int64(42), int64(42)},
		{true, true},
		{nil, nil},
	} {
		got, err := variantConverter(tt.in)
		require.NoError(t, err)
		require.Equal(t, tt.want, got)
	}
}

func TestVariantMigrations(t *testing.T) {
	for _, from := range []string{"string", "long", "double", "decimal(38, 0)", "boolean", "binary", "date", "timestamptz"} {
		require.True(t, allowedMigrations.CanMigrate(from, iceberg.VariantType{}), from)
	}
	require.True(t, allowedMigrations.CanMigrate("variant", iceberg.StringType{}))
	require.False(t, allowedMigrations.CanMigrate("variant", iceberg.Int64Type{}))
	require.False(t, allowedMigrations.CanMigrate("variant", iceberg.BinaryType{}))
}
