package connector

import (
	"fmt"
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

	update := boilerplate.MaterializerBindingUpdate[mapped]{
		NewProjections:      []boilerplate.MappedProjection[mapped]{mappedProjection("new", false, iceberg.StringType{})},
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
