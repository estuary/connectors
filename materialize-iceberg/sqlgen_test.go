package connector

import (
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestTemplates(t *testing.T) {
	templates := parseTemplates()

	var snap strings.Builder

	makeProjection := func(field string) boilerplate.Projection {
		return boilerplate.Projection{
			Projection: pf.Projection{Field: field},
		}
	}

	keys := []boilerplate.MappedProjection[mapped]{
		{Projection: makeProjection("first-key"), Mapped: mapped{iceberg.StringType{}}},
		{Projection: makeProjection("second-key"), Mapped: mapped{iceberg.BinaryType{}}},
		{Projection: makeProjection("third-key"), Mapped: mapped{iceberg.Int64Type{}}},
	}

	values := []boilerplate.MappedProjection[mapped]{
		{Projection: makeProjection("first-val"), Mapped: mapped{iceberg.StringType{}}},
		{Projection: makeProjection("second-val"), Mapped: mapped{iceberg.StringType{}}},
		{Projection: makeProjection("third-val"), Mapped: mapped{iceberg.BinaryType{}}},
		{Projection: makeProjection("fourth-val"), Mapped: mapped{iceberg.StringType{}}},
	}

	doc := &boilerplate.MappedProjection[mapped]{Projection: makeProjection("flow_document"), Mapped: mapped{iceberg.StringType{}}}

	input := templateInput{
		binding: binding{
			Idx: 0,
			Mapped: &boilerplate.MappedBinding[config, resource, mapped]{
				MaterializationSpec_Binding: pf.MaterializationSpec_Binding{
					ResourcePath: []string{"foo", "bar"},
				},
				Keys:     keys,
				Values:   values,
				Document: doc,
			},
		},
		Bounds: []mergeBound{
			{
				MappedProjection: keys[0],
				LiteralLower:     "1",
				LiteralUpper:     "10",
			},
			{
				MappedProjection: keys[1],
			},
			{
				MappedProjection: keys[2],
				LiteralLower:     "'aaaSomeString'",
				LiteralUpper:     "'zzzSomeString'",
			},
		},
	}

	mInput := migrateInput{
		ResourcePath: []string{"some", "table"},
		Migrations: []migrateColumn{
			{
				Field:      "long_to_decimal",
				FromType:   "long",
				TargetType: iceberg.DecimalTypeOf(38, 0),
			},
			{
				Field:      "datetime_to_string",
				FromType:   "timestamptz",
				TargetType: iceberg.StringType{},
			},
			{
				Field:      "binary_to_string",
				FromType:   "binary",
				TargetType: iceberg.StringType{},
			},
		},
	}

	snap.WriteString("--- Begin load query ---\n")
	require.NoError(t, templates.loadQuery.Execute(&snap, input))
	snap.WriteString("--- End load query ---")

	snap.WriteString("\n\n")

	snap.WriteString("--- Begin merge query ---\n")
	require.NoError(t, templates.mergeQuery.Execute(&snap, input))
	snap.WriteString("--- End merge query ---")

	snap.WriteString("\n\n")

	snap.WriteString("--- Begin migrate query ---\n")
	require.NoError(t, templates.migrateQuery.Execute(&snap, mInput))
	snap.WriteString("--- End migrate query ---")

	cupaloy.SnapshotT(t, snap.String())

}
