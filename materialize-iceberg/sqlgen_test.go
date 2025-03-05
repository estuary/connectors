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

	keys := []boilerplate.MappedProjection[mapped]{
		{Projection: pf.Projection{Field: "first-key"}},
		{Projection: pf.Projection{Field: "second-key"}, Mapped: mapped{type_: iceberg.BinaryType{}}},
		{Projection: pf.Projection{Field: "third-key"}},
	}

	values := []boilerplate.MappedProjection[mapped]{
		{Projection: pf.Projection{Field: "first-val"}},
		{Projection: pf.Projection{Field: "second-val"}},
		{Projection: pf.Projection{Field: "third-val"}},
		{Projection: pf.Projection{Field: "fourth-val"}},
	}

	doc := &boilerplate.MappedProjection[mapped]{Projection: pf.Projection{Field: "flow_document"}}

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

	snap.WriteString("--- Begin load query ---\n")
	require.NoError(t, templates.loadQuery.Execute(&snap, input))
	snap.WriteString("--- End load query ---")

	snap.WriteString("\n\n")

	snap.WriteString("--- Begin merge query ---\n")
	require.NoError(t, templates.mergeQuery.Execute(&snap, input))
	snap.WriteString("--- End merge query ---")

	cupaloy.SnapshotT(t, snap.String())

}
