package sql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDialect() Dialect {
	mapper := NewDDLMapper(
		FlatTypeMappings{
			ARRAY:          MapStatic("JSON"),
			BINARY:         MapStatic("BYTEA"),
			BOOLEAN:        MapStatic("BOOLEAN"),
			INTEGER:        MapStatic("BIGINT"),
			MULTIPLE:       MapStatic("JSON"),
			NUMBER:         MapStatic("DOUBLE PRECISION"),
			OBJECT:         MapStatic("JSON"),
			STRING_INTEGER: MapStatic("NUMERIC"),
			STRING_NUMBER:  MapStatic("DECIMAL"),
			STRING: MapString(StringMappings{
				Fallback: MapStatic("TEXT"),
				WithFormat: map[string]MapProjectionFn{
					"date-time": MapStatic("TIMESTAMPTZ"),
				},
			}),
		},
		WithNotNullText("NOT NULL"),
	)

	return Dialect{
		TableLocatorer: TableLocatorFn(func(path []string) InfoTableLocation {
			return InfoTableLocation{TableSchema: path[1], TableName: path[2]}
		}),
		ColumnLocatorer: ColumnLocatorFn(func(field string) string { return field }),
		Identifierer: IdentifierFn(JoinTransform(".",
			PassThroughTransform(
				func(s string) bool {
					return IsSimpleIdentifier(s) && strings.ToLower(s) != "reserved"
				},
				QuoteTransform("\"", "\\\""),
			))),
		Literaler: ToLiteralFn(QuoteTransform("'", "''")),
		Placeholderer: PlaceholderFn(func(index int) string {
			return fmt.Sprintf("$%d", index+1)
		}),
		TypeMapper: mapper,
	}
}

func TestTableTemplate(t *testing.T) {
	var (
		shape      = FlowCheckpointsTable([]string{"one", "reserved"})
		dialect    = newTestDialect()
		table, err = ResolveTable(*shape, dialect)
	)
	assert.NoError(t, err)

	var tpl = MustParseTemplate(dialect, "template", `
	CREATE TABLE {{$.Identifier}} (
		{{- range $ind, $col := $.Columns }}
			{{- if $ind}},{{end}}
			{{$col.Identifier}} {{$col.DDL}}
		{{- end }}
		{{- if not $.DeltaUpdates }},
			PRIMARY KEY (
		{{- range $ind, $key := $.Keys }}
			{{- if $ind}}, {{end -}}
			{{$key.Identifier}}
		{{- end -}}
		)
		{{ end }}
	);

	COMMENT ON TABLE {{$.Identifier}} IS {{Literal $.Comment}};
	{{- range $col := .Columns }}
	COMMENT ON COLUMN {{$.Identifier}}.{{$col.Identifier}} IS {{Literal $col.Comment}};
	{{- end}}
	`)

	out, err := RenderTableTemplate(table, tpl)
	require.NoError(t, err)
	cupaloy.SnapshotT(t, out)
}

func TestMergeBoundsBuilder(t *testing.T) {
	literaler := ToLiteralFn(QuoteTransform("'", "''"))

	colA := Column{Identifier: "colA"}
	colB := Column{Identifier: "colB", Projection: Projection{Projection: pf.Projection{
		Inference: pf.Inference{
			Types: []string{"boolean"},
		},
	}}}
	colC := Column{Identifier: "colC"}

	for _, tt := range []struct {
		name       string
		keyColumns []Column
		keys       [][]any
		want       []MergeBound
	}{
		{
			name:       "single key",
			keyColumns: []Column{colA},
			keys: [][]any{
				{"a"},
				{"b"},
				{"c"},
			},
			want: []MergeBound{
				{colA, literaler("a"), literaler("c")},
			},
		},
		{
			name:       "multiple keys ordered",
			keyColumns: []Column{colA, colB, colC},
			keys: [][]any{
				{"a", true, int64(1)},
				{"b", false, int64(2)},
				{"c", true, int64(3)},
			},
			want: []MergeBound{
				{colA, literaler("a"), literaler("c")},
				{colB, "", ""},
				{colC, literaler(int64(1)), literaler(int64(3))},
			},
		},
		{
			name:       "multiple keys unordered",
			keyColumns: []Column{colA, colB, colC},
			keys: [][]any{
				{"a", true, int64(3)},
				{"b", false, int64(1)},
				{"c", true, int64(2)},
			},
			want: []MergeBound{
				{colA, literaler("a"), literaler("c")},
				{colB, "", ""},
				{colC, literaler(int64(1)), literaler(int64(3))},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := NewMergeBoundsBuilder(tt.keyColumns, literaler)
			for _, store := range tt.keys {
				b.NextKey(store)
			}
			require.Equal(t, tt.want, b.Build())
		})
	}
}
