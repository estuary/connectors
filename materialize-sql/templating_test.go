package sql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
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
		Literaler: LiteralFn(QuoteTransform("'", "''")),
		Placeholderer: PlaceholderFn(func(index int) string {
			return fmt.Sprintf("$%d", index+1)
		}),
		TypeMapper: mapper,
	}
}

func TestTableTemplate(t *testing.T) {
	var (
		shape      = FlowCheckpointsTable("one", "reserved", "checkpoints")
		dialect    = newTestDialect()
		table, err = ResolveTable(shape, dialect)
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
