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
	var typeMappings TypeMapper = ProjectionTypeMapper{
		INTEGER:  NewStaticMapper("BIGINT"),
		NUMBER:   NewStaticMapper("DOUBLE PRECISION"),
		BOOLEAN:  NewStaticMapper("BOOLEAN"),
		OBJECT:   NewStaticMapper("JSON"),
		ARRAY:    NewStaticMapper("JSON"),
		BINARY:   NewStaticMapper("BYTEA"),
		MULTIPLE: NewStaticMapper("JSON"),
		STRING: StringTypeMapper{
			Fallback: NewStaticMapper("TEXT"),
			WithFormat: map[string]TypeMapper{
				"integer":   NewStaticMapper("NUMERIC"),
				"number":    NewStaticMapper("DECIMAL"),
				"date-time": NewStaticMapper("TIMESTAMPTZ"),
			},
		},
	}
	var nullable = MaybeNullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    typeMappings,
	}

	return Dialect{
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
		TypeMapper:               nullable,
		AlwaysNullableTypeMapper: AlwaysNullableMapper{Delegate: typeMappings},
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
