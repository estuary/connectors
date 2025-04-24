package connector

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/apache/iceberg-go"
)

type templates struct {
	loadQuery    *template.Template
	mergeQuery   *template.Template
	migrateQuery *template.Template
}

type templateInput struct {
	binding
	Bounds []mergeBound
}

type migrateColumn struct {
	Name       string
	FromType   string
	TargetType iceberg.Type
}

type migrateInput struct {
	ResourcePath []string
	Migrations   []migrateColumn
}

func quoteIdentifier(in string) string { return "`" + in + "`" }

func parseTemplates() templates {
	tpl := template.New("root").Funcs(template.FuncMap{
		"QuoteIdentifier": quoteIdentifier,
		"TableFQN": func(in []string) string {
			quotedParts := make([]string, len(in))
			for i, part := range in {
				quotedParts[i] = quoteIdentifier(part)
			}
			return strings.Join(quotedParts, ".")
		},
		"IsBinary":          func(m mapped) bool { return m.type_.Equals(iceberg.BinaryType{}) },
		"MigrateColumnName": func(f string) string { return f + migrateFieldSuffix },
		"CastSQL": func(m migrateColumn) string {
			ident := quoteIdentifier(m.Name)
			switch m.FromType {
			case "binary":
				return fmt.Sprintf("BASE64(%s)", ident)
			case "timestamptz":
				// timestamptz columns have an internal storage representation
				// in UTC so this will always result in a Z timestamp string,
				// which is consistent with what you get if you read a
				// timestamptz column.
				return fmt.Sprintf(`DATE_FORMAT(%s, 'yyyy-MM-dd\'T\'HH:mm:ss.SSSSSS\'Z\'')`, ident)
			default:
				return fmt.Sprintf("CAST(%s AS %s)", ident, m.TargetType.Type())
			}
		},
	})

	parsed := template.Must(tpl.Parse(`
{{ define "maybe_unbase64_rhs" -}}
{{- if IsBinary $.Mapped -}}UNBASE64(r.{{ QuoteIdentifier $.Mapped.Name }}){{ else }}r.{{ QuoteIdentifier $.Mapped.Name }}{{ end }}
{{- end }}

{{ define "loadQuery" -}}
SELECT {{ $.Idx }}, l.{{ QuoteIdentifier $.Mapped.Document.Mapped.Name }}
FROM {{ TableFQN $.Mapped.ResourcePath }} AS l
JOIN load_view_{{ $.Idx }} AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ QuoteIdentifier $bound.Mapped.Name }} = {{ template "maybe_unbase64_rhs" $bound }}
	{{- if $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Mapped.Name }} >= {{ $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Mapped.Name }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }}
{{ end }}

{{ define "mergeQuery" -}}
MERGE INTO {{ TableFQN $.Mapped.ResourcePath }} AS l
USING merge_view_{{ $.Idx }} AS r
ON {{ range $ind, $bound := $.Bounds }}
	{{ if $ind -}} AND {{end -}}
	l.{{ QuoteIdentifier $bound.Mapped.Name }} = {{ template "maybe_unbase64_rhs" $bound }}
	{{- if $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Mapped.Name }} >= {{ $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Mapped.Name }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end}}
WHEN MATCHED AND r.{{ QuoteIdentifier $.Mapped.Document.Mapped.Name }} = '"delete"' THEN DELETE
WHEN MATCHED THEN UPDATE SET {{ range $ind, $proj := $.Mapped.SelectedProjections }}
	{{- if $ind }}, {{ end -}}
	l.{{ QuoteIdentifier $proj.Mapped.Name }} = {{ template "maybe_unbase64_rhs" $proj }}
{{- end }}
WHEN NOT MATCHED AND r.{{ QuoteIdentifier $.Mapped.Document.Mapped.Name }} != '"delete"' THEN INSERT (
{{- range $ind, $proj := $.Mapped.SelectedProjections }}
	{{- if $ind }}, {{ end -}}
	{{ QuoteIdentifier $proj.Mapped.Name -}}
{{- end -}}
) VALUES (
{{- range $ind, $proj := $.Mapped.SelectedProjections }}
	{{- if $ind }}, {{ end -}}
	{{ template "maybe_unbase64_rhs" $proj }}
{{- end -}}
)
{{ end }}

{{ define "migrateQuery" -}}
UPDATE {{ TableFQN $.ResourcePath }}
SET
{{- range $ind, $col := $.Migrations }}
	{{- if $ind }}, {{ end }}
	{{ QuoteIdentifier (MigrateColumnName $col.Name) }} = {{ CastSQL $col }}
{{- end }}
{{ end }}
`))

	return templates{
		loadQuery:    parsed.Lookup("loadQuery"),
		mergeQuery:   parsed.Lookup("mergeQuery"),
		migrateQuery: parsed.Lookup("migrateQuery"),
	}
}
