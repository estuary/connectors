package connector

import (
	"strings"
	"text/template"
)

type templates struct {
	loadQuery  *template.Template
	mergeQuery *template.Template
}

type templateInput struct {
	binding
	Bounds []mergeBound
}

func parseTemplates() templates {
	tpl := template.New("root").Funcs(template.FuncMap{
		"QuoteIdentifier": quoteIdentifier,
		"TableFQN":        tableFQN,
	})

	parsed := template.Must(tpl.Parse(`
{{ define "maybe_unbase64_rhs" -}}
{{- if $.Mapped.IsBinary -}}unbase64(r.{{ QuoteIdentifier $.Field }}){{ else }}r.{{ QuoteIdentifier $.Field }}{{ end }}
{{- end }}

{{ define "loadQuery" -}}
SELECT {{ $.Idx }}, l.{{ QuoteIdentifier $.Mapped.Document.Field }}
FROM {{ TableFQN $.Mapped.ResourcePath }} AS l
JOIN load_view_{{ $.Idx }} AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ QuoteIdentifier $bound.Field }} = {{ template "maybe_unbase64_rhs" $bound }}
	{{- if $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Field }} >= {{ $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Field }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }}
{{ end }}

{{ define "mergeQuery" -}}
MERGE INTO {{ TableFQN $.Mapped.ResourcePath }} AS l
USING merge_view_{{ $.Idx }} AS r
ON {{ range $ind, $bound := $.Bounds }}
	{{ if $ind -}} AND {{end -}}
	l.{{ QuoteIdentifier $bound.Field }} = {{ template "maybe_unbase64_rhs" $bound }}
	{{- if $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Field }} >= {{ $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Field }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end}}
WHEN MATCHED AND r.{{ QuoteIdentifier $.Mapped.Document.Field }} = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET {{ range $ind, $proj := $.Mapped.SelectedProjections }}
	{{- if $ind }}, {{ end -}}
	l.{{ QuoteIdentifier $proj.Field }} = {{ template "maybe_unbase64_rhs" $proj }}
{{- end }}
WHEN NOT MATCHED AND r.{{ QuoteIdentifier $.Mapped.Document.Field }} != 'delete' THEN INSERT (
{{- range $ind, $proj := $.Mapped.SelectedProjections }}
	{{- if $ind }}, {{ end -}}
	{{ QuoteIdentifier $proj.Field -}}
{{- end -}}
) VALUES (
{{- range $ind, $proj := $.Mapped.SelectedProjections }}
	{{- if $ind }}, {{ end -}}
	{{ template "maybe_unbase64_rhs" $proj }}
{{- end -}}
)
{{ end }}
`))

	return templates{
		loadQuery:  parsed.Lookup("loadQuery"),
		mergeQuery: parsed.Lookup("mergeQuery"),
	}
}

func quoteIdentifier(in string) string { return "`" + in + "`" }

func tableFQN(in []string) string {
	quotedParts := make([]string, len(in))
	for i, part := range in {
		quotedParts[i] = quoteIdentifier(part)
	}
	return strings.Join(quotedParts, ".")
}
