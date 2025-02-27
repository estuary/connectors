package main

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
{{ define "loadQuery" -}}
SELECT {{ $.Idx }}, l.{{ QuoteIdentifier $.Mapped.Document.Field }}
FROM {{ TableFQN $.Mapped.ResourcePath }} AS l
JOIN load_view_{{ $.Idx }} AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ QuoteIdentifier $bound.Field }} = r.{{ QuoteIdentifier $bound.Field }}
	{{- if $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Field }} >= {{ $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Field }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }}
{{ end }}


MERGE INTO whb_namespace.test_table_{idx} AS l
USING merge_view_{idx} AS r
ON l.key = r.key
WHEN MATCHED AND r.flow_document = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    l.value = r.value,
    l.flow_document = r.flow_document
WHEN NOT MATCHED THEN INSERT (key, value, flow_document)
VALUES (r.key, r.value, r.flow_document)

{{ define "mergeQuery" -}}
MERGE INTO {{ TableFQN $.Mapped.ResourcePath }} AS l
USING merge_view_{{ $.Idx }} AS r
ON {{ range $ind, $bound := $.Bounds }}
	{{ if $ind -}} AND {{end -}}
	l.{{ QuoteIdentifier $bound.Field }} = r.{{ QuoteIdentifier $bound.Field }}
	{{- if $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Field }} >= {{ $bound.LiteralLower }} AND l.{{ QuoteIdentifier $bound.Field }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end}}
WHEN MATCHED AND r.{{ QuoteIdentifier $.Mapped.Document.Field }} = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET {{ range $ind, $proj := $.Mapped.SelectedProjections }}
	{{- if $ind }}, {{ end -}}
	l.{{ QuoteIdentifier $proj.Field }} = r.{{ QuoteIdentifier $proj.Field }}
{{- end }}
WHEN NOT MATCHED AND r.{{ QuoteIdentifier $.Mapped.Document.Field }} != 'delete' THEN INSERT (
{{- range $ind, $proj := $.Mapped.SelectedProjections }}
	{{- if $ind }}, {{ end -}}
	{{ QuoteIdentifier $proj.Field -}}
{{- end -}}
) VALUES (
{{- range $ind, $proj := $.Mapped.SelectedProjections }}
	{{- if $ind }}, {{ end -}}
	r.{{ QuoteIdentifier $proj.Field -}}
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
