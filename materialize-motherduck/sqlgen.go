package main

import (
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
)

var duckDialect = func() sql.Dialect {
	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER:        sql.MapStatic("BIGINT"),
			sql.NUMBER:         sql.MapStatic("DOUBLE"),
			sql.BOOLEAN:        sql.MapStatic("BOOLEAN"),
			sql.OBJECT:         sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.ARRAY:          sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.BINARY:         sql.MapStatic("VARCHAR"),
			sql.MULTIPLE:       sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.STRING_INTEGER: sql.MapStatic("HUGEINT", sql.UsingConverter(sql.StrToInt)),
			// https://duckdb.org/docs/sql/data_types/numeric.html#floating-point-types
			sql.STRING_NUMBER: sql.MapStatic("DOUBLE", sql.UsingConverter(sql.StrToFloat("NaN", "Infinity", "-Infinity"))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("VARCHAR"),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      sql.MapStatic("DATE"),
					"date-time": sql.MapStatic("TIMESTAMP WITH TIME ZONE"),
					"duration":  sql.MapStatic("INTERVAL"),
					"time":      sql.MapStatic("TIME"),
					"uuid":      sql.MapStatic("UUID"),
				},
			}),
		},
		sql.WithNotNullText("NOT NULL"),
	)

	return sql.Dialect{
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			return sql.InfoTableLocation{TableSchema: path[1], TableName: path[2]}
		}),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return field }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(DUCKDB_RESERVED_WORDS, strings.ToUpper(s))
				},
				sql.QuoteTransform(`"`, `""`),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		MaxColumnCharLength:    0, // Duckdb has no apparent limit on how long column names can be
		CaseInsensitiveColumns: true,
	}
}()

type queryParams struct {
	sql.Table
	Files []string
}

var (
	tplAll = sql.MustParseTemplate(duckDialect, "root", `
-- Templated creation of a materialized table definition.

{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
{{- range $ind, $col := $.Columns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.DDL}}
{{- end }}
);
{{ end }}

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }} AS binding, l.{{ $.Document.Identifier }} AS doc
FROM {{ $.Identifier }} AS l
JOIN read_json(
	[
	{{- range $ind, $f := $.Files }}
	{{- if $ind }}, {{ end }}'{{ $f }}'
	{{- end -}}
	],
	format='newline_delimited',
	compression='gzip',
	columns={
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{$key.Identifier}}: '{{$key.DDL}}'
	{{- end }}
	}
) AS r
{{- range $ind, $key := $.Keys }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
{{- end -}}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc
{{- end }}
{{ end }}

-- Templated query for merging documents from S3 into the target table.

{{ define "storeDeleteQuery" }}
DELETE FROM {{$.Identifier}} AS l
USING read_json(
	[
	{{- range $ind, $f := $.Files }}
	{{- if $ind }}, {{ end }}'{{ $f }}'
	{{- end -}}
	],
	format='newline_delimited',
	compression='gzip',
	columns={
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}: '{{$col.DDL}}'
	{{- end }}
	}
) AS r
{{- range $ind, $key := $.Keys }}
	{{ if $ind }} AND {{ else }} WHERE {{ end -}}
	l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
{{- end }};
{{ end }}

{{ define "storeQuery" }}
INSERT INTO {{$.Identifier}} BY NAME
SELECT * FROM read_json(
	[
	{{- range $ind, $f := $.Files }}
	{{- if $ind }}, {{ end }}'{{ $f }}'
	{{- end -}}
	],
	format='newline_delimited',
	compression='gzip',
	columns={
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}: '{{$col.DDL}}'
	{{- end }}
	}
){{ if $.Document }} WHERE {{ $.Document.Identifier }} != '"delete"'{{- end }};
{{ end }}

-- Templated update of a fence checkpoint.

{{ define "updateFence" }}
UPDATE {{ Identifier $.TablePath }}
	SET   checkpoint = {{ Literal (Base64Std $.Checkpoint) }}
	WHERE materialization = {{ Literal $.Materialization.String }}
	AND   key_begin = {{ $.KeyBegin }}
	AND   key_end   = {{ $.KeyEnd }}
	AND   fence     = {{ $.Fence }};
{{ end }}
`)
	tplCreateTargetTable = tplAll.Lookup("createTargetTable")
	tplStoreQuery        = tplAll.Lookup("storeQuery")
	tplStoreDeleteQuery  = tplAll.Lookup("storeDeleteQuery")
	tplLoadQuery         = tplAll.Lookup("loadQuery")
	tplUpdateFence       = tplAll.Lookup("updateFence")
)
