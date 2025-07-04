package main

import (
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql-v2"
)

var sqliteDialect = func() sql.Dialect {
	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.ARRAY:          sql.MapStatic("TEXT"),
			sql.BINARY:         sql.MapStatic("TEXT"),
			sql.BOOLEAN:        sql.MapStatic("BOOLEAN"),
			sql.INTEGER:        sql.MapStatic("INTEGER"),
			sql.NUMBER:         sql.MapStatic("REAL"),
			sql.OBJECT:         sql.MapStatic("TEXT"),
			sql.MULTIPLE:       sql.MapStatic("TEXT"),
			sql.STRING_INTEGER: sql.MapStatic("INTEGER"),
			sql.STRING_NUMBER:  sql.MapStatic("REAL"),
			sql.STRING:         sql.MapStatic("TEXT"),
		},
		sql.WithNotNullText("NOT NULL"),
	)

	return sql.Dialect{
		// TableLocator and ColumnLocator are not used for sqlite, since sqlite re-creates all tables
		// from scratch every time it starts up.
		TableLocatorer:  sql.TableLocatorFn(func(path []string) sql.InfoTableLocation { return sql.InfoTableLocation{} }),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return field }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(SQLITE_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform("\"", "\\\""),
			))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		TypeMapper: mapper,
	}
}()

var (
	tplAll = sql.MustParseTemplate(sqliteDialect, "root", `
  {{ define "temp_name" -}}
  flow_temp_table_{{ $.Binding }}
  {{- end }}

  -- Templated creation of a materialized table definition and comments:
  {{ define "createTargetTable" }}
  CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
    {{- range $ind, $col := $.Columns }}
    {{- if $ind }},{{ end }}
    {{$col.Identifier}} {{$col.DDL}}
    {{- end }}
    {{- if not $.DeltaUpdates }},
    PRIMARY KEY (
      {{- range $ind, $key := $.Keys }}
      {{- if $ind }}, {{end -}}
      {{$key.Identifier}}
      {{- end -}}
    )
    {{- end }}
  );
  {{ end }}

  -- Create temporary table for loading keys
  {{ define "createLoadTable" }}
  CREATE TABLE {{ template "temp_name" . }} (
    {{- range $ind, $key := $.Keys }}
    {{- if $ind }},{{ end }}
    {{ $key.Identifier }} {{ $key.DDL }}
    {{- end }}
  );
  {{ end }}

  -- Insert keys into temporary table
  {{ define "loadInsert" }}
  INSERT INTO {{ template "temp_name" . }} (
    {{- range $ind, $key := $.Keys }}
    {{- if $ind }}, {{ end -}}
    {{ $key.Identifier }}
    {{- end -}}
  )
  VALUES (
    {{- range $ind, $key := $.Keys }}
    {{- if $ind }}, {{ end -}}
    {{ $key.Placeholder }}
    {{- end -}}
  );
  {{ end }}

  -- Load
  {{ define "loadQuery" }}
  {{ if $.Document -}}
  SELECT {{ $.Binding }}, r.{{$.Document.Identifier}}
  FROM {{ template "temp_name" . }} AS l
  JOIN {{ $.Identifier}} AS r
  {{- range $ind, $key := $.Keys }}
  {{ if $ind }} AND {{ else }} ON  {{ end -}}
  {{ if $key.MustExist -}}
  l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
  {{- else -}}
  (l.{{ $key.Identifier }} = r.{{ $key.Identifier }} and l.{{ $key.Identifier }} is not null and r.{{ $key.Identifier }} is not null) or (l.{{ $key.Identifier }} is null and r.{{ $key.Identifier }} is null)
  {{- end }}
  {{- end }}
  {{ else -}}
  SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc
  {{ end }}
  {{- end }}

  -- Truncate
  {{ define "loadTruncate" }}
  DELETE FROM {{ template "temp_name" . }};
  {{ end }}


  {{ define "storeInsert" }}
  INSERT INTO {{ $.Identifier }} (
    {{- range $ind, $col := $.Columns }}
    {{- if $ind }},{{ end }}
    {{$col.Identifier}}
    {{- end }}
  ) VALUES (
    {{- range $ind, $col := $.Columns }}
    {{- if $ind}}, {{ end -}}
    {{ $col.Placeholder }}
    {{- end -}}
  );
  {{ end }}

  {{ define "storeUpdate" }}
  UPDATE {{$.Identifier}} SET
  {{- range $ind, $val := $.Values }}
  {{- if $ind }},{{ end }}
  {{ $val.Identifier}} = {{ $val.Placeholder }}
  {{- end }}
  {{- if $.Document }},
  {{ $.Document.Identifier }} = {{ $.Document.Placeholder }}
  {{- end -}}
  {{ range $ind, $key := $.Keys }}
  {{ if $ind }} AND   {{ else }} WHERE {{ end -}}
  {{ if $key.MustExist -}}
  {{ $key.Identifier }} = {{ $key.Placeholder }}
  {{- else -}}
  ({{ $key.Identifier }} = {{ $key.Placeholder }} and {{ $key.Identifier }} is not null and {{ $key.Placeholder }} is not null) or ({{ $key.Identifier }} is null and {{ $key.Placeholder }} is null)
  {{- end }}
  {{- end -}}
  ;
  {{ end }}
  `)
	tplCreateTargetTable = tplAll.Lookup("createTargetTable")
	tplCreateLoadTable   = tplAll.Lookup("createLoadTable")

	tplLoadInsert   = tplAll.Lookup("loadInsert")
	tplLoadQuery    = tplAll.Lookup("loadQuery")
	tplLoadTruncate = tplAll.Lookup("loadTruncate")

	tplStoreInsert = tplAll.Lookup("storeInsert")
	tplStoreUpdate = tplAll.Lookup("storeUpdate")
)

const attachSQL = "ATTACH DATABASE '' AS load ;"
