package main

import (
	"slices"
	"strings"
	"text/template"

	sql "github.com/estuary/connectors/materialize-sql"
)

// databricksDialect returns a representation of the Databricks SQL dialect.
// https://docs.databricks.com/en/sql/language-manual/index.html
var databricksDialect = func() sql.Dialect {
  // Although databricks does support ARRAY and MAP types, they are statically
  // typed and the MAP type is not comparable.
  // Databricks supports JSON extraction using the : operator, this seems like
  // a simpler method for persisting JSON values:
  // https://docs.databricks.com/en/sql/language-manual/sql-ref-json-path-expression.html
	var jsonMapper = sql.NewStaticMapper("STRING", sql.WithElementConverter(sql.JsonBytesConverter))

  // https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
	var typeMappings = sql.ProjectionTypeMapper{
		sql.ARRAY:    jsonMapper,
		sql.BINARY:   sql.NewStaticMapper("BINARY"),
		sql.BOOLEAN:  sql.NewStaticMapper("BOOLEAN"),
		sql.INTEGER:  sql.NewStaticMapper("BIGINT"),
		sql.NUMBER:   sql.NewStaticMapper("DOUBLE"),
		sql.OBJECT:   jsonMapper,
		sql.MULTIPLE: jsonMapper,
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.NewStaticMapper("STRING"),
			WithFormat: map[string]sql.TypeMapper{
				"integer": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("STRING"),
					Delegate:   sql.NewStaticMapper("INTEGER", sql.WithElementConverter(sql.StdStrToInt())), // Equivalent to NUMBER(38,0)
				},
				"number": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("STRING"),
					Delegate:   sql.NewStaticMapper("DOUBLE", sql.WithElementConverter(sql.StdStrToFloat("NaN", "Inf", "-Inf"))),
				},
				"date":      sql.NewStaticMapper("DATE"),
				"date-time": sql.NewStaticMapper("TIMESTAMP"),
			},
		},
	}
	var nullable sql.TypeMapper = sql.MaybeNullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    typeMappings,
	}

	return sql.Dialect{
    // https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".", 
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(DATABRICKS_RESERVED_WORDS, strings.ToLower(s))
				},
        sql.QuoteTransform("`", "``"),
      ))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "\\'")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		TypeMapper:               nullable,
		AlwaysNullableTypeMapper: sql.AlwaysNullableMapper{Delegate: typeMappings},
	}
}()

var (
	tplAll = sql.MustParseTemplate(databricksDialect, "root", `
{{ define "temp_name_load" -}}
flow_temp_load_table_{{ $.Binding }}
{{- end }}

{{ define "temp_name_store" -}}
flow_temp_store_table_{{ $.Binding }}
{{- end }}

-- Idempotent creation of the load table for staging load keys.
{{ define "createLoadTable" }}
CREATE TABLE IF NOT EXISTS {{ template "temp_name_load" . }} (
{{- range $ind, $key := $.Keys }}
	{{- if $ind }},{{ end }}
	{{ $key.Identifier }} {{ $key.DDL }}
{{- end }}
);
{{ end }}

-- Templated truncation of the temporary load table:
{{ define "truncateLoadTable" }}
TRUNCATE TABLE {{ template "temp_name_load" . }};
{{ end }}

{{ define "dropLoadTable" }}
DROP TABLE IF EXISTS {{ template "temp_name_load" . }}
{{ end }}

-- Idempotent creation of the store table for staging new records.
{{ define "createStoreTable" }}
CREATE TABLE IF NOT EXISTS {{ template "temp_name_store" . }} (
	_metadata_file_name STRING,
  {{- range $ind, $col := $.Columns }}
  ,
  {{$col.Identifier}} {{$col.DDL}}
  {{- end }}
);
{{ end }}

-- Templated truncation of the temporary store table:
{{ define "truncateStoreTable" }}
TRUNCATE TABLE {{ template "temp_name_store" . }};
{{ end }}

{{ define "dropStoreTable" }}
DROP TABLE IF EXISTS {{ template "temp_name_store" . }}
{{ end }}

-- Templated creation of a materialized table definition and comments:
{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
  {{- range $ind, $col := $.Columns }}
  {{- if $ind }},{{ end }}
  {{$col.Identifier}} {{$col.DDL}} COMMENT {{ Literal $col.Comment }}
  {{- end }}
) COMMENT {{ Literal $.Comment }};
{{ end }}

-- Templated query which joins keys from the load table with the target table, and returns values. It
-- deliberately skips the trailing semi-colon as these queries are composed with a UNION ALL.

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }}, {{ $.Identifier }}.{{ $.Document.Identifier }}
	FROM {{ $.Identifier }}
	JOIN {{ template "temp_name_load" . }} AS r
	ON {{ range $ind, $key := $.Keys }}
	{{- if $ind }} AND {{ end -}}
	{{ $.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
{{ else -}}
SELECT -1, NULL
{{ end -}}
{{ end }}

-- Directly copy into the target table
{{ define "copyIntoDirect" }}
	COPY INTO {{ $.Table.Identifier }} FROM (
    SELECT
		{{ range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}
		{{- end }}
  FROM {{ Literal $.StagingPath }}
	)
  FILEFORMAT = JSON
  FILES = (%s)
  FORMAT_OPTIONS ( 'mode' = 'FAILFAST', 'inferTimestamp' = 'true' )
  ;
{{ end }}

-- Copy into temporary store table
{{ define "copyIntoStore" }}
	COPY INTO {{ template "temp_name_store" $.Table }} FROM (
    SELECT
		_metadata.file_name as _metadata_file_name,
		{{ range $ind, $key := $.Table.Columns }}
			,
			{{$key.Identifier -}}
		{{- end }}
    FROM {{ Literal $.StagingPath }}
	)
  FILEFORMAT = JSON
  FILES = (%s)
  FORMAT_OPTIONS ( 'mode' = 'FAILFAST', 'inferTimestamp' = 'true' )
  ;
{{ end }}

-- Copy into temporary load table
{{ define "copyIntoLoad" }}
	COPY INTO {{ template "temp_name_load" $.Table }} FROM (
    SELECT
      {{ range $ind, $key := $.Table.Keys }}
        {{- if $ind }}, {{ end -}}
        {{$key.Identifier -}}
      {{- end }}
    FROM {{ Literal $.StagingPath }}
  )
  FILEFORMAT = JSON
  FILES = (%s)
  FORMAT_OPTIONS ( 'mode' = 'FAILFAST', 'inferTimestamp' = 'true' )
  ;
{{ end }}

{{ define "mergeInto" }}
	MERGE INTO {{ $.Identifier }} AS l
	USING {{ template "temp_name_store" . }} AS r
	ON {{ range $ind, $key := $.Keys }}
		{{- if $ind }} AND {{ end -}}
		l.{{ $key.Identifier }} = r.{{ $key.Identifier }} AND
		r._metadata_file_name IN (%s)
	{{- end }}
	{{- if $.Document }}
	WHEN MATCHED AND r.{{ $.Document.Identifier }} <=> NULL THEN
		DELETE
	{{- end }}
	WHEN MATCHED THEN
		UPDATE SET {{ range $ind, $key := $.Values }}
		{{- if $ind }}, {{ end -}}
		l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end -}}
	{{- if $.Document -}}
	{{ if $.Values }}, {{ end }}l.{{ $.Document.Identifier}} = r.{{ $.Document.Identifier }}
	{{- end }}
	WHEN NOT MATCHED THEN
		INSERT (
		{{- range $ind, $key := $.Columns }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}
		{{- end -}}
	)
		VALUES (
		{{- range $ind, $key := $.Columns }}
			{{- if $ind }}, {{ end -}}
			r.{{$key.Identifier -}}
		{{- end -}}
	);
{{ end }}

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
	tplCreateLoadTable   = tplAll.Lookup("createLoadTable")
	tplCreateStoreTable  = tplAll.Lookup("createStoreTable")
	tplLoadQuery         = tplAll.Lookup("loadQuery")
  tplTruncateLoad      = tplAll.Lookup("truncateLoadTable")
  tplTruncateStore     = tplAll.Lookup("truncateStoreTable")
  tplDropLoad          = tplAll.Lookup("dropLoadTable")
  tplDropStore         = tplAll.Lookup("dropStoreTable")
	tplCopyIntoDirect    = tplAll.Lookup("copyIntoDirect")
	tplCopyIntoLoad      = tplAll.Lookup("copyIntoLoad")
	tplCopyIntoStore     = tplAll.Lookup("copyIntoStore")
	tplMergeInto         = tplAll.Lookup("mergeInto")
	tplUpdateFence       = tplAll.Lookup("updateFence")
)

type CopyTemplate struct {
  StagingPath string
  Table *sql.Table
}

func RenderTableWithStagingPath(table sql.Table, stagingPath string, tpl *template.Template) (string, error) {
	var w strings.Builder
  if err := tpl.Execute(&w, &CopyTemplate{Table: &table, StagingPath: stagingPath}); err != nil {
		return "", err
	}
	return w.String(), nil
}
