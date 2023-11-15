package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"text/template"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

var jsonConverter sql.ElementConverter = func(te tuple.TupleElement) (interface{}, error) {
	switch ii := te.(type) {
	case []byte:
		return string(ii), nil
	case json.RawMessage:
		return string(ii), nil
	case nil:
		return string(json.RawMessage(nil)), nil
	default:
		var m, err = json.Marshal(ii)
		if err != nil {
			return nil, fmt.Errorf("cannot marshal %#v to json", ii)
		}

		return string(json.RawMessage(m)), nil
	}
}

// databricksDialect returns a representation of the Databricks SQL dialect.
// https://docs.databricks.com/en/sql/language-manual/index.html
var databricksDialect = func() sql.Dialect {
  // Although databricks does support ARRAY and MAP types, they are statically
  // typed and the MAP type is not comparable.
  // Databricks supports JSON extraction using the : operator, this seems like
  // a simpler method for persisting JSON values:
  // https://docs.databricks.com/en/sql/language-manual/sql-ref-json-path-expression.html
	var jsonMapper = sql.NewStaticMapper("STRING", sql.WithElementConverter(jsonConverter))

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
					Delegate:   sql.NewStaticMapper("BIGINT", sql.WithElementConverter(sql.StdStrToInt())),
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


// TODO: shard key in temporary table names
// TODO: use create table USING location instead of copying data into temporary table
var (
	tplAll = sql.MustParseTemplate(databricksDialect, "root", `
{{ define "temp_name_load" -}}
flow_temp_load_table_{{ $.ShardRange }}_{{ $.Table.Binding }}
{{- end }}

{{ define "temp_name_store" -}}
flow_temp_store_table_{{ $.ShardRange }}_{{ $.Table.Binding }}
{{- end }}

-- Idempotent creation of the load table for staging load keys.
{{ define "createLoadTable" }}
CREATE TABLE IF NOT EXISTS {{ template "temp_name_load" . }} (
{{- range $ind, $key := $.Table.Keys }}
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
{{- range $ind, $key := $.Table.Columns }}
	{{- if $ind }},{{ end }}
	{{ $key.Identifier }} {{ $key.DDL }}
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
{{ if $.Table.Document -}}
SELECT {{ $.Table.Binding }}, {{ $.Table.Identifier }}.{{ $.Table.Document.Identifier }}
	FROM {{ $.Table.Identifier }}
	JOIN {{ template "temp_name_load" . }} AS r
	ON {{ range $ind, $key := $.Table.Keys }}
	{{- if $ind }} AND {{ end -}}
	{{ $.Table.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
{{ else -}}
SELECT -1, ""
{{ end -}}
{{ end }}

{{ define "cast" }}
{{- if Contains $ "DATE" -}}
	::DATE
{{- else if Contains $ "TIMESTAMP" -}}
	::TIMESTAMP
{{- else if Contains $ "DOUBLE" -}}
	::DOUBLE
{{- else if Contains $ "BIGINT" -}}
	::BIGINT
{{- else if Contains $ "BOOLEAN" -}}
	::BOOLEAN
{{- else if Contains $ "BINARY" -}}
	::BINARY
{{- else if Contains $ "STRING" -}}
	::STRING
{{- end }}
{{ end }}

-- Directly copy into the target table
{{ define "copyIntoDirect" }}
	COPY INTO {{ $.Table.Identifier }} FROM (
    SELECT
		{{ range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}{{ template "cast" $key.DDL }}
		{{- end }}
  FROM {{ Literal $.StagingPath }}
	)
  FILEFORMAT = JSON
  FILES = (%s)
  FORMAT_OPTIONS ( 'mode' = 'FAILFAST', 'ignoreMissingFiles' = 'false' )
	COPY_OPTIONS ( 'mergeSchema' = 'true' )
  ;
{{ end }}

-- Copy into temporary store table
{{ define "copyIntoStore" }}
	COPY INTO {{ template "temp_name_store" . }} FROM (
    SELECT
		_metadata.file_name as _metadata_file_name,
		{{ range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}{{ template "cast" $key.DDL }}
		{{- end }}
    FROM {{ Literal $.StagingPath }}
	)
  FILEFORMAT = JSON
  FILES = (%s)
  FORMAT_OPTIONS ( 'mode' = 'FAILFAST', 'primitivesAsString' = 'true', 'ignoreMissingFiles' = 'false' )
	COPY_OPTIONS ( 'mergeSchema' = 'true' )
  ;
{{ end }}

-- Copy into temporary load table
{{ define "copyIntoLoad" }}
	COPY INTO {{ template "temp_name_load" . }} FROM (
    SELECT
      {{ range $ind, $key := $.Table.Keys }}
        {{- if $ind }}, {{ end -}}
        {{$key.Identifier -}}{{ template "cast" $key.DDL }}
      {{- end }}
    FROM {{ Literal $.StagingPath }}
  )
  FILEFORMAT = JSON
  FILES = (%s)
  FORMAT_OPTIONS ( 'mode' = 'FAILFAST', 'ignoreMissingFiles' = 'false' )
	COPY_OPTIONS ( 'mergeSchema' = 'true' )
  ;
{{ end }}

{{ define "mergeInto" }}
	MERGE INTO {{ $.Table.Identifier }} AS l
	USING {{ template "temp_name_store" . }} AS r
	ON {{ range $ind, $key := $.Table.Keys }}
		{{- if $ind }} AND {{ end -}}
		l.{{ $key.Identifier }} = r.{{ $key.Identifier }}{{ template "cast" $key.DDL }}
	{{- end }}
	AND r._metadata_file_name IN (%s)
	{{- if $.Table.Document }}
	WHEN MATCHED AND r.{{ $.Table.Document.Identifier }} <=> NULL THEN
		DELETE
	{{- end }}
	WHEN MATCHED THEN
		UPDATE SET {{ range $ind, $key := $.Table.Values }}
		{{- if $ind }}, {{ end -}}
		l.{{ $key.Identifier }} = r.{{ $key.Identifier }}{{ template "cast" $key.DDL }}
	{{- end -}}
	{{- if $.Table.Document -}}
	{{ if $.Table.Values }}, {{ end }}l.{{ $.Table.Document.Identifier}} = r.{{ $.Table.Document.Identifier }}
	{{- end }}
	WHEN NOT MATCHED THEN
		INSERT (
		{{- range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}
		{{- end -}}
	)
		VALUES (
		{{- range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			r.{{ $key.Identifier }}{{ template "cast" $key.DDL }}
		{{- end -}}
	);
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
)

type Template struct {
  StagingPath string
  ShardRange string
  Table *sql.Table
}

func RenderTable(table sql.Table, stagingPath string, shardRange string, tpl *template.Template) (string, error) {
	var w strings.Builder
  if err := tpl.Execute(&w, &Template{Table: &table, StagingPath: stagingPath, ShardRange: shardRange}); err != nil {
		return "", err
	}
	return w.String(), nil
}
