package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"text/template"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// Databricks does not allow column names to contain these characters. Attempting to create a column
// with any of them causes an error, so they must be replaced with underscores. Ref:
// https://docs.databricks.com/en/error-messages/error-classes.html#delta_invalid_characters_in_column_name
var columnSanitizerRegexp = regexp.MustCompile(`[ ,;{}\(\)]`)

func translateFlowField(f string) string {
	return columnSanitizerRegexp.ReplaceAllString(f, "_")
}

var jsonConverter sql.ElementConverter = func(te tuple.TupleElement) (interface{}, error) {
	switch ii := te.(type) {
	case []byte:
		return string(ii), nil
	case json.RawMessage:
		return string(ii), nil
	case nil:
		return string(json.RawMessage(nil)), nil
	default:
		var m, err = json.Marshal(te)
		if err != nil {
			return nil, fmt.Errorf("cannot marshal %#v to json", te)
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
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
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

	mapper = sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    mapper,
	}

	columnValidator := sql.NewColumnValidator(
		sql.ColValidation{Types: []string{"string"}, Validate: stringCompatible},
		sql.ColValidation{Types: []string{"boolean"}, Validate: sql.BooleanCompatible},
		sql.ColValidation{Types: []string{"long"}, Validate: sql.IntegerCompatible},
		sql.ColValidation{Types: []string{"double"}, Validate: sql.NumberCompatible},
		sql.ColValidation{Types: []string{"date"}, Validate: sql.DateCompatible},
		sql.ColValidation{Types: []string{"timestamp"}, Validate: sql.DateTimeCompatible},
	)

	return sql.Dialect{
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			return sql.InfoTableLocation{
				// Object names (including schemas and table names) are lowercased in Databricks.
				// Column names are case-sensitive though.
				TableSchema: strings.ToLower(path[0]),
				TableName:   strings.ToLower(path[1]),
			}
		}),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return translateFlowField(field) }),
		Identifierer: sql.IdentifierFn(func(path ...string) string {
			// Sanitize column names per Databricks' restrictions. Table names do not have to be
			// sanitized in the same way, although they have different requirements. Table names are
			// sanitized via the resource path response, so further sanitization for table names is
			// not necessary or desirable.
			//
			// Column names are specifically sanitized here by relying on the fact that a column
			// name will always be a single element, whereas a table name will be a resource path
			// consisting of both the schema and table that is two elements long.

			notQuoted := func(s string) bool {
				return sql.IsSimpleIdentifier(s) && !slices.Contains(DATABRICKS_RESERVED_WORDS, strings.ToLower(s))
			}
			quoteTf := sql.QuoteTransform("`", "``")

			if len(path) == 1 {
				translated := translateFlowField(path[0])
				if notQuoted(translated) {
					return translated
				} else {
					return quoteTf(translated)
				}
			} else {
				return sql.JoinTransform(".", sql.PassThroughTransform(notQuoted, quoteTf))(path...)
			}
		}),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "\\'")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		ColumnValidator:        columnValidator,
		MaxColumnCharLength:    255,
		CaseInsensitiveColumns: true,
	}
}()

// stringCompatible allow strings of any format, arrays, objects, or fields with multiple types to
// be materialized since they are all converted to strings.
func stringCompatible(p pf.Projection) bool {
	return sql.StringCompatible(p) || sql.JsonCompatible(p)
}

// TODO: use create table USING location instead of copying data into temporary table
var (
	tplAll = sql.MustParseTemplate(databricksDialect, "root", `
{{ define "temp_name_load" -}}
`+"`"+`flow_temp_load_table_{{ $.ShardRange }}_{{ $.Table.Binding }}_{{ Last $.Table.Path }}`+"`"+`
{{- end }}

{{ define "temp_name_store" -}}
`+"`"+`flow_temp_store_table_{{ $.ShardRange }}_{{ $.Table.Binding }}_{{ Last $.Table.Path }}`+"`"+`
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

-- Templated creation or replacement of a target table. It's exactly the
-- same as createTargetTable, except it uses CREATE OR REPLACE.
{{ define "replaceTargetTable" }}
CREATE OR REPLACE TABLE {{$.Identifier}} (
  {{- range $ind, $col := $.Columns }}
  {{- if $ind }},{{ end }}
  {{$col.Identifier}} {{$col.DDL}} COMMENT {{ Literal $col.Comment }}
  {{- end }}
) COMMENT {{ Literal $.Comment }};
{{ end }}

-- Templated query which performs table alterations by adding columns.
-- Dropping nullability constraints must be handled separately, since
-- Databricks does not support modifying multiple columns in a single
-- statement.

{{ define "alterTableColumns" }}
ALTER TABLE {{$.Identifier}} ADD COLUMN
{{- range $ind, $col := $.AddColumns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.NullableDDL}}
{{- end }};
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
{{- end -}}
{{ end }}

-- Directly copy into the target table
{{ define "copyIntoDirect" }}
	COPY INTO {{ $.Table.Identifier }} FROM (
    SELECT
		{{ range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}{{ template "cast" $key.DDL -}}
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
			{{$key.Identifier -}}{{ template "cast" $key.DDL -}}
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
        {{$key.Identifier -}}{{ template "cast" $key.DDL -}}
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
		l.{{ $key.Identifier }} = r.{{ $key.Identifier }}{{ template "cast" $key.DDL -}}
	{{- end }}
	AND r._metadata_file_name IN (%s)
	{{- if $.Table.Document }}
	WHEN MATCHED AND r.{{ $.Table.Document.Identifier }} <=> NULL THEN
		DELETE
	{{- end }}
	WHEN MATCHED THEN
		UPDATE SET {{ range $ind, $key := $.Table.Values }}
		{{- if $ind }}, {{ end -}}
		l.{{ $key.Identifier }} = r.{{ $key.Identifier }}{{ template "cast" $key.DDL -}}
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
			r.{{ $key.Identifier }}{{ template "cast" $key.DDL -}}
		{{- end -}}
	);
{{ end }}
  `)
	tplCreateTargetTable  = tplAll.Lookup("createTargetTable")
	tplReplaceTargetTable = tplAll.Lookup("replaceTargetTable")
	tplAlterTableColumns  = tplAll.Lookup("alterTableColumns")
	tplCreateLoadTable    = tplAll.Lookup("createLoadTable")
	tplCreateStoreTable   = tplAll.Lookup("createStoreTable")
	tplLoadQuery          = tplAll.Lookup("loadQuery")
	tplTruncateLoad       = tplAll.Lookup("truncateLoadTable")
	tplTruncateStore      = tplAll.Lookup("truncateStoreTable")
	tplDropLoad           = tplAll.Lookup("dropLoadTable")
	tplDropStore          = tplAll.Lookup("dropStoreTable")
	tplCopyIntoDirect     = tplAll.Lookup("copyIntoDirect")
	tplCopyIntoLoad       = tplAll.Lookup("copyIntoLoad")
	tplCopyIntoStore      = tplAll.Lookup("copyIntoStore")
	tplMergeInto          = tplAll.Lookup("mergeInto")
)

type Template struct {
	StagingPath string
	ShardRange  string
	Table       *sql.Table
}

func RenderTable(table sql.Table, stagingPath string, shardRange string, tpl *template.Template) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &Template{Table: &table, StagingPath: stagingPath, ShardRange: shardRange}); err != nil {
		return "", err
	}
	return w.String(), nil
}
