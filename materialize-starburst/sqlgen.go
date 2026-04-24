package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"text/template"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/trinodb/trino-go-client/trino"
)

var simpleIdentifierRegexp = regexp.MustCompile(`^[_\pL]+[_\pL\pN]*$`)

func isSimpleIdentifier(s string) bool {
	return simpleIdentifierRegexp.MatchString(s) && !slices.Contains(TRINO_RESERVED_WORDS, strings.ToLower(s))
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

		return string(m), nil
	}
}

var doubleConverter sql.ElementConverter = func(te tuple.TupleElement) (interface{}, error) {
	return fmt.Sprintf("%v", te), nil
}

var timestampConverter sql.ElementConverter = func(te tuple.TupleElement) (interface{}, error) {
	parsed, err := time.Parse(time.RFC3339, te.(string))
	if err != nil {
		return nil, err
	}
	timestamp := trino.Timestamp(parsed.Year(), parsed.Month(), parsed.Day(), parsed.Hour(), parsed.Minute(), parsed.Second(), parsed.Nanosecond())
	return timestamp, nil
}

// starburstTrinoDialect returns a representation of the Starburst Trino SQL dialect used for target table.
var starburstTrinoDialect = func() sql.Dialect {
	dateTimeMapper := sql.MapStatic("TIMESTAMP(6) WITH TIME ZONE", sql.UsingConverter(timestampConverter))
	return starburstDialect(sql.MapStatic("DATE"), dateTimeMapper)
}()

// starburstHiveDialect returns a representation of the Starburst Hive format SQL dialect used for temp table.
var starburstHiveDialect = func() sql.Dialect {
	return starburstDialect(sql.MapStatic("VARCHAR"), sql.MapStatic("VARCHAR"))
}()

var starburstDialect = func(dateMapper sql.MapProjectionFn, dateTimeMapper sql.MapProjectionFn) sql.Dialect {
	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.ARRAY:          sql.MapStatic("VARCHAR", sql.UsingConverter(jsonConverter)),
			sql.BINARY:         sql.MapStatic("VARCHAR"),
			sql.BOOLEAN:        sql.MapStatic("BOOLEAN"),
			sql.INTEGER:        sql.MapStatic("BIGINT"),
			sql.NUMBER:         sql.MapStatic("DOUBLE", sql.UsingConverter(doubleConverter)),
			sql.OBJECT:         sql.MapStatic("VARCHAR", sql.UsingConverter(jsonConverter)),
			sql.MULTIPLE:       sql.MapStatic("VARCHAR", sql.UsingConverter(jsonConverter)),
			sql.STRING_INTEGER: sql.MapStatic("BIGINT", sql.UsingConverter(sql.StrToInt)),
			sql.STRING_NUMBER:  sql.MapStatic("DOUBLE", sql.UsingConverter(sql.StrToFloat("NaN", "inf", "-inf"))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("VARCHAR"),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      dateMapper,
					"date-time": dateTimeMapper,
				},
			}),
		},
		// We are not using NOT NULL TEXT so that all columns are created as nullable. This is
		// necessary because Hive temp table which does not support NOT NULL
	)

	return sql.Dialect{
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			if len(path) == 1 {
				return sql.InfoTableLocation{
					// If schema is not defined schema from resource configuration will be used
					TableName: strings.ToLower(path[0]),
				}
			} else {
				return sql.InfoTableLocation{
					TableSchema: strings.ToLower(path[0]),
					TableName:   strings.ToLower(path[1]),
				}
			}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string { return strings.ToLower(schema) }),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return strings.ToLower(field) }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				isSimpleIdentifier,
				sql.QuoteTransform(`"`, `""`),
			))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		// We are not using sql.NullableMapper so that all columns are created as nullable. This is
		// necessary because Hive temp table which does not support NOT NULL
		TypeMapper:             mapper,
		MaxColumnCharLength:    0, // Starburst has no limit on how long column names can be that I can find
		CaseInsensitiveColumns: true,
	}
}

type templates struct {
	fetchVersionAndSpec  *template.Template
	createTargetTable    *template.Template
	alterTableColumns    *template.Template
	createLoadTempTable  *template.Template
	dropLoadTempTable    *template.Template
	loadQuery            *template.Template
	createStoreTempTable *template.Template
	dropStoreTempTable   *template.Template
	mergeIntoTarget      *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
	var tplAll = sql.MustParseTemplate(dialect, "root", `
  {{ define "temp_name" -}}
  flow_temp_table_{{ $.Binding }}
  {{- end }}

{{ define "fetchVersionAndSpec" }}
SELECT 
	{{- range $ind, $val := $.Values }}
		{{- if $ind }},{{ end }}
		{{ $val.Identifier}}
	{{- end }}
FROM {{ $.Identifier}} 
WHERE
	{{- range $ind, $key := $.Keys }}
		{{ if $ind }} AND {{ else }}   {{ end -}}
		{{ $key.Identifier }} = {{ $key.Placeholder }}
	{{- end }}
{{ end }}

  -- Templated creation of a materialized table definition and comments:

{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
    {{- range $ind, $col := $.Columns }}
    {{- if $ind }},{{ end }}
    {{$col.Identifier}} {{$col.DDL}}
    {{- end }}
)
COMMENT {{ Literal $.Comment }}
{{ end }}

{{ define "createLoadTempTable" }}
CREATE TABLE {{$.Identifier}}_load_temp (
{{- range $ind, $key := $.Keys }}
	{{- if $ind }},{{ end }}
	{{ $key.Identifier }} {{ $key.DDL }}
{{- end }}
)
WITH (
   format = 'PARQUET',
   external_location = ?,
   type = 'HIVE'
)
{{ end }}

{{ define "dropLoadTempTable" }}
DROP TABLE IF EXISTS {{$.Identifier}}_load_temp
{{ end }}

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }}, {{$.Document.Identifier}}
	FROM {{ $.Identifier}} AS l
    JOIN {{$.Identifier}}_load_temp AS r
	ON {{ range $ind, $key := $.Keys }}
	{{- if $ind }} AND {{ end -}}
    l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc
{{ end }}
{{ end }}

-- Idempotent creation of the store table for staging new records.
{{ define "createStoreTempTable" }}
CREATE TABLE {{$.Identifier}}_store_temp (
    {{- range $ind, $col := $.Columns }}
    {{- if $ind }},{{ end }}
    {{$col.Identifier}} {{$col.DDL}}
    {{- end }}
)
WITH (
   format = 'PARQUET',
   external_location = ?,
   type = 'HIVE'
)
{{ end }}

{{ define "mergeIntoTarget" }}
MERGE INTO {{ $.Identifier }} AS l
	USING {{$.Identifier}}_store_temp AS r
	ON {{ range $ind, $key := $.Keys }}
	{{- if $ind }} AND {{ end -}}
	l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
	WHEN MATCHED THEN
	UPDATE SET {{ range $ind, $val := $.Values }}
	{{- if $ind }}, {{ end -}}
		{{ $val.Identifier }} = {{ template "cast" $val }}
	{{- end -}}
	{{- if $.Document -}}
	{{ if $.Values }}, {{ end }}{{ $.Document.Identifier}} = r.{{ $.Document.Identifier }}
	{{- end }}
	WHEN NOT MATCHED THEN
	INSERT (
	{{- range $ind, $col := $.Columns }}
	{{- if $ind }}, {{ end -}}
		{{$col.Identifier -}}
	{{- end -}}
	)
	VALUES (
	{{- range $ind, $col := $.Columns }}
	{{- if $ind }}, {{ end -}}
		{{ template "cast" $col -}}
	{{- end -}}
	)
{{ end }}

{{ define "dropStoreTempTable" }}
DROP TABLE IF EXISTS {{$.Identifier}}_store_temp
{{ end }}

{{ define "alterTableColumns" }}
ALTER TABLE {{.TableIdentifier}} ADD COLUMN {{.ColumnIdentifier}} {{.NullableDDL}}
{{ end }}

{{ define "cast" }}
{{- if Contains $.DDL "DATE" -}}
	from_iso8601_date(r.{{ $.Identifier}})
{{- else if Contains $.DDL  "TIMESTAMP" -}}
	from_iso8601_timestamp_nanos(r.{{ $.Identifier}})
{{- else -}}
	r.{{ $.Identifier }}
{{- end -}}
{{ end }}

  `)
	return templates{
		fetchVersionAndSpec:  tplAll.Lookup("fetchVersionAndSpec"),
		createTargetTable:    tplAll.Lookup("createTargetTable"),
		alterTableColumns:    tplAll.Lookup("alterTableColumns"),
		createLoadTempTable:  tplAll.Lookup("createLoadTempTable"),
		dropLoadTempTable:    tplAll.Lookup("dropLoadTempTable"),
		loadQuery:            tplAll.Lookup("loadQuery"),
		createStoreTempTable: tplAll.Lookup("createStoreTempTable"),
		dropStoreTempTable:   tplAll.Lookup("dropStoreTempTable"),
		mergeIntoTarget:      tplAll.Lookup("mergeIntoTarget"),
	}
}
