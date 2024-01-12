package main

import (
	"encoding/json"
	"fmt"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/trinodb/trino-go-client/trino"
	"regexp"
	"slices"
	"strings"
	"text/template"
	"time"
)

var simpleIdentifierRegexp = regexp.MustCompile(`^[_\pL]+[_\pL\pN]*$`)

func isSimpleIdentifier(s string) bool {
	return simpleIdentifierRegexp.MatchString(s) && !slices.Contains(GALAXY_RESERVED_WORDS, strings.ToLower(s))
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

var timestampConverter sql.ElementConverter = func(te tuple.TupleElement) (interface{}, error) {
	parsed, err := time.Parse(time.RFC3339, te.(string))
	if err != nil {
		return nil, err
	}
	timestamp := trino.Timestamp(parsed.Year(), parsed.Month(), parsed.Day(), parsed.Hour(), parsed.Minute(), parsed.Second(), parsed.Nanosecond())
	return timestamp, nil

}

var doubleConverter sql.ElementConverter = func(te tuple.TupleElement) (interface{}, error) {
	return fmt.Sprintf("%v", te), nil
}

// galaxyDialect returns a representation of the Galaxy SQL dialect.
var galaxyDialect = func(configSchema string) sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.ARRAY:    sql.NewStaticMapper("ARRAY"),
		sql.BINARY:   sql.NewStaticMapper("VARBINARY"),
		sql.BOOLEAN:  sql.NewStaticMapper("BOOLEAN"),
		sql.INTEGER:  sql.NewStaticMapper("BIGINT"),
		sql.NUMBER:   sql.NewStaticMapper("DOUBLE", sql.WithElementConverter(doubleConverter)),
		sql.OBJECT:   sql.NewStaticMapper("VARCHAR", sql.WithElementConverter(jsonConverter)),
		sql.MULTIPLE: sql.NewStaticMapper("VARCHAR", sql.WithElementConverter(jsonConverter)),
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.NewStaticMapper("VARCHAR"),
			WithFormat: map[string]sql.TypeMapper{
				"integer": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("STRING"),
					Delegate:   sql.NewStaticMapper("BIGINT", sql.WithElementConverter(sql.StdStrToInt())),
				},
				"number": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("STRING"),
					Delegate:   sql.NewStaticMapper("DOUBLE", sql.WithElementConverter(sql.StdStrToFloat("NaN", "inf", "-inf"))),
				},
				"date":      sql.NewStaticMapper("DATE"),
				"date-time": sql.NewStaticMapper("TIMESTAMP", sql.WithElementConverter(timestampConverter)),
			},
		},
	}
	mapper = sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    mapper,
	}

	translateIdentifier := func(in string) string {
		if isSimpleIdentifier(in) {
			return strings.ToLower(in)
		}
		return in
	}

	return sql.Dialect{
		TableLocatorer: sql.TableLocatorFn(func(path ...string) sql.InfoTableLocation {
			if len(path) == 1 {
				return sql.InfoTableLocation{
					TableSchema: translateIdentifier(configSchema),
					TableName:   translateIdentifier(path[0]),
				}
			} else {
				return sql.InfoTableLocation{
					TableSchema: translateIdentifier(path[0]),
					TableName:   translateIdentifier(path[1]),
				}
			}
		}),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string {
			return translateIdentifier(field)
		}),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				isSimpleIdentifier,
				sql.QuoteTransform("\"", "\\\""),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(_ int, flatType sql.FlatType) string {
			if flatType == sql.NUMBER {
				return "CAST(? AS DOUBLE)"
			} else {
				return "?"
			}
		}),
		TypeMapper: mapper,
	}
}

type templates struct {
	fetchVersionAndSpec *template.Template
	createTargetTable   *template.Template
	storeInsert         *template.Template
	storeUpdate         *template.Template
	loadQuery           *template.Template
	alterTableColumns   *template.Template
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


{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }}, {{$.Document.Identifier}}
	FROM {{ $.Identifier}}
WHERE
	{{- range $ind, $key := $.Keys }}
		{{ if $ind }} AND {{ else }}   {{ end -}}
		{{ $key.Identifier }} = {{ $key.Placeholder }}
	{{- end }}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc
{{ end }}
{{ end }}

//TODO: To be changed with MERGE with temporally table
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
)
{{ end }}

{{ define "storeUpdate" }}
UPDATE {{$.Identifier}} SET
	{{- range $ind, $val := $.Values }}
		{{- if $ind }},{{ end }}
		{{ $val.Identifier}} = {{ $val.Placeholder }}
	{{- end }}
	{{- if $.Document -}}
		{{ if $.Values }},{{ end }}
		{{ $.Document.Identifier }} = {{ $.Document.Placeholder }}
	{{- end -}}
	{{ range $ind, $key := $.Keys }}
	{{ if $ind }} AND   {{ else }} WHERE {{ end -}}
	{{ $key.Identifier }} = {{ $key.Placeholder }}
	{{- end -}}
{{ end }}

{{ define "alterTableColumns" }}
ALTER TABLE {{$.Identifier}} ADD COLUMN
{{- range $ind, $col := $.AddColumns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.NullableDDL}}
{{- end }}
{{ end }}

  `)
	return templates{
		fetchVersionAndSpec: tplAll.Lookup("fetchVersionAndSpec"),
		createTargetTable:   tplAll.Lookup("createTargetTable"),
		storeInsert:         tplAll.Lookup("storeInsert"),
		storeUpdate:         tplAll.Lookup("storeUpdate"),
		loadQuery:           tplAll.Lookup("loadQuery"),
		alterTableColumns:   tplAll.Lookup("alterTableColumns"),
	}
}
