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

// starburstDialect returns a representation of the Starburst SQL dialect.
var starburstDialect = func() sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.ARRAY:    sql.NewStaticMapper("VARCHAR", sql.WithElementConverter(jsonConverter)),
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
			},
		},
	}

	columnValidator := sql.NewColumnValidator(
		sql.ColValidation{Types: []string{"varchar"}, Validate: stringCompatible},
		sql.ColValidation{Types: []string{"boolean"}, Validate: sql.BooleanCompatible},
		sql.ColValidation{Types: []string{"bigint"}, Validate: sql.IntegerCompatible},
		sql.ColValidation{Types: []string{"double"}, Validate: sql.NumberCompatible},
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
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string {
			return strings.ToLower(field)
		}),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				isSimpleIdentifier,
				sql.QuoteTransform(`"`, `""`),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		// We are not using sql.NullableMapper so that all columns are created as nullable. This is
		// necessary because Hive temp table which does not support NOT NULL
		TypeMapper:             mapper,
		ColumnValidator:        columnValidator,
		MaxColumnCharLength:    0, // Starburst has no limit on how long column names can be that I can find
		CaseInsensitiveColumns: true,
	}
}()

// stringCompatible allow strings of any format, arrays, objects, or fields with multiple types to
// be materialized, since these are all materialized as VARCHAR columns.
func stringCompatible(p pf.Projection) bool {
	if sql.StringCompatible(p) {
		return true
	} else if sql.TypesOrNull(p.Inference.Types, []string{"array"}) {
		return true
	} else if sql.TypesOrNull(p.Inference.Types, []string{"object"}) {
		return true
	} else if sql.MultipleCompatible(p) {
		return true
	}

	return false
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
	UPDATE SET {{ range $ind, $key := $.Values }}
	{{- if $ind }}, {{ end -}}
	{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end -}}
	{{- if $.Document -}}
	{{ if $.Values }}, {{ end }}{{ $.Document.Identifier}} = r.{{ $.Document.Identifier }}
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
	r.{{ $key.Identifier }}
	{{- end -}}
	)
{{ end }}

{{ define "dropStoreTempTable" }}
DROP TABLE IF EXISTS {{$.Identifier}}_store_temp
{{ end }}

{{ define "alterTableColumns" }}
ALTER TABLE {{.TableIdentifier}} ADD COLUMN {{.ColumnIdentifier}} {{.NullableDDL}}
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
