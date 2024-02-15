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
)

// For historical reasons, we do not quote identifiers starting with an underscore or any letter,
// and containing only letters, numbers & underscores. See
// https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html for some details about how
// Snowflake handles unquoted identifiers. Crucially, unquoted identifiers are resolved as
// UPPERCASE, making this historical quoting important for backward compatibility.
var simpleIdentifierRegexp = regexp.MustCompile(`^[_\pL]+[_\pL\pN]*$`)

func isSimpleIdentifier(s string) bool {
	return simpleIdentifierRegexp.MatchString(s) && !slices.Contains(SF_RESERVED_WORDS, strings.ToLower(s))
}

var jsonConverter sql.ElementConverter = func(te tuple.TupleElement) (interface{}, error) {
	switch ii := te.(type) {
	case []byte:
		return json.RawMessage(ii), nil
	case json.RawMessage:
		return ii, nil
	case nil:
		return json.RawMessage(nil), nil
	default:
		return nil, fmt.Errorf("invalid type %#v for variant", te)
	}
}

// snowflakeDialect returns a representation of the Snowflake SQL dialect.
var snowflakeDialect = func(configSchema string) sql.Dialect {
	var variantMapper = sql.NewStaticMapper("VARIANT", sql.WithElementConverter(jsonConverter))
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.ARRAY:    variantMapper,
		sql.BINARY:   sql.NewStaticMapper("BINARY"),
		sql.BOOLEAN:  sql.NewStaticMapper("BOOLEAN"),
		sql.INTEGER:  sql.NewStaticMapper("INTEGER"),
		sql.NUMBER:   sql.NewStaticMapper("DOUBLE"),
		sql.OBJECT:   variantMapper,
		sql.MULTIPLE: sql.NewStaticMapper("VARIANT", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.NewStaticMapper("STRING"),
			WithFormat: map[string]sql.TypeMapper{
				"integer": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("STRING"),
					Delegate:   sql.NewStaticMapper("INTEGER", sql.WithElementConverter(sql.StdStrToInt())), // Equivalent to NUMBER(38,0)
				},
				"number": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("STRING"),
					// https://docs.snowflake.com/en/sql-reference/data-types-numeric#special-values
					Delegate: sql.NewStaticMapper("DOUBLE", sql.WithElementConverter(sql.StdStrToFloat("NaN", "inf", "-inf"))),
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
		sql.ColValidation{Types: []string{"text"}, Validate: sql.StringCompatible},
		sql.ColValidation{Types: []string{"boolean"}, Validate: sql.BooleanCompatible},
		sql.ColValidation{Types: []string{"float"}, Validate: sql.NumberCompatible},
		sql.ColValidation{Types: []string{"number"}, Validate: sql.IntegerCompatible}, // "number" is what Snowflake calls INTEGER.
		sql.ColValidation{Types: []string{"variant"}, Validate: sql.JsonCompatible},
		sql.ColValidation{Types: []string{"date"}, Validate: sql.DateCompatible},
		sql.ColValidation{Types: []string{"timestamp_ntz"}, Validate: sql.DateTimeCompatible},
	)

	translateIdentifier := func(in string) string {
		if isSimpleIdentifier(in) {
			// Snowflake uppercases all identifiers unless they are quoted. We don't quote identifiers if
			// isSimpleIdentifier is true.
			return strings.ToUpper(in)
		}
		return in
	}

	return sql.Dialect{
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			if len(path) == 1 {
				// A schema isn't required to be set on any resource, but the endpoint configuration
				// will always have one set. Also, as a matter of backwards compatibility, if a
				// resource has the exact same schema set as the endpoint schema it will not be
				// included in the path.
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
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		ColumnValidator:        columnValidator,
		MaxColumnCharLength:    255,
		CaseInsensitiveColumns: false,
	}
}

func renderTemplates(dialect sql.Dialect) map[string]*template.Template {
	var tplAll = sql.MustParseTemplate(dialect, "root", `
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

COMMENT ON TABLE {{$.Identifier}} IS {{Literal $.Comment}};
{{- range $col := .Columns }}
COMMENT ON COLUMN {{$.Identifier}}.{{$col.Identifier}} IS {{Literal $col.Comment}};
{{- end}}
{{ end }}

-- Templated creation or replacement of a target table. It's exactly the
-- same as createTargetTable, except it uses CREATE OR REPLACE.

{{ define "replaceTargetTable" }}
CREATE OR REPLACE TABLE {{$.Identifier}} (
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

COMMENT ON TABLE {{$.Identifier}} IS {{Literal $.Comment}};
{{- range $col := .Columns }}
COMMENT ON COLUMN {{$.Identifier}}.{{$col.Identifier}} IS {{Literal $col.Comment}};
{{- end}}
{{ end }}

-- Templated query which performs table alterations by adding columns and/or
-- dropping nullability constraints. Snowflake does not allow adding columns and
-- modifying columns together in the same statement, but either one of those
-- things can be grouped together in separate statements, so this template will
-- actually generate two separate statements if needed.

{{ define "alterTableColumns" }}
{{ if $.AddColumns -}}
ALTER TABLE {{$.Identifier}} ADD COLUMN
{{- range $ind, $col := $.AddColumns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.NullableDDL}}
{{- end }};
{{- end -}}
{{- if $.DropNotNulls -}}
{{- if $.AddColumns }}

{{ end -}}
ALTER TABLE {{$.Identifier}} ALTER COLUMN
{{- range $ind, $col := $.DropNotNulls }}
	{{- if $ind }},{{ end }}
	{{ ColumnIdentifier $col.Name }} DROP NOT NULL
{{- end }};
{{- end }}
{{ end }}

-- Templated query which joins keys from the load table with the target table, and returns values. It
-- deliberately skips the trailing semi-colon as these queries are composed with a UNION ALL.

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }}, {{ $.Identifier }}.{{ $.Document.Identifier }}
	FROM {{ $.Identifier }}
	JOIN (
		SELECT {{ range $ind, $key := $.Keys }}
		{{- if $ind }}, {{ end -}}
		$1[{{$ind}}] AS {{$key.Identifier -}}
		{{- end }}
		FROM %s
	) AS r
	ON {{ range $ind, $key := $.Keys }}
	{{- if $ind }} AND {{ end -}}
	{{ $.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS VARIANT) LIMIT 0) as nodoc
{{ end -}}
{{ end }}

{{ define "copyInto" }}
COPY INTO {{ $.Identifier }} (
	{{ range $ind, $key := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$key.Identifier -}}
	{{- end }}
) FROM (
	SELECT {{ range $ind, $key := $.Columns }}
	{{- if $ind }}, {{ end -}}
	$1[{{$ind}}] AS {{$key.Identifier -}}
	{{- end }}
	FROM %s
);
{{ end }}


{{ define "mergeInto" }}
MERGE INTO {{ $.Identifier }} AS l
USING (
	SELECT {{ range $ind, $key := $.Columns }}
		{{- if $ind }}, {{ end -}}
		$1[{{$ind}}] AS {{$key.Identifier -}}
	{{- end }}
	FROM %s
) AS r
ON {{ range $ind, $key := $.Keys }}
	{{- if $ind }} AND {{ end -}}
	l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
{{- end }}
{{- if $.Document }}
WHEN MATCHED AND IS_NULL_VALUE(r.{{ $.Document.Identifier }}) THEN
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
  `)

	return map[string]*template.Template{
		"createTargetTable":  tplAll.Lookup("createTargetTable"),
		"replaceTargetTable": tplAll.Lookup("replaceTargetTable"),
		"alterTableColumns":  tplAll.Lookup("alterTableColumns"),
		"loadQuery":          tplAll.Lookup("loadQuery"),
		"copyInto":           tplAll.Lookup("copyInto"),
		"mergeInto":          tplAll.Lookup("mergeInto"),
	}
}

var createStageSQL = `
CREATE STAGE IF NOT EXISTS flow_v1
FILE_FORMAT = (
  TYPE = JSON
  BINARY_FORMAT = BASE64
)
COMMENT = 'Internal stage used by Estuary Flow to stage loaded & stored documents'
;`
