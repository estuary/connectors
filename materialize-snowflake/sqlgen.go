package main

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"text/template"

	sql "github.com/estuary/connectors/materialize-sql"
	log "github.com/sirupsen/logrus"
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

// See https://docs.snowflake.com/en/sql-reference/data-types-datetime#timestamp
// for the official description of the different types of timestamps.
type timestampTypeMapping string

var (
	// NTZ is the default type if it hasn't been otherwise set for the database,
	// but it's generally not a very good choice since it ignores timezone
	// information and stores the time directly as a wallclock time without
	// adjusting it to UTC.
	timestampNTZ timestampTypeMapping = "TIMESTAMP_NTZ"

	// LTZ stores the time in Snowflake as UTC and performs operations on it
	// using the session timezone. We use LTZ for timestamp columns unless the
	// TIMESTAMP_TYPE_MAPPING has explicitly been set to TZ.
	timestampLTZ timestampTypeMapping = "TIMESTAMP_LTZ"

	// TZ stores the time in Snowflake as UTC with a time zone offset.
	timestampTZ timestampTypeMapping = "TIMESTAMP_TZ"
)

func (m timestampTypeMapping) valid() bool {
	return m == timestampNTZ || m == timestampLTZ || m == timestampTZ
}

var snowflakeDialect = func(configSchema string, timestampMapping timestampTypeMapping, featureFlags map[string]bool) sql.Dialect {
	// Define base date/time mappings without primary key wrapper
	primaryKeyTextType := sql.MapStatic("TEXT")
	dateMapping := sql.MapStatic("DATE")
	datetimeMapping := sql.MapStatic(
		string(timestampMapping),
		sql.AlsoCompatibleWith("timestamp_ntz", "timestamp_tz", "timestamp_ltz"),
	)

	// If feature flag is enabled, wrap with MapPrimaryKey to use string types for primary keys
	if featureFlags["datetime_keys_as_string"] {
		dateMapping = sql.MapPrimaryKey(primaryKeyTextType, dateMapping)
		datetimeMapping = sql.MapPrimaryKey(primaryKeyTextType, datetimeMapping)
	}

	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.ARRAY:    sql.MapStatic("VARIANT", sql.UsingConverter(sql.ToJsonBytes)),
			sql.BINARY:   sql.MapStatic("TEXT"),
			sql.BOOLEAN:  sql.MapStatic("BOOLEAN"),
			sql.INTEGER:  sql.MapStatic("INTEGER", sql.AlsoCompatibleWith("fixed")),
			sql.NUMBER:   sql.MapStatic("FLOAT", sql.AlsoCompatibleWith("real")),
			sql.OBJECT:   sql.MapStatic("VARIANT", sql.UsingConverter(sql.ToJsonBytes)),
			sql.MULTIPLE: sql.MapStatic("VARIANT", sql.UsingConverter(sql.ToJsonBytes)),
			sql.STRING_INTEGER: sql.MapStringMaxLen(
				sql.MapStatic("INTEGER", sql.AlsoCompatibleWith("fixed"), sql.UsingConverter(sql.StrToInt)), // Equivalent to NUMBER(38,0)
				sql.MapStatic("TEXT", sql.UsingConverter(sql.ToStr)),
				38,
			),
			sql.STRING_NUMBER: sql.MapStatic("FLOAT", sql.AlsoCompatibleWith("real"), sql.UsingConverter(sql.StrToFloat("NaN", "inf", "-inf"))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("TEXT"),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      dateMapping,
					"date-time": datetimeMapping,
				},
			}),
		},
		sql.WithNotNullText("NOT NULL"),
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
		MigratableTypes: sql.MigrationSpecs{
			"fixed":         {sql.NewMigrationSpec([]string{"float", "text"})},
			"real":          {sql.NewMigrationSpec([]string{"text"})},
			"date":          {sql.NewMigrationSpec([]string{"text"})},
			"timestamp_ntz": {sql.NewMigrationSpec([]string{"text"}, sql.WithCastSQL(datetimeNoTzToStringCast))},
			"timestamp_tz":  {sql.NewMigrationSpec([]string{"text"}, sql.WithCastSQL(datetimeToStringCast))},
			"timestamp_ltz": {sql.NewMigrationSpec([]string{"text"}, sql.WithCastSQL(datetimeToStringCast))},
			"*":             {sql.NewMigrationSpec([]string{"variant"}, sql.WithCastSQL(toJsonCast))},
		},
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
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string { return translateIdentifier(schema) }),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return translateIdentifier(field) }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				isSimpleIdentifier,
				sql.QuoteTransform(`"`, `""`),
			))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransformEscapedBackslash("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		MaxColumnCharLength:    255,
		CaseInsensitiveColumns: false,
	}
}

func datetimeToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`TO_VARCHAR(CONVERT_TIMEZONE('UTC', %s), 'YYYY-MM-DD"T"HH24:MI:SS.FF9"Z"')`, migration.Identifier)
}

func datetimeNoTzToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`TO_VARCHAR(%s, 'YYYY-MM-DD"T"HH24:MI:SS.FF3')`, migration.Identifier)
}

func toJsonCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`TO_VARIANT(%s)`, migration.Identifier)
}

type templates struct {
	createTargetTable *template.Template
	alterTableColumns *template.Template
	loadQuery         *template.Template
	copyInto          *template.Template
	mergeInto         *template.Template
	pipeName          *template.Template
	createPipe        *template.Template
	copyHistory       *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
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
)
DEFAULT_DDL_COLLATION = '';

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
{{ if $.Table.Document -}}
SELECT {{ $.Table.Binding }}, TO_JSON({{ $.Table.Identifier }}.{{ $.Table.Document.Identifier }})
	FROM {{ $.Table.Identifier }}
	JOIN (
		SELECT {{ range $ind, $bound := $.Bounds }}
		{{- if $ind }}, {{ end -}}
		$1[{{$ind}}] AS {{$bound.Identifier -}}
		{{- end }}
		FROM {{ $.File }}
	) AS r
	{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }}AND {{ else }}ON {{ end -}}
	{{ $.Table.Identifier }}.{{ $bound.Identifier }} = r.{{ $bound.Identifier }}
	{{- if $bound.LiteralLower }} AND {{ $.Table.Identifier }}.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND {{ $.Table.Identifier }}.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
	{{- end }}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS VARIANT) LIMIT 0) as nodoc
{{ end -}}
{{ end }}

{{ define "createPipe" }}
CREATE PIPE {{ $.PipeName }}
  COMMENT = 'Pipe for table {{ $.Table.Path }}'
  AS COPY INTO {{ $.Table.Identifier }} (
	{{ range $ind, $key := $.Table.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$key.Identifier -}}
	{{- end }}
) FROM (
	SELECT {{ range $ind, $key := $.Table.Columns }}
	{{- if $ind }}, {{ end -}}
	{{ if eq $key.DDL "VARIANT" }}NULLIF($1[{{$ind}}], PARSE_JSON('null')){{ else }}$1[{{$ind}}]{{ end }} AS {{$key.Identifier -}}
	{{- end }}
	FROM @flow_v1
);
{{ end }}

{{ define "copyInto" }}
COPY INTO {{ $.Table.Identifier }} (
	{{ range $ind, $key := $.Table.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$key.Identifier -}}
	{{- end }}
) FROM (
	SELECT {{ range $ind, $key := $.Table.Columns }}
	{{- if $ind }}, {{ end -}}
	{{ if eq $key.DDL "VARIANT" }}NULLIF($1[{{$ind}}], PARSE_JSON('null')){{ else }}$1[{{$ind}}]{{ end }} AS {{$key.Identifier -}}
	{{- end }}
	FROM {{ $.File }}
);
{{ end }}


{{ define "mergeInto" }}
MERGE INTO {{ $.Table.Identifier }} AS l
USING (
	SELECT {{ range $ind, $key := $.Table.Columns }}
		{{- if $ind }}, {{ end -}}
		{{ if eq $key.DDL "VARIANT" }}NULLIF($1[{{$ind}}], PARSE_JSON('null')){{ else }}$1[{{$ind}}]{{ end }} AS {{$key.Identifier -}}
	{{- end }}, $1[{{ len $.Table.Columns }}] AS _flow_delete
	FROM {{ $.File }}
) AS r
ON {{ range $ind, $bound := $.Bounds }}
	{{ if $ind -}} AND {{ end -}}
	l.{{ $bound.Identifier }} = r.{{ $bound.Identifier }}
	{{- if $bound.LiteralLower }} AND l.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND l.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }}
{{- if $.Table.Document }}
WHEN MATCHED AND r._flow_delete=true THEN
	DELETE
{{- end }}
WHEN MATCHED THEN
	UPDATE SET {{ range $ind, $key := $.Table.Values }}
	{{- if $ind }}, {{ end -}}
	l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
{{- end -}}
{{- if $.Table.Document -}}
{{ if $.Table.Values }}, {{ end }}l.{{ $.Table.Document.Identifier}} = r.{{ $.Table.Document.Identifier }}
{{- end }}
WHEN NOT MATCHED AND r._flow_delete=false THEN
	INSERT (
	{{- range $ind, $key := $.Table.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$key.Identifier -}}
	{{- end -}}
)
	VALUES (
	{{- range $ind, $key := $.Table.Columns }}
		{{- if $ind }}, {{ end -}}
		r.{{$key.Identifier -}}
	{{- end -}}
);
{{ end }}

{{ define "copyHistory" }}
SELECT FILE_NAME, STATUS, FIRST_ERROR_MESSAGE FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME=>'{{ $.TableName }}',
  START_TIME=>DATEADD(DAY, -14, CURRENT_TIMESTAMP())
)) WHERE
FILE_NAME IN ('{{ Join $.Files "','" }}')
{{ end }}
  `)

	return templates{
		createTargetTable: tplAll.Lookup("createTargetTable"),
		alterTableColumns: tplAll.Lookup("alterTableColumns"),
		loadQuery:         tplAll.Lookup("loadQuery"),
		copyInto:          tplAll.Lookup("copyInto"),
		mergeInto:         tplAll.Lookup("mergeInto"),
		pipeName:          tplAll.Lookup("pipe_name"),
		createPipe:        tplAll.Lookup("createPipe"),
		copyHistory:       tplAll.Lookup("copyHistory"),
	}
}

var createStageSQL = `
CREATE STAGE IF NOT EXISTS flow_v1
FILE_FORMAT = (
  TYPE = JSON
  BINARY_FORMAT = BASE64
  ALLOW_DUPLICATE = TRUE
)
COMMENT = 'Internal stage used by Estuary Flow to stage loaded & stored documents'
;`

type tablePipe struct {
	Table    sql.Table
	PipeName string
}

func renderTablePipeTemplate(table sql.Table, pipeName string, tpl *template.Template) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &tablePipe{Table: table, PipeName: pipeName}); err != nil {
		return "", err
	}
	var s = w.String()
	log.WithFields(log.Fields{
		"rendered": s,
		"table":    table,
		"pipeName": pipeName,
	}).Debug("rendered template")
	return s, nil
}

type tableAndFile struct {
	Table sql.Table
	File  string
}

func renderTableAndFileTemplate(table sql.Table, file string, tpl *template.Template) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &tableAndFile{Table: table, File: file}); err != nil {
		return "", err
	}
	var s = w.String()
	log.WithFields(log.Fields{
		"rendered": s,
		"table":    table,
		"file":     file,
	}).Debug("rendered template")
	return s, nil
}

type copyHistory struct {
	TableName string
	Files     []string
}

func renderCopyHistoryTemplate(tableName string, files []string, tpl *template.Template) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &copyHistory{TableName: tableName, Files: files}); err != nil {
		return "", err
	}
	var s = w.String()
	log.WithFields(log.Fields{
		"rendered":  s,
		"tableName": tableName,
		"files":     files,
	}).Debug("rendered template")
	return s, nil
}

type boundedQueryInput struct {
	Table  sql.Table
	File   string
	Bounds []sql.MergeBound
}

func renderBoundedQueryTemplate(tpl *template.Template, table sql.Table, file string, bounds []sql.MergeBound) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &boundedQueryInput{
		Table:  table,
		File:   file,
		Bounds: bounds,
	}); err != nil {
		return "", err
	}

	return w.String(), nil
}
