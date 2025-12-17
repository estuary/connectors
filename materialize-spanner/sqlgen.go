package main

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"text/template"

	sql "github.com/estuary/connectors/materialize-sql"
)

// Valid characters are: letters (a-z, A-Z), numbers (0-9), and underscores (_).
var identifierSanitizerRegexp = regexp.MustCompile(`[^_0-9a-zA-Z]`)

// Regex to check if identifier starts with a letter
var identifierStartsWithLetterRegexp = regexp.MustCompile(`^[a-zA-Z]`)

// sanitizeSpannerIdentifier normalizes an identifier to meet Spanner's rules:
// - Replaces invalid characters with underscores
// - Ensures it starts with a letter (prefixes with 'c_' if it starts with a digit or underscore)
func sanitizeSpannerIdentifier(identifier string) string {
	if len(identifier) == 0 {
		panic("cannot sanitize empty identifier")
	}

	// Replace invalid characters with underscores
	sanitized := identifierSanitizerRegexp.ReplaceAllString(identifier, "_")

	// Ensure it starts with a letter (Spanner requirement)
	if !identifierStartsWithLetterRegexp.MatchString(sanitized) {
		sanitized = "c_" + sanitized
	}

	return sanitized
}

func createSpannerDialect(featureFlags map[string]bool) sql.Dialect {
	primaryKeyTextType := sql.MapStatic("STRING(MAX)")

	// Define base date/time mappings without primary key wrapper
	dateMapping := sql.MapStatic("DATE", sql.UsingConverter(sql.ClampDate))
	datetimeMapping := sql.MapStatic("TIMESTAMP", sql.UsingConverter(sql.ClampDatetime))

	// If feature flag is enabled, wrap with MapPrimaryKey to use string types for primary keys
	if featureFlags["datetimes_as_string"] {
		dateMapping = sql.MapPrimaryKey(primaryKeyTextType, dateMapping)
		datetimeMapping = sql.MapPrimaryKey(primaryKeyTextType, datetimeMapping)
	}

	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("INT64"),
				sql.MapStatic("NUMERIC"),
			),
			sql.NUMBER:  sql.MapStatic("FLOAT64"),
			sql.BOOLEAN: sql.MapStatic("BOOL"),
			// Cloud Spanner supports native JSON type
			sql.OBJECT: sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.ARRAY:  sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			// Store binary data as BYTES
			sql.BINARY:         sql.MapStatic("BYTES(MAX)"),
			sql.MULTIPLE:       sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.STRING_INTEGER: sql.MapStatic("NUMERIC", sql.UsingConverter(sql.StrToInt)),
			sql.STRING_NUMBER:  sql.MapStatic("FLOAT64", sql.UsingConverter(sql.StrToFloat("NaN", "Infinity", "-Infinity"))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("STRING(MAX)"),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      dateMapping,
					"date-time": datetimeMapping,
					// Duration is not natively supported, store as STRING
					"duration": sql.MapStatic("STRING(MAX)"),
					// Network address types are not natively supported in Spanner
					"ipv4":    sql.MapStatic("STRING(MAX)"),
					"ipv6":    sql.MapStatic("STRING(MAX)"),
					"macaddr": sql.MapStatic("STRING(MAX)"),
					// Time without date is not natively supported, store as STRING
					"time": sql.MapStatic("STRING(MAX)"),
					"uuid": sql.MapStatic("STRING(36)"),
				},
			}),
		},
		sql.WithNotNullText("NOT NULL"),
	)

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"numeric":  {sql.NewMigrationSpec([]string{"float64", "string"})},
			"int64":    {sql.NewMigrationSpec([]string{"float64", "numeric", "string"})},
			"float64":  {sql.NewMigrationSpec([]string{"string"})},
			"date":     {sql.NewMigrationSpec([]string{"string"})},
			"timestamp": {sql.NewMigrationSpec([]string{"string"}, sql.WithCastSQL(timestampToStringCast))},
			"*":        {sql.NewMigrationSpec([]string{"JSON"}, sql.WithCastSQL(toJsonCast))},
		},
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			if len(path) == 2 {
				return sql.InfoTableLocation{TableSchema: path[0], TableName: path[1]}
			}
			return sql.InfoTableLocation{TableSchema: "", TableName: path[0]}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string {
			return schema
		}),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string {
			return sanitizeSpannerIdentifier(field)
		}),
		Identifierer: sql.IdentifierFn(func(path ...string) string {
			var parts []string
			for _, part := range path {
				sanitized := sanitizeSpannerIdentifier(part)
				if slices.Contains(SPANNER_RESERVED_WORDS, strings.ToLower(sanitized)) {
					parts = append(parts, sql.QuoteTransform("`", "``")(sanitized))
				} else {
					parts = append(parts, sanitized)
				}
			}
			return strings.Join(parts, ".")
		}),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			// Cloud Spanner uses @p0, @p1, @p2, etc. for placeholders
			return fmt.Sprintf("@p%d", index)
		}),
		TypeMapper:             mapper,
		MaxColumnCharLength:    128, // Spanner has a 128-character limit for identifiers
		CaseInsensitiveColumns: false,
	}
}

func timestampToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%E6SZ', %s, 'UTC')`, migration.Identifier)
}

func toJsonCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`TO_JSON(%s)`, migration.Identifier)
}

type templates struct {
	createLoadTable         *template.Template
	createTargetTable       *template.Template
	alterTableColumns       *template.Template
	loadInsert              *template.Template
	loadQuery               *template.Template
	loadQueryNoFlowDocument *template.Template
	dropLoadTable           *template.Template
	updateFence             *template.Template
}

func renderTemplates(dialect sql.Dialect, keyDistributionOptimization bool) templates {
	// Conditionally add flow_key_hash column to schema definitions
	hashColumnDef := ""
	hashKeyPrefix := ""
	if keyDistributionOptimization {
		hashColumnDef = "flow_key_hash INT64 NOT NULL,"
		hashKeyPrefix = "flow_key_hash, "
	}

	var tplAll = sql.MustParseTemplate(dialect, "root", fmt.Sprintf(`
{{ define "temp_name" -}}
flow_internal.flow_temp_table_{{ $.Binding }}
{{- end }}

-- Templated creation of a materialized table definition:

{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
	%s
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}} {{$col.DDL}}
	{{- end }}
	{{- if not $.DeltaUpdates }},

		PRIMARY KEY (%s
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
	{{- end -}}
	)
	{{- end }}
)
{{ end }}

-- Templated query which performs table alterations by adding columns and/or
-- dropping nullability constraints. All table modifications are done in a
-- single statement for efficiency.

{{ define "alterTableColumns" }}
ALTER TABLE {{$.Identifier}}
{{- range $ind, $col := $.AddColumns }}
	{{- if $ind }},{{ end }}
	ADD COLUMN {{$col.Identifier}} {{$col.NullableDDL}}
{{- end }}
{{- if and $.DropNotNulls $.AddColumns}},{{ end }}
{{- range $ind, $col := $.DropNotNulls }}
	{{- if $ind }},{{ end }}
	ALTER COLUMN {{ ColumnIdentifier $col.Name }} {{$col.Type}}
{{- end }}
{{ end }}

-- Templated creation of a temporary load table:

{{ define "createLoadTable" }}
CREATE TABLE {{ template "temp_name" . }} (
	%s
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{ $key.Identifier }} {{ $key.DDL }}
	{{- end }},

	PRIMARY KEY (%s
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
	{{- end -}}
	)
)
{{ end }}

-- Templated insertion into the temporary load table:

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
)
{{ end }}

-- Templated query which joins keys from the load table with the target table, and returns values.
-- It deliberately skips the trailing semi-colon as these queries are composed with a UNION ALL.

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }}, r.{{$.Document.Identifier}}
	FROM {{ template "temp_name" . }} AS l
	JOIN {{ $.Identifier}} AS r
	{{- range $ind, $key := $.Keys }}
		{{ if $ind }} AND {{ else }} ON  {{ end -}}
		l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc
{{ end }}
{{ end }}

-- Templated query for no_flow_document feature - reconstructs JSON from root-level columns

{{ define "uncast" -}}
{{ $ident := printf "%%s.%%s" $.Alias $.Identifier }}
{{- if eq $.AsFlatType "string_integer" -}}
	CAST({{ $ident }} AS STRING)
{{- else if eq $.AsFlatType "string_number" -}}
	CAST({{ $ident }} AS STRING)
{{- else if and (eq $.AsFlatType "string") (eq $.Format "date") (not $.IsPrimaryKey) -}}
	CAST({{ $ident }} AS STRING)
{{- else if and (eq $.AsFlatType "string") (eq $.Format "date-time") (not $.IsPrimaryKey) -}}
	FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%E6SZ', {{ $ident }}, 'UTC')
{{- else -}}
	{{ $ident }}
{{- end -}}
{{- end }}

{{ define "loadQueryNoFlowDocument" }}
SELECT {{ $.Binding }},
TO_JSON(STRUCT(
{{- range $i, $col := $.RootLevelColumns}}
	{{- if $i}}, {{end}}
	{{ template "uncast" (ColumnWithAlias $col "r") }} AS {{ $col.Field }}
{{- end}}
)) as flow_document
FROM {{ template "temp_name" . }} AS l
JOIN {{ $.Identifier}} AS r
{{- range $ind, $key := $.Keys }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
{{- end }}
{{ end }}

-- Templated drop of the temporary load table:

{{ define "dropLoadTable" }}
DROP TABLE {{ template "temp_name" . }}
{{ end }}

{{ define "updateFence" }}
UPDATE {{ Identifier $.TablePath }}
	SET checkpoint = {{ Literal (Base64Std $.Checkpoint) }}
	WHERE materialization = {{ Literal $.Materialization.String }}
	AND key_begin = {{ $.KeyBegin }}
	AND key_end = {{ $.KeyEnd }}
	AND fence = {{ $.Fence }}

-- Verify the update succeeded (Spanner doesn't have @@ROWCOUNT)
-- We'll handle verification in the Go code instead
{{ end }}
`, hashColumnDef, hashKeyPrefix, hashColumnDef, hashKeyPrefix))

	return templates{
		createLoadTable:         tplAll.Lookup("createLoadTable"),
		createTargetTable:       tplAll.Lookup("createTargetTable"),
		alterTableColumns:       tplAll.Lookup("alterTableColumns"),
		loadInsert:              tplAll.Lookup("loadInsert"),
		loadQuery:               tplAll.Lookup("loadQuery"),
		loadQueryNoFlowDocument: tplAll.Lookup("loadQueryNoFlowDocument"),
		dropLoadTable:           tplAll.Lookup("dropLoadTable"),
		updateFence:             tplAll.Lookup("updateFence"),
	}
}

// Spanner reserved words (subset of commonly used ones)
// Full list: https://cloud.google.com/spanner/docs/lexical#reserved-keywords
var SPANNER_RESERVED_WORDS = []string{
	"all", "and", "any", "array", "as", "asc", "assert_rows_modified",
	"at", "between", "by", "case", "cast", "collate", "create",
	"cross", "cube", "current", "default", "define", "desc",
	"distinct", "else", "end", "enum", "escape", "except",
	"exists", "extract", "false", "fetch", "following", "for",
	"from", "full", "group", "grouping", "hash", "having",
	"if", "ignore", "in", "inner", "intersect", "interval",
	"into", "is", "join", "left", "like", "limit", "lookup",
	"merge", "natural", "new", "no", "not", "null", "nulls",
	"of", "on", "or", "order", "outer", "over", "partition",
	"preceding", "proto", "range", "recursive", "respect",
	"right", "rollup", "rows", "select", "set", "some",
	"struct", "table", "then", "to", "treat", "true",
	"unbounded", "union", "unnest", "using", "when", "where",
	"window", "with", "within",
}
