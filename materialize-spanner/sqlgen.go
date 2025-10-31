package main

import (
	"fmt"
	"slices"
	"strings"
	"text/template"

	sql "github.com/estuary/connectors/materialize-sql"
)

func createSpannerDialect() sql.Dialect {
	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("INT64"),
				sql.MapStatic("NUMERIC"),
			),
			sql.NUMBER:  sql.MapStatic("FLOAT64"),
			sql.BOOLEAN: sql.MapStatic("BOOL"),
			// Cloud Spanner supports native JSON type
			sql.OBJECT: sql.MapStatic("JSON"),
			sql.ARRAY:  sql.MapStatic("JSON"),
			// Store binary data as BYTES
			sql.BINARY:         sql.MapStatic("BYTES(MAX)"),
			sql.MULTIPLE:       sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.STRING_INTEGER: sql.MapStatic("NUMERIC"),
			sql.STRING_NUMBER:  sql.MapStatic("NUMERIC"),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("STRING(MAX)"),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      sql.MapStatic("DATE", sql.UsingConverter(sql.ClampDate)),
					"date-time": sql.MapStatic("TIMESTAMP", sql.UsingConverter(sql.ClampDatetime)),
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
			"NUMERIC":  {sql.NewMigrationSpec([]string{"FLOAT64", "STRING(MAX)"})},
			"INT64":    {sql.NewMigrationSpec([]string{"FLOAT64", "NUMERIC", "STRING(MAX)"})},
			"FLOAT64":  {sql.NewMigrationSpec([]string{"STRING(MAX)"})},
			"DATE":     {sql.NewMigrationSpec([]string{"STRING(MAX)"})},
			"TIMESTAMP": {sql.NewMigrationSpec([]string{"STRING(MAX)"}, sql.WithCastSQL(timestampToStringCast))},
			"*":        {sql.NewMigrationSpec([]string{"JSON"}, sql.WithCastSQL(toJsonCast))},
		},
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			// Spanner doesn't have schemas, just table names
			if len(path) == 1 {
				return sql.InfoTableLocation{TableSchema: "", TableName: path[0]}
			} else {
				// If a schema is provided, ignore it and use the table name
				return sql.InfoTableLocation{TableSchema: "", TableName: path[len(path)-1]}
			}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string {
			// Spanner doesn't have schemas, so return empty string
			return ""
		}),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return field }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(SPANNER_RESERVED_WORDS, strings.ToUpper(s))
				},
				sql.QuoteTransform("`", "``"),
			))),
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
	createLoadTable   *template.Template
	createTargetTable *template.Template
	alterTableColumns *template.Template
	loadInsert        *template.Template
	loadQuery         *template.Template
	installFence      *template.Template
	updateFence       *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
	var tplAll = sql.MustParseTemplate(dialect, "root", `
{{ define "temp_name" -}}
flow_temp_table_{{ $.Binding }}
{{- end }}

-- Templated creation of a materialized table definition:

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
{{ end }}

-- Templated query which performs table alterations by adding columns.
-- Note: Spanner does NOT support dropping NOT NULL constraints after table creation,
-- so we only handle adding columns here.

{{ define "alterTableColumns" }}
{{- if $.AddColumns }}
ALTER TABLE {{$.Identifier}}
{{- range $ind, $col := $.AddColumns }}
	{{- if $ind }},{{ end }}
	ADD COLUMN {{$col.Identifier}} {{$col.NullableDDL}}
{{- end }}
{{- end }}
{{ end }}

-- Templated creation of a temporary load table:

{{ define "createLoadTable" }}
CREATE TEMPORARY TABLE {{ template "temp_name" . }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{ $key.Identifier }} {{ $key.DDL }}
	{{- end }}
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

-- Templated fence installation for checkpoints table:

{{ define "installFence" }}
-- Insert or update checkpoint fence for Spanner
MERGE INTO {{ Identifier $.TablePath }} AS target
USING (
	SELECT
		{{ Literal $.Materialization.String }} AS materialization,
		{{ $.KeyBegin }} AS key_begin,
		{{ $.KeyEnd }} AS key_end
) AS source
ON target.materialization = source.materialization
	AND target.key_begin = source.key_begin
	AND target.key_end = source.key_end
WHEN MATCHED THEN
	UPDATE SET fence = target.fence + 1
WHEN NOT MATCHED THEN
	INSERT (materialization, key_begin, key_end, fence, checkpoint)
	VALUES (
		{{ Literal $.Materialization.String }},
		{{ $.KeyBegin }},
		{{ $.KeyEnd }},
		{{ $.Fence }},
		{{ Literal (Base64Std $.Checkpoint) }}
	)

-- Return the fence and checkpoint
SELECT fence, FROM_BASE64(checkpoint) AS checkpoint
FROM {{ Identifier $.TablePath }}
WHERE materialization = {{ Literal $.Materialization.String }}
	AND key_begin = {{ $.KeyBegin }}
	AND key_end = {{ $.KeyEnd }}
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
`)

	return templates{
		createLoadTable:   tplAll.Lookup("createLoadTable"),
		createTargetTable: tplAll.Lookup("createTargetTable"),
		alterTableColumns: tplAll.Lookup("alterTableColumns"),
		loadInsert:        tplAll.Lookup("loadInsert"),
		loadQuery:         tplAll.Lookup("loadQuery"),
		installFence:      tplAll.Lookup("installFence"),
		updateFence:       tplAll.Lookup("updateFence"),
	}
}

// Spanner reserved words (subset of commonly used ones)
// Full list: https://cloud.google.com/spanner/docs/lexical#reserved-keywords
var SPANNER_RESERVED_WORDS = []string{
	"ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "ASSERT_ROWS_MODIFIED",
	"AT", "BETWEEN", "BY", "CASE", "CAST", "COLLATE", "CREATE",
	"CROSS", "CUBE", "CURRENT", "DEFAULT", "DEFINE", "DESC",
	"DISTINCT", "ELSE", "END", "ENUM", "ESCAPE", "EXCEPT",
	"EXISTS", "EXTRACT", "FALSE", "FETCH", "FOLLOWING", "FOR",
	"FROM", "FULL", "GROUP", "GROUPING", "HASH", "HAVING",
	"IF", "IGNORE", "IN", "INNER", "INTERSECT", "INTERVAL",
	"INTO", "IS", "JOIN", "LEFT", "LIKE", "LIMIT", "LOOKUP",
	"MERGE", "NATURAL", "NEW", "NO", "NOT", "NULL", "NULLS",
	"OF", "ON", "OR", "ORDER", "OUTER", "OVER", "PARTITION",
	"PRECEDING", "PROTO", "RANGE", "RECURSIVE", "RESPECT",
	"RIGHT", "ROLLUP", "ROWS", "SELECT", "SET", "SOME",
	"STRUCT", "TABLE", "THEN", "TO", "TREAT", "TRUE",
	"UNBOUNDED", "UNION", "UNNEST", "USING", "WHEN", "WHERE",
	"WINDOW", "WITH", "WITHIN",
}
