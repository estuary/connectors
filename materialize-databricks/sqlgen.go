package main

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"text/template"

	sql "github.com/estuary/connectors/materialize-sql"
)

// Databricks does not allow column names to contain these characters. Attempting to create a column
// with any of them causes an error, so they must be replaced with underscores. Ref:
// https://docs.databricks.com/en/error-messages/error-classes.html#delta_invalid_characters_in_column_name
var columnSanitizerRegexp = regexp.MustCompile(`[ ,;{}\(\)]`)

func translateFlowField(f string) string {
	return columnSanitizerRegexp.ReplaceAllString(f, "_")
}

// databricksDialect returns a representation of the Databricks SQL dialect.
// https://docs.databricks.com/en/sql/language-manual/index.html
var databricksDialect = func() sql.Dialect {
	// Although databricks does support ARRAY and MAP types, they are statically
	// typed and the MAP type is not comparable.
	// Databricks supports JSON extraction using the : operator, this seems like
	// a simpler method for persisting JSON values:
	// https://docs.databricks.com/en/sql/language-manual/sql-ref-json-path-expression.html
	var jsonMapper = sql.MapStatic("STRING", sql.UsingConverter(sql.ToJsonString))

	// https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.ARRAY:   jsonMapper,
			sql.BINARY:  sql.MapStatic("BINARY"),
			sql.BOOLEAN: sql.MapStatic("BOOLEAN"),
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("LONG"),
				sql.MapStatic("NUMERIC(38,0)", sql.AlsoCompatibleWith("decimal")),
			),
			sql.NUMBER:   sql.MapStatic("DOUBLE"),
			sql.OBJECT:   jsonMapper,
			sql.MULTIPLE: jsonMapper,
			sql.STRING_INTEGER: sql.MapStringMaxLen(
				sql.MapStatic("NUMERIC(38,0)", sql.AlsoCompatibleWith("decimal"), sql.UsingConverter(sql.StrToInt)),
				sql.MapStatic("STRING", sql.UsingConverter(sql.ToStr)),
				38,
			),
			sql.STRING_NUMBER: sql.MapStatic("DOUBLE", sql.UsingConverter(sql.StrToFloat("NaN", "Inf", "-Inf"))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("STRING"),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      sql.MapStatic("DATE"),
					"date-time": sql.MapStatic("TIMESTAMP"),
				},
			}),
		},
		sql.WithNotNullText("NOT NULL"),
	)

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"decimal":   {sql.NewMigrationSpec([]string{"double", "string"})},
			"long":      {sql.NewMigrationSpec([]string{"double", "numeric(38,0)", "string"})},
			"double":    {sql.NewMigrationSpec([]string{"string"})},
			"timestamp": {sql.NewMigrationSpec([]string{"string"}, sql.WithCastSQL(datetimeToStringCast))},
			"date":      {sql.NewMigrationSpec([]string{"string"})},
		},
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
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "\\'")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		MaxColumnCharLength:    255,
		CaseInsensitiveColumns: true,
	}
}()

func datetimeToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`date_format(from_utc_timestamp(%s, 'UTC'), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'")`, migration.Identifier)
}

var (
	tplAll = sql.MustParseTemplate(databricksDialect, "root", `
-- Templated creation of a materialized table definition and comments:
-- delta.columnMapping.mode enables column renaming in Databricks. Column renaming was introduced in Databricks Runtime 10.4 LTS which was released in March 2022.
-- See https://docs.databricks.com/en/release-notes/runtime/10.4lts.html
{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
  {{- range $ind, $col := $.Columns }}
  {{- if $ind }},{{ end }}
  {{$col.Identifier}} {{$col.DDL}} COMMENT {{ Literal $col.Comment }}
  {{- end }}
) COMMENT {{ Literal $.Comment }} TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
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
	JOIN (
		{{- range $fi, $file := $.Files }}
		{{ if $fi }} UNION ALL {{ end -}}
		(
			SELECT
			{{ range $ind, $key := $.Table.Keys }}
			{{- if $ind }}, {{ end -}}
			{{ template "cast" $key -}}
			{{- end }}
			FROM json.`+"`{{ $file }}`"+`
		)
		{{- end }}
	) AS r
	{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }}AND {{ else }}ON {{ end -}}
	{{ $.Table.Identifier }}.{{ $bound.Identifier }} = r.{{ $bound.Identifier }}
	{{- if $bound.LiteralLower }} AND {{ $.Table.Identifier }}.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND {{ $.Table.Identifier }}.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
	{{- end }}
{{ else -}}
SELECT -1, ""
{{ end -}}
{{ end }}

-- TODO: this will not work with custom type definitions that require more than a single word
-- namely: ARRAY, MAP and INTERVAL. We don't have these types ourselves, but users may be able
-- to specify them as a custom DDL
{{ define "cast" -}}
{{ $DDL := First (Split $.DDL " ") }}
{{- if eq $DDL "BINARY" -}}
	unbase64({{ $.Identifier }})::BINARY as {{ $.Identifier }}
{{- else -}}
	{{ $.Identifier }}::{{- $DDL -}}
{{- end -}}
{{- end }}

-- Directly copy into the target table
{{ define "copyIntoDirect" }}
	COPY INTO {{ $.Table.Identifier }} FROM (
    SELECT
		{{ range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			{{ template "cast" $key -}}
		{{- end }}
  FROM {{ Literal $.StagingPath }}
	)
  FILEFORMAT = JSON
  FILES = ('{{ Join $.Files "','" }}')
  FORMAT_OPTIONS ( 'mode' = 'FAILFAST', 'ignoreMissingFiles' = 'false' )
	COPY_OPTIONS ( 'mergeSchema' = 'true' )
  ;
{{ end }}

{{ define "mergeInto" }}
	MERGE INTO {{ $.Table.Identifier }} AS l
	USING (
		{{- range $fi, $file := $.Files }}
		{{ if $fi }} UNION ALL {{ end -}}
		(
			SELECT
			{{ range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			{{ template "cast" $key -}}
			{{- end }}
			FROM json.`+"`{{ $file }}`"+`
		)
		{{- end }}
	) AS r
  ON {{ range $ind, $bound := $.Bounds }}
    {{ if $ind -}} AND {{ end -}}
    l.{{ $bound.Identifier }} = r.{{ $bound.Identifier }}
    {{- if $bound.LiteralLower }} AND l.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND l.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
  {{- end }}
	{{- if $.Table.Document }}
	WHEN MATCHED AND r.{{ $.Table.Document.Identifier }}='"delete"' THEN
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
	WHEN NOT MATCHED AND r.{{ $.Table.Document.Identifier }}!='"delete"' THEN
		INSERT (
		{{- range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}
		{{- end -}}
	)
		VALUES (
		{{- range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			r.{{ $key.Identifier }}
		{{- end -}}
	);
{{ end }}
  `)
	tplCreateTargetTable = tplAll.Lookup("createTargetTable")
	tplAlterTableColumns = tplAll.Lookup("alterTableColumns")
	tplLoadQuery         = tplAll.Lookup("loadQuery")
	tplCopyIntoDirect    = tplAll.Lookup("copyIntoDirect")
	tplMergeInto         = tplAll.Lookup("mergeInto")
)

type tableWithFiles struct {
	Files       []string
	StagingPath string
	Table       *sql.Table
	Bounds      []sql.MergeBound
}

func RenderTableWithFiles(table sql.Table, files []string, stagingPath string, tpl *template.Template, bounds []sql.MergeBound) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &tableWithFiles{Table: &table, Files: files, StagingPath: stagingPath, Bounds: bounds}); err != nil {
		return "", err
	}
	return w.String(), nil
}
