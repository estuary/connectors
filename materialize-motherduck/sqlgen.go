package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"text/template"

	sql "github.com/estuary/connectors/materialize-sql"
)

const (
	// Maximum size of a JSON object in bytes.
	//
	// This determines the amount of memory that MotherDuck will allocate for
	// processing a row when reading a newline_delimited JSON file. Since our
	// documents are limited to less than 64MiB, it is expected that the
	// encoded ndjson will be similar in size.
	MAX_OBJECT_BYTES = 64 * 1024 * 1024 // 64MiB
)

func createDuckDialect(featureFlags map[string]bool) sql.Dialect {
	// Define base date/time mappings without primary key wrapper
	primaryKeyTextType := sql.MapStatic("VARCHAR")
	dateMapping := sql.MapStatic("DATE")
	datetimeMapping := sql.MapStatic("TIMESTAMP WITH TIME ZONE", sql.UsingConverter(sql.NormalizeDatetimeString))
	timeMapping := sql.MapStatic("TIME")

	// If feature flag is enabled, wrap with MapPrimaryKey to use string types for primary keys
	if featureFlags["datetime_keys_as_string"] {
		dateMapping = sql.MapPrimaryKey(primaryKeyTextType, dateMapping)
		datetimeMapping = sql.MapPrimaryKey(primaryKeyTextType, datetimeMapping)
		timeMapping = sql.MapPrimaryKey(primaryKeyTextType, timeMapping)
	}

	binaryMapping := sql.MapStatic("VARCHAR")
	if featureFlags["native_binary_column_type"] {
		binaryMapping = sql.MapStatic("BLOB")
	}

	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("BIGINT"),
				sql.MapStatic("HUGEINT"),
			),
			sql.NUMBER:   sql.MapStatic("DOUBLE"),
			sql.BOOLEAN:  sql.MapStatic("BOOLEAN"),
			sql.OBJECT:   sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.ARRAY:    sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.BINARY:   binaryMapping,
			sql.MULTIPLE: sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.STRING_INTEGER: sql.MapStringMaxLen(
				sql.MapStatic("HUGEINT", sql.UsingConverter(sql.StrToInt)),
				sql.MapStatic("VARCHAR", sql.UsingConverter(sql.ToStr)),
				// A 96-bit integer is 39 characters long, but not all 39 digit
				// integers will fit in one.
				38,
			),
			// https://duckdb.org/docs/sql/data_types/numeric.html#floating-point-types
			sql.STRING_NUMBER: sql.MapStatic("DOUBLE", sql.UsingConverter(sql.StrToFloat("NaN", "Infinity", "-Infinity"))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("VARCHAR"),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      dateMapping,
					"date-time": datetimeMapping,
					"time":      timeMapping,
					"uuid":      sql.MapStatic("UUID"),
				},
			}),
		},
		sql.WithNotNullSuffix("NOT NULL"),
	)

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"double":                   {sql.NewMigrationSpec([]string{"varchar"}, sql.WithDirectCast())},
			"bigint":                   {sql.NewMigrationSpec([]string{"double", "hugeint", "varchar"}, sql.WithDirectCast())},
			"hugeint":                  {sql.NewMigrationSpec([]string{"double", "varchar"}, sql.WithDirectCast())},
			"date":                     {sql.NewMigrationSpec([]string{"varchar"}, sql.WithDirectCast())},
			"timestamp with time zone": {sql.NewMigrationSpec([]string{"varchar"}, sql.WithDirectCast(), sql.WithCastSQL(datetimeToStringCast))},
			"time":                     {sql.NewMigrationSpec([]string{"varchar"}, sql.WithDirectCast())},
			"uuid":                     {sql.NewMigrationSpec([]string{"varchar"}, sql.WithDirectCast())},
			"varchar":                  {sql.NewMigrationSpec([]string{"blob"}, sql.WithDirectCast(), sql.WithCastSQL(stringToBlobCast))},
			"blob":                     {sql.NewMigrationSpec([]string{"varchar"}, sql.WithDirectCast(), sql.WithCastSQL(blobToStringCast))},
			// DuckDB's CAST(JSON AS VARCHAR) yields the JSON text, used when
			// reverting object/array fields from JSON storage back to varchar.
			"json": {sql.NewMigrationSpec([]string{"varchar"}, sql.WithDirectCast())},
			"*":    {sql.NewMigrationSpec([]string{"json"}, sql.WithDirectCast(), sql.WithCastSQL(toJsonCast))},
		},
		DirectCastSQL: func(table sql.Table, m sql.ColumnTypeMigration) string {
			return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s",
				table.Identifier, m.Identifier, m.BareDDL, m.CastSQL(m))
		},
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			return sql.InfoTableLocation{TableSchema: path[1], TableName: path[2]}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string { return schema }),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return field }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(DUCKDB_RESERVED_WORDS, strings.ToUpper(s))
				},
				sql.QuoteTransform(`"`, `""`),
			))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		MaxColumnCharLength:    0, // Duckdb has no apparent limit on how long column names can be
		CaseInsensitiveColumns: true,
	}
}

type templates struct {
	createTargetTable       *template.Template
	storeQuery              *template.Template
	storeDeleteQuery        *template.Template
	loadQuery               *template.Template
	loadQueryNoFlowDocument *template.Template
	updateFence             *template.Template
}

func renderTemplates(dialect sql.Dialect) *templates {
	tplAll := sql.MustParseTemplate(dialect, "root", tplRoot)
	return &templates{
		createTargetTable:       tplAll.Lookup("createTargetTable"),
		storeQuery:              tplAll.Lookup("storeQuery"),
		storeDeleteQuery:        tplAll.Lookup("storeDeleteQuery"),
		loadQuery:               tplAll.Lookup("loadQuery"),
		loadQueryNoFlowDocument: tplAll.Lookup("loadQueryNoFlowDocument"),
		updateFence:             tplAll.Lookup("updateFence"),
	}
}

func datetimeToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`strftime(timezone('UTC', %s), '%%Y-%%m-%%dT%%H:%%M:%%S.%%fZ')`, migration.Identifier)
}

func toJsonCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`to_json(%s)`, migration.Identifier)
}

// stringToBlobCast decodes a base64-encoded VARCHAR column into a native BLOB.
// Used when the native_binary_column_type feature flag is enabled on a task
// that previously stored binary fields as base64 text.
func stringToBlobCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`from_base64(%s)`, migration.Identifier)
}

// blobToStringCast encodes a BLOB column as a base64 VARCHAR, the canonical
// textual representation of binary data in Flow. Used when reverting from
// native binary back to base64 text storage.
func blobToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`to_base64(%s)`, migration.Identifier)
}

type queryParams struct {
	sql.Table
	Bounds []sql.MergeBound
	Files  []string
}

var tplRoot = `
-- Templated creation of a materialized table definition.

{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
{{- range $ind, $col := $.Columns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.DDL}}
{{- end }}
);
{{ end }}

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }} AS binding, l.{{ $.Document.Identifier }} AS doc
FROM {{ $.Identifier }} AS l
JOIN read_json(
	[
	{{- range $ind, $f := $.Files }}
	{{- if $ind }}, {{ end }}'{{ $f }}'
	{{- end -}}
	],
	format='newline_delimited',
	compression='gzip',
	columns={
	{{- range $ind, $bound := $.Bounds }}
		{{- if $ind }},{{ end }}
		{{$bound.Identifier}}: '{{ if eq $bound.BareDDL "BLOB" }}VARCHAR{{ if $bound.MustExist }} NOT NULL{{ end }}{{ else }}{{$bound.DDL}}{{ end }}'
	{{- end }}
	}
) AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ $bound.Identifier }} = {{ if eq $bound.BareDDL "BLOB" }}FROM_BASE64(r.{{ $bound.Identifier }}){{ else }}r.{{ $bound.Identifier }}{{ end }}
	{{- if $bound.LiteralLower }} AND l.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND l.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end -}}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc
{{- end }}
{{ end }}

-- Templated query for merging documents from S3 into the target table.

{{ define "storeDeleteQuery" }}
DELETE FROM {{$.Identifier}} AS l
USING read_json(
	[
	{{- range $ind, $f := $.Files }}
	{{- if $ind }}, {{ end }}'{{ $f }}'
	{{- end -}}
	],
	format='newline_delimited',
	compression='gzip',
	maximum_object_size=` + strconv.FormatUint(MAX_OBJECT_BYTES, 10) + `,
	columns={
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}: '{{ if eq $col.BareDDL "BLOB" }}VARCHAR{{ if $col.MustExist }} NOT NULL{{ end }}{{ else }}{{$col.DDL}}{{ end }}'
	{{- end }}
	}
) AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} WHERE {{ end -}}
	l.{{ $bound.Identifier }} = {{ if eq $bound.BareDDL "BLOB" }}FROM_BASE64(r.{{ $bound.Identifier }}){{ else }}r.{{ $bound.Identifier }}{{ end }}
	{{- if $bound.LiteralLower }} AND l.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND l.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }};
{{ end }}

{{ define "storeQuery" }}
INSERT INTO {{$.Identifier}} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}}
	{{- end }}
)
SELECT
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end }}
		{{ if eq $col.BareDDL "BLOB" }}FROM_BASE64({{$col.Identifier}}){{ else }}{{$col.Identifier}}{{ end }}
	{{- end }}
FROM read_json(
	[
	{{- range $ind, $f := $.Files }}
	{{- if $ind }}, {{ end }}'{{ $f }}'
	{{- end -}}
	],
	format='newline_delimited',
	compression='gzip',
	maximum_object_size=` + strconv.FormatUint(MAX_OBJECT_BYTES, 10) + `,
	columns={
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}: '{{ if eq $col.BareDDL "BLOB" }}VARCHAR{{ if $col.MustExist }} NOT NULL{{ end }}{{ else }}{{$col.DDL}}{{ end }}'
	{{- end }}
	, _flow_delete: 'BOOLEAN NOT NULL'
	}
) WHERE NOT _flow_delete;
{{ end }}

{{ define "uncast" -}}
{{ $ident := printf "%s.%s" $.Alias $.Identifier }}
{{- if eq $.AsFlatType "string_integer" -}}
	CAST({{ $ident }} AS VARCHAR)
{{- else if eq $.AsFlatType "string_number" -}}
	CAST({{ $ident }} AS VARCHAR)
{{- else if and (eq $.AsFlatType "string") (eq $.Format "date") (not $.IsPrimaryKey) -}}
	strftime({{ $ident }}, '%Y-%m-%d')
{{- else if and (eq $.AsFlatType "string") (eq $.Format "date-time") (not $.IsPrimaryKey) -}}
	strftime(timezone('UTC', {{ $ident }}), '%Y-%m-%dT%H:%M:%S.%fZ')
{{- else if and (eq $.AsFlatType "string") (eq $.Format "time") (not $.IsPrimaryKey) -}}
	CAST({{ $ident }} AS VARCHAR)
{{- else if eq $.BareDDL "BLOB" -}}
	TO_BASE64({{ $ident }})
{{- else -}}
	{{ $ident }}
{{- end -}}
{{- end }}

{{ define "loadQueryNoFlowDocument" }}                                                                                                                             
{{ if $.DeltaUpdates -}}                                                                                                                                           
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc                                                                                                     
{{ else -}}                                                                                                                                                        
SELECT {{ $.Binding }} AS binding,                                                                                                                                 
json_object(                                                                                                                                                       
{{- range $i, $col := $.RootLevelColumns}}                                                                                                                         
       {{- if $i}}, {{end}}                                                                                                                                        
       '{{$col.Field}}', {{ template "uncast" (ColumnWithAlias $col "l") }}                                                                                                                     
{{- end}}                                                                                                                                                          
, '_meta', json_object('uuid', {{ template "uncast" (ColumnWithAlias $.MetaUUIDColumn "l") }})
) as doc                                                                                                                                                           
FROM {{ $.Identifier }} AS l                                                                                                                                       
JOIN read_json(                                                                                                                                                    
       [                                                                                                                                                           
       {{- range $ind, $f := $.Files }}                                                                                                                            
       {{- if $ind }}, {{ end }}'{{ $f }}'                                                                                                                         
       {{- end -}}                                                                                                                                                 
       ],                                                                                                                                                          
       format='newline_delimited',                                                                                                                                 
       compression='gzip',                                                                                                                                         
       columns={                                                                                                                                                   
       {{- range $ind, $bound := $.Bounds }}                                                                                                                       
               {{- if $ind }},{{ end }}                                                                                                                            
               {{$bound.Identifier}}: '{{ if eq $bound.BareDDL "BLOB" }}VARCHAR{{ if $bound.MustExist }} NOT NULL{{ end }}{{ else }}{{$bound.DDL}}{{ end }}'
       {{- end }}                                                                                                                                                  
       }                                                                                                                                                           
) AS r                                                                                                                                                             
{{- range $ind, $bound := $.Bounds }}                                                                                                                              
       {{ if $ind }} AND {{ else }} ON  {{ end -}}                                                                                                                 
       l.{{ $bound.Identifier }} = {{ if eq $bound.BareDDL "BLOB" }}FROM_BASE64(r.{{ $bound.Identifier }}){{ else }}r.{{ $bound.Identifier }}{{ end }}
       {{- if $bound.LiteralLower }} AND l.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND l.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end -}}                                                                                                                                                        
{{- end }}                                                                                                                                                         
{{ end }} 

-- Templated update of a fence checkpoint.

{{ define "updateFence" }}
UPDATE {{ Identifier $.TablePath }}
	SET   checkpoint = {{ Literal (Base64Std $.Checkpoint) }}
	WHERE materialization = {{ Literal $.Materialization.String }}
	AND   key_begin = {{ $.KeyBegin }}
	AND   key_end   = {{ $.KeyEnd }}
	AND   fence     = {{ $.Fence }};
{{ end }}
`
