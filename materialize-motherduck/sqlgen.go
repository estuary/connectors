package main

import (
	"fmt"
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
)

var duckDialect = func() sql.Dialect {
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
			sql.BINARY:   sql.MapStatic("VARCHAR"),
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
					"date":      sql.MapStatic("DATE"),
					"date-time": sql.MapStatic("TIMESTAMP WITH TIME ZONE"),
					"time":      sql.MapStatic("TIME"),
					"uuid":      sql.MapStatic("UUID"),
				},
			}),
		},
		sql.WithNotNullText("NOT NULL"),
	)

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"double":                   {sql.NewMigrationSpec([]string{"varchar"})},
			"bigint":                   {sql.NewMigrationSpec([]string{"double", "hugeint", "varchar"})},
			"hugeint":                  {sql.NewMigrationSpec([]string{"double", "varchar"})},
			"date":                     {sql.NewMigrationSpec([]string{"varchar"})},
			"timestamp with time zone": {sql.NewMigrationSpec([]string{"varchar"}, sql.WithCastSQL(datetimeToStringCast))},
			"time":                     {sql.NewMigrationSpec([]string{"varchar"})},
			"uuid":                     {sql.NewMigrationSpec([]string{"varchar"})},
			"*":                        {sql.NewMigrationSpec([]string{"json"}, sql.WithCastSQL(toJsonCast))},
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
}()

func datetimeToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`strftime(timezone('UTC', %s), '%%Y-%%m-%%dT%%H:%%M:%%S.%%fZ')`, migration.Identifier)
}

func toJsonCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`to_json(%s)`, migration.Identifier)
}

type queryParams struct {
	sql.Table
	Bounds []sql.MergeBound
	Files  []string
}

var (
	tplAll = sql.MustParseTemplate(duckDialect, "root", `
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
		{{$bound.Identifier}}: '{{$bound.DDL}}'
	{{- end }}
	}
) AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ $bound.Identifier }} = r.{{ $bound.Identifier }}
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
	columns={
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}: '{{$col.DDL}}'
	{{- end }}
	}
) AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} WHERE {{ end -}}
	l.{{ $bound.Identifier }} = r.{{ $bound.Identifier }}
	{{- if $bound.LiteralLower }} AND l.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND l.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }};
{{ end }}

{{ define "storeQuery" }}
INSERT INTO {{$.Identifier}} BY NAME
SELECT 
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }} {{ $col.Identifier -}}
	{{- end }} FROM (
		SELECT * FROM read_json(
			[
			{{- range $ind, $f := $.Files }}
			{{- if $ind }}, {{ end }}'{{ $f }}'
			{{- end -}}
			],
			format='newline_delimited',
			compression='gzip',
			maximum_object_size=1073741824,
			columns={
			{{- range $ind, $col := $.Columns }}
				{{- if $ind }},{{ end }}
				{{$col.Identifier}}: '{{$col.DDL}}'
			{{- end -}}
			, _flow_delete: 'BOOLEAN'
			}
		) WHERE NOT _flow_delete
	);
{{ end }}

{{ define "loadQueryNoFlowDocument" }}                                                                                                                             
{{ if $.DeltaUpdates -}}                                                                                                                                           
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc                                                                                                     
{{ else -}}                                                                                                                                                        
SELECT {{ $.Binding }} AS binding,                                                                                                                                 
json_object(                                                                                                                                                       
{{- range $i, $col := $.RootLevelColumns}}                                                                                                                         
       {{- if $i}}, {{end}}                                                                                                                                        
       '{{$col.Field}}', l.{{$col.Identifier}}                                                                                                                     
{{- end}}                                                                                                                                                          
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
               {{$bound.Identifier}}: '{{$bound.DDL}}'                                                                                                             
       {{- end }}                                                                                                                                                  
       }                                                                                                                                                           
) AS r                                                                                                                                                             
{{- range $ind, $bound := $.Bounds }}                                                                                                                              
       {{ if $ind }} AND {{ else }} ON  {{ end -}}                                                                                                                 
       l.{{ $bound.Identifier }} = r.{{ $bound.Identifier }}                                                                                                       
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
`)
	tplCreateTargetTable       = tplAll.Lookup("createTargetTable")
	tplStoreQuery              = tplAll.Lookup("storeQuery")
	tplStoreDeleteQuery        = tplAll.Lookup("storeDeleteQuery")
	tplLoadQuery               = tplAll.Lookup("loadQuery")
	tplLoadQueryNoFlowDocument = tplAll.Lookup("loadQueryNoFlowDocument")
	tplUpdateFence             = tplAll.Lookup("updateFence")
)
