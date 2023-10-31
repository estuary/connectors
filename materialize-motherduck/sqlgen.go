package main

import (
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
)

var duckDialect = func() sql.Dialect {
	var typeMappings sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.INTEGER:  sql.NewStaticMapper("BIGINT"),
		sql.NUMBER:   sql.NewStaticMapper("DOUBLE"),
		sql.BOOLEAN:  sql.NewStaticMapper("BOOLEAN"),
		sql.OBJECT:   sql.NewStaticMapper("JSON", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.ARRAY:    sql.NewStaticMapper("JSON", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.BINARY:   sql.NewStaticMapper("BLOB"),
		sql.MULTIPLE: sql.NewStaticMapper("JSON", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.NewStaticMapper("VARCHAR"),
			WithFormat: map[string]sql.TypeMapper{
				"integer": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("VARCHAR"),
					Delegate:   sql.NewStaticMapper("HUGEINT", sql.WithElementConverter(sql.StdStrToInt())),
				},
				"number": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("VARCHAR"),
					// https://duckdb.org/docs/sql/data_types/numeric.html#floating-point-types
					Delegate: sql.NewStaticMapper("DOUBLE", sql.WithElementConverter(sql.StdStrToFloat("NaN", "Infinity", "-Infinity"))),
				},
				"date":      sql.NewStaticMapper("DATE"),
				"date-time": sql.NewStaticMapper("TIMESTAMP WITH TIME ZONE"),
				"duration":  sql.NewStaticMapper("INTERVAL"),
				"time":      sql.NewStaticMapper("TIME"),
				"uuid":      sql.NewStaticMapper("UUID"),
			},
		},
	}

	var nullable = sql.MaybeNullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    typeMappings,
	}

	return sql.Dialect{
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(DUCKDB_RESERVED_WORDS, strings.ToUpper(s))
				},
				sql.QuoteTransform("\"", "\"\""),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			return "?"
		}),
		TypeMapper:               nullable,
		AlwaysNullableTypeMapper: sql.AlwaysNullableMapper{Delegate: typeMappings},
	}
}()

type s3Params struct {
	sql.Table
	Bucket string
	Key    string
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

-- Templated query for merging documents from S3 into the target table.

{{ define "storeQuery" }}
INSERT INTO {{$.Identifier}} BY NAME
SELECT * FROM read_json(
	's3://{{$.Bucket}}/{{$.Key}}',
	format='newline_delimited',
	columns={
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}: '{{$col.DDL}}'
	{{- end }}
	}
);
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
	tplCreateTargetTable = tplAll.Lookup("createTargetTable")
	tplStoreQuery        = tplAll.Lookup("storeQuery")
	tplUpdateFence       = tplAll.Lookup("updateFence")
)
