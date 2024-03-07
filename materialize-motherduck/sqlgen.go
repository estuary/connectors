package main

import (
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
)

var duckDialect = func() sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
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
				// UUIDs are currently broken in the DuckDB version used by MotherDuck, see
				// https://github.com/duckdb/duckdb/issues/9193. To support UUIDs in the future,
				// we'll need to re-backfill tables that have UUIDs as strings, or allow string
				// columns to validate as uuid columns.
				// "uuid":      sql.NewStaticMapper("UUID"),
			},
		},
	}

	mapper = sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    mapper,
	}

	columnValidator := sql.NewColumnValidator(
		sql.ColValidation{Types: []string{"bigint", "hugeint"}, Validate: sql.IntegerCompatible},
		sql.ColValidation{Types: []string{"double"}, Validate: sql.NumberCompatible},
		sql.ColValidation{Types: []string{"boolean"}, Validate: sql.BooleanCompatible},
		sql.ColValidation{Types: []string{"json"}, Validate: sql.JsonCompatible},
		sql.ColValidation{Types: []string{"varchar"}, Validate: sql.StringCompatible},
		sql.ColValidation{Types: []string{"date"}, Validate: sql.DateCompatible},
		sql.ColValidation{Types: []string{"timestamp with time zone"}, Validate: sql.DateTimeCompatible},
		sql.ColValidation{Types: []string{"interval"}, Validate: sql.DurationCompatible},
		sql.ColValidation{Types: []string{"time"}, Validate: sql.TimeCompatible},
		sql.ColValidation{Types: []string{"uuid"}, Validate: sql.UuidCompatible},
	)

	return sql.Dialect{
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			return sql.InfoTableLocation{TableSchema: path[1], TableName: path[2]}
		}),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return field }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(DUCKDB_RESERVED_WORDS, strings.ToUpper(s))
				},
				sql.QuoteTransform(`"`, `""`),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		ColumnValidator:        columnValidator,
		MaxColumnCharLength:    0, // Duckdb has no apparent limit on how long column names can be
		CaseInsensitiveColumns: true,
	}
}()

type storeParams struct {
	sql.Table
	Files []string
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
