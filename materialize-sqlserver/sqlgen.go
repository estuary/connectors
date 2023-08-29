package main

import (
	"strings"
	"time"
	"fmt"

	"slices"
	sql "github.com/estuary/connectors/materialize-sql"
)

var sqlServerDialect = func() sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.INTEGER: sql.NewStaticMapper("BIGINT", sql.WithElementConverter(sql.StdStrToInt())),
		sql.NUMBER:  sql.NewStaticMapper("DOUBLE PRECISION", sql.WithElementConverter(sql.StdStrToFloat())),
		sql.BOOLEAN: sql.NewStaticMapper("BIT"),
		sql.OBJECT:  sql.NewStaticMapper("VARCHAR(MAX)", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.ARRAY:   sql.NewStaticMapper("VARCHAR(MAX)", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.BINARY:  sql.NewStaticMapper("VARBINARY(MAX)"),
		sql.STRING:  sql.PrimaryKeyMapper {
			// sqlserver cannot do varchar primary keys larger than 900 bytes, and in
			// sqlserver, the number N passed to varchar(N), denotes the maximum bytes
			// stored in the column, not the character length.
			// see https://learn.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql?view=sql-server-2017#remarks
			// and https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server?view=sql-server-2017
			PrimaryKey: sql.NewStaticMapper("VARCHAR(900)"),
			Delegate: sql.StringTypeMapper{
				Fallback: sql.NewStaticMapper("VARCHAR(MAX)"),
				WithFormat: map[string]sql.TypeMapper{
					"date":      sql.NewStaticMapper("DATE"),
					"date-time": sql.NewStaticMapper("DATETIME2", sql.WithElementConverter(rfc3339ToUTC())),
					"time":      sql.NewStaticMapper("TIME", sql.WithElementConverter(rfc3339TimeToUTC())),
				},
			},
		},
		sql.MULTIPLE: sql.NewStaticMapper("VARCHAR(MAX)", sql.WithElementConverter(sql.JsonBytesConverter)),
	}
	mapper = sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    mapper,
	}

	return sql.Dialect{
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(SQLSERVER_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform("\"", "\\\""),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			// parameterIndex starts at 0, but sqlserver parameters start at @p1
			return fmt.Sprintf("@p%d", index+1)
		}),
		TypeMapper: mapper,
	}
}()

func rfc3339ToUTC() sql.ElementConverter {
	return sql.StringCastConverter(func(str string) (interface{}, error) {
		if t, err := time.Parse(time.RFC3339Nano, str); err != nil {
			return nil, fmt.Errorf("could not parse %q as RFC3339 date-time: %w", str, err)
		} else {
			return t.UTC(), nil
		}
	})
}

func rfc3339TimeToUTC() sql.ElementConverter {
	return sql.StringCastConverter(func(str string) (interface{}, error) {
		if t, err := time.Parse("15:04:05.999999999Z07:00", str); err != nil {
			return nil, fmt.Errorf("could not parse %q as RFC3339 time: %w", str, err)
		} else {
			return t.UTC(), nil
		}
	})
}

var (
	tplAll = sql.MustParseTemplate(sqlServerDialect, "root", `
-- Local (session-level) temporary tables are prefixed with a # sign in SQLServer
{{ define "temp_load_name" -}}
#flow_temp_load_{{ $.Binding }}
{{- end }}

{{ define "temp_store_name" -}}
#flow_temp_store_{{ $.Binding }}
{{- end }}

-- Templated creation of a materialized table definition and comments:

{{ define "createTargetTable" }}
IF OBJECT_ID(N'{{Join $.Path "."}}', 'U') IS NULL BEGIN
CREATE TABLE {{$.Identifier}} (
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
END;
{{ end }}

-- Templated creation of a temporary load table:

{{ define "createLoadTable" }}
CREATE TABLE {{ template "temp_load_name" . }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{ $key.Identifier }} {{ $key.DDL }}
	{{- end -}}
);
{{ end }}

-- Query for inserting to temporary load table

{{ define "loadInsert" }}
INSERT INTO {{ template "temp_load_name" . }} (
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
);
{{ end }}

-- Templated creation of a temporary store table:

{{ define "createStoreTable" }}
CREATE TABLE {{ template "temp_store_name" . }} (
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
{{ end }}

-- Templated truncation of the temporary load table:

{{ define "truncateTempLoadTable" }}
TRUNCATE TABLE {{ template "temp_load_name" . }};
{{ end }}

-- Templated truncation of the temporary store table:

{{ define "truncateTempStoreTable" }}
TRUNCATE TABLE {{ template "temp_store_name" . }};
{{ end }}

-- Templated query which joins keys from the load table with the target table, and returns values. It
-- deliberately skips the trailing semi-colon as these queries are composed with a UNION ALL.

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }}, r.{{$.Document.Identifier}}
	FROM {{ template "temp_load_name" . }} AS l
	JOIN {{ $.Identifier}} AS r
	{{- range $ind, $key := $.Keys }}
		{{ if $ind }} AND {{ else }} ON  {{ end -}}
		l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
{{ else -}}
SELECT TOP 0 -1, NULL
{{ end }}
{{ end }}

-- If there are no updates to a table, we can just do a direct copy from temporary table into target table

{{ define "directCopy" }}
	INSERT INTO {{ $.Identifier }} 
		(
			{{ range $ind, $key := $.Columns }}
				{{- if $ind }}, {{ end -}}
				{{ $key.Identifier -}}
			{{- end }}
		)
	SELECT
			{{ range $ind, $key := $.Columns }}
				{{- if $ind }}, {{ end -}}
				{{ $key.Identifier -}}
			{{- end }}
	FROM {{ template "temp_store_name" . }}
{{ end }}

{{ define "mergeInto" }}
	MERGE INTO {{ $.Identifier }}
	USING (
		SELECT {{ range $ind, $key := $.Columns }}
			{{- if $ind }}, {{ end -}}
			{{ $key.Identifier -}}
		{{- end }}
		FROM {{ template "temp_store_name" . }}
	) AS r
	ON {{ range $ind, $key := $.Keys }}
		{{- if $ind }} AND {{ end -}}
		{{ $.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
	{{- if $.Document }}
	WHEN MATCHED AND r.{{ $.Document.Identifier }}=NULL THEN
		DELETE
	{{- end }}
	WHEN MATCHED THEN
		UPDATE SET {{ range $ind, $key := $.Values }}
		{{- if $ind }}, {{ end -}}
		{{ $.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end -}}
	{{- if $.Document -}}
	{{ if $.Values }}, {{ end }}{{ $.Identifier }}.{{ $.Document.Identifier}} = r.{{ $.Document.Identifier }}
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

{{ define "updateFence" }}
UPDATE {{ Identifier $.TablePath }}
	SET   "checkpoint" = {{ Literal (Base64Std $.Checkpoint) }}
	WHERE materialization = {{ Literal $.Materialization.String }}
	AND   key_begin = {{ $.KeyBegin }}
	AND   key_end   = {{ $.KeyEnd }}
	AND   fence     = {{ $.Fence }};
{{ end }}
`)
	tplTempLoadTableName  = tplAll.Lookup("temp_load_name")
	tplTempStoreTableName = tplAll.Lookup("temp_store_name")
	tplTempLoadTruncate   = tplAll.Lookup("truncateTempLoadTable")
	tplTempStoreTruncate  = tplAll.Lookup("truncateTempStoreTable")
	tplCreateLoadTable    = tplAll.Lookup("createLoadTable")
	tplCreateStoreTable   = tplAll.Lookup("createStoreTable")
	tplCreateTargetTable  = tplAll.Lookup("createTargetTable")
	tplDirectCopy         = tplAll.Lookup("directCopy")
	tplMergeInto          = tplAll.Lookup("mergeInto")
	tplLoadInsert         = tplAll.Lookup("loadInsert")
	tplLoadQuery          = tplAll.Lookup("loadQuery")
	tplUpdateFence        = tplAll.Lookup("updateFence")
)
