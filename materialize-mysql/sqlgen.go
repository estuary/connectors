package main

import (
	"fmt"
	"slices"
	"strings"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
)

var mysqlDialect = func() sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.INTEGER: sql.NewStaticMapper("BIGINT", sql.WithElementConverter(sql.StdStrToInt())),
		sql.NUMBER:  sql.NewStaticMapper("DOUBLE PRECISION", sql.WithElementConverter(sql.StdStrToFloat())),
		sql.BOOLEAN: sql.NewStaticMapper("BOOLEAN"),
		sql.OBJECT:  sql.NewStaticMapper("JSON"),
		sql.ARRAY:   sql.NewStaticMapper("JSON"),
		sql.BINARY:  sql.NewStaticMapper("LONGBLOB"),
		sql.STRING: sql.PrimaryKeyMapper{
			PrimaryKey: sql.NewStaticMapper("VARCHAR(256)"),
			Delegate: sql.StringTypeMapper{
				Fallback: sql.NewStaticMapper("LONGTEXT"),
				WithFormat: map[string]sql.TypeMapper{
					"date":      sql.NewStaticMapper("DATE"),
					"date-time": sql.NewStaticMapper("TIMESTAMP", sql.WithElementConverter(rfc3339ToUTC())),
					"time":      sql.NewStaticMapper("TIME", sql.WithElementConverter(rfc3339TimeToUTC())),
				},
				WithContentType: map[string]sql.TypeMapper{
					// The largest allowable size for a LONGBLOB is 2^32 bytes (4GB). Our stored specs and
					// checkpoints can be quite long, so we need to use as large of column size as
					// possible for these tables.
					"application/x-protobuf; proto=flow.MaterializationSpec": sql.NewStaticMapper("LONGBLOB"),
					"application/x-protobuf; proto=consumer.Checkpoint":      sql.NewStaticMapper("LONGBLOB"),
				},
			},
		},
		sql.MULTIPLE: sql.NewStaticMapper("JSON", sql.WithElementConverter(sql.JsonBytesConverter)),
	}
	mapper = sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    mapper,
	}

	return sql.Dialect{
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(MYSQL_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform("`", "\\`"),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			return "?"
		}),
		TypeMapper: mapper,
	}
}()

func rfc3339ToUTC() sql.ElementConverter {
	return sql.StringCastConverter(func(str string) (interface{}, error) {
		if t, err := time.Parse(time.RFC3339Nano, str); err != nil {
			return nil, fmt.Errorf("could not parse %q as RFC3339 date-time: %w", str, err)
		} else {
			return t.UTC().Format("2006-01-02T15:04:05.999999999"), nil
		}

	})
}

func rfc3339TimeToUTC() sql.ElementConverter {
	return sql.StringCastConverter(func(str string) (interface{}, error) {
		if t, err := time.Parse("15:04:05.999999999Z07:00", str); err != nil {
			return nil, fmt.Errorf("could not parse %q as RFC3339 time: %w", str, err)
		} else {
			return t.UTC().Format("15:04:05.999999999"), nil
		}
	})
}

var (
	tplAll = sql.MustParseTemplate(mysqlDialect, "root", `
{{ define "temp_load_name" -}}
flow_temp_load_table_{{ $.Binding }}
{{- end }}

{{ define "temp_update_name" -}}
flow_temp_update_table_{{ $.Binding }}
{{- end }}

-- Templated creation of a materialized table definition and comments:

{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}} {{$col.DDL}} {{- if $col.Comment }} COMMENT {{Literal $col.Comment}}{{- end }}
	{{- end }}
	{{- if not $.DeltaUpdates }},

		PRIMARY KEY (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
	{{- end -}}
	)
	{{- end }}
) CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin {{- if $.Comment }} COMMENT={{Literal $.Comment}} {{- end }};
{{ end }}

-- Templated creation of a temporary load table:

{{ define "createLoadTable" }}
CREATE TEMPORARY TABLE {{ template "temp_load_name" . }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{ $key.Identifier }} {{ $key.DDL }}
	{{- end }}
	,
		PRIMARY KEY (
		{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
		{{- end -}}
	)
) CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin;
{{ end }}

-- Templated truncation of the temporary load table:

{{ define "truncateTempTable" }}
TRUNCATE {{ template "temp_load_name" . }};
{{ end }}

-- Templated load into the temporary load table:

{{ define "loadLoad" }}
LOAD DATA LOCAL INFILE 'Reader::batch_data_load_{{ $.Binding }}' INTO TABLE {{ template "temp_load_name" . }}
	FIELDS
		TERMINATED BY ','
		OPTIONALLY ENCLOSED BY '"'
		ESCAPED BY ''
	LINES
		TERMINATED BY '\n'
(
	{{- range $ind, $col := $.Keys }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
);
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
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc
{{ end }}
{{ end }}

-- Template to load data into target table

{{ define "storeLoad" }}
LOAD DATA LOCAL INFILE 'Reader::batch_data_store_{{ $.Binding }}' INTO TABLE {{ $.Identifier }}
	FIELDS
		TERMINATED BY ','
		OPTIONALLY ENCLOSED BY '"'
		ESCAPED BY ''
	LINES
		TERMINATED BY '\n'
(
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
);
{{ end }}

{{ define "createUpdateTable" }}
CREATE TEMPORARY TABLE {{ template "temp_update_name" . }} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}} {{$col.DDL}}
	{{- end }}
	,
	PRIMARY KEY (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
	{{- end -}}
	)
) CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin;
{{ end }}

{{ define "updateLoad" }}
LOAD DATA LOCAL INFILE 'Reader::batch_data_update_{{ $.Binding }}' INTO TABLE {{ template "temp_update_name" . }}
	FIELDS
		TERMINATED BY ','
		OPTIONALLY ENCLOSED BY '"'
		ESCAPED BY ''
	LINES
		TERMINATED BY '\n'
(
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
);
{{ end }}

{{ define "updateReplace" }}
REPLACE INTO {{ $.Identifier }}
(
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
)
SELECT
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
FROM {{ template "temp_update_name" . }};
{{ end }}


{{ define "truncateUpdateTable" }}
TRUNCATE {{ template "temp_update_name" . }};
{{ end }}

{{ define "installFence" }}
with
-- Increment the fence value of _any_ checkpoint which overlaps our key range.
update_covered as (
	update {{ Identifier $.TablePath }}
		set   fence = fence + 1
		where materialization = {{ Literal $.Materialization.String }}
		and   key_end >= {{ $.KeyBegin }}
		and   key_begin <= {{ $.KeyEnd }}
	returning *
),
-- Read the checkpoint with the narrowest [key_begin, key_end] which fully overlaps our range.
best_match as (
	select materialization, key_begin, key_end, fence, checkpoint from update_covered
		where materialization = {{ Literal $.Materialization.String }}
		and 	key_begin <= {{ $.KeyBegin }}
		and   key_end >= {{ $.KeyEnd }}
		order by key_end - key_begin asc
		limit 1
),
-- Install a new checkpoint if best_match is not an exact match.
install_new as (
	insert into {{ Identifier $.TablePath }} (materialization, key_begin, key_end, fence, checkpoint)
		-- Case: best_match is a non-empty covering span but not an exact match
		select {{ Literal $.Materialization.String }}, {{ $.KeyBegin}}, {{ $.KeyEnd }}, fence, checkpoint
			from best_match where key_begin != {{ $.KeyBegin }} or key_end != {{ $.KeyEnd }}
		union all
		-- Case: best_match is empty
		select {{ Literal $.Materialization.String }}, {{ $.KeyBegin}}, {{ $.KeyEnd }}, {{ $.Fence }}, {{ Literal (Base64Std $.Checkpoint) }}
			where (select count(*) from best_match) = 0
	returning *
)
select fence, decode(checkpoint, 'base64') from install_new
union all
select fence, decode(checkpoint, 'base64') from best_match
limit 1
;
{{ end }}

{{ define "updateFence" }}
UPDATE {{ Identifier $.TablePath }}
	SET   checkpoint = {{ Literal (Base64Std $.Checkpoint) }}
	WHERE materialization = {{ Literal $.Materialization.String }}
	AND   key_begin = {{ $.KeyBegin }}
	AND   key_end   = {{ $.KeyEnd }}
	AND   fence     = {{ $.Fence }};
{{ end }}
`)
	tplTempTableName     = tplAll.Lookup("temp_load_name")
	tplTempTruncate      = tplAll.Lookup("truncateTempTable")
	tplCreateLoadTable   = tplAll.Lookup("createLoadTable")
	tplCreateUpdateTable = tplAll.Lookup("createUpdateTable")
	tplCreateTargetTable = tplAll.Lookup("createTargetTable")
	tplUpdateLoad        = tplAll.Lookup("updateLoad")
	tplUpdateReplace     = tplAll.Lookup("updateReplace")
	tplUpdateTruncate    = tplAll.Lookup("truncateUpdateTable")
	tplStoreLoad         = tplAll.Lookup("storeLoad")
	tplLoadQuery         = tplAll.Lookup("loadQuery")
	tplLoadLoad          = tplAll.Lookup("loadLoad")
	tplInstallFence      = tplAll.Lookup("installFence")
	tplUpdateFence       = tplAll.Lookup("updateFence")
)

const varcharTableAlter = "ALTER TABLE %s MODIFY COLUMN %s VARCHAR(%d);"
