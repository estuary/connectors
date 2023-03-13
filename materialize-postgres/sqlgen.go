package main

import (
	"fmt"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
)

var pgDialect = func() sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.INTEGER: sql.NewStaticMapper("BIGINT", sql.WithElementConverter(sql.StdStrToInt())),
		sql.NUMBER:  sql.NewStaticMapper("DOUBLE PRECISION", sql.WithElementConverter(sql.StdStrToFloat())),
		sql.BOOLEAN: sql.NewStaticMapper("BOOLEAN"),
		sql.OBJECT:  sql.NewStaticMapper("JSON"),
		sql.ARRAY:   sql.NewStaticMapper("JSON"),
		sql.BINARY:  sql.NewStaticMapper("BYTEA"),
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.NewStaticMapper("TEXT"),
			WithFormat: map[string]sql.TypeMapper{
				"date":      sql.NewStaticMapper("DATE"),
				"date-time": sql.NewStaticMapper("TIMESTAMPTZ"),
				"duration":  sql.NewStaticMapper("INTERVAL"),
				"ipv4":      sql.NewStaticMapper("CIDR"),
				"ipv6":      sql.NewStaticMapper("CIDR"),
				"macaddr":   sql.NewStaticMapper("MACADDR"),
				"macaddr8":  sql.NewStaticMapper("MACADDR8"),
				"time":      sql.NewStaticMapper("TIME"),
			},
		},
	}
	mapper = sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    mapper,
	}

	return sql.Dialect{
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !sql.SliceContains(strings.ToLower(s), PG_RESERVED_WORDS)
				},
				sql.QuoteTransform("\"", "\\\""),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			// parameterIndex starts at 0, but postgres parameters start at $1
			return fmt.Sprintf("$%d", index+1)
		}),
		TypeMapper: mapper,
	}
}()

var (
	tplAll = sql.MustParseTemplate(pgDialect, "root", `
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
);

COMMENT ON TABLE {{$.Identifier}} IS {{Literal $.Comment}};
{{- range $col := .Columns }}
COMMENT ON COLUMN {{$.Identifier}}.{{$col.Identifier}} IS {{Literal $col.Comment}};
{{- end}}
{{ end }}

-- Alter column and mark it as nullable

{{ define "alterColumnNullable" }}
ALTER TABLE {{ $.Table.Identifier }} ALTER COLUMN {{ $.Identifier }} DROP NOT NULL;
{{ end }}

-- Alter table and add a new column

{{ define "alterTableAddColumn" }}
ALTER TABLE {{ $.Table.Identifier }} ADD COLUMN
	{{ range $ind, $col := $.Table.Columns -}}
		{{- if (eq $col.Identifier $.Identifier) -}}
			{{ $col.Identifier }} {{ $col.DDL }}
		{{- end -}}
	{{- end }};
{{ end }}

-- Templated creation of a temporary load table:

{{ define "createLoadTable" }}
CREATE TEMPORARY TABLE {{ template "temp_name" . }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{ $key.Identifier }} {{ $key.DDL }}
	{{- end }}
) ON COMMIT DROP;
{{ end }}

-- Templated insertion into the temporary load table:

{{ define "prepLoadInsert" }}
PREPARE load_{{ $.Binding }} AS
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
);
{{ end }}

{{ define "execLoadInsert" -}}
EXECUTE load_{{ $.Binding }}
{{- end }}

-- Templated query which joins keys from the load table with the target table, and returns values. It
-- deliberately skips the trailing semi-colon as these queries are composed with a UNION ALL. If the
-- column is not nullable, an efficient direct comparison can be made in the join condition. For
-- columns that may contain null, a less efficient comparison must be used. For this we use a more
-- cumbersome comparison operator than "is not distinct from" so that the table btree index will be
-- utilized.

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }}, r.{{$.Document.Identifier}}
	FROM {{ template "temp_name" . }} AS l
	JOIN {{ $.Identifier}} AS r
	{{- range $ind, $key := $.Keys }}
		{{ if $ind }} AND {{ else }} ON  {{ end -}}
		{{ if $key.MustExist -}}
			l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
		{{- else -}}
			(l.{{ $key.Identifier }} = r.{{ $key.Identifier }} and l.{{ $key.Identifier }} is not null and r.{{ $key.Identifier }} is not null) or (l.{{ $key.Identifier }} is null and r.{{ $key.Identifier }} is null)
		{{- end }}
	{{- end }}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc
{{ end }}
{{ end }}

-- Templated query which inserts a new, complete row to the target table:

{{ define "prepStoreInsert" }}
PREPARE insert_{{ $.Binding }} AS
INSERT INTO {{ $.Identifier }} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
) VALUES (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind}}, {{ end -}}
		{{ $col.Placeholder }}
	{{- end -}}
);
{{ end }}

{{ define "execStoreInsert" -}}
EXECUTE insert_{{ $.Binding }}
{{- end }}

-- Templated query which updates an existing row in the target table:

{{ define "prepStoreUpdate" }}
PREPARE update_{{ $.Binding }} AS
UPDATE {{$.Identifier}} SET
	{{- range $ind, $val := $.Values }}
		{{- if $ind }},{{ end }}
		{{ $val.Identifier}} = {{ $val.Placeholder }}
	{{- end }}
	{{- if $.Document }},
		{{ $.Document.Identifier }} = {{ $.Document.Placeholder }}
	{{- end -}}
	{{ range $ind, $key := $.Keys }}
	{{ if $ind }} AND   {{ else }} WHERE {{ end -}}
	{{ if $key.MustExist -}}
		{{ $key.Identifier }} = {{ $key.Placeholder }}
	{{- else -}}
		({{ $key.Identifier }} = {{ $key.Placeholder }} and {{ $key.Identifier }} is not null and {{ $key.Placeholder }} is not null) or ({{ $key.Identifier }} is null and {{ $key.Placeholder }} is null)
	{{- end }}
	{{- end -}}
	;
{{ end }}

{{ define "execStoreUpdate" -}}
EXECUTE update_{{ $.Binding }}
{{- end }}

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
DO $$
BEGIN
	UPDATE {{ Identifier $.TablePath }}
		SET   checkpoint = {{ Literal (Base64Std $.Checkpoint) }}
		WHERE materialization = {{ Literal $.Materialization.String }}
		AND   key_begin = {{ $.KeyBegin }}
		AND   key_end   = {{ $.KeyEnd }}
		AND   fence     = {{ $.Fence }};

	IF NOT FOUND THEN
		RAISE 'This instance was fenced off by another';
	END IF;
END $$;
{{ end }}
`)
	tplCreateLoadTable     = tplAll.Lookup("createLoadTable")
	tplCreateTargetTable   = tplAll.Lookup("createTargetTable")
	tplExecLoadInsert      = tplAll.Lookup("execLoadInsert")
	tplExecStoreInsert     = tplAll.Lookup("execStoreInsert")
	tplExecStoreUpdate     = tplAll.Lookup("execStoreUpdate")
	tplLoadQuery           = tplAll.Lookup("loadQuery")
	tplPrepLoadInsert      = tplAll.Lookup("prepLoadInsert")
	tplPrepStoreInsert     = tplAll.Lookup("prepStoreInsert")
	tplPrepStoreUpdate     = tplAll.Lookup("prepStoreUpdate")
	tplInstallFence        = tplAll.Lookup("installFence")
	tplUpdateFence         = tplAll.Lookup("updateFence")
	tplAlterColumnNullable = tplAll.Lookup("alterColumnNullable")
	tplAlterTableAddColumn = tplAll.Lookup("alterTableAddColumn")
)
