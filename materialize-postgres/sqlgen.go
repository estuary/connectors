package main

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	sql "github.com/estuary/connectors/materialize-sql-v2"
)

var pgDialect = func() sql.Dialect {
	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("BIGINT", sql.AlsoCompatibleWith("integer")),
				sql.MapStatic("NUMERIC"),
			),
			sql.NUMBER:         sql.MapStatic("DOUBLE PRECISION"),
			sql.BOOLEAN:        sql.MapStatic("BOOLEAN"),
			sql.OBJECT:         sql.MapStatic("JSON"),
			sql.ARRAY:          sql.MapStatic("JSON"),
			sql.BINARY:         sql.MapStatic("TEXT", sql.AlsoCompatibleWith("character varying")),
			sql.MULTIPLE:       sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.STRING_INTEGER: sql.MapStatic("NUMERIC"),
			sql.STRING_NUMBER:  sql.MapStatic("DECIMAL", sql.AlsoCompatibleWith("numeric")),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic(
					"TEXT",
					sql.AlsoCompatibleWith("character varying"),
					sql.UsingConverter(sql.StringCastConverter(func(in string) (any, error) {
						// Postgres doesn't allow fields with null bytes, so they must be stripped out if
						// present.
						return strings.ReplaceAll(in, "\u0000", ""), nil
					})),
				),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      sql.MapStatic("DATE", sql.UsingConverter(sql.ClampDate)),
					"date-time": sql.MapStatic("TIMESTAMPTZ", sql.AlsoCompatibleWith("timestamp with time zone"), sql.UsingConverter(sql.ClampDatetime)),
					"duration":  sql.MapStatic("INTERVAL"),
					"ipv4":      sql.MapStatic("CIDR"),
					"ipv6":      sql.MapStatic("CIDR"),
					"macaddr":   sql.MapStatic("MACADDR"),
					"macaddr8":  sql.MapStatic("MACADDR8"),
					"time":      sql.MapStatic("TIME", sql.AlsoCompatibleWith("time without time zone")),
					// UUID format was added on 30-Sept-2024, and pre-existing
					// text type of columns are allowed to validate for
					// compatibility with pre-existing columns.
					"uuid": sql.MapStatic("UUID", sql.AlsoCompatibleWith("text", "character varying")),
				},
			}),
		},
		sql.WithNotNullText("NOT NULL"),
	)

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"numeric":                  {sql.NewMigrationSpec([]string{"double precision", "text"})},
			"bigint":                   {sql.NewMigrationSpec([]string{"double precision", "numeric", "text"})},
			"double precision":         {sql.NewMigrationSpec([]string{"text"})},
			"date":                     {sql.NewMigrationSpec([]string{"text"})},
			"time without time zone":   {sql.NewMigrationSpec([]string{"text"})},
			"timestamp with time zone": {sql.NewMigrationSpec([]string{"text"}, sql.WithCastSQL(datetimeToStringCast))},
			"*":                        {sql.NewMigrationSpec([]string{"json"}, sql.WithCastSQL(toJsonCast))},
		},
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			if len(path) == 1 {
				// A schema isn't required to be set on the endpoint or any resource, and if its empty the
				// default postgres schema "public" will implicitly be used.
				return sql.InfoTableLocation{TableSchema: "public", TableName: truncatedIdentifier(path[0])}
			} else {
				return sql.InfoTableLocation{TableSchema: truncatedIdentifier(path[0]), TableName: truncatedIdentifier(path[1])}
			}
		}),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return truncatedIdentifier(field) }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(PG_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform(`"`, `""`),
			))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			// parameterIndex starts at 0, but postgres parameters start at $1
			return fmt.Sprintf("$%d", index+1)
		}),
		TypeMapper:             mapper,
		MaxColumnCharLength:    0, // Postgres automatically truncates column names that are too long
		CaseInsensitiveColumns: false,
	}
}()

type loadTableKey struct {
	Identifier string
	DDL        string
}

type loadTableColumns struct {
	Binding int
	Keys    []loadTableKey
}

func datetimeToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`to_char(%s AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`, migration.Identifier)
}

func toJsonCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`to_json(%s)`, migration.Identifier)
}

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
	ALTER COLUMN {{ ColumnIdentifier $col.Name }} DROP NOT NULL
{{- end }};
{{ end }}

-- Templated creation of a temporary load table:

{{ define "createLoadTable" }}
CREATE TEMPORARY TABLE {{ template "temp_name" . }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{ $key.Identifier }} {{ $key.DDL }}
	{{- end }}
) ON COMMIT DELETE ROWS;
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
);
{{ end }}

-- Templated query which joins keys from the load table with the target table, and returns values. It
-- deliberately skips the trailing semi-colon as these queries are composed with a UNION ALL.

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

-- Templated query which inserts a new, complete row to the target table:

{{ define "storeInsert" }}
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

-- Templated query which updates an existing row in the target table:

{{ define "storeUpdate" }}
UPDATE {{$.Identifier}} SET
	{{- range $ind, $val := $.Values }}
		{{- if $ind }},{{ end }}
		{{ $val.Identifier}} = {{ $val.Placeholder }}
	{{- end }}
	{{- if $.Document -}}
		{{ if $.Values }},{{ end }}
		{{ $.Document.Identifier }} = {{ $.Document.Placeholder }}
	{{- end -}}
	{{ range $ind, $key := $.Keys }}
	{{ if $ind }} AND   {{ else }} WHERE {{ end -}}
	{{ $key.Identifier }} = {{ $key.Placeholder }}
	{{- end -}}
	;
{{ end }}

{{ define "deleteQuery" }}
DELETE FROM {{$.Identifier}} WHERE
{{ range $ind, $key := $.Keys -}}
	{{- if $ind }} AND {{ end -}}
	{{ $key.Identifier }} = {{ $key.Placeholder }}
{{- end -}}
	;
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
	tplCreateLoadTable   = tplAll.Lookup("createLoadTable")
	tplCreateTargetTable = tplAll.Lookup("createTargetTable")
	tplAlterTableColumns = tplAll.Lookup("alterTableColumns")
	tplLoadInsert        = tplAll.Lookup("loadInsert")
	tplStoreInsert       = tplAll.Lookup("storeInsert")
	tplStoreUpdate       = tplAll.Lookup("storeUpdate")
	tplDeleteQuery       = tplAll.Lookup("deleteQuery")
	tplLoadQuery         = tplAll.Lookup("loadQuery")
	tplInstallFence      = tplAll.Lookup("installFence")
	tplUpdateFence       = tplAll.Lookup("updateFence")
)

// truncatedIdentifier produces a truncated form of an identifier, in accordance with Postgres'
// automatic truncation of identifiers that are over 63 bytes in length. For example, if a Flow
// collection or field name is over 63 bytes in length, Postgres will let a table/column be created
// with that as an identifier, but automatically truncates the identifier when interacting with it.
// This means we have to be aware of this possible truncation when querying the information_schema
// view.
func truncatedIdentifier(in string) string {
	maxByteLength := 63

	if len(in) <= maxByteLength {
		return in
	}

	bytes := []byte(in)
	for maxByteLength >= 0 && !utf8.Valid(bytes[:maxByteLength]) {
		// Don't mangle multi-byte characters; this seems to be consistent with Postgres truncation
		// as well.
		maxByteLength -= 1
	}

	return string(bytes[:maxByteLength])
}
