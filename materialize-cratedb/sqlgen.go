package main

import (
	"encoding/json"
	"fmt"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"slices"
	"strings"
)

var jsonConverter sql.ElementConverter = func(te tuple.TupleElement) (interface{}, error) {
	switch ii := te.(type) {
	case []byte:
		return string(ii), nil
	case json.RawMessage:
		return string(ii), nil
	case nil:
		return string(json.RawMessage(nil)), nil
	default:
		return nil, fmt.Errorf("invalid type %#v for variant", te)
	}
}
var crateDialect = func() sql.Dialect {
	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("BIGINT", sql.AlsoCompatibleWith("integer")),
				sql.MapStatic("NUMERIC(20)", sql.AlsoCompatibleWith("numeric")),
			),
			sql.NUMBER:         sql.MapStatic("DOUBLE PRECISION"),
			sql.BOOLEAN:        sql.MapStatic("BOOLEAN"),
			sql.OBJECT:         sql.MapStatic("OBJECT"),
			sql.ARRAY:          sql.MapStatic("TEXT", sql.UsingConverter(jsonConverter)),
			sql.BINARY:         sql.MapStatic("TEXT", sql.AlsoCompatibleWith("character varying")),
			sql.MULTIPLE:       sql.MapStatic("OBJECT", sql.UsingConverter(sql.ToJsonBytes)),
			sql.STRING_INTEGER: sql.MapStatic("NUMERIC(18, 0)", sql.AlsoCompatibleWith("numeric"), sql.AlsoCompatibleWith("integer")),
			sql.STRING_NUMBER:  sql.MapStatic("DOUBLE PRECISION", sql.AlsoCompatibleWith("numeric"), sql.AlsoCompatibleWith("integer")),
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
					"date":      sql.MapStatic("TIMESTAMP WITH TIME ZONE", sql.UsingConverter(sql.ClampDate)),
					"date-time": sql.MapStatic("TIMESTAMP WITH TIME ZONE", sql.AlsoCompatibleWith("timestamp with time zone"), sql.UsingConverter(sql.ClampDatetime)),
					"duration":  sql.MapStatic("INTEGER"),
					"ipv4":      sql.MapStatic("IP"),
					"ipv6":      sql.MapStatic("IP"),
					"macaddr":   sql.MapStatic("TEXT"),
					"macaddr8":  sql.MapStatic("TEXT"),
					"time":      sql.MapStatic("TEXT"),
					// UUID format was added on 30-Sept-2024, and pre-existing
					// text type of columns are allowed to validate for
					// compatibility with pre-existing columns.
					"uuid": sql.MapStatic("TEXT"),
				},
			}),
		},
		//sql.WithNotNullText("NOT NULL"), CrateDB does not support dropping NOT NULL columns.
	)

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"numeric":                  {sql.NewMigrationSpec([]string{"text"})},
			"integer":                  {sql.NewMigrationSpec([]string{"text"})},
			"double precision":         {sql.NewMigrationSpec([]string{"text"})},
			"date":                     {sql.NewMigrationSpec([]string{"text"})},
			"time without time zone":   {sql.NewMigrationSpec([]string{"text"})},
			"timestamp with time zone": {sql.NewMigrationSpec([]string{"text"}, sql.WithCastSQL(datetimeToStringCast))},
			"*":                        {sql.NewMigrationSpec([]string{"OBJECT"}, sql.WithCastSQL(toJsonCast))},
		},
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			if len(path) == 1 {
				// A schema isn't required to be set on the endpoint or any resource, and if its empty the
				// default postgres schema "public" will implicitly be used.
				return sql.InfoTableLocation{TableSchema: "doc", TableName: normalizeColumn(path[0])}
			} else {
				return sql.InfoTableLocation{TableSchema: path[0], TableName: normalizeColumn(path[1])}
			}
		}),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return normalizeColumn(field) }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			identifierSanitizer(sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(PG_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform(`"`, `""`),
			)))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			// parameterIndex starts at 0, but postgres parameters start at $1
			return fmt.Sprintf("$%d", index+1)
		}),
		TypeMapper:             mapper,
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
	tplAll = sql.MustParseTemplate(crateDialect, "root", `
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
{{ end }}

-- Templated creation of a temporary load table:

{{ define "createLoadTable" }}
CREATE TABLE {{ template "temp_name" . }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{ $key.Identifier }} {{ $key.DDL }}
	{{- end }}
);
{{ end }}

-- Templated deletion of a temporary load table:
{{ define "dropLoadTable" }}
DROP TABLE IF EXISTS {{ template "temp_name" . }};
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
UPDATE {{ Identifier $.TablePath }}
	SET   checkpoint = {{ Literal (Base64Std $.Checkpoint) }}
	WHERE materialization = {{ Literal $.Materialization.String }}
	AND   key_begin = {{ $.KeyBegin }}
	AND   key_end   = {{ $.KeyEnd }}
	AND   fence     = {{ $.Fence }};
{{ end }}
`)
	tplDropLoadTable     = tplAll.Lookup("dropLoadTable")
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

// extendPrefixBy extends a `prefix` by `by` units if the `in` string starts with `prefix`.
// Examples:
//
//	>>> extendPrefixBy("_mystring", "_", 1)
//	'__mystring'
//	>>> extendPrefixBy("sometring", "some", 2)
//	'somesomesomestring'
func extendPrefixBy(in string, prefix string, by int) string {
	if strings.HasPrefix(in, prefix) {
		return strings.Repeat(prefix, by) + in
	}
	return in
}

// normalizeColumn Returns a normalized column name valid in CrateDB.
// Current normalizations applied:
// underscore: columns that start with an underscore are added an extra underscore: https://github.com/crate/cratedb-estuary/issues/13
func normalizeColumn(column string) string {
	column = extendPrefixBy(column, "_", 1)
	// Apply more normalizations as needed here.
	return column
}

// Delegated function to apply normalizeColumn to sql.Dialect.Identifierer
func identifierSanitizer(delegate func(string) string) func(string) string {
	return func(text string) string {
		return delegate(normalizeColumn(text))
	}
}
