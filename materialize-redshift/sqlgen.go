package main

import (
	"fmt"
	"regexp"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
)

// Identifiers matching the this pattern do not need to be quoted. See
// https://docs.aws.amazon.com/redshift/latest/dg/r_names.html.
// Identifiers are case-insensitive, even if they are quoted. There is a cluster setting to make them case-sensitive.
var simpleIdentifierRegexp = regexp.MustCompile(`(?i)^[a-z_][a-z0-9_]*$`)

var rsDialect = func() sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.INTEGER: sql.NewStaticMapper("BIGINT", sql.WithElementConverter(sql.StdStrToInt())),
		sql.NUMBER:  sql.NewStaticMapper("DOUBLE PRECISION", sql.WithElementConverter(sql.StdStrToFloat())),
		sql.BOOLEAN: sql.NewStaticMapper("BOOLEAN"),
		sql.OBJECT:  sql.NewStaticMapper("SUPER", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.ARRAY:   sql.NewStaticMapper("SUPER", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.BINARY:  sql.NewStaticMapper("VARBYTE"),
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.MaxLengthMapper{
				WithLength: sql.NewStaticMapper("VARCHAR(%d)"),
				// The Redshift TEXT type is currently equivalent to VARCHAR(256). 256 is pretty
				// long, but probably not long enough to handle everything. Using the
				// MaxLengthMapper allows for longer strings to be handled by setting the maxLength
				// constraint for the desired string fields in the JSON schema for the materialized
				// collection.
				Fallback: sql.NewStaticMapper("TEXT"),
			},
			WithFormat: map[string]sql.TypeMapper{
				"date":      sql.NewStaticMapper("DATE"),
				"date-time": sql.NewStaticMapper("TIMESTAMPTZ"),
				"time":      sql.NewStaticMapper("TIMETZ"),
			},
		},
	}

	// NB: We are not using sql.NullableMapper so that all columns are created as nullable. This is
	// necessary because Redshift does not support dropping a NOT NULL constraint, so we need to
	// create columns as nullable to preserve the ability to change collection schema fields from
	// required to not required.

	return sql.Dialect{
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return simpleIdentifierRegexp.MatchString(s) && !sql.SliceContains(strings.ToLower(s), REDSHIFT_RESERVED_WORDS)
				},
				sql.QuoteTransform("\"", "\"\""),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			// parameterIndex starts at 0, but postgres (and redshift, which is based on postgres)
			// parameters start at $1
			return fmt.Sprintf("$%d", index+1)
		}),
		TypeMapper: mapper,
	}
}()

type copyFromS3Params struct {
	Destination    string
	ObjectLocation string
	Config         config
}

var (
	tplAll = sql.MustParseTemplate(rsDialect, "root", `
{{ define "temp_name" -}}
flow_temp_table_{{ $.Binding }}
{{- end }}

-- Templated creation of a materialized table definition and comments.
-- DISTSTYLE and SORTKEY are omitted to allow for automatic optimization by Redshift.
-- Note that Redshift does not support primary keys or unique constraints.

{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
{{- range $ind, $col := $.Columns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.DDL}}
{{- end }}
);

COMMENT ON TABLE {{$.Identifier}} IS {{Literal $.Comment}};
{{- range $col := .Columns }}
COMMENT ON COLUMN {{$.Identifier}}.{{$col.Identifier}} IS {{Literal $col.Comment}};
{{- end}}
{{ end }}

-- Redshift does not support dropping NOT NULL constraints. Instead, Redshift columns are always
-- created as nullable and alterColumnNullable is a noop.

{{ define "alterColumnNullable" }}
SELECT NULL LIMIT 0;
{{ end }}

-- Idempotent creation of the load table for staging load keys.

{{ define "createLoadTable" }}
CREATE TEMPORARY TABLE IF NOT EXISTS {{ template "temp_name" . }} (
{{- range $ind, $key := $.Keys }}
	{{- if $ind }},{{ end }}
	{{ $key.Identifier }} {{ $key.DDL }}
{{- end }}
);
{{ end }}

-- Idempotent creation of the store table for staging new records.

{{ define "createStoreTable" }}
CREATE TEMPORARY TABLE IF NOT EXISTS {{ template "temp_name" . }} (
	LIKE {{$.Identifier}}
);
{{ end }}

-- The load and store tables will be truncated on each transaction round.

{{ define "truncateTempTable" }}
TRUNCATE {{ template "temp_name" . }};
{{ end }}

-- Merging data from a source table to a target table in redshift is accomplish
-- by first deleting comming records from the target table then copying all records
-- from the source into the target.

{{ define "storeUpdateDeleteExisting" }}
DELETE FROM {{$.Identifier}}
USING {{ template "temp_name" . }}
WHERE {{ range $ind, $key := $.Keys }}
{{- if $ind }} AND {{end -}}
	{{$.Identifier}}.{{$key.Identifier}} = {{ template "temp_name" $ }}.{{$key.Identifier}}
{{- end}};
{{ end }}

{{ define "storeUpdate" }}
INSERT INTO {{$.Identifier}}  
SELECT * FROM {{ template "temp_name" . }};
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
{{- else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS SUPER) LIMIT 0) as nodoc
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

{{ define "copyFromS3" }}
COPY {{ $.Destination }}
FROM '{{ $.ObjectLocation }}'
CREDENTIALS 'aws_access_key_id={{ $.Config.AWSAccessKeyID }};aws_secret_access_key={{ $.Config.AWSSecretAccessKey }}'
REGION '{{ $.Config.Region }}'
json 'auto';
{{ end }}
`)
	tplCreateTargetTable         = tplAll.Lookup("createTargetTable")
	tplCreateLoadTable           = tplAll.Lookup("createLoadTable")
	tplCreateStoreTable          = tplAll.Lookup("createStoreTable")
	tplTruncateTempTable         = tplAll.Lookup("truncateTempTable")
	tplStoreUpdateDeleteExisting = tplAll.Lookup("storeUpdateDeleteExisting")
	tplStoreUpdate               = tplAll.Lookup("storeUpdate")
	tplLoadQuery                 = tplAll.Lookup("loadQuery")
	tplAlterColumnNullable       = tplAll.Lookup("alterColumnNullable")
	tplUpdateFence               = tplAll.Lookup("updateFence")
	tplCopyFromS3                = tplAll.Lookup("copyFromS3")
)
