package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/estuary/connectors/go/pkg/slices"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

// Identifiers matching the this pattern do not need to be quoted. See
// https://docs.aws.amazon.com/redshift/latest/dg/r_names.html. Identifiers (table names and column
// names) are case-insensitive, even if they are quoted. They are always converted to lowercase by
// default.
var simpleIdentifierRegexp = regexp.MustCompile(`(?i)^[a-z_][a-z0-9_]*$`)

var rsDialect = func() sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.INTEGER:  sql.NewStaticMapper("BIGINT", sql.WithElementConverter(sql.StdStrToInt())),
		sql.NUMBER:   sql.NewStaticMapper("DOUBLE PRECISION", sql.WithElementConverter(sql.StdStrToFloat())),
		sql.BOOLEAN:  sql.NewStaticMapper("BOOLEAN"),
		sql.OBJECT:   sql.NewStaticMapper("SUPER", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.ARRAY:    sql.NewStaticMapper("SUPER", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.BINARY:   sql.NewStaticMapper("VARBYTE"),
		sql.MULTIPLE: sql.NewStaticMapper("SUPER", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.NewStaticMapper("TEXT", sql.WithElementConverter( // Note: Actually a VARCHAR(256)
				func(te tuple.TupleElement) (interface{}, error) {
					if s, ok := te.(string); ok {
						// Redshift will terminate values going into VARCHAR columns where the null
						// escape sequence occurs. This is especially a problem when it is the first
						// thing in the string and is followed by other characters, as this results
						// in the load error "Invalid null byte - field longer than 1 byte". Adding
						// an additional escape to the beginning of the string allows the value to
						// be loaded with the initial "\u0000" as a regular string
						if strings.HasPrefix(s, "\u0000") {
							return `\` + s, nil
						}
					}

					return te, nil
				})),
			WithFormat: map[string]sql.TypeMapper{
				"date": sql.NewStaticMapper("DATE"),
				"date-time": sql.NewStaticMapper("TIMESTAMPTZ", sql.WithElementConverter(
					func(te tuple.TupleElement) (interface{}, error) {
						// Redshift supports timestamps with microsecond precision. It will reject
						// timestamps with higher precision than that, so we truncate anything
						// beyond microseconds.
						if s, ok := te.(string); ok {
							parsed, err := time.Parse(time.RFC3339Nano, s)
							if err != nil {
								return nil, fmt.Errorf("could not parse date-time value %q as time: %w", s, err)
							}

							return parsed.Truncate(time.Microsecond).Format(time.RFC3339Nano), nil
						}

						return te, nil
					})),
				// "time" is not currently support due to limitations with loading time values from
				// staged JSON.
				// "time": sql.NewStaticMapper("TIMETZ"),
			},
			WithContentType: map[string]sql.TypeMapper{
				// The largest allowable size for a VARBYTE is 1,024,000 bytes. Our stored specs and
				// checkpoints can be quite long, so we need to use as large of column size as
				// possible for these tables.
				"application/x-protobuf; proto=flow.MaterializationSpec": sql.NewStaticMapper("VARBYTE(1024000)"),
				"application/x-protobuf; proto=consumer.Checkpoint":      sql.NewStaticMapper("VARBYTE(1024000)"),
			},
		},
	}

	// NB: We are not using sql.NullableMapper so that all columns are created as nullable. This is
	// necessary because Redshift does not support dropping a NOT NULL constraint, so we need to
	// create columns as nullable to preserve the ability to change collection schema fields from
	// required to not required or remove fields from the materialization.

	return sql.Dialect{
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return simpleIdentifierRegexp.MatchString(s) && !slices.Contains(REDSHIFT_RESERVED_WORDS, strings.ToLower(s))
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
	Destination     string
	ObjectLocation  string
	Config          config
	TruncateColumns bool
}

type loadTableParams struct {
	Target        sql.Table
	VarCharLength int
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

-- Idempotent creation of the load table for staging load keys.

{{ define "createLoadTable" }}
CREATE TEMPORARY TABLE {{ template "temp_name" $.Target }} (
{{- range $ind, $key := $.Target.Keys }}
	{{- if $ind }},{{ end }}
	{{ $key.Identifier }} {{ if and (eq $key.DDL "TEXT") (not (eq $.VarCharLength 0)) -}}
		VARCHAR({{ $.VarCharLength }})
	{{- else }}
		{{- $key.DDL }}
	{{- end }}
{{- end }}
);
{{ end }}

-- Idempotent creation of the store table for staging new records.

{{ define "createStoreTable" }}
CREATE TEMPORARY TABLE {{ template "temp_name" . }} (
	LIKE {{$.Identifier}}
);
{{ end }}

-- The load and store tables will be truncated on each transaction round.

{{ define "truncateTempTable" }}
TRUNCATE {{ template "temp_name" . }};
{{ end }}

-- Merging data from a source table to a target table in redshift is accomplished
-- by first deleting common records from the target table then copying all records
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

-- Templated command to copy data from an S3 file into the destination table. Note the 'ignorecase'
-- JSON option: This is necessary since by default Redshift lowercases all identifiers.

{{ define "copyFromS3" }}
COPY {{ $.Destination }}
FROM '{{ $.ObjectLocation }}'
CREDENTIALS 'aws_access_key_id={{ $.Config.AWSAccessKeyID }};aws_secret_access_key={{ $.Config.AWSSecretAccessKey }}'
REGION '{{ $.Config.Region }}'
JSON 'auto ignorecase'
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
{{- if $.TruncateColumns }}
TRUNCATECOLUMNS;
{{- else -}}
;
{{- end }}
{{ end }}
`)
	tplCreateTargetTable         = tplAll.Lookup("createTargetTable")
	tplCreateLoadTable           = tplAll.Lookup("createLoadTable")
	tplCreateStoreTable          = tplAll.Lookup("createStoreTable")
	tplStoreUpdateDeleteExisting = tplAll.Lookup("storeUpdateDeleteExisting")
	tplStoreUpdate               = tplAll.Lookup("storeUpdate")
	tplLoadQuery                 = tplAll.Lookup("loadQuery")
	tplUpdateFence               = tplAll.Lookup("updateFence")
	tplCopyFromS3                = tplAll.Lookup("copyFromS3")
)

const varcharTableAlter = "ALTER TABLE %s ALTER COLUMN %s TYPE VARCHAR(MAX);"
