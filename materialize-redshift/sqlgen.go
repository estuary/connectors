package main

import (
	"fmt"
	"slices"
	"strings"
	"text/template"
	"time"
	"unicode/utf8"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

var rsDialect = func(caseSensitiveIdentifierEnabled bool) sql.Dialect {
	textConverter := func(te tuple.TupleElement) (interface{}, error) {
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
	}

	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("BIGINT"),
				sql.MapStatic("NUMERIC(38,0)", sql.AlsoCompatibleWith("numeric")),
			),
			sql.NUMBER:   sql.MapStatic("DOUBLE PRECISION"),
			sql.BOOLEAN:  sql.MapStatic("BOOLEAN"),
			sql.OBJECT:   sql.MapStatic("SUPER", sql.UsingConverter(sql.ToJsonBytes)),
			sql.ARRAY:    sql.MapStatic("SUPER", sql.UsingConverter(sql.ToJsonBytes)),
			sql.BINARY:   sql.MapStatic("TEXT", sql.AlsoCompatibleWith("character varying")),
			sql.MULTIPLE: sql.MapStatic("SUPER", sql.UsingConverter(sql.ToJsonBytes)),
			sql.STRING_INTEGER: sql.MapStringMaxLen(
				sql.MapStatic("NUMERIC(38,0)", sql.AlsoCompatibleWith("numeric"), sql.UsingConverter(sql.StrToInt)),
				sql.MapStatic("TEXT", sql.AlsoCompatibleWith("character varying"), sql.UsingConverter(sql.ToStr)),
				38,
			),
			// NOTE(johnny): I can't find any documentation on Redshift Nan/Infinity/-Infinity handling.
			// There's some indication that others have resorted to mapping these to NULL:
			// https://stitch-docs.netlify.app/docs/data-structure/redshift-data-loading-behavior#new-table-scenarios
			sql.STRING_NUMBER: sql.MapStatic("DOUBLE PRECISION", sql.UsingConverter(sql.StrToFloat(nil, nil, nil))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("TEXT", sql.AlsoCompatibleWith("character varying"), sql.UsingConverter(textConverter)), // Note: Actually a VARCHAR(256)
				WithFormat: map[string]sql.MapProjectionFn{
					"date": sql.MapStatic("DATE"),
					"date-time": sql.MapStatic("TIMESTAMPTZ", sql.AlsoCompatibleWith("timestamp with time zone"), sql.UsingConverter(sql.StringCastConverter(func(s string) (any, error) {
						// Redshift supports timestamps with microsecond precision. It will reject
						// timestamps with higher precision than that, so we truncate anything
						// beyond microseconds.
						parsed, err := time.Parse(time.RFC3339Nano, s)
						if err != nil {
							return nil, fmt.Errorf("could not parse date-time value %q as time: %w", s, err)
						}

						return parsed.Truncate(time.Microsecond).Format(time.RFC3339Nano), nil
					}))),
					// "time" is not currently support due to limitations with loading time values from
					// staged JSON.
					// "time": sql.NewStaticMapper("TIMETZ"),
				},
				WithContentType: map[string]sql.MapProjectionFn{
					// The largest allowable size for a VARBYTE is 1,024,000 bytes. Our stored specs and
					// checkpoints can be quite long, so we need to use as large of column size as
					// possible for these tables.
					"application/x-protobuf; proto=flow.MaterializationSpec": sql.MapStatic("VARBYTE(1024000)"),
					"application/x-protobuf; proto=consumer.Checkpoint":      sql.MapStatic("VARBYTE(1024000)"),
				},
			}),
		},
		// NB: We are not using NOT NULL text so that all columns are created as nullable. This is
		// necessary because Redshift does not support dropping a NOT NULL constraint, so we need to
		// create columns as nullable to preserve the ability to change collection schema fields from
		// required to not required or remove fields from the materialization.
	)

	// Redshift lowercases all identifiers by default, unless the parameter
	// `enable_case_sensitive_identifier` is TRUE, which causes quoted identifiers preserve their
	// case.
	identifierTransform := func(in string) string {
		return truncatedIdentifier(strings.ToLower(in))
	}
	if caseSensitiveIdentifierEnabled {
		identifierTransform = func(in string) string {
			return truncatedIdentifier(in)
		}
	}

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"numeric":                {sql.NewMigrationSpec([]string{"double precision", "text"})},
			"bigint":                 {sql.NewMigrationSpec([]string{"double precision", "numeric(38,0)", "text"})},
			"double precision":       {sql.NewMigrationSpec([]string{"text"})},
			"date":                   {sql.NewMigrationSpec([]string{"text"})},
			"time without time zone": {sql.NewMigrationSpec([]string{"text"})},
			"timestamp with time zone": {
				sql.NewMigrationSpec([]string{"text"}, sql.WithCastSQL(datetimeToStringCast)),
				sql.NewMigrationSpec([]string{"super"}, sql.WithCastSQL(datetimeToSuperCast)),
			},
			"character varying": {sql.NewMigrationSpec([]string{"super"}, sql.WithCastSQL(jsonQuoteCast))},
			"*":                 {sql.NewMigrationSpec([]string{"super"}, sql.WithCastSQL(toJsonCast))},
		},
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			if len(path) == 1 {
				// A schema isn't required to be set on the endpoint or any resource, and if its
				// empty the default Redshift schema "public" will implicitly be used.
				return sql.InfoTableLocation{
					TableSchema: "public",
					TableName:   identifierTransform(path[0]),
				}
			} else {
				return sql.InfoTableLocation{
					TableSchema: identifierTransform(path[0]),
					TableName:   identifierTransform(path[1]),
				}
			}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string { return identifierTransform(schema) }),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string {
			return identifierTransform(field)
		}),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(REDSHIFT_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform(`"`, `""`),
			))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			// parameterIndex starts at 0, but postgres (and redshift, which is based on postgres)
			// parameters start at $1
			return fmt.Sprintf("$%d", index+1)
		}),
		// NB: We are not using sql.NullableMapper so that all columns are created as nullable. This is
		// necessary because Redshift does not support dropping a NOT NULL constraint, so we need to
		// create columns as nullable to preserve the ability to change collection schema fields from
		// required to not required or remove fields from the materialization.
		TypeMapper:             mapper,
		MaxColumnCharLength:    0, // Redshift automatically truncates column names that are too long
		CaseInsensitiveColumns: true,
	}
}

func datetimeToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`to_char(%s AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`, migration.Identifier)
}

func datetimeToSuperCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`CAST(%s as SUPER)`, datetimeToStringCast(migration))
}

func toJsonCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`CAST(%s as SUPER)`, migration.Identifier)
}

func jsonQuoteCast(migration sql.ColumnTypeMigration) string {
	// The escape character (\) or double quotes (") in the input must be
	// escaped so that the Redshift JSON parser doesn't try to interpret the
	// input as JSON, which it may not be. There's no reasonable way to try to
	// parse as JSON first and then escape if that doesn't work, which would be
	// nicer for migrating from stringified JSON to actual JSON. Also note that
	// CHR(92) is an escape (\), and it needs to be written in this weird way
	// since Redshift does not support C-style escape sequences in SQL strings.
	return fmt.Sprintf(`json_parse('"' || REPLACE(REPLACE(%s, CHR(92), CHR(92)||CHR(92)), '"', CHR(92)||'"') || '"')`, migration.Identifier)
}

type copyFromS3Params struct {
	Target                         string
	Columns                        []*sql.Column
	ManifestURL                    string
	Config                         config
	CaseSensitiveIdentifierEnabled bool
	TruncateColumns                bool
}

type loadTableParams struct {
	Target        sql.Table
	VarCharLength int
}

type templates struct {
	createTargetTable       *template.Template
	createLoadTable         *template.Template
	createStoreTable        *template.Template
	createDeleteTable       *template.Template
	mergeInto               *template.Template
	deleteQuery             *template.Template
	deleteQueryNoFlowDocument *template.Template
	loadQuery               *template.Template
	loadQueryNoFlowDocument *template.Template
	copyFromS3              *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
	var tplAll = sql.MustParseTemplate(dialect, "root", `
{{ define "temp_name" -}}
flow_temp_table_{{ $.Binding }}
{{- end }}

{{ define "temp_name_deleted" -}}
{{ template "temp_name" $ }}_deleted
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

{{ define "createDeleteTable" }}
CREATE TEMPORARY TABLE {{ template "temp_name_deleted" . }} (
{{- range $ind, $key := $.Keys }}
	{{- if $ind }},{{ end }}
  {{ $key.Identifier }} {{ $key.DDL }}
{{- end }}
);
{{ end }}

-- Templated query which updates an existing row in the target table. Redshift does not support
-- "WHEN MATCHED AND", so if/when deletion is implemented using a NULL document, a separate delete
-- statement will be needed in addition to this merge statement.

{{ define "mergeInto" }}
MERGE INTO {{ $.Identifier }}
USING {{ template "temp_name" . }} AS r
ON {{ range $ind, $key := $.Keys }}
{{- if $ind }} AND {{end -}}
	{{$.Identifier}}.{{$key.Identifier}} = r.{{$key.Identifier}}
{{- end}}
WHEN MATCHED THEN
	UPDATE SET {{ range $ind, $val := $.Values }}
	{{- if $ind }}, {{end -}}
		{{$val.Identifier}} = r.{{$val.Identifier}}
	{{- end}} 
	{{- if $.Document -}}
		{{ if $.Values  }}, {{ end }}{{$.Document.Identifier}} = r.{{$.Document.Identifier}}
	{{- end }}
WHEN NOT MATCHED THEN
	INSERT (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}}
	{{- end -}}
	)
	VALUES (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		r.{{$col.Identifier}}
	{{- end -}}
	);
{{ end }}

{{ define "deleteQuery" }}
{{ if $.Document -}}
DELETE FROM {{ $.Identifier }}
USING {{ template "temp_name_deleted" . }} AS r
WHERE {{ range $ind, $key := $.Keys }}
{{- if $ind }} AND {{end -}}
	{{$.Identifier}}.{{$key.Identifier}} = r.{{$key.Identifier}}
{{- end }}
{{- end }}
{{ end }}

-- Alternative delete query for no_flow_document feature flag - uses _meta/op for deletion detection

{{ define "deleteQueryNoFlowDocument" }}
{{- if $.MetaOpColumn }}
DELETE FROM {{ $.Identifier }}
USING {{ template "temp_name" . }} AS r
WHERE {{ range $ind, $key := $.Keys }}
{{- if $ind }} AND {{end -}}
	{{$.Identifier}}.{{$key.Identifier}} = r.{{$key.Identifier}}
{{- end }}
	AND r.{{ $.MetaOpColumn.Identifier }} = 'd'
{{- end }}
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

-- Templated query for no_flow_document feature flag - reconstructs JSON from root-level columns

{{ define "loadQueryNoFlowDocument" }}
SELECT {{ $.Binding }}, 
OBJECT(
{{- range $i, $col := $.RootLevelColumns}}
	{{- if $i}},{{end}}
	{{Literal $col.Field}}, r.{{$col.Identifier}}
{{- end}}
) as flow_document
FROM {{ template "temp_name" . }} AS l
JOIN {{ $.Identifier}} AS r
{{- range $ind, $key := $.Keys }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
		l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
{{- end }}
{{ end }}

-- Templated command to copy data from an S3 file into the destination table. Note the 'ignorecase'
-- JSON option: This is necessary since by default Redshift lowercases all identifiers.

{{ define "copyFromS3" }}
COPY {{ $.Target }}
FROM '{{ $.ManifestURL }}'
MANIFEST
CREDENTIALS 'aws_access_key_id={{ $.Config.AWSAccessKeyID }};aws_secret_access_key={{ $.Config.AWSSecretAccessKey }}'
REGION '{{ $.Config.Region }}'
{{ if $.CaseSensitiveIdentifierEnabled -}}
JSON 'auto'
{{- else -}}
JSON 'auto ignorecase'
{{- end }}
GZIP
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
{{- if $.TruncateColumns }}
TRUNCATECOLUMNS;
{{- else -}}
;
{{- end }}
{{ end }}
	`)
	return templates{
		createTargetTable:         tplAll.Lookup("createTargetTable"),
		createLoadTable:           tplAll.Lookup("createLoadTable"),
		createStoreTable:          tplAll.Lookup("createStoreTable"),
		createDeleteTable:         tplAll.Lookup("createDeleteTable"),
		mergeInto:                 tplAll.Lookup("mergeInto"),
		deleteQuery:               tplAll.Lookup("deleteQuery"),
		deleteQueryNoFlowDocument: tplAll.Lookup("deleteQueryNoFlowDocument"),
		loadQuery:                 tplAll.Lookup("loadQuery"),
		loadQueryNoFlowDocument:   tplAll.Lookup("loadQueryNoFlowDocument"),
		copyFromS3:                tplAll.Lookup("copyFromS3"),
	}
}

const varcharTableAlter = "ALTER TABLE %s ALTER COLUMN %s TYPE VARCHAR(MAX);"

// truncatedIdentifier produces a truncated form of an identifier, in accordance with Redshift's
// automatic truncation of identifiers that are over 127 bytes in length. For example, if a Flow
// collection or field name is over 127 bytes in length, Redshift will let a table/column be created
// with that as an identifier, but automatically truncates the identifier when interacting with it.
// This means we have to be aware of this possible truncation when querying the information_schema
// view.
func truncatedIdentifier(in string) string {
	maxByteLength := 127

	if len(in) <= maxByteLength {
		return in
	}

	bytes := []byte(in)
	for maxByteLength >= 0 && !utf8.Valid(bytes[:maxByteLength]) {
		// Don't mangle multi-byte characters; this seems to be consistent with Redshift truncation
		// as well.
		maxByteLength -= 1
	}

	return string(bytes[:maxByteLength])
}
