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

func createRsDialect(caseSensitiveIdentifierEnabled bool, featureFlags map[string]bool) sql.Dialect {
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

	primaryKeyTextType := sql.MapStatic("TEXT", sql.AlsoCompatibleWith("character varying"), sql.UsingConverter(textConverter))

	// Define base date/time mappings without primary key wrapper
	dateMapping := sql.MapStatic("DATE", sql.UsingConverter(sql.NormalizeDatetimeString))
	datetimeMapping := sql.MapStatic("TIMESTAMPTZ", sql.AlsoCompatibleWith("timestamp with time zone"), sql.UsingConverter(sql.StringCastConverter(func(s string) (any, error) {
		// Redshift supports timestamps with microsecond precision. It will reject
		// timestamps with higher precision than that, so we truncate anything
		// beyond microseconds.
		s = strings.Replace(s, "z", "Z", 1)
		parsed, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, fmt.Errorf("could not parse date-time value %q as time: %w", s, err)
		}

		return parsed.Truncate(time.Microsecond).Format(time.RFC3339Nano), nil
	})))

	// If feature flag is enabled, wrap with MapPrimaryKey to use string types for primary keys
	if featureFlags["datetime_keys_as_string"] {
		dateMapping = sql.MapPrimaryKey(primaryKeyTextType, dateMapping)
		datetimeMapping = sql.MapPrimaryKey(primaryKeyTextType, datetimeMapping)
	}

	binaryMapping := sql.MapStatic("TEXT", sql.AlsoCompatibleWith("character varying"))
	if featureFlags["native_binary_column_type"] {
		// VARBYTE's maximum size is 1,024,000 bytes. The element converter
		// re-encodes base64 input as hex so the staged JSON files contain
		// hex strings, which Redshift's COPY parses natively into VARBYTE
		// (the default decoding for VARBYTE-from-JSON is hex). Staging temp
		// tables also declare VARBYTE columns directly, so COPY decodes hex
		// straight into bytes; this avoids the 65,535-byte VARCHAR(MAX) cap
		// that would otherwise truncate binary values larger than ~32KB on
		// the merge path.
		// Redshift's information_schema.columns.data_type reports VARBYTE as
		// "binary varying", so the compatibility list must include that
		// spelling for re-applies to recognize an existing VARBYTE column.
		binaryMapping = sql.MapStatic("VARBYTE(1024000)", sql.AlsoCompatibleWith("varbyte", "binary varying"), sql.UsingConverter(sql.Base64ToHex))
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
			sql.BINARY:   binaryMapping,
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
				Fallback: primaryKeyTextType, // Note: Actually a VARCHAR(256)
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      dateMapping,
					"date-time": datetimeMapping,
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
			"character varying": {
				sql.NewMigrationSpec([]string{"super"}, sql.WithCastSQL(jsonQuoteCast)),
				sql.NewMigrationSpec([]string{"varbyte(1024000)"}, sql.WithCastSQL(stringToVarbyteCast)),
			},
			"binary varying": {
				sql.NewMigrationSpec([]string{"text"}, sql.WithCastSQL(varbyteToStringCast)),
				sql.NewMigrationSpec([]string{"super"}, sql.WithCastSQL(varbyteToSuperCast)),
			},
			"*": {sql.NewMigrationSpec([]string{"super"}, sql.WithCastSQL(toJsonCast))},
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
		// Redshift treats backslashes as escape characters in single-quoted
		// string literals (with no STANDARD_CONFORMING_STRINGS option exposed,
		// at least on Redshift Serverless). The literal 'foo\bar' parses to
		// "fooar" — the backslash and following char are consumed. So we
		// double every backslash in literal output to round-trip correctly.
		Literaler: sql.ToLiteralFn(sql.QuoteTransformEscapedBackslash("'", "''")),
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

// varbyteToStringCast decodes a VARBYTE column's raw bytes as a base64 string,
// which is the canonical textual representation of binary data in Flow.
func varbyteToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`FROM_VARBYTE(%s, 'base64')`, migration.Identifier)
}

// stringToVarbyteCast decodes a base64-encoded VARCHAR/TEXT column into native
// VARBYTE bytes. Used when the native_binary_column_type feature flag is
// enabled on a task that previously stored binary fields as base64 text.
func stringToVarbyteCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`TO_VARBYTE(%s, 'base64')`, migration.Identifier)
}

// varbyteToSuperCast converts a VARBYTE column into a SUPER value containing
// the base64-encoded JSON string representation of the binary data. A direct
// CAST from VARBYTE to SUPER is not supported by Redshift. FROM_VARBYTE may
// emit base64 with embedded line breaks for large inputs, which would produce
// invalid JSON when wrapped in a string literal, so the line breaks are
// stripped before json_parse. CHR(10) is LF and CHR(13) is CR.
func varbyteToSuperCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`json_parse('"' || REPLACE(REPLACE(FROM_VARBYTE(%s, 'base64'), CHR(10), ''), CHR(13), '') || '"')`, migration.Identifier)
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

// queryParams is the template input for templates that need per-transaction
// key bounds (loadQuery, loadQueryNoFlowDocument, mergeInto). Bounds is
// computed from the observed keys for the transaction, allowing Redshift to
// prune target-table blocks via zone maps on the key columns.
type queryParams struct {
	sql.Table
	Bounds []sql.MergeBound
}

func renderQueryWithBounds(table sql.Table, tpl *template.Template, bounds []sql.MergeBound) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &queryParams{Table: table, Bounds: bounds}); err != nil {
		return "", err
	}
	return w.String(), nil
}

type templates struct {
	createTargetTable         *template.Template
	createLoadTable           *template.Template
	createStoreTable          *template.Template
	createDeleteTable         *template.Template
	mergeInto                 *template.Template
	deleteQuery               *template.Template
	deleteQueryNoFlowDocument *template.Template
	loadQuery                 *template.Template
	loadQueryNoFlowDocument   *template.Template
	copyFromS3                *template.Template
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

-- Idempotent creation of the load table for staging load keys. DISTSTYLE ALL
-- replicates the staged keys to every compute slice so the join
-- against the target table is node-local — Redshift's default EVEN distribution
-- would otherwise require a full redistribution of one side of the join.

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
)
DISTSTYLE ALL;
{{ end }}

-- Idempotent creation of the store table for staging new records.

{{ define "createStoreTable" }}
CREATE TEMPORARY TABLE {{ template "temp_name" . }} (
{{- range $ind, $col := $.Columns }}
	{{- if $ind }},{{ end }}
	{{ $col.Identifier }} {{ $col.DDL }}
{{- end }}
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
--
-- The per-transaction min/max bounds from $.Bounds are emitted as additional
-- predicates on the target side of the merge. Like the load query, these are
-- redundant with the equality match but allow Redshift to prune target blocks
-- via zone maps on the key columns when the target is sorted on them.

{{ define "mergeInto" }}
MERGE INTO {{ $.Identifier }}
USING {{ template "temp_name" . }} AS r
ON {{ range $ind, $bound := $.Bounds }}
{{- if $ind }} AND {{end -}}
	{{$.Identifier}}.{{$bound.Identifier}} = r.{{$bound.Identifier}}
	{{- if $bound.LiteralLower }} AND {{$.Identifier}}.{{$bound.Identifier}} >= {{ $bound.LiteralLower }} AND {{$.Identifier}}.{{$bound.Identifier}} <= {{ $bound.LiteralUpper }}{{ end }}
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
{{ if not $.DeltaUpdates -}}
DELETE FROM {{ $.Identifier }}
USING {{ template "temp_name_deleted" . }} AS r
WHERE {{ range $ind, $key := $.Keys }}
{{- if $ind }} AND {{end -}}
	{{$.Identifier}}.{{$key.Identifier}} = r.{{$key.Identifier}}
{{- end }}
{{- end }}
{{ end }}

-- Templated query which joins keys from the load table with the target table, and returns values. It
-- deliberately skips the trailing semi-colon as these queries are composed with a UNION ALL.
--
-- The per-transaction min/max bounds from $.Bounds are emitted as additional
-- predicates on the target alias r. They are logically redundant with the
-- equality join against the staged keys, but allow Redshift to prune target
-- blocks via zone maps on the key columns when the target table is sorted on
-- them.

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }}, r.{{$.Document.Identifier}}
	FROM {{ template "temp_name" . }} AS l
	JOIN {{ $.Identifier}} AS r
	{{- range $ind, $bound := $.Bounds }}
		{{ if $ind }} AND {{ else }} ON  {{ end -}}
			l.{{ $bound.Identifier }} = r.{{ $bound.Identifier }}
			{{- if $bound.LiteralLower }} AND r.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND r.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
	{{- end }}
{{- else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS SUPER) LIMIT 0) as nodoc
{{- end }}
{{ end }}

-- Templated query for no_flow_document feature flag - reconstructs JSON from root-level columns

{{ define "uncast" -}}
{{ $ident := printf "%s.%s" $.Alias $.Identifier }}
{{- if eq $.AsFlatType "string_integer" -}}
	CAST({{ $ident }} AS VARCHAR)
{{- else if eq $.AsFlatType "string_number" -}}
	CAST({{ $ident }} AS VARCHAR)
{{- else if and (eq $.AsFlatType "string") (eq $.Format "date") (not $.IsPrimaryKey) -}}
	TO_CHAR({{ $ident }}, 'YYYY-MM-DD')
{{- else if and (eq $.AsFlatType "string") (eq $.Format "date-time") (not $.IsPrimaryKey) -}}
	TO_CHAR({{ $ident }} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
{{- else if eq $.BareDDL "VARBYTE(1024000)" -}}
	FROM_VARBYTE({{ $ident }}, 'base64')
{{- else -}}
	{{ $ident }}
{{- end -}}
{{- end }}

{{ define "loadQueryNoFlowDocument" }}
{{ if not $.DeltaUpdates -}}
SELECT {{ $.Binding }},
OBJECT(
{{- range $i, $col := $.RootLevelColumns}}
	{{- if $i}},{{end}}
	{{Literal $col.Field}}, {{ template "uncast" (ColumnWithAlias $col "r") }}
{{- end}}
) as flow_document
FROM {{ template "temp_name" . }} AS l
JOIN {{ $.Identifier}} AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
		l.{{ $bound.Identifier }} = r.{{ $bound.Identifier }}
		{{- if $bound.LiteralLower }} AND r.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND r.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS SUPER) LIMIT 0) as nodoc
{{ end }}
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
