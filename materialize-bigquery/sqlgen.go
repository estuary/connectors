package connector

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

// Identifiers matching the this pattern do not need to be quoted. See
// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#identifiers.
var simpleIdentifierRegexp = regexp.MustCompile(`(?i)^[a-z_][a-z0-9_]*$`)

// Bigquery only allows underscore, letters, numbers, and sometimes hyphens for identifiers. Convert everything else to underscore.
var identifierSanitizerRegexp = regexp.MustCompile(`[^\-\._0-9a-zA-Z]`)

func identifierSanitizer(delegate func(string) string) func(string) string {
	return func(text string) string {
		return delegate(identifierSanitizerRegexp.ReplaceAllString(text, "_"))
	}
}

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

var bqTypeMapper = sql.ProjectionTypeMapper{
	sql.ARRAY:   sql.NewStaticMapper("STRING", sql.WithElementConverter(jsonConverter)),
	sql.BINARY:  sql.NewStaticMapper("BYTES"),
	sql.BOOLEAN: sql.NewStaticMapper("BOOL"),
	sql.INTEGER: sql.NewStaticMapper("INT64"),
	sql.NUMBER:  sql.NewStaticMapper("BIGNUMERIC"),
	sql.OBJECT:  sql.NewStaticMapper("STRING", sql.WithElementConverter(jsonConverter)),
	sql.STRING: sql.StringTypeMapper{
		Fallback: sql.NewStaticMapper("STRING"),
		WithFormat: map[string]sql.TypeMapper{
			"date":      sql.NewStaticMapper("DATE"),
			"date-time": sql.NewStaticMapper("TIMESTAMP"),
		},
	},
}

var bqDialect = func() sql.Dialect {
	typeMapper := sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    bqTypeMapper,
	}

	return sql.Dialect{
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			identifierSanitizer(sql.PassThroughTransform(
				func(s string) bool {
					return simpleIdentifierRegexp.MatchString(s) && !sql.SliceContains(strings.ToUpper(s), BQ_RESERVED_WORDS)
				},
				sql.QuoteTransform("`", "\\`"),
			)))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "\\'")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		TypeMapper: typeMapper,
	}
}()

var (
	tplAll = sql.MustParseTemplate(bqDialect, "root", `
{{ define "tempTableName" -}}
flow_temp_table_{{ $.Binding }}
{{- end }}

-- Templated creation of a materialized table definition and comments.
-- Note: BigQuery only allows a maximum of 4 columns for clustering.

{{ define "createTargetTable" -}}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}} {{$col.DDL}}
	{{- end }}
)
CLUSTER BY {{ range $ind, $key := $.Keys }}
	{{- if lt $ind 4 -}}
		{{- if $ind }}, {{end -}}
			{{$key.Identifier}}
		{{- end -}}
	{{- end}};
{{ end }}

-- Templated query which joins keys from the load table with the target table,
-- and returns values. It deliberately skips the trailing semi-colon
-- as these queries are composed with a UNION ALL.

{{ define "loadQuery" -}}
{{ if $.Document -}}
SELECT {{ $.Binding }}, l.{{$.Document.Identifier}}
	FROM {{ $.Identifier }} AS l
	JOIN {{ template "tempTableName" . }} AS r
	{{- range $ind, $key := $.Keys }}
		{{ if $ind }} AND {{ else }} ON {{ end -}}
		l.{{ $key.Identifier }} is not distinct from r.{{ $key.Identifier }}
	{{- end }}
{{ else }}
SELECT -1, NULL LIMIT 0
{{ end }}
{{ end }}

-- Templated query which bulk inserts rows from a source bucket into a target table.

{{ define "storeInsert" -}}
INSERT INTO {{ $.Identifier }} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}}
	{{- end -}}
)
SELECT {{ range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}}
	{{- end }} FROM {{ template "tempTableName" . }};
{{ end }}

-- Templated query which updates an existing row in the target table:

{{ define "storeUpdate" -}}
MERGE INTO {{ $.Identifier }} AS l
USING {{ template "tempTableName" . }} AS r
ON {{ range $ind, $key := $.Keys }}
{{- if $ind }} AND {{end -}}
	l.{{$key.Identifier}} = r.{{$key.Identifier}}
{{- end}}
{{- if $.Document }}
WHEN MATCHED AND r.{{$.Document.Identifier}} IS NULL THEN
	DELETE
{{- end }}
WHEN MATCHED THEN
	UPDATE SET {{ range $ind, $val := $.Values }}
	{{- if $ind }}, {{end -}}
		l.{{$val.Identifier}} = r.{{$val.Identifier}}
	{{- end}} 
	{{- if $.Document -}}
		{{ if $.Values  }}, {{ end }}l.{{$.Document.Identifier}} = r.{{$.Document.Identifier}}
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

{{ define "installFence" }}
-- Our desired fence
DECLARE vMaterialization STRING DEFAULT {{ Literal $.Materialization.String }};
DECLARE vKeyBegin INT64 DEFAULT {{ $.KeyBegin }};
DECLARE vKeyEnd INT64 DEFAULT {{ $.KeyEnd }};

-- The current values
DECLARE curFence INT64;
DECLARE curKeyBegin INT64;
DECLARE curKeyEnd INT64;
DECLARE curCheckpoint STRING;

BEGIN TRANSACTION;

-- Increment the fence value of _any_ checkpoint which overlaps our key range.
UPDATE {{ Identifier $.TablePath }}
	SET fence=fence+1
	WHERE materialization = vMaterialization
	AND key_end >= vKeyBegin
	AND key_begin <= vKeyEnd;

-- Read the checkpoint with the narrowest [key_begin, key_end] which fully overlaps our range.
SET (curFence, curKeyBegin, curKeyEnd, curCheckpoint) = (
	SELECT AS STRUCT fence, key_begin, key_end, checkpoint
		FROM {{ Identifier $.TablePath }}
		WHERE materialization = vMaterialization
		AND key_begin <= vKeyBegin
		AND key_end >= vKeyEnd
		ORDER BY key_end - key_begin ASC
		LIMIT 1
);

-- Create a new fence if none exists.
IF curFence IS NULL THEN
	SET curFence = {{ $.Fence }};
	SET curKeyBegin = 1;
	SET curKeyEnd = 0;
	SET curCheckpoint = {{ Literal (Base64Std $.Checkpoint) }};
END IF;

-- If any of the key positions don't line up, create a new fence.
-- Either it's new or we are starting a split shard.
IF vKeyBegin <> curKeyBegin OR vKeyEnd <> curKeyEnd THEN
	INSERT INTO {{ Identifier $.TablePath }} (materialization, key_begin, key_end, fence, checkpoint)
	VALUES (vMaterialization, vKeyBegin, vKeyEnd, curFence, curCheckpoint);
END IF;

COMMIT TRANSACTION;

-- Get the current value
SELECT curFence AS fence, curCheckpoint AS checkpoint;
{{ end }}

{{ define "updateFence" }}
IF (
	SELECT fence
	FROM {{ Identifier $.TablePath }}
	WHERE materialization={{ Literal $.Materialization.String }} AND key_begin={{ $.KeyBegin }} AND key_end={{ $.KeyEnd }} AND fence={{ $.Fence }}
) IS NULL THEN
	RAISE USING MESSAGE = 'This instance was fenced off by another';
END IF;

UPDATE {{ Identifier $.TablePath }}
	SET checkpoint={{ Literal (Base64Std $.Checkpoint) }}
	WHERE materialization={{ Literal $.Materialization.String }}
	AND key_begin={{ $.KeyBegin }}
	AND key_end={{ $.KeyEnd }}
	AND fence={{ $.Fence }};
{{ end }}
`)
	tplTempTableName     = tplAll.Lookup("tempTableName")
	tplCreateTargetTable = tplAll.Lookup("createTargetTable")
	tplInstallFence      = tplAll.Lookup("installFence")
	tplUpdateFence       = tplAll.Lookup("updateFence")
	tplLoadQuery         = tplAll.Lookup("loadQuery")
	tplStoreInsert       = tplAll.Lookup("storeInsert")
	tplStoreUpdate       = tplAll.Lookup("storeUpdate")
)
