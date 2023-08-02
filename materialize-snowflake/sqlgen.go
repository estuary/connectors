package main

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"encoding/json"
	"text/template"

	"github.com/estuary/connectors/go/pkg/slices"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

// For historical reasons, we do not quote identifiers starting with an underscore or any letter,
// and containing only letters, numbers & underscores. See
// https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html for some details about how
// Snowflake handles unquoted identifiers. Crucially, unquoted identifiers are resolved as
// UPPERCASE, making this historical quoting important for backward compatibility.
var simpleIdentifierRegexp = regexp.MustCompile(`^[_\pL]+[_\pL\pN]*$`)

var jsonConverter sql.ElementConverter = func(te tuple.TupleElement) (interface{}, error) {
	switch ii := te.(type) {
	case []byte:
		return json.RawMessage(ii), nil
	case json.RawMessage:
		return ii, nil
	case nil:
		return json.RawMessage(nil), nil
	default:
		return nil, fmt.Errorf("invalid type %#v for variant", te)
	}
}

// Snowflake INTEGER values support up to 38 digits, which is more than an int64.
func strToSfInt(str string) (interface{}, error) {
	// Strings ending in a 0 decimal part like "1.0" or "3.00" are considered valid as integers per
	// JSON specification so we must handle this possibility here. Anything after the decimal is
	// discarded on the assumption that Flow has validated the data and verified that the decimal
	// component is all 0's.
	if idx := strings.Index(str, "."); idx != -1 {
		str = str[:idx]
	}

	var i big.Int
	out, ok := i.SetString(str, 10)
	if !ok {
		return nil, fmt.Errorf("could not convert %q to big.Int", str)
	}

	return out, nil
}

// snowflakeDialect returns a representation of the Snowflake SQL dialect.
var snowflakeDialect = func() sql.Dialect {
	var variantMapper = sql.NewStaticMapper("VARIANT", sql.WithElementConverter(jsonConverter))
	var typeMappings = sql.ProjectionTypeMapper{
		sql.ARRAY:   variantMapper,
		sql.BINARY:  sql.NewStaticMapper("BINARY"),
		sql.BOOLEAN: sql.NewStaticMapper("BOOLEAN"),
		sql.INTEGER: sql.NewStaticMapper(
			"INTEGER",
			sql.WithElementConverter(sql.StringCastConverter(strToSfInt)),
		),
		sql.NUMBER:   sql.NewStaticMapper("DOUBLE", sql.WithElementConverter(sql.StdStrToFloat())),
		sql.OBJECT:   variantMapper,
		sql.MULTIPLE: sql.NewStaticMapper("VARIANT", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.NewStaticMapper("STRING"),
			WithFormat: map[string]sql.TypeMapper{
				"date":      sql.NewStaticMapper("DATE"),
				"date-time": sql.NewStaticMapper("TIMESTAMP"),
			},
		},
	}
	var nullable sql.TypeMapper = sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    typeMappings,
	}

	return sql.Dialect{
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return simpleIdentifierRegexp.MatchString(s) && !slices.Contains(SF_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform("\"", "\\\""),
			))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		TypeMapper: nullable,
	}
}()

var (
	tplAll = sql.MustParseTemplate(snowflakeDialect, "root", `
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

-- Templated query which joins keys from the load table with the target table, and returns values. It
-- deliberately skips the trailing semi-colon as these queries are composed with a UNION ALL.

{{ define "loadQuery" }}
{{ if $.Table.Document -}}
SELECT {{ $.Table.Binding }}, {{ $.Table.Identifier }}.{{ $.Table.Document.Identifier }}
	FROM {{ $.Table.Identifier }}
	JOIN (
		SELECT {{ range $ind, $key := $.Table.Keys }}
		{{- if $ind }}, {{ end -}}
		$1[{{$ind}}] AS {{$key.Identifier -}}
		{{- end }}
		FROM @flow_v1/{{ $.RandomUUID }}
	) AS r
	ON {{ range $ind, $key := $.Table.Keys }}
	{{- if $ind }} AND {{ end -}}
	{{ $.Table.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
{{ else -}}
SELECT * FROM (SELECT -1, CAST(NULL AS VARIANT) LIMIT 0) as nodoc
{{ end -}}
{{ end }}

{{ define "copyInto" }}
	COPY INTO {{ $.Table.Identifier }} (
		{{ range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}
		{{- end }}
	) FROM (
		SELECT {{ range $ind, $key := $.Table.Columns }}
		{{- if $ind }}, {{ end -}}
		$1[{{$ind}}] AS {{$key.Identifier -}}
		{{- end }}
		FROM @flow_v1/{{ $.RandomUUID }}
	);
{{ end }}


{{ define "mergeInto" }}
	MERGE INTO {{ $.Table.Identifier }}
	USING (
		SELECT {{ range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			$1[{{$ind}}] AS {{$key.Identifier -}}
		{{- end }}
		FROM @flow_v1/{{ $.RandomUUID }}
	) AS r
	ON {{ range $ind, $key := $.Table.Keys }}
		{{- if $ind }} AND {{ end -}}
		{{ $.Table.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
	{{- if $.Table.Document }}
	WHEN MATCHED AND IS_NULL_VALUE(r.{{ $.Table.Document.Identifier }}) THEN
		DELETE
	{{- end }}
	WHEN MATCHED THEN
		UPDATE SET {{ range $ind, $key := $.Table.Values }}
		{{- if $ind }}, {{ end -}}
		{{ $.Table.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end -}}
	{{- if $.Table.Document -}}
	{{ if $.Table.Values }}, {{ end }}{{ $.Table.Identifier }}.{{ $.Table.Document.Identifier}} = r.{{ $.Table.Document.Identifier }}
	{{- end }}
	WHEN NOT MATCHED THEN
		INSERT (
		{{- range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}
		{{- end -}}
	)
		VALUES (
		{{- range $ind, $key := $.Table.Columns }}
			{{- if $ind }}, {{ end -}}
			r.{{$key.Identifier -}}
		{{- end -}}
	);
{{ end }}

{{ define "updateFence" }}
EXECUTE IMMEDIATE $$
DECLARE
    fenced_excp EXCEPTION (-20002, 'This instance was fenced off by another');
BEGIN
	UPDATE {{ Identifier $.TablePath }}
		SET   checkpoint = {{ Literal (Base64Std $.Checkpoint) }}
		WHERE materialization = {{ Literal $.Materialization.String }}
		AND   key_begin = {{ $.KeyBegin }}
		AND   key_end   = {{ $.KeyEnd }}
		AND   fence     = {{ $.Fence }};

	IF (SQLNOTFOUND = true) THEN
		RAISE fenced_excp;
	END IF;

  RETURN SQLROWCOUNT;
END $$;
{{ end }}
  `)
	tplCreateTargetTable = tplAll.Lookup("createTargetTable")
	tplLoadQuery         = tplAll.Lookup("loadQuery")
	tplCopyInto          = tplAll.Lookup("copyInto")
	tplMergeInto         = tplAll.Lookup("mergeInto")
	tplUpdateFence       = tplAll.Lookup("updateFence")
)

var createStageSQL = `
CREATE STAGE IF NOT EXISTS flow_v1
FILE_FORMAT = (
  TYPE = JSON
  BINARY_FORMAT = BASE64
)
COMMENT = 'Internal stage used by Estuary Flow to stage loaded & stored documents'
;`

func RenderTableWithRandomUUIDTemplate(table sql.Table, randomUUID string, tpl *template.Template) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &TableWithUUID{Table: &table, RandomUUID: randomUUID}); err != nil {
		return "", err
	}
	return w.String(), nil
}
