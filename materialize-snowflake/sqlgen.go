package main
import (
  "fmt"
  "strings"

	"encoding/json"
  sql "github.com/estuary/connectors/materialize-sql"
	"text/template"
	"github.com/google/uuid"
  "github.com/estuary/flow/go/protocols/fdb/tuple"
)

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

// snowflakeDialect returns a representation of the Snowflake SQL dialect.
var snowflakeDialect = func () sql.Dialect {
  var variantMapper = sql.NewStaticMapper("VARIANT", sql.WithElementConverter(jsonConverter))
  var typeMappings = sql.ProjectionTypeMapper{
    sql.ARRAY:   variantMapper,
    sql.BINARY:  sql.NewStaticMapper("BINARY"),
    sql.BOOLEAN: sql.NewStaticMapper("BOOLEAN"),
    sql.INTEGER: sql.NewStaticMapper("INTEGER"),
    sql.NUMBER:  sql.NewStaticMapper("DOUBLE"),
    sql.OBJECT:  variantMapper,
    sql.STRING: sql.StringTypeMapper{
      Fallback: sql.NewStaticMapper("TEXT"),
      WithFormat: map[string]sql.TypeMapper{
        "date-time": sql.NewStaticMapper("TIMESTAMPT"),
      },
    },
  }
  var nullable sql.TypeMapper = sql.NullableMapper{
    NotNullText: "NOT NULL",
    Delegate:       typeMappings,
  }

  return sql.Dialect{
		Identifierer:       sql.IdentifierFn(sql.JoinTransform(".", 
      sql.PassThroughTransform(
        func(s string) bool {
          return sql.IsSimpleIdentifier(s) && !sliceContains(strings.ToLower(s), SF_RESERVED_WORDS)
        },
        sql.QuoteTransform("\"", "\\\""),
    ))),
		Literaler:          sql.LiteralFn(sql.QuoteTransform("'", "''")),
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

{{ define "loadQuery" }}
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
{{- end }}

{{ define "copyInto" }}
	COPY INTO {{ $.Table.Identifier }} (
		{{ range $ind, $key := (AllFields $.Table) }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}
		{{- end }}
	) FROM (
		SELECT {{ range $ind, $key := (AllFields $.Table) }}
		{{- if $ind }}, {{ end -}}
		$1[{{$ind}}] AS {{$key.Identifier -}}
		{{- end }}
		FROM @flow_v1/{{ $.RandomUUID }}
	);
{{- end }}


{{ define "mergeInto" }}
	MERGE INTO {{ $.Table.Identifier }}
	USING (
		SELECT {{ range $ind, $key := (AllFields $.Table) }}
			{{- if $ind }}, {{ end -}}
			$1[{{$ind}}] AS {{$key.Identifier -}}
		{{- end }}
		FROM @flow_v1/{{ $.RandomUUID }}
	) AS r
	ON {{ range $ind, $key := $.Table.Keys }}
		{{- if $ind }} AND {{ end -}}
		{{ $.Table.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
	WHEN MATCHED AND IS_NULL_VALUE(r.{{ $.Table.Document.Identifier }}) THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET {{ range $ind, $key := $.Table.Values }}
		{{- if $ind }}, {{ end -}}
		{{ $.Table.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end -}}
	, {{ $.Table.Identifier }}.{{ $.Table.Document.Identifier}} = r.{{ $.Table.Document.Identifier }}
	WHEN NOT MATCHED THEN
		INSERT (
		{{- range $ind, $key := (AllFields $.Table) }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}
		{{- end -}}
	)
		VALUES (
		{{- range $ind, $key := (AllFields $.Table) }}
			{{- if $ind }}, {{ end -}}
			r.{{$key.Identifier -}}
		{{- end -}}
	);
{{- end }}

{{ define "selectByKeys" }}
	SELECT {{ $.Document.Identifier }}
	FROM {{ $.Identifier }}
	WHERE
		{{- range $ind, $key := $.Keys }}
			{{- if $ind }} AND {{ end -}}
			{{ $key.Identifier }} = {{ $key.Placeholder }}
		{{- end -}}
	;
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
  tplSelectByKeys      = tplAll.Lookup("selectByKeys")
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

func RenderTableWithRandomUUIDTemplate(table sql.Table, randomUUID uuid.UUID, tpl *template.Template) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &TableWithUUID { Table: &table, RandomUUID: randomUUID }); err != nil {
		return "", err
	}
	return w.String(), nil
}

