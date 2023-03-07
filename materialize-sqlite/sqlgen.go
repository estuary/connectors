package main
import (
  "strings"

  sql "github.com/estuary/connectors/materialize-sql"
)

var sqliteDialect = func () sql.Dialect {
  var typeMappings = sql.ProjectionTypeMapper{
    sql.ARRAY:   sql.NewStaticMapper("TEXT"),
    sql.BINARY:  sql.NewStaticMapper("BLOB"),
    sql.BOOLEAN: sql.NewStaticMapper("BOOLEAN"),
    sql.INTEGER: sql.NewStaticMapper("INTEGER"),
    sql.NUMBER:  sql.NewStaticMapper("REAL"),
    sql.OBJECT:  sql.NewStaticMapper("TEXT"),
    sql.STRING: sql.StringTypeMapper{
      Fallback: sql.NewStaticMapper("TEXT"),
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
        return sql.IsSimpleIdentifier(s) && !sliceContains(strings.ToLower(s), SQLITE_RESERVED_WORDS)
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
  tplAll = sql.MustParseTemplate(sqliteDialect, "root", `
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

  -- Create temporary table for loading keys
  {{ define "createLoadTable" }}
  CREATE TABLE {{ template "temp_name" . }} (
    {{- range $ind, $key := $.Keys }}
    {{- if $ind }},{{ end }}
    {{ $key.Identifier }} {{ $key.DDL }}
    {{- end }}
  );
  {{ end }}

  -- Insert keys into temporary table
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

  -- Load
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
  {{- end }}

  -- Truncate
  {{ define "loadTruncate" }}
  DELETE FROM {{ template "temp_name" . }};
  {{ end }}


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

  {{ define "storeUpdate" }}
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

  {{ define "updateFence" }}
  UPDATE {{ Identifier $.TablePath }}
  SET   checkpoint = {{ Literal (Base64Std $.Checkpoint) }}
  WHERE materialization = {{ Literal $.Materialization.String }}
  AND   key_begin = {{ $.KeyBegin }}
  AND   key_end   = {{ $.KeyEnd }}
  AND   fence     = {{ $.Fence }};
  {{ end }}
  `)
  tplCreateTargetTable = tplAll.Lookup("createTargetTable")
  tplCreateLoadTable   = tplAll.Lookup("createLoadTable")

  tplLoadInsert        = tplAll.Lookup("loadInsert")
  tplLoadQuery         = tplAll.Lookup("loadQuery")
  tplLoadTruncate      = tplAll.Lookup("loadTruncate")

  tplStoreInsert       = tplAll.Lookup("storeInsert")
  tplStoreUpdate       = tplAll.Lookup("storeUpdate")

  tplUpdateFence       = tplAll.Lookup("updateFence")
)

const createStageSQL = `
CREATE STAGE IF NOT EXISTS flow_v1
FILE_FORMAT = (
  TYPE = JSON
  BINARY_FORMAT = BASE64
)
COMMENT = 'Internal stage used by Estuary Flow to stage loaded & stored documents'
;`

const attachSQL = "ATTACH DATABASE '' AS load ;"

func sliceContains(expected string, actual []string) bool {
  for _, ty := range actual {
    if ty == expected {
      return true
    }
  }
  return false
}
