package main

import (
	"fmt"
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
)

var dialect = func() sql.Dialect {
	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("BIGINT"),
				sql.MapStatic("DECIMAL(38,0)", sql.AlsoCompatibleWith("DECIMAL")),
			),
			sql.NUMBER:   sql.MapStatic("FLOAT"),
			sql.BOOLEAN:  sql.MapStatic("BIT"),
			sql.OBJECT:   sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR"), sql.UsingConverter(sql.ToJsonString)),
			sql.ARRAY:    sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR"), sql.UsingConverter(sql.ToJsonString)),
			sql.BINARY:   sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR")), // TODO(whb): Binary support?
			sql.MULTIPLE: sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR"), sql.UsingConverter(sql.ToJsonString)),
			sql.STRING_INTEGER: sql.MapStringMaxLen(
				sql.MapStatic("DECIMAL(38,0)", sql.AlsoCompatibleWith("DECIMAL"), sql.UsingConverter(sql.StrToInt)),
				sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR"), sql.UsingConverter(sql.ToStr)),
				// A 96-bit integer is 39 characters long, but not all 39 digit
				// integers will fit in one.
				38,
			),
			sql.STRING_NUMBER: sql.MapStatic("FLOAT", sql.UsingConverter(sql.StrToFloat(nil, nil, nil))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR")),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      sql.MapStatic("DATE", sql.UsingConverter(sql.ClampDate)),
					"date-time": sql.MapStatic("DATETIME2(6)", sql.AlsoCompatibleWith("DATETIME2"), sql.UsingConverter(sql.ClampDatetime)),
					"time":      sql.MapStatic("TIME(6)", sql.AlsoCompatibleWith("TIME")),
				},
			}),
		},
		// NB: We are not using NOT NULL text so that all columns are created as
		// nullable. This is necessary because Fabric Warehouse does not support
		// dropping a NOT NULL constraint, so we need to create columns as
		// nullable to preserve the ability to change collection schema fields
		// from required to not required or add/remove fields from the
		// materialization.
	)

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{}, // TODO: Support column migrations.
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			return sql.InfoTableLocation{TableSchema: path[1], TableName: path[2]}
		}),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return field }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(SQLSERVER_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform(`"`, `""`),
			))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			// parameterIndex starts at 0, but sqlserver parameters start at @p1
			return fmt.Sprintf("@p%d", index+1)
		}),
		TypeMapper:             mapper,
		MaxColumnCharLength:    0,
		CaseInsensitiveColumns: true,
	}
}()

type queryParams struct {
	sql.Table
	URIs              []string
	StorageAccountKey string
}

var (
	tplAll = sql.MustParseTemplate(dialect, "root", `
{{ define "temp_name_load" -}}
flow_temp_table_load_{{ $.Binding }}
{{- end }}

{{ define "temp_name_store" -}}
flow_temp_table_store_{{ $.Binding }}
{{- end }}

{{ define "createTargetTable" }}
CREATE TABLE {{$.Identifier}} (
{{- range $ind, $col := $.Columns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.DDL}}
{{- end }}
);
{{ end }}

{{ define "alterTableColumns" }}
ALTER TABLE {{$.Identifier}} ADD
{{- range $ind, $col := $.AddColumns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.NullableDDL}}
{{- end }};
{{ end }}

{{ define "createLoadTable" }}
CREATE TABLE {{ template "temp_name_load" $ }} (
{{- range $ind, $key := $.Keys }}
	{{- if $ind }},{{ end }}
	{{$key.Identifier}} {{$key.DDL}}
{{- end }}
);

COPY INTO {{ template "temp_name_load" $ }}
({{- range $ind, $key := $.Keys }}{{- if $ind }}, {{ end }}{{$key.Identifier}}{{- end }})
FROM '{{- range $ind, $uri := $.URIs }}{{- if $ind }},{{ end }}{{$uri}}{{- end }}'
WITH (
    FILE_TYPE = 'CSV',
    COMPRESSION = 'Gzip',
    CREDENTIAL = (IDENTITY='Storage Account Key', SECRET='{{ $.StorageAccountKey }}')
);
{{ end }}

{{ define "loadQuery" }}
SELECT {{ $.Binding }}, r.{{$.Document.Identifier}}
FROM {{ template "temp_name_load" . }} AS l
JOIN {{ $.Identifier}} AS r
{{- range $ind, $key := $.Keys }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
{{- end }}
{{ end }}

{{ define "dropLoadTable" }}
DROP TABLE {{ template "temp_name_load" $ }};
{{- end }}

-- Azure Fabric Warehouse doesn't yet support an actual "merge" query,
-- so the best we can do is a delete followed by an insert. A true
-- merge query may eventually be supported and we should switch to using
-- that when it is.

{{ define "storeMergeQuery" }}
CREATE TABLE {{ template "temp_name_store" $ }} (
{{- range $ind, $col := $.Columns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.DDL}}
{{- end }}
);

COPY INTO {{ template "temp_name_store" $ }}
({{- range $ind, $col := $.Columns }}{{- if $ind }}, {{ end }}{{$col.Identifier}}{{- end }})
FROM '{{- range $ind, $uri := $.URIs }}{{- if $ind }}, {{ end }}{{$uri}}{{- end }}'
WITH (
    FILE_TYPE = 'CSV',
    COMPRESSION = 'Gzip',
    CREDENTIAL = (IDENTITY='Storage Account Key', SECRET='{{ $.StorageAccountKey }}')
);

DELETE l
FROM {{$.Identifier}} AS l
INNER JOIN {{ template "temp_name_store" $ }} AS r
{{- range $ind, $key := $.Keys }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
{{- end }};

INSERT INTO {{$.Identifier}} ({{- range $ind, $col := $.Columns }}{{- if $ind }},{{ end }}{{$col.Identifier}}{{- end }})
SELECT {{ range $ind, $col := $.Columns }}{{- if $ind }}, {{ end }}{{$col.Identifier}}{{- end }}
FROM {{ template "temp_name_store" $ }}
WHERE {{$.Document.Identifier}} <> '"delete"';

DROP TABLE {{ template "temp_name_store" $ }};
{{ end }}

{{ define "storeCopyIntoQuery" }}
COPY INTO {{$.Identifier}}
({{- range $ind, $col := $.Columns }}{{- if $ind }}, {{ end }}{{$col.Identifier}}{{- end }})
FROM '{{- range $ind, $uri := $.URIs }}{{- if $ind }}, {{ end }}{{$uri}}{{- end }}'
WITH (
	FILE_TYPE = 'CSV',
	COMPRESSION = 'Gzip',
	CREDENTIAL = (IDENTITY='Storage Account Key', SECRET='{{ $.StorageAccountKey }}')
);
{{ end }}

{{ define "updateFence" }}
UPDATE {{ Identifier $.TablePath }}
	SET   "checkpoint" = {{ Literal (Base64Std $.Checkpoint) }}
	WHERE materialization = {{ Literal $.Materialization.String }}
	AND   key_begin = {{ $.KeyBegin }}
	AND   key_end   = {{ $.KeyEnd }}
	AND   fence     = {{ $.Fence }};
{{ end }}
`)
	tplCreateTargetTable  = tplAll.Lookup("createTargetTable")
	tplAlterTableColumns  = tplAll.Lookup("alterTableColumns")
	tplCreateLoadTable    = tplAll.Lookup("createLoadTable")
	tplLoadQuery          = tplAll.Lookup("loadQuery")
	tplDropLoadTable      = tplAll.Lookup("dropLoadTable")
	tplStoreMergeQuery    = tplAll.Lookup("storeMergeQuery")
	tplStoreCopyIntoQuery = tplAll.Lookup("storeCopyIntoQuery")
	tplUpdateFence        = tplAll.Lookup("updateFence")
)
