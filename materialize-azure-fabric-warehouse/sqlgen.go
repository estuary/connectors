package main

import (
	"fmt"
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

var dialect = func() sql.Dialect {
	// Although the documentation states this limit as 1MB, via empirical
	// testing I have determined it to actually be 1 MiB.
	const maxFabricStringLength = 1024 * 1024

	checkedStringLength := func(conv sql.ElementConverter) sql.ElementConverter {
		return func(te tuple.TupleElement) (any, error) {
			var err error
			if conv != nil {
				te, err = conv(te)
				if err != nil {
					return nil, err
				}
			}

			if str, ok := te.(string); ok {
				if len(str) > maxFabricStringLength {
					return nil, fmt.Errorf("string byte size too large: %d vs maximum allowed by Fabric Warehouse %d", len(str), maxFabricStringLength)
				}
			}

			return te, nil
		}
	}

	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("BIGINT"),
				sql.MapStatic("DECIMAL(38,0)", sql.AlsoCompatibleWith("DECIMAL")),
			),
			sql.NUMBER:   sql.MapStatic("FLOAT"),
			sql.BOOLEAN:  sql.MapStatic("BIT"),
			sql.OBJECT:   sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR"), sql.UsingConverter(checkedStringLength(sql.ToJsonString))),
			sql.ARRAY:    sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR"), sql.UsingConverter(checkedStringLength(sql.ToJsonString))),
			sql.BINARY:   sql.MapStatic("VARBINARY(MAX)", sql.AlsoCompatibleWith("VARBINARY")),
			sql.MULTIPLE: sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR"), sql.UsingConverter(checkedStringLength(sql.ToJsonString))),
			sql.STRING_INTEGER: sql.MapStringMaxLen(
				sql.MapStatic("DECIMAL(38,0)", sql.AlsoCompatibleWith("DECIMAL"), sql.UsingConverter(sql.StrToInt)),
				sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR"), sql.UsingConverter(sql.ToStr)),
				// A 96-bit integer is 39 characters long, but not all 39 digit
				// integers will fit in one.
				38,
			),
			sql.STRING_NUMBER: sql.MapStatic("FLOAT", sql.UsingConverter(sql.StrToFloat(nil, nil, nil))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("VARCHAR(MAX)", sql.AlsoCompatibleWith("VARCHAR"), sql.UsingConverter(checkedStringLength(nil))),
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
		MigratableTypes: sql.MigrationSpecs{
			"bigint":    {sql.NewMigrationSpec([]string{"FLOAT", "DECIMAL(38,0)", "VARCHAR(MAX)"})},
			"decimal":   {sql.NewMigrationSpec([]string{"FLOAT", "VARCHAR(MAX)"})},
			"float":     {sql.NewMigrationSpec([]string{"VARCHAR(MAX)"})},
			"bit":       {sql.NewMigrationSpec([]string{"VARCHAR(MAX)"}, sql.WithCastSQL(bitToStringCast))},
			"date":      {sql.NewMigrationSpec([]string{"VARCHAR(MAX)"})},
			"datetime2": {sql.NewMigrationSpec([]string{"VARCHAR(MAX)"})},
			"time":      {sql.NewMigrationSpec([]string{"VARCHAR(MAX)"})},
		},
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			return sql.InfoTableLocation{TableSchema: path[1], TableName: path[2]}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string { return schema }),
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

func bitToStringCast(m sql.ColumnTypeMigration) string {
	return fmt.Sprintf(
		`CAST(CASE WHEN %s = 1 THEN 'true' WHEN %s = 0 THEN 'false' ELSE NULL END AS %s)`,
		m.Identifier, m.Identifier, m.NullableDDL,
	)
}

type queryParams struct {
	sql.Table
	URIs              []string
	StorageAccountKey string
	Bounds            []sql.MergeBound
}

type migrateParams struct {
	SourceTable string
	TmpName     string
	Columns     []migrateColumn
}

type migrateColumn struct {
	Identifier string
	CastSQL    string
}

var (
	tplAll = sql.MustParseTemplate(dialect, "root", `
{{ define "temp_name_load" -}}
flow_temp_table_load_{{ $.Binding }}
{{- end }}

{{ define "temp_name_store" -}}
flow_temp_table_store_{{ $.Binding }}
{{- end }}

{{ define "maybe_unbase64" -}}
{{- if eq $.DDL "VARBINARY(MAX)" -}}BASE64_DECODE({{$.Identifier}}){{ else }}{{$.Identifier}}{{ end }}
{{- end }}

{{ define "maybe_unbase64_lhs" -}}
{{- if eq $.DDL "VARBINARY(MAX)" -}}BASE64_DECODE(l.{{$.Identifier}}){{ else }}l.{{$.Identifier}}{{ end }}
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

{{ define "createMigrationTable" }}
CREATE TABLE {{$.TmpName}} AS SELECT
{{- range $ind, $col := $.Columns }}
	{{- if $ind }},{{ end }}
	{{ if $col.CastSQL -}} {{ $col.CastSQL }} AS {{$col.Identifier}} {{- else -}} {{$col.Identifier}} {{- end }}
{{- end }}
	FROM {{$.SourceTable}};
{{ end }}

{{ define "createLoadTable" }}
CREATE TABLE {{ template "temp_name_load" $ }} (
{{- range $ind, $key := $.Keys }}
	{{- if $ind }},{{ end }}
	{{$key.Identifier}} {{- if eq $key.DDL "VARBINARY(MAX)" }} VARCHAR(MAX) {{- else }} {{$key.DDL}} {{- end }}
{{- end }}
);

COPY INTO {{ template "temp_name_load" $ }}
({{- range $ind, $key := $.Keys }}{{- if $ind }}, {{ end }}{{$key.Identifier}}{{- end }})
FROM {{ range $ind, $uri := $.URIs }}{{- if $ind }}, {{ end }}'{{$uri}}'{{- end }}
WITH (
	FILE_TYPE = 'CSV',
	COMPRESSION = 'Gzip',
	FIELDQUOTE = '{{ Backtick }}',
	CREDENTIAL = (IDENTITY='Storage Account Key', SECRET='{{ $.StorageAccountKey }}')
);
{{ end }}

{{ define "loadQuery" }}
SELECT {{ $.Binding }}, r.{{$.Document.Identifier}}
FROM {{ template "temp_name_load" . }} AS l
JOIN {{ $.Identifier}} AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	{{ template "maybe_unbase64_lhs" $bound }} = r.{{ $bound.Identifier }}
	{{- if $bound.LiteralLower }} AND r.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND r.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }}
{{ end }}

-- Templated query for no_flow_document feature flag - reconstructs JSON from root-level columns

{{ define "loadQueryNoFlowDocument" }}
SELECT {{ $.Binding }}, 
	JSON_OBJECT(
		{{- range $i, $col := $.RootLevelColumns}}
			{{- if $i}},{{end}}
		{{Literal $col.Field}}: r.{{$col.Identifier}}
		{{- end}}
	) as flow_document
FROM {{ template "temp_name_load" . }} AS l
JOIN {{ $.Identifier}} AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	{{ template "maybe_unbase64_lhs" $bound }} = r.{{ $bound.Identifier }}
	{{- if $bound.LiteralLower }} AND r.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND r.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }}
{{ end }}

{{ define "dropLoadTable" }}
DROP TABLE {{ template "temp_name_load" $ }};
{{- end }}

{{ define "create_store_staging_table" -}}
CREATE TABLE {{ template "temp_name_store" $ }} (
{{- range $ind, $col := $.Columns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{- if eq $col.DDL "VARBINARY(MAX)" }} VARCHAR(MAX) {{- else }} {{$col.DDL}} {{- end }}
{{- end }},
	_flow_delete BIT
);

COPY INTO {{ template "temp_name_store" $ }}
({{- range $ind, $col := $.Columns }}{{- if $ind }}, {{ end }}{{$col.Identifier}}{{- end }}, _flow_delete)
FROM {{ range $ind, $uri := $.URIs }}{{- if $ind }}, {{ end }}'{{$uri}}'{{- end }}
WITH (
	FILE_TYPE = 'CSV',
	COMPRESSION = 'Gzip',
	FIELDQUOTE = '{{ Backtick }}',
	CREDENTIAL = (IDENTITY='Storage Account Key', SECRET='{{ $.StorageAccountKey }}')
);
{{- end }}

-- Azure Fabric Warehouse doesn't yet support an actual "merge" query,
-- so the best we can do is a delete followed by an insert. A true
-- merge query may eventually be supported and we should switch to using
-- that when it is.

{{ define "storeMergeQuery" }}
{{ template "create_store_staging_table" $ }}

DELETE r
FROM {{$.Identifier}} AS r
INNER JOIN {{ template "temp_name_store" $ }} AS l
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	{{ template "maybe_unbase64_lhs" $bound }} = r.{{ $bound.Identifier }}
	{{- if $bound.LiteralLower }} AND r.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND r.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }};

INSERT INTO {{$.Identifier}} ({{- range $ind, $col := $.Columns }}{{- if $ind }}, {{ end }}{{$col.Identifier}}{{- end }})
SELECT {{ range $ind, $col := $.Columns }}{{- if $ind }}, {{ end }}{{ template "maybe_unbase64" $col }}{{- end }}
FROM {{ template "temp_name_store" $ }}
WHERE _flow_delete = 0;

DROP TABLE {{ template "temp_name_store" $ }};
{{ end }}


-- storeCopyIntoFromStagedQuery is used when there is no data to
-- merge, but there are binary columns that must be converted from
-- the staged CSV data, which is base64 encoded.

{{ define "storeCopyIntoFromStagedQuery" }}
{{ template "create_store_staging_table" $ }}

INSERT INTO {{$.Identifier}} ({{- range $ind, $col := $.Columns }}{{- if $ind }}, {{ end }}{{$col.Identifier}}{{ end }})
SELECT {{ range $ind, $col := $.Columns }}{{- if $ind }}, {{ end }}{{ template "maybe_unbase64" $col }}{{- end }}
FROM {{ template "temp_name_store" $ }};

DROP TABLE {{ template "temp_name_store" $ }};
{{ end }}

-- storeCopyIntoDirectQuery is used when there is no data to
-- merge and none of the columns are binary. In this case the
-- data can be loaded directly into the target table.

{{ define "storeCopyIntoDirectQuery" }}
COPY INTO {{$.Identifier}}
({{- range $ind, $col := $.Columns }}{{- if $ind }}, {{ end }}{{$col.Identifier}}{{- end }})
FROM {{ range $ind, $uri := $.URIs }}{{- if $ind }}, {{ end }}'{{$uri}}'{{- end }}
WITH (
	FILE_TYPE = 'CSV',
	COMPRESSION = 'Gzip',
	FIELDQUOTE = '{{ Backtick }}',
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
	tplCreateTargetTable            = tplAll.Lookup("createTargetTable")
	tplAlterTableColumns            = tplAll.Lookup("alterTableColumns")
	tplCreateMigrationTable         = tplAll.Lookup("createMigrationTable")
	tplCreateLoadTable              = tplAll.Lookup("createLoadTable")
	tplLoadQuery                    = tplAll.Lookup("loadQuery")
	tplLoadQueryNoFlowDocument      = tplAll.Lookup("loadQueryNoFlowDocument")
	tplDropLoadTable                = tplAll.Lookup("dropLoadTable")
	tplStoreMergeQuery              = tplAll.Lookup("storeMergeQuery")
	tplStoreCopyIntoFromStagedQuery = tplAll.Lookup("storeCopyIntoFromStagedQuery")
	tplStoreCopyIntoDirectQuery     = tplAll.Lookup("storeCopyIntoDirectQuery")
	tplUpdateFence                  = tplAll.Lookup("updateFence")
)
