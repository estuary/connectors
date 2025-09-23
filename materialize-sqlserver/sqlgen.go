package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
)

// strToInt is used for sqlserver specific conversion from an integer-formatted string or integer to
// an integer. The sqlserver driver doesn't appear to have any way to provide an integer value
// larger than 8 bytes as a parameter (such as may go in a NUMERIC(38,0) column). The value provided
// must always be an integer that fits in an int64.
var strToInt sql.ElementConverter = sql.StringCastConverter(func(str string) (interface{}, error) {
	// Strings ending in a 0 decimal part like "1.0" or "3.00" are considered valid as integers
	// per JSON specification so we must handle this possibility here. Anything after the
	// decimal is discarded on the assumption that Flow has validated the data and verified that
	// the decimal component is all 0's.
	if idx := strings.Index(str, "."); idx != -1 {
		str = str[:idx]
	}

	out, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("could not convert %q to int64: %w", str, err)
	}

	return out, nil
})

func createSqlServerDialect(collation string, defaultSchema string, featureFlags map[string]bool) sql.Dialect {
	var stringType = "varchar"
	// If the collation does not support UTF8, we fallback to using nvarchar
	// for string columns
	if !strings.Contains(collation, "UTF8") {
		stringType = "nvarchar"
	}

	var textType = fmt.Sprintf("%s(MAX) COLLATE %s", stringType, collation)
	var textPKType = fmt.Sprintf("%s(900) COLLATE %s", stringType, collation)

	// Define base date/time mappings without primary key wrapper
	primaryKeyTextType := sql.MapStatic(textPKType, sql.AlsoCompatibleWith(stringType))
	dateMapping := sql.MapStatic("DATE")
	datetimeMapping := sql.MapStatic("DATETIME2", sql.UsingConverter(rfc3339ToUTC()))
	timeMapping := sql.MapStatic("TIME", sql.UsingConverter(rfc3339TimeToUTC()))

	// If feature flag is enabled, wrap with MapPrimaryKey to use string types for primary keys
	if featureFlags["datetime_keys_as_string"] {
		dateMapping = sql.MapPrimaryKey(primaryKeyTextType, dateMapping)
		datetimeMapping = sql.MapPrimaryKey(primaryKeyTextType, datetimeMapping)
		timeMapping = sql.MapPrimaryKey(primaryKeyTextType, timeMapping)
	}

	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("BIGINT"),
				sql.MapStatic(textType, sql.AlsoCompatibleWith(stringType), sql.UsingConverter(sql.ToStr)),
			),
			sql.NUMBER:  sql.MapStatic("DOUBLE PRECISION", sql.AlsoCompatibleWith("float")),
			sql.BOOLEAN: sql.MapStatic("BIT"),
			sql.OBJECT:  sql.MapStatic(textType, sql.AlsoCompatibleWith(stringType), sql.UsingConverter(sql.ToJsonString)),
			sql.ARRAY:   sql.MapStatic(textType, sql.AlsoCompatibleWith(stringType), sql.UsingConverter(sql.ToJsonString)),
			sql.BINARY: sql.MapPrimaryKey(
				sql.MapStatic(textPKType, sql.AlsoCompatibleWith(stringType)),
				sql.MapStatic(textType, sql.AlsoCompatibleWith(stringType)),
			),
			sql.MULTIPLE: sql.MapStatic(textType, sql.AlsoCompatibleWith(stringType), sql.UsingConverter(sql.ToJsonString)),
			sql.STRING_INTEGER: sql.MapStringMaxLen(
				sql.MapStatic("BIGINT", sql.UsingConverter(strToInt)),
				sql.MapStatic(textType, sql.AlsoCompatibleWith(stringType), sql.UsingConverter(sql.ToStr)),
				// The maximum length of a signed 64-bit integer is 19
				// characters.
				18,
			),
			// SQL Server doesn't handle non-numeric float types and we must map them to NULL.
			sql.STRING_NUMBER: sql.MapStatic("DOUBLE PRECISION", sql.AlsoCompatibleWith("float"), sql.UsingConverter(sql.StrToFloat(nil, nil, nil))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapPrimaryKey(
					// sqlserver cannot do varchar/nvarchar primary keys larger than 900 bytes, and in
					// sqlserver, the number N passed to varchar(N), denotes the maximum bytes
					// stored in the column, not the character count.
					// see https://learn.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql?view=sql-server-2017#remarks
					// and https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server?view=sql-server-2017
					sql.MapStatic(textPKType, sql.AlsoCompatibleWith(stringType)),
					sql.MapStatic(textType, sql.AlsoCompatibleWith(stringType)),
				),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      dateMapping,
					"date-time": datetimeMapping,
					"time":      timeMapping,
				},
			}),
		},
		sql.WithNotNullText("NOT NULL"),
	)

	var nocast = sql.WithCastSQL(migrationIdentifier)

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"float":     {sql.NewMigrationSpec([]string{textType}, nocast)},
			"bigint":    {sql.NewMigrationSpec([]string{textType}, nocast), sql.NewMigrationSpec([]string{"double precision"})},
			"date":      {sql.NewMigrationSpec([]string{textType}, nocast)},
			"time":      {sql.NewMigrationSpec([]string{textType}, nocast)},
			"datetime2": {sql.NewMigrationSpec([]string{textType}, sql.WithCastSQL(datetimeToStringCast))},
		},
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			if len(path) == 1 {
				// A schema isn't required to be set on the endpoint or any
				// resource, and if its empty the default schema for the
				// configured user will implicitly be used.
				return sql.InfoTableLocation{TableSchema: defaultSchema, TableName: path[0]}
			} else {
				return sql.InfoTableLocation{TableSchema: path[0], TableName: path[1]}
			}
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
		MaxColumnCharLength:    128,
		CaseInsensitiveColumns: true,
	}
}

// by default we don't want to do `CAST(%s AS %s)` for SQLserver
func migrationIdentifier(migration sql.ColumnTypeMigration) string {
	return migration.Identifier
}

func rfc3339ToUTC() sql.ElementConverter {
	return sql.StringCastConverter(func(str string) (interface{}, error) {
		if t, err := time.Parse(time.RFC3339Nano, str); err != nil {
			return nil, fmt.Errorf("could not parse %q as RFC3339 date-time: %w", str, err)
		} else {
			return t.UTC(), nil
		}
	})
}

func rfc3339TimeToUTC() sql.ElementConverter {
	return sql.StringCastConverter(func(str string) (interface{}, error) {
		if t, err := time.Parse("15:04:05.999999999Z07:00", str); err != nil {
			return nil, fmt.Errorf("could not parse %q as RFC3339 time: %w", str, err)
		} else {
			return t.UTC(), nil
		}
	})
}

func datetimeToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`FORMAT(%s AT TIME ZONE 'UTC', 'yyyy-MM-ddTHH:mm:ss.FFFFFFF') + 'Z'`, migration.Identifier)
}

type templates struct {
	tempLoadTableName  *template.Template
	tempStoreTableName *template.Template
	tempLoadTruncate   *template.Template
	tempStoreTruncate  *template.Template
	createLoadTable    *template.Template
	createStoreTable   *template.Template
	alterTableColumns  *template.Template
	createTargetTable  *template.Template
	directCopy         *template.Template
	mergeInto          *template.Template
	loadInsert         *template.Template
	loadQuery          *template.Template
	updateFence        *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
	var tplAll = sql.MustParseTemplate(dialect, "root", `
-- Local (session-level) temporary tables are prefixed with a # sign in SQLServer
{{ define "temp_load_name" -}}
#flow_temp_load_{{ $.Binding }}
{{- end }}

{{ define "temp_store_name" -}}
#flow_temp_store_{{ $.Binding }}
{{- end }}

-- Templated creation of a materialized table definition and comments:

{{ define "createTargetTable" }}
IF OBJECT_ID(N'{{Join $.Path "."}}', 'U') IS NULL BEGIN
CREATE TABLE {{$.Identifier}} (
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
END;
{{ end }}

-- Templated query which performs table alterations by adding columns.
-- Dropping nullability constraints must be handled separately, since SQL
-- Server does not support modifying multiple columns in a single statement.

{{ define "alterTableColumns" }}
ALTER TABLE {{$.Identifier}} ADD
{{- range $ind, $col := $.AddColumns }}
	{{- if $ind }},{{ end }}
	{{$col.Identifier}} {{$col.NullableDDL}}
{{- end }};
{{ end }}

-- Templated creation of a temporary load table:

{{ define "createLoadTable" }}
CREATE TABLE {{ template "temp_load_name" . }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{ $key.Identifier }} {{ $key.DDL }}
	{{- end -}}
	,
		PRIMARY KEY (
		{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
		{{- end -}}
	)
);
{{ end }}

-- Query for inserting to temporary load table

{{ define "loadInsert" }}
INSERT INTO {{ template "temp_load_name" . }} (
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

-- Templated creation of a temporary store table:

{{ define "createStoreTable" }}
CREATE TABLE {{ template "temp_store_name" . }} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}} {{$col.DDL}}
	{{- end }},
	_flow_delete BIT
	{{- if not $.DeltaUpdates }},

		PRIMARY KEY (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
	{{- end -}}
	)
	{{- end }}
);
{{ end }}

-- Templated truncation of the temporary load table:

{{ define "truncateTempLoadTable" }}
TRUNCATE TABLE {{ template "temp_load_name" . }};
{{ end }}

-- Templated truncation of the temporary store table:

{{ define "truncateTempStoreTable" }}
TRUNCATE TABLE {{ template "temp_store_name" . }};
{{ end }}

-- Templated query which joins keys from the load table with the target table, and returns values. It
-- deliberately skips the trailing semi-colon as these queries are composed with a UNION ALL.

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{ $.Binding }}, r.{{$.Document.Identifier}}
	FROM {{ template "temp_load_name" . }} AS l
	JOIN {{ $.Identifier}} AS r
	{{- range $ind, $key := $.Keys }}
		{{ if $ind }} AND {{ else }} ON  {{ end -}}
		l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
{{ else -}}
SELECT TOP 0 -1, NULL
{{ end }}
{{ end }}

-- If there are no updates to a table, we can just do a direct copy from temporary table into target table

{{ define "directCopy" }}
	INSERT INTO {{ $.Identifier }} 
		(
			{{ range $ind, $key := $.Columns }}
				{{- if $ind }}, {{ end -}}
				{{ $key.Identifier -}}
			{{- end }}
		)
	SELECT
			{{ range $ind, $key := $.Columns }}
				{{- if $ind }}, {{ end -}}
				{{ $key.Identifier -}}
			{{- end }}
	FROM {{ template "temp_store_name" . }}
{{ end }}

{{ define "mergeInto" }}
	MERGE INTO {{ $.Identifier }}
	USING (
		SELECT {{ range $ind, $key := $.Columns }}
			{{- if $ind }}, {{ end -}}
			{{ $key.Identifier -}}
		{{- end }}, _flow_delete
		FROM {{ template "temp_store_name" . }}
	) AS r
	ON {{ range $ind, $key := $.Keys }}
		{{- if $ind }} AND {{ end -}}
		{{ $.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end }}
	{{- if $.Document }}
	WHEN MATCHED AND r._flow_delete = 1 THEN
		DELETE
	{{- end }}
	WHEN MATCHED THEN
		UPDATE SET {{ range $ind, $key := $.Values }}
		{{- if $ind }}, {{ end -}}
		{{ $.Identifier }}.{{ $key.Identifier }} = r.{{ $key.Identifier }}
	{{- end -}}
	{{- if $.Document -}}
	{{ if $.Values }}, {{ end }}{{ $.Identifier }}.{{ $.Document.Identifier}} = r.{{ $.Document.Identifier }}
	{{- end }}
	WHEN NOT MATCHED AND r._flow_delete = 0 THEN
		INSERT (
		{{- range $ind, $key := $.Columns }}
			{{- if $ind }}, {{ end -}}
			{{$key.Identifier -}}
		{{- end -}}
	)
		VALUES (
		{{- range $ind, $key := $.Columns }}
			{{- if $ind }}, {{ end -}}
			r.{{$key.Identifier -}}
		{{- end -}}
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

	return templates{
		tempLoadTableName:  tplAll.Lookup("temp_load_name"),
		tempStoreTableName: tplAll.Lookup("temp_store_name"),
		tempLoadTruncate:   tplAll.Lookup("truncateTempLoadTable"),
		tempStoreTruncate:  tplAll.Lookup("truncateTempStoreTable"),
		createLoadTable:    tplAll.Lookup("createLoadTable"),
		createStoreTable:   tplAll.Lookup("createStoreTable"),
		alterTableColumns:  tplAll.Lookup("alterTableColumns"),
		createTargetTable:  tplAll.Lookup("createTargetTable"),
		directCopy:         tplAll.Lookup("directCopy"),
		mergeInto:          tplAll.Lookup("mergeInto"),
		loadInsert:         tplAll.Lookup("loadInsert"),
		loadQuery:          tplAll.Lookup("loadQuery"),
		updateFence:        tplAll.Lookup("updateFence"),
	}
}
