package main

import (
	"fmt"
	"slices"
	"strings"
	"text/template"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
)

// MySQL does not allow identifiers to have trailing spaces or characters with code points greater
// than U+FFFF. (ref https://dev.mysql.com/doc/refman/8.0/en/identifiers.html). These disallowed
// characters will be sanitized with an underscore.
func translateFlowIdentifier(f string) string {
	var out string
	onlySpaces := true

	// Build the output string from the reversed input, since we need to account for trailing
	// spaces.
	for idx := len(f) - 1; idx >= 0; idx-- {
		c := f[idx]

		if c != ' ' {
			onlySpaces = false
		}

		if onlySpaces || rune(c) > 0xFFFF {
			out = "_" + out
			continue
		}
		out = string(c) + out
	}

	return out
}

func identifierSanitizer(delegate func(string) string) func(string) string {
	return func(text string) string {
		return delegate(translateFlowIdentifier(text))
	}
}

var mysqlDialect = func(tzLocation *time.Location, database string, product string, featureFlags map[string]bool) sql.Dialect {
	var jsonType = "JSON"
	var jsonMapper = sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes))
	if product == "mariadb" {
		jsonType = "LONGTEXT"
		jsonMapper = sql.MapStatic("LONGTEXT", sql.UsingConverter(sql.ToJsonString))
	}
	primaryKeyTextType := sql.MapStatic("VARCHAR(256)", sql.AlsoCompatibleWith("varchar"))

	// Define base date/time mappings without primary key wrapper
	dateMapping := sql.MapStatic("DATE")
	datetimeMapping := sql.MapStatic("DATETIME(6)", sql.AlsoCompatibleWith("datetime"), sql.UsingConverter(rfc3339ToTZ(tzLocation)))
	timeMapping := sql.MapStatic("TIME(6)", sql.AlsoCompatibleWith("time"), sql.UsingConverter(rfc3339TimeToTZ(tzLocation)))

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
				sql.MapStatic("NUMERIC(65,0)", sql.AlsoCompatibleWith("decimal")),
			),
			sql.NUMBER:  sql.MapStatic("DOUBLE PRECISION", sql.AlsoCompatibleWith("double")),
			sql.BOOLEAN: sql.MapStatic("BOOLEAN", sql.AlsoCompatibleWith("tinyint")),
			sql.OBJECT:  sql.MapStatic(jsonType),
			sql.ARRAY:   sql.MapStatic(jsonType),
			sql.BINARY: sql.MapPrimaryKey(
				sql.MapStatic("VARCHAR(256)"),
				sql.MapStatic("LONGTEXT"),
			),
			sql.MULTIPLE: jsonMapper,
			sql.STRING_INTEGER: sql.MapStringMaxLen(
				sql.MapStatic("NUMERIC(65,0)", sql.AlsoCompatibleWith("decimal"), sql.UsingConverter(sql.StrToInt)),
				sql.MapStatic("LONGTEXT", sql.UsingConverter(sql.ToStr)),
				65,
			),
			// We encode as CSV and must send MySQL string sentinels for
			// non-numeric float values.
			sql.STRING_NUMBER: sql.MapStatic("DOUBLE PRECISION", sql.AlsoCompatibleWith("double"), sql.UsingConverter(sql.StrToFloat("NaN", "+inf", "-inf"))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapPrimaryKey(
					primaryKeyTextType,
					sql.MapStatic("LONGTEXT"),
				),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      dateMapping,
					"date-time": datetimeMapping,
					"time":      timeMapping,
				},
				WithContentType: map[string]sql.MapProjectionFn{
					// The largest allowable size for a LONGBLOB is 2^32 bytes (4GB). Our stored specs and
					// checkpoints can be quite long, so we need to use as large of column size as
					// possible for these tables.
					"application/x-protobuf; proto=flow.MaterializationSpec": sql.MapStatic("LONGBLOB"),
					"application/x-protobuf; proto=consumer.Checkpoint":      sql.MapStatic("LONGBLOB"),
				},
			}),
		},
		sql.WithNotNullText("NOT NULL"),
	)

	var nocast = sql.WithCastSQL(migrationIdentifier)

	var migrationSpecs = sql.MigrationSpecs{
		"decimal":  {sql.NewMigrationSpec([]string{"double precision", "varchar", "longtext"}, nocast)},
		"bigint":   {sql.NewMigrationSpec([]string{"double precision", "numeric(65,0)", "varchar", "longtext"}, nocast)},
		"double":   {sql.NewMigrationSpec([]string{"varchar", "longtext"}, nocast)},
		"date":     {sql.NewMigrationSpec([]string{"varchar", "longtext"}, nocast)},
		"time":     {sql.NewMigrationSpec([]string{"varchar", "longtext"}, nocast)},
		"datetime": {sql.NewMigrationSpec([]string{"varchar", "longtext"}, sql.WithCastSQL(prepareDatetimeToStringCast(tzLocation)))},
	}

	// MariaDB uses longtext for JSON type, so migrating to JSON on MariaDB is not distinguishable from migrating
	// to a normal longtext
	if product != "mariadb" {
		migrationSpecs["varchar"] = []sql.MigrationSpec{sql.NewMigrationSpec([]string{"json"}, sql.WithCastSQL(jsonQuoteCast))}
		migrationSpecs["longtext"] = []sql.MigrationSpec{sql.NewMigrationSpec([]string{"json"}, sql.WithCastSQL(jsonQuoteCast))}
		migrationSpecs["*"] = []sql.MigrationSpec{sql.NewMigrationSpec([]string{"json"})}
	}

	return sql.Dialect{
		MigratableTypes: migrationSpecs,
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			// For MySQL, the table_catalog is always "def", and table_schema is the name of the
			// database. This is sort of weird, since in most other systems the table_catalog is the
			// name of the database.
			return sql.InfoTableLocation{TableSchema: database, TableName: path[0]}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string { return schema }),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return translateFlowIdentifier(field) }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			identifierSanitizer(sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(MYSQL_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform("`", "``"),
			)))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		MaxColumnCharLength:    64,
		CaseInsensitiveColumns: true,
	}
}

func formatOffset(loc *time.Location) string {
	now := time.Now().In(loc)
	_, offset := now.Zone()

	sign := "+"
	if offset < 0 {
		sign = "-"
		offset = -offset
	}
	hours := offset / 3600
	minutes := (offset % 3600) / 60
	return fmt.Sprintf("%s%02d:%02d", sign, hours, minutes)
}

// by default we don't want to do `CAST(%s AS %s)` for MySQL
func migrationIdentifier(migration sql.ColumnTypeMigration) string {
	return migration.Identifier
}

func jsonQuoteCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`JSON_QUOTE(%s)`, migration.Identifier)
}

func prepareDatetimeToStringCast(loc *time.Location) sql.CastSQLFunc {
	return func(migration sql.ColumnTypeMigration) string {
		return fmt.Sprintf(`DATE_FORMAT(CONVERT_TZ(%s, '%s', '+00:00'), '%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ')`, migration.Identifier, formatOffset(loc))
	}
}

func rfc3339ToTZ(loc *time.Location) sql.ElementConverter {
	return sql.StringCastConverter(func(str string) (interface{}, error) {
		// sanity check, this should not happen
		if loc == nil {
			return nil, fmt.Errorf("no timezone has been specified either in server or in connector configuration, cannot materialize date-time field. Consider setting a timezone in your database or in the connector configuration to continue")
		}

		if t, err := time.Parse(time.RFC3339Nano, str); err != nil {
			return nil, fmt.Errorf("could not parse %q as RFC3339 date-time: %w", str, err)
		} else {
			return t.In(loc).Format("2006-01-02T15:04:05.999999999"), nil
		}
	})
}

func rfc3339TimeToTZ(loc *time.Location) sql.ElementConverter {
	return sql.StringCastConverter(func(str string) (any, error) {
		// sanity check, this should not happen
		if loc == nil {
			return nil, fmt.Errorf("no timezone has been specified either in server or in connector configuration, cannot materialize time field: Consider setting a timezone in your database or in the connector configuration to continue")
		}

		normalized := strings.ReplaceAll(str, "z", "Z")
		formats := []string{
			"15:04:05.999999999Z07:00",
			"15:04:05.999999999Z",
			"15:04:05Z",
		}

		var t time.Time
		var err error
		for _, format := range formats {
			if t, err = time.Parse(format, normalized); err == nil {
				break
			}
		}
		if err != nil {
			return nil, fmt.Errorf("could not parse %q as time: %w", str, err)
		}

		return t.In(loc).Format("15:04:05.999999999"), nil
	})
}

type templates struct {
	tempTableName           *template.Template
	tempTruncate            *template.Template
	createLoadTable         *template.Template
	createUpdateTable       *template.Template
	createDeleteTable       *template.Template
	createTargetTable       *template.Template
	alterTableColumns       *template.Template
	updateLoad              *template.Template
	updateReplace           *template.Template
	updateTruncate          *template.Template
	insertLoad              *template.Template
	loadQuery               *template.Template
	loadQueryNoFlowDocument *template.Template
	loadLoad                *template.Template
	deleteLoad              *template.Template
	deleteQuery             *template.Template
	deleteTruncate          *template.Template
	installFence            *template.Template
	updateFence             *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
	var tplAll = sql.MustParseTemplate(dialect, "root", `
{{ define "temp_load_name" -}}
flow_temp_load_table_{{ $.Binding }}
{{- end }}

{{ define "temp_update_name" -}}
flow_temp_update_table_{{ $.Binding }}
{{- end }}

{{ define "temp_delete_name" -}}
flow_temp_delete_table_{{ $.Binding }}
{{- end }}

-- Templated creation of a materialized table definition and comments:

{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}} {{$col.DDL}} {{- if $col.Comment }} COMMENT {{Literal $col.Comment}}{{- end }}
	{{- end }}
	{{- if not $.DeltaUpdates }},

		PRIMARY KEY (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
	{{- end -}}
	)
	{{- end }}
) CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin {{- if $.Comment }} COMMENT={{Literal $.Comment}} {{- end }};
{{ end }}

-- Templated query which performs table alterations by adding columns and/or
-- dropping nullability constraints. All table modifications are done in a 
-- single statement for efficiency.

{{ define "alterTableColumns" }}
ALTER TABLE {{$.Identifier}}
{{- range $ind, $col := $.AddColumns }}
	{{- if $ind }},{{ end }}
	ADD COLUMN {{$col.Identifier}} {{$col.NullableDDL}}
{{- end }}
{{- if and $.DropNotNulls $.AddColumns}},{{ end }}
{{- range $ind, $col := $.DropNotNulls }}
	{{- if $ind }},{{ end }}
	MODIFY {{ ColumnIdentifier $col.Name }} {{$col.Type}}
{{- end }};
{{ end }}

-- Templated creation of a temporary load table:

{{ define "createLoadTable" }}
CREATE TEMPORARY TABLE {{ template "temp_load_name" . }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{ $key.Identifier }} {{ $key.DDL }}
	{{- end }}
	,
		PRIMARY KEY (
		{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
		{{- end -}}
	)
) CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin;
{{ end }}

-- Templated truncation of the temporary load table:

{{ define "truncateTempTable" }}
TRUNCATE {{ template "temp_load_name" . }};
{{ end }}

-- Templated load into the temporary load table:

{{ define "loadLoad" }}
LOAD DATA LOCAL INFILE 'Reader::flow_batch_data_load' INTO TABLE {{ template "temp_load_name" . }} CHARACTER SET utf8mb4
	FIELDS
		TERMINATED BY ','
		OPTIONALLY ENCLOSED BY '"'
		ESCAPED BY ''
	LINES
		TERMINATED BY '\n'
(
	{{- range $ind, $col := $.Keys }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
);
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
SELECT * FROM (SELECT -1, NULL LIMIT 0) as nodoc
{{ end }}
{{ end }}

-- Templated query for no_flow_document feature flag - reconstructs JSON from root-level columns

{{ define "loadQueryNoFlowDocument" }}
SELECT {{ $.Binding }}, 
JSON_OBJECT(
{{- range $i, $col := $.RootLevelColumns}}
	{{- if $i}},{{end}}
    {{Literal $col.Field}}, r.{{$col.Identifier}}
{{- end}}
) as flow_document
FROM {{ $.Identifier}} AS r
JOIN {{ template "temp_load_name" . }} AS l
{{- range $ind, $key := $.Keys }}
	{{ if $ind }} AND {{ else }} ON  {{ end -}}
	l.{{ $key.Identifier }} = r.{{ $key.Identifier }}
{{- end }}
{{ end }}

-- Template to load data into target table

{{ define "insertLoad" }}
LOAD DATA LOCAL INFILE 'Reader::flow_batch_data_insert' INTO TABLE {{ $.Identifier }} CHARACTER SET utf8mb4
	FIELDS
		TERMINATED BY ','
		OPTIONALLY ENCLOSED BY '"'
		ESCAPED BY ''
	LINES
		TERMINATED BY '\n'
(
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
);
{{ end }}

{{ define "createUpdateTable" }}
CREATE TEMPORARY TABLE {{ template "temp_update_name" . }} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}} {{$col.DDL}}
	{{- end }}
	,
	PRIMARY KEY (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
	{{- end -}}
	)
) CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin;
{{ end }}

{{ define "updateLoad" }}
LOAD DATA LOCAL INFILE 'Reader::flow_batch_data_update' INTO TABLE {{ template "temp_update_name" . }} CHARACTER SET utf8mb4
	FIELDS
		TERMINATED BY ','
		OPTIONALLY ENCLOSED BY '"'
		ESCAPED BY ''
	LINES
		TERMINATED BY '\n'
(
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
);
{{ end }}

{{ define "updateReplace" }}
REPLACE INTO {{ $.Identifier }}
(
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
)
SELECT
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
FROM {{ template "temp_update_name" . }};
{{ end }}


{{ define "truncateUpdateTable" }}
TRUNCATE {{ template "temp_update_name" . }};
{{ end }}


{{ define "createDeleteTable" }}
CREATE TEMPORARY TABLE {{ template "temp_delete_name" . }} (
	{{- range $ind, $col := $.Keys }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}} {{$col.DDL}}
	{{- end }}
	,
	PRIMARY KEY (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{end -}}
		{{$key.Identifier}}
	{{- end -}}
	)
) CHARACTER SET=utf8mb4 COLLATE=utf8mb4_bin;
{{ end }}

{{ define "deleteLoad" }}
LOAD DATA LOCAL INFILE 'Reader::flow_batch_data_delete' INTO TABLE {{ template "temp_delete_name" . }} CHARACTER SET utf8mb4
	FIELDS
		TERMINATED BY ','
		OPTIONALLY ENCLOSED BY '"'
		ESCAPED BY ''
	LINES
		TERMINATED BY '\n'
(
	{{- range $ind, $col := $.Keys }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}}
	{{- end }}
);
{{ end }}

{{ define "deleteQuery" }}
DELETE original FROM {{ $.Identifier }} original, {{ template "temp_delete_name" . }} temp
WHERE 
	{{- range $ind, $col := $.Keys }}
		{{- if $ind }} AND {{ end }}
		original.{{$col.Identifier}} = temp.{{$col.Identifier}}
	{{- end -}}
;
{{ end }}

{{ define "truncateDeleteTable" }}
TRUNCATE {{ template "temp_delete_name" . }};
{{ end }}

{{ define "installFence" }}
with
-- Increment the fence value of _any_ checkpoint which overlaps our key range.
update_covered as (
	update {{ Identifier $.TablePath }}
		set   fence = fence + 1
		where materialization = {{ Literal $.Materialization.String }}
		and   key_end >= {{ $.KeyBegin }}
		and   key_begin <= {{ $.KeyEnd }}
	returning *
),
-- Read the checkpoint with the narrowest [key_begin, key_end] which fully overlaps our range.
best_match as (
	select materialization, key_begin, key_end, fence, checkpoint from update_covered
		where materialization = {{ Literal $.Materialization.String }}
		and 	key_begin <= {{ $.KeyBegin }}
		and   key_end >= {{ $.KeyEnd }}
		order by key_end - key_begin asc
		limit 1
),
-- Install a new checkpoint if best_match is not an exact match.
install_new as (
	insert into {{ Identifier $.TablePath }} (materialization, key_begin, key_end, fence, checkpoint)
		-- Case: best_match is a non-empty covering span but not an exact match
		select {{ Literal $.Materialization.String }}, {{ $.KeyBegin}}, {{ $.KeyEnd }}, fence, checkpoint
			from best_match where key_begin != {{ $.KeyBegin }} or key_end != {{ $.KeyEnd }}
		union all
		-- Case: best_match is empty
		select {{ Literal $.Materialization.String }}, {{ $.KeyBegin}}, {{ $.KeyEnd }}, {{ $.Fence }}, {{ Literal (Base64Std $.Checkpoint) }}
			where (select count(*) from best_match) = 0
	returning *
)
select fence, decode(checkpoint, 'base64') from install_new
union all
select fence, decode(checkpoint, 'base64') from best_match
limit 1
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

	return templates{
		tempTableName:           tplAll.Lookup("temp_load_name"),
		tempTruncate:            tplAll.Lookup("truncateTempTable"),
		createLoadTable:         tplAll.Lookup("createLoadTable"),
		createUpdateTable:       tplAll.Lookup("createUpdateTable"),
		createTargetTable:       tplAll.Lookup("createTargetTable"),
		createDeleteTable:       tplAll.Lookup("createDeleteTable"),
		alterTableColumns:       tplAll.Lookup("alterTableColumns"),
		updateLoad:              tplAll.Lookup("updateLoad"),
		updateReplace:           tplAll.Lookup("updateReplace"),
		updateTruncate:          tplAll.Lookup("truncateUpdateTable"),
		insertLoad:              tplAll.Lookup("insertLoad"),
		deleteLoad:              tplAll.Lookup("deleteLoad"),
		deleteQuery:             tplAll.Lookup("deleteQuery"),
		deleteTruncate:          tplAll.Lookup("truncateDeleteTable"),
		loadQuery:               tplAll.Lookup("loadQuery"),
		loadQueryNoFlowDocument: tplAll.Lookup("loadQueryNoFlowDocument"),
		loadLoad:                tplAll.Lookup("loadLoad"),
		installFence:            tplAll.Lookup("installFence"),
		updateFence:             tplAll.Lookup("updateFence"),
	}
}

const varcharTableAlter = "ALTER TABLE %s MODIFY COLUMN %s VARCHAR(%d);"
