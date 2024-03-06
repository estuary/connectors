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

var mysqlDialect = func(tzLocation *time.Location, database string) sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.INTEGER: sql.NewStaticMapper("BIGINT"),
		sql.NUMBER:  sql.NewStaticMapper("DOUBLE PRECISION"),
		sql.BOOLEAN: sql.NewStaticMapper("BOOLEAN"),
		sql.OBJECT:  sql.NewStaticMapper("JSON"),
		sql.ARRAY:   sql.NewStaticMapper("JSON"),
		sql.BINARY:  sql.NewStaticMapper("LONGBLOB"),
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.PrimaryKeyMapper{
				PrimaryKey: sql.NewStaticMapper("VARCHAR(256)"),
				Delegate:   sql.NewStaticMapper("LONGTEXT"),
			},
			WithFormat: map[string]sql.TypeMapper{
				"integer": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("VARCHAR(256)"),
					Delegate:   sql.NewStaticMapper("NUMERIC(65,0)", sql.WithElementConverter(sql.StdStrToInt())),
				},
				"number": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("VARCHAR(256)"),
					// We encode as CSV and must send MySQL string sentinels.
					Delegate: sql.NewStaticMapper("DOUBLE PRECISION", sql.WithElementConverter(sql.StdStrToFloat("NaN", "+inf", "-inf"))),
				},
				"date":      sql.NewStaticMapper("DATE"),
				"date-time": sql.NewStaticMapper("DATETIME(6)", sql.WithElementConverter(rfc3339ToTZ(tzLocation))),
				"time":      sql.NewStaticMapper("TIME(6)", sql.WithElementConverter(rfc3339TimeToTZ(tzLocation))),
			},
			WithContentType: map[string]sql.TypeMapper{
				// The largest allowable size for a LONGBLOB is 2^32 bytes (4GB). Our stored specs and
				// checkpoints can be quite long, so we need to use as large of column size as
				// possible for these tables.
				"application/x-protobuf; proto=flow.MaterializationSpec": sql.NewStaticMapper("LONGBLOB"),
				"application/x-protobuf; proto=consumer.Checkpoint":      sql.NewStaticMapper("LONGBLOB"),
			},
		},
		sql.MULTIPLE: sql.NewStaticMapper("JSON", sql.WithElementConverter(sql.JsonBytesConverter)),
	}

	mapper = sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    mapper,
	}

	columnValidator := sql.NewColumnValidator(
		// "decimal" is for NUMERIC(65,0) that is used for { type: string, format: integer }
		sql.ColValidation{Types: []string{"bigint", "decimal"}, Validate: sql.IntegerCompatible},
		sql.ColValidation{Types: []string{"double"}, Validate: sql.NumberCompatible},
		sql.ColValidation{Types: []string{"tinyint"}, Validate: sql.BooleanCompatible},
		sql.ColValidation{Types: []string{"json"}, Validate: sql.JsonCompatible},
		sql.ColValidation{Types: []string{"varchar", "longtext"}, Validate: sql.StringCompatible},
		sql.ColValidation{Types: []string{"date"}, Validate: sql.DateCompatible},
		sql.ColValidation{Types: []string{"datetime"}, Validate: sql.DateTimeCompatible},
		sql.ColValidation{Types: []string{"time"}, Validate: sql.TimeCompatible},
	)

	return sql.Dialect{
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			// For MySQL, the table_catalog is always "def", and table_schema is the name of the
			// database. This is sort of weird, since in most other systems the table_catalog is the
			// name of the database.
			return sql.InfoTableLocation{TableSchema: database, TableName: path[0]}
		}),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return translateFlowIdentifier(field) }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			identifierSanitizer(sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(MYSQL_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform("`", "``"),
			)))),
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		ColumnValidator:        columnValidator,
		MaxColumnCharLength:    64,
		CaseInsensitiveColumns: true,
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
	return sql.StringCastConverter(func(str string) (interface{}, error) {
		// sanity check, this should not happen
		if loc == nil {
			return nil, fmt.Errorf("no timezone has been specified either in server or in connector configuration, cannot materialize time field: Consider setting a timezone in your database or in the connector configuration to continue")
		}

		if t, err := time.Parse("15:04:05.999999999Z07:00", str); err != nil {
			return nil, fmt.Errorf("could not parse %q as RFC3339 time: %w", str, err)
		} else {
			return t.In(loc).Format("15:04:05.999999999"), nil
		}
	})
}

type templates struct {
	tempTableName     *template.Template
	tempTruncate      *template.Template
	createLoadTable   *template.Template
	createUpdateTable *template.Template
	createTargetTable *template.Template
	alterTableColumns *template.Template
	updateLoad        *template.Template
	updateReplace     *template.Template
	updateTruncate    *template.Template
	insertLoad        *template.Template
	loadQuery         *template.Template
	loadLoad          *template.Template
	installFence      *template.Template
	updateFence       *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
	var tplAll = sql.MustParseTemplate(dialect, "root", `
{{ define "temp_load_name" -}}
flow_temp_load_table_{{ $.Binding }}
{{- end }}

{{ define "temp_update_name" -}}
flow_temp_update_table_{{ $.Binding }}
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
LOAD DATA LOCAL INFILE 'Reader::flow_batch_data_load' INTO TABLE {{ template "temp_load_name" . }}
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
SELECT * FROM (SELECT -1, CAST(NULL AS JSON) LIMIT 0) as nodoc
{{ end }}
{{ end }}

-- Template to load data into target table

{{ define "insertLoad" }}
LOAD DATA LOCAL INFILE 'Reader::flow_batch_data_insert' INTO TABLE {{ $.Identifier }}
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
LOAD DATA LOCAL INFILE 'Reader::flow_batch_data_update' INTO TABLE {{ template "temp_update_name" . }}
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
		tempTableName:     tplAll.Lookup("temp_load_name"),
		tempTruncate:      tplAll.Lookup("truncateTempTable"),
		createLoadTable:   tplAll.Lookup("createLoadTable"),
		createUpdateTable: tplAll.Lookup("createUpdateTable"),
		createTargetTable: tplAll.Lookup("createTargetTable"),
		alterTableColumns: tplAll.Lookup("alterTableColumns"),
		updateLoad:        tplAll.Lookup("updateLoad"),
		updateReplace:     tplAll.Lookup("updateReplace"),
		updateTruncate:    tplAll.Lookup("truncateUpdateTable"),
		insertLoad:        tplAll.Lookup("insertLoad"),
		loadQuery:         tplAll.Lookup("loadQuery"),
		loadLoad:          tplAll.Lookup("loadLoad"),
		installFence:      tplAll.Lookup("installFence"),
		updateFence:       tplAll.Lookup("updateFence"),
	}
}

const varcharTableAlter = "ALTER TABLE %s MODIFY COLUMN %s VARCHAR(%d);"
