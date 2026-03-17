package main

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"text/template"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
)

const (
	clickhouseMinimumDate     = "1900-01-01"
	clickhouseMinimumDatetime = "1925-01-01T00:00:00Z"
)

// clickHouseClampDate is similar to sql.ClampDate but clamps to 1900-01-01 instead of year 0,
// since ClickHouse's Date32 type has a minimum value of 1900-01-01. It also returns a time.Time
// rather than a string, as required by the ClickHouse driver.
var clickHouseClampDate = sql.StringCastConverter(func(str string) (interface{}, error) {
	parsed, err := time.Parse(time.DateOnly, str)
	if err != nil {
		return nil, fmt.Errorf("could not parse %q as date: %w", str, err)
	}
	if parsed.Year() < 1900 {
		parsed, _ = time.Parse(time.DateOnly, clickhouseMinimumDate)
	}
	return parsed, nil
})

// clickHouseClampDatetime is similar to sql.ClampDatetime but clamps to 1925-01-01 instead of
// year 0, since ClickHouse's DateTime type has a minimum value of 1925-01-01. It also returns a
// time.Time rather than a string, as required by the ClickHouse driver.
var clickHouseClampDatetime = sql.StringCastConverter(func(str string) (interface{}, error) {
	if strings.HasSuffix(str, "z") {
		str = str[:len(str)-1] + "Z"
	}
	parsed, err := time.Parse(time.RFC3339Nano, str)
	if err != nil {
		return nil, fmt.Errorf("could not parse %q as RFC3339 date-time: %w", str, err)
	}
	if parsed.Year() < 1925 {
		parsed, _ = time.Parse(time.RFC3339Nano, clickhouseMinimumDatetime)
	}
	return parsed.UTC(), nil
})

var clickHouseDialect = func(database string) sql.Dialect {
	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("Int64"),
				sql.MapStatic("String", sql.UsingConverter(sql.ToStr)),
			),
			sql.NUMBER:         sql.MapStatic("Float64"),
			sql.BOOLEAN:        sql.MapStatic("Bool"),
			sql.OBJECT:         sql.MapStatic("String", sql.UsingConverter(sql.ToJsonString)),
			sql.ARRAY:          sql.MapStatic("String", sql.UsingConverter(sql.ToJsonString)),
			sql.BINARY:         sql.MapStatic("String"),
			sql.MULTIPLE:       sql.MapStatic("String", sql.UsingConverter(sql.ToJsonString)),
			sql.STRING_INTEGER: sql.MapStatic("String", sql.UsingConverter(sql.ToStr)),
			sql.STRING_NUMBER:  sql.MapStatic("Float64", sql.UsingConverter(sql.StrToFloat(math.NaN(), math.Inf(1), math.Inf(-1)))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("String"),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      sql.MapStatic("Date32", sql.UsingConverter(clickHouseClampDate)),
					"date-time": sql.MapStatic("DateTime64(6, 'UTC')", sql.UsingConverter(clickHouseClampDatetime)),
				},
				WithContentType: map[string]sql.MapProjectionFn{
					"application/x-protobuf; proto=flow.MaterializationSpec": sql.MapStatic("String"),
					"application/x-protobuf; proto=consumer.Checkpoint":      sql.MapStatic("String"),
				},
			}),
		},
		// ClickHouse columns are NOT NULL by default. We handle Nullable wrapping in templates.
	)

	return sql.Dialect{
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			return sql.InfoTableLocation{TableSchema: database, TableName: path[0]}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string { return schema }),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return field }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					return sql.IsSimpleIdentifier(s) && !slices.Contains(CLICKHOUSE_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform("`", "``"),
			))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "\\'")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			return "?"
		}),
		TypeMapper:          mapper,
		MaxColumnCharLength: 256,
	}
}

type templates struct {
	createTargetTable *template.Template
	loadQuery         *template.Template
	storeInsert       *template.Template
	alterTableColumns *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
	var tplAll = sql.MustParseTemplate(dialect, "root", `
-- Templated creation of a materialized table definition.
-- ClickHouse is an "append-only" database. We use ReplacingMergeTree so that
-- multiple versions of an Estuary key (as ClickHouse ORDER BY key) can exist
-- concurrently, with monotonically increasing values in the _version column.
-- Primary keys are deduplicated in a background ClickHouse process, but queries
-- must use the FINAL qualifier, which (1) deduplicates the order by key,
-- selecting the highest _version row per key but (2) does NOT exclude rows
-- where _is_deleted != 0.
--
-- The SETTINGS block enables automatic background CLEANUP merges:
--   allow_experimental_replacing_merge_with_cleanup: enables the CLEANUP merge
--     feature (experimental). Required for the other settings to have any effect,
--     and also enables manual: OPTIMIZE TABLE ... FINAL CLEANUP.
--   min_age_to_force_merge_seconds: minimum age (in seconds) of all parts in a
--     partition before forcing a merge. Only partitions where every part is older
--     than this threshold are eligible. 604800 = 1 week.
--   min_age_to_force_merge_on_partition_only: restricts forced merges to only run
--     when merging an entire partition into one part. Required for CLEANUP to safely
--     remove deleted rows (ensures no older versions remain).
--   enable_replacing_merge_with_cleanup_for_min_age_to_force_merge: actually enables
--     automatic background CLEANUP merges when the age threshold is met. Without this,
--     cleanup only happens via OPTIMIZE ... FINAL CLEANUP.

{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
	{{- range $ind, $col := $.Columns }}
		{{$col.Identifier}} {{ if not $col.MustExist }}Nullable({{ end }}{{$col.DDL}}{{ if not $col.MustExist }}){{ end }},
	{{- end }}
		`+"`_version`"+` UInt64,
		`+"`_is_deleted`"+` UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(`+"`_version`"+`, `+"`_is_deleted`"+`)
ORDER BY (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{ end -}}
		{{$key.Identifier}}
	{{- end -}}
)
SETTINGS
	allow_experimental_replacing_merge_with_cleanup = 1,
	min_age_to_force_merge_seconds = 604800,
	min_age_to_force_merge_on_partition_only = 1,
	enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = 1;
{{ end }}

-- Templated query which looks up existing documents by primary key using a native
-- IN (?) parameter. The caller passes a []clickhouse.GroupSet of key tuples.
-- FINAL deduplicates per-key at query time; WHERE _is_deleted=0 excludes tombstones.

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT {{$.Document.Identifier}}
	FROM {{$.Identifier}} FINAL
	WHERE `+"`_is_deleted`"+` = 0
	AND (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{ end -}}
		{{$key.Identifier}}
	{{- end -}}
	) IN (?)
{{ else -}}
SELECT * FROM (SELECT -1, NULL LIMIT 0) as nodoc
{{ end }}
{{ end }}

-- Templated INSERT for storing documents into the target table.
-- Always includes _version and _is_deleted for ReplacingMergeTree deduplication.

{{ define "storeInsert" }}
INSERT INTO {{$.Identifier}} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}}
	{{- end -}}
	, `+"`_version`"+`, `+"`_is_deleted`"+`
)
{{ end }}

-- Templated ALTER TABLE for adding columns and modifying nullable constraints.

{{ define "alterTableColumns" }}
{{- range $ind, $col := $.AddColumns }}
ALTER TABLE {{$.Identifier}} ADD COLUMN IF NOT EXISTS {{$col.Identifier}} Nullable({{$col.NullableDDL}});
{{ end -}}
{{- range $ind, $col := $.DropNotNulls }}
ALTER TABLE {{$.Identifier}} MODIFY COLUMN {{ ColumnIdentifier $col.Name }} Nullable({{$col.Type}});
{{ end -}}
{{ end }}
`)

	return templates{
		createTargetTable: tplAll.Lookup("createTargetTable"),
		loadQuery:         tplAll.Lookup("loadQuery"),
		storeInsert:       tplAll.Lookup("storeInsert"),
		alterTableColumns: tplAll.Lookup("alterTableColumns"),
	}
}
