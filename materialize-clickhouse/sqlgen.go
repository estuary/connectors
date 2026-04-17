package main

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	log "github.com/sirupsen/logrus"
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
	str = strings.ToUpper(str)
	parsed, err := time.Parse(time.RFC3339Nano, str)
	if err != nil {
		return nil, fmt.Errorf("could not parse %q as RFC3339 date-time: %w", str, err)
	}
	if parsed.Year() < 1925 {
		parsed, _ = time.Parse(time.RFC3339Nano, clickhouseMinimumDatetime)
	}
	return parsed.UTC(), nil
})

var toBigInt = func(e tuple.TupleElement) (interface{}, error) {
	if e == nil {
		return nil, nil
	}
	var v big.Int
	switch q := e.(type) {
	case int64:
		v.SetInt64(q)
	case int:
		v.SetInt64(int64(q))
	case uint64:
		v.SetUint64(q)
	case uint:
		v.SetUint64(uint64(q))
	case float64:
		big.NewFloat(q).Int(&v)
	case big.Int:
		v.Set(&q)
	case *big.Int:
		v.Set(q)
	case string:
		if _, ok := v.SetString(q, 10); !ok {
			// Handle strings with decimal points like "1.0" by truncating.
			var f big.Float
			if _, ok := f.SetString(q); !ok {
				return nil, fmt.Errorf("cannot parse %q as big.Int or big.Float", q)
			}
			f.Int(&v)
		}
	default:
		return nil, fmt.Errorf("cannot convert %T to big.Int", q)
	}
	return v, nil
}

// mapSignedIntegers uses an alternate mapping for numeric integer fields where
// numeric inference is available, and the minimum or maximum of the inferred
// range is outside the bounds of what will fit in a signed 64 bit integer.
func mapSignedIntegers() sql.MapProjectionFn {
	// Int128 spans [-2^127, 2^127 - 1]; Int256 spans [-2^255, 2^255 - 1].
	// Convert through big.Int since these don't fit in int64 literals; float64
	// precision is sufficient because inferred Numeric.Minimum/Maximum are
	// themselves carried as float64 powers of 10.
	halfInt128 := new(big.Int).Lsh(big.NewInt(1), 127)
	maxInt128, _ := new(big.Int).Sub(halfInt128, big.NewInt(1)).Float64()
	minInt128, _ := new(big.Int).Neg(halfInt128).Float64()
	halfInt256 := new(big.Int).Lsh(big.NewInt(1), 255)
	maxInt256, _ := new(big.Int).Sub(halfInt256, big.NewInt(1)).Float64()
	minInt256, _ := new(big.Int).Neg(halfInt256).Float64()

	fn64 := sql.MapStatic("Int64", sql.UsingConverter(sql.StrToInt))
	fn128 := sql.MapStatic("Int128", sql.UsingConverter(toBigInt))
	fn256 := sql.MapStatic("Int256", sql.UsingConverter(toBigInt))
	fnString := sql.MapStatic("String", sql.UsingConverter(sql.ToStr))

	return func(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
		// STRING_INTEGER projections (format: "integer") route by declared
		// maxLength — values arrive as JSON strings and can exceed the
		// precision that Numeric.Minimum/Maximum can carry in float64.
		// MaxLength == 0 means no length was declared; fall back to the
		// widest bounded integer rather than the narrowest.
		if p.Inference.String_ != nil && p.Inference.String_.Format == "integer" {
			switch maxLen := p.Inference.String_.MaxLength; {
			case maxLen == 0:
				return fn256(p)
			case maxLen <= 18:
				return fn64(p)
			case maxLen <= 38:
				return fn128(p)
			case maxLen <= 76:
				return fn256(p)
			default:
				return fnString(p)
			}
		}

		// INTEGER projections dispatch by the inferred numeric range.
		// Numeric Minimum and Maximums are stated as powers of 10, so
		// comparisons to exact integer values aren't precise, but this
		// logic works out since 1e19 is the next power of 10 past int64
		// max, 1e39 past int128, and 1e77 past int256.
		if p.Inference.Numeric != nil {
			switch {
			case p.Inference.Numeric.Minimum >= math.MinInt64 && p.Inference.Numeric.Maximum <= math.MaxInt64:
				return fn64(p)
			case p.Inference.Numeric.Minimum >= minInt128 && p.Inference.Numeric.Maximum <= maxInt128:
				return fn128(p)
			case p.Inference.Numeric.Minimum >= minInt256 && p.Inference.Numeric.Maximum <= maxInt256:
				return fn256(p)
			default:
				return fnString(p)
			}
		}

		// No inference available — default to the narrowest integer type,
		// matching the legacy MapSignedInt64 behavior.
		return fn64(p)
	}
}

var clickHouseDialect = func(database string) sql.Dialect {
	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER:        mapSignedIntegers(),
			sql.NUMBER:         sql.MapStatic("Float64"),
			sql.BOOLEAN:        sql.MapStatic("Bool"),
			sql.OBJECT:         sql.MapStatic("String", sql.UsingConverter(sql.ToJsonString)),
			sql.ARRAY:          sql.MapStatic("String", sql.UsingConverter(sql.ToJsonString)),
			sql.BINARY:         sql.MapStatic("String"),
			sql.MULTIPLE:       sql.MapStatic("String", sql.UsingConverter(sql.ToJsonString)),
			sql.STRING_INTEGER: mapSignedIntegers(),
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

	simpleIdentifier := regexp.MustCompile(`^[a-zA-Z_][0-9a-zA-Z_]*$`)

	return sql.Dialect{
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			return sql.InfoTableLocation{TableSchema: database, TableName: path[0]}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string { return schema }),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return field }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(
				func(s string) bool {
					// Identifiers can contain any special characters, wrapping with backticks
					// is required in those cases. Backtick is the only escaped character.
					// https://clickhouse.com/docs/sql-reference/syntax#identifiers
					return simpleIdentifier.MatchString(s) && !slices.Contains(CLICKHOUSE_RESERVED_WORDS, strings.ToLower(s))
				},
				sql.QuoteTransform("`", "``"),
			))),
		// String literals can be much more complicated, but
		// backslashes and single-quotes cover our needs.
		// https://clickhouse.com/docs/sql-reference/syntax#string
		Literaler:           sql.ToLiteralFn(sql.QuoteTransformEscapedBackslash("'", "\\'")),
		Placeholderer:       sql.PlaceholderFn(func(index int) string { return "?" }),
		TypeMapper:          mapper,
		MaxColumnCharLength: 256,
	}
}

type templates struct {
	createTargetTable            *template.Template
	alterTargetColumns           *template.Template
	createLoadTable              *template.Template
	insertLoadTable              *template.Template
	queryLoadTable               *template.Template
	queryLoadTableNoFlowDocument *template.Template
	dropLoadTable                *template.Template
	createStoreTable             *template.Template
	insertStoreTable             *template.Template
	queryStoreParts              *template.Template
	moveStorePartition           *template.Template
	existsStoreTable             *template.Template
	dropStoreTable               *template.Template
}

func renderTemplates(dialect sql.Dialect, hardDelete bool) templates {
	var isDeletedColumn string
	var isDeletedEngineArg string
	var isDeletedInsert string
	if hardDelete {
		isDeletedColumn = ",\n\t\t_is_deleted UInt8 DEFAULT 0"
		isDeletedEngineArg = ", _is_deleted"
		isDeletedInsert = ", _is_deleted"
	}

	var tplAll = sql.MustParseTemplate(dialect, "root", `
---- Target tables

-- ClickHouse is an "append-only" database. When DeltaUpdates is enabled, we use
-- a plain MergeTree: every Store is appended as-is with no deduplication or
-- deletion — rows accumulate and are never removed.
--
-- When DeltaUpdates is disabled (standard mode), we use ReplacingMergeTree so
-- that multiple versions of an Estuary key (as ClickHouse ORDER BY key) can
-- exist concurrently, with monotonically increasing flow_published_at timestamps.
-- Primary keys are deduplicated in a background ClickHouse process, but queries
-- must use the FINAL qualifier, which deduplicates per ORDER BY key (selecting
-- the highest flow_published_at record) and excludes rows where _is_deleted = 1.
--
-- The SETTINGS block enables automatic background CLEANUP merges:
--   allow_experimental_replacing_merge_with_cleanup: enables the CLEANUP merge
--     feature (experimental). Required for the other settings to have any effect,
--     and also enables manual: OPTIMIZE TABLE ... FINAL CLEANUP.
--     Added in ClickHouse 23.12 and 24.1.
--   min_age_to_force_merge_seconds: minimum age (in seconds) of all parts in a
--     partition before forcing a merge. Only partitions where every part is older
--     than this threshold are eligible. 604800 = 1 week.
--     Added in ClickHouse 22.10.
--   min_age_to_force_merge_on_partition_only: restricts forced merges to only run
--     when merging an entire partition into one part. Required for CLEANUP to safely
--     remove deleted rows (ensures no older versions remain).
--     Added in ClickHouse 22.10.
--   enable_replacing_merge_with_cleanup_for_min_age_to_force_merge: actually enables
--     automatic background CLEANUP merges when the age threshold is met. Without this,
--     cleanup only happens via OPTIMIZE ... FINAL CLEANUP.
--     Added in ClickHouse 25.3.

{{ define "createTargetTable" }}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}} {{ if not $col.MustExist }}Nullable({{ end }}{{$col.DDL}}{{ if not $col.MustExist }}){{ end }}
	{{- end -}}
    {{ if not $.DeltaUpdates }}`+isDeletedColumn+`{{ end }}
)
{{ if $.DeltaUpdates -}}
ENGINE = MergeTree
{{ else -}}
ENGINE = ReplacingMergeTree(flow_published_at`+isDeletedEngineArg+`)
{{ end -}}
ORDER BY (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{ end -}}
		{{$key.Identifier}}
	{{- end -}}
)
{{ if not $.DeltaUpdates -}}
SETTINGS
	allow_experimental_replacing_merge_with_cleanup = 1,
	min_age_to_force_merge_seconds = 604800,
	min_age_to_force_merge_on_partition_only = 1,
	enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = 1
{{- end -}}
;
{{ end }}

{{ define "alterTargetColumns" }}
{{- range $ind, $col := $.AddColumns }}
ALTER TABLE {{$.Identifier}} ADD COLUMN IF NOT EXISTS {{$col.Identifier}} Nullable({{$col.NullableDDL}});
{{ end -}}
{{- range $ind, $col := $.DropNotNulls }}
ALTER TABLE {{$.Identifier}} MODIFY COLUMN {{ ColumnIdentifier $col.Name }} Nullable({{$col.Type}});
{{ end -}}
{{ end }}

---- Load tables

-- Load tables stage the keys of documents the connector needs to look up during a
-- transaction's load phase. The connector inserts keys into the staging load table,
-- then joins the target table (using FINAL for deduplication) against the staging
-- table to retrieve the current version of documents for those keys.
--
-- Load tables use the MergeTree engine so that large key sets are spilled to disk
-- rather than held entirely in memory, avoiding server memory limit issues.
--
-- CREATE OR REPLACE TABLE is used so that the table is reset on connector restart,
-- discarding any stale state from a previous run. The table is dropped when the
-- connector shuts down.

{{ define "loadTableName" -}}
{{ printf "flow_temp_load_%s_%s" $.RangeKey (index $.Path 0) | ColumnIdentifier }}
{{- end }}

{{ define "createLoadTable" }}
CREATE OR REPLACE TABLE {{ template "loadTableName" . }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }},{{ end }}
		{{ $key.Identifier }} {{ $key.DDL }}
	{{- end }}
)
ENGINE = MergeTree
ORDER BY (
	{{- range $ind, $key := $.Keys -}}
		{{- if $ind }}, {{ end -}}
		{{ $key.Identifier }}
	{{- end -}}
);
{{ end }}

{{ define "insertLoadTable" }}
INSERT INTO {{ template "loadTableName" . }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{ end -}}
		{{$key.Identifier}}
	{{- end -}}
)
{{ end }}

{{ define "queryLoadTable" }}
{{ if $.Document -}}
SELECT {{ $.Binding }}::Int32, r.{{$.Document.Identifier}}
	FROM {{$.Identifier}} AS r FINAL
	JOIN {{ template "loadTableName" . }} AS l
	{{- range $ind, $key := $.Keys }}
		{{ if $ind }} AND {{ else }} ON {{ end -}}
		l.{{$key.Identifier}} = r.{{$key.Identifier}}
	{{- end }}
{{ end -}}
{{ end }}

-- Templated query for no_flow_document mode - reconstructs JSON from root-level
-- columns using concat(). Each column value is individually serialized:
--   - OBJECT, ARRAY, MULTIPLE: stored as String containing valid JSON, embedded
--     directly via ifNull(col, 'null') so they appear as proper JSON
--     objects/arrays rather than double-encoded strings.
--   - DateTime64: formatDateTime produces RFC3339, wrapped in toJSONString for
--     proper quoting.
--   - STRING_NUMBER: stored as Float64 but must appear as a JSON string;
--     toString() converts before toJSONString quotes it.
--   - All other types: toJSONString handles correct JSON serialization
--     (quoting strings, bare numbers/bools).
-- Nullable columns use ifNull(..., 'null') to emit JSON null.

{{ define "concatValue" -}}
{{ $ident := printf "%s.%s" $.Alias $.Identifier }}
{{- if or (eq $.AsFlatType "object") (eq $.AsFlatType "array") (eq $.AsFlatType "multiple") -}}
	ifNull({{ $ident }}, 'null')
{{- else if and (eq $.AsFlatType "string") (eq $.Format "date-time") -}}
	ifNull(toJSONString(formatDateTime({{ $ident }}, '%Y-%m-%dT%H:%i:%S.%f', 'UTC') || 'Z'), 'null')
{{- else if eq $.AsFlatType "string_number" -}}
	ifNull(toJSONString(toString({{ $ident }})), 'null')
{{- else -}}
	ifNull(toJSONString({{ $ident }}), 'null')
{{- end -}}
{{- end }}

{{ define "queryLoadTableNoFlowDocument" }}
{{ if not $.DeltaUpdates -}}
SELECT {{ $.Binding }}::Int32,
concat('{',
{{- range $i, $col := $.RootLevelColumns -}}
	{{- if $i }}, ',',{{ end }}
	'"{{ $col.Field }}":', {{ template "concatValue" (ColumnWithAlias $col "r") }}
{{- end }}
, '}') AS flow_document
	FROM {{$.Identifier}} AS r FINAL
	JOIN {{ template "loadTableName" . }} AS l
	{{- range $ind, $key := $.Keys }}
		{{ if $ind }} AND {{ else }} ON {{ end -}}
		l.{{$key.Identifier}} = r.{{$key.Identifier}}
	{{- end }}
{{ else -}}
SELECT * FROM (SELECT -1::Int32, ''::String LIMIT 0) as nodoc
{{ end -}}
{{ end }}

{{ define "dropLoadTable" }}
DROP TABLE IF EXISTS {{ template "loadTableName" . }};
{{ end }}

---- Store tables

-- Store tables stage documents written during a transaction's store phase. They are
-- created with CREATE TABLE ... AS, which clones the target table's schema and engine
-- (ReplacingMergeTree), so that their on-disk parts are partition-compatible with the
-- target table.
--
-- Documents are inserted into the store table during the store phase. On commit, the
-- connector enumerates the store table's parts via system.parts and moves each
-- partition to the target table using ALTER TABLE ... MOVE PARTITION. Moving a
-- partition is a metadata-only operation (it relinks existing parts rather than
-- copying data), so the commit phase is very low latency regardless of transaction
-- size. This is significantly faster than INSERT ... SELECT or other bulk-copy
-- methods that would rewrite data.
--
-- The commit is not atomic across bindings: partitions are moved one at a time and a
-- failure mid-commit will leave some partitions moved and others not. This is safe
-- because the target table uses ReplacingMergeTree, which deduplicates by ORDER BY
-- key and flow_published_at version — re-applying a partial commit is idempotent.
--
-- CREATE OR REPLACE TABLE resets the store table on connector restart. The table is
-- truncated between transactions and dropped when the connector shuts down.

{{ define "storeTableNameIdentifier" -}}
{{ printf "flow_temp_store_%s_%s" $.RangeKey (index $.Path 0) | ColumnIdentifier }}
{{- end }}

{{ define "storeTableNameString" -}}
{{ printf "flow_temp_store_%s_%s" $.RangeKey (index $.Path 0) | Literal }}
{{- end }}

{{ define "createStoreTable" }}
CREATE OR REPLACE TABLE {{ template "storeTableNameIdentifier" . }}
AS {{$.Identifier}};
{{ end }}

{{ define "insertStoreTable" }}
INSERT INTO {{ template "storeTableNameIdentifier" . }} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}}
	{{- end -}}
    {{- if not $.DeltaUpdates }}`+isDeletedInsert+`{{ end -}}
)
{{ end }}

{{ define "queryStoreParts" }}
SELECT DISTINCT partition_id FROM system.parts
WHERE table = {{ template "storeTableNameString" . }}
  AND database = ? AND active;
{{ end }}

{{ define "moveStorePartition" }}
ALTER TABLE {{ template "storeTableNameIdentifier" . }}
MOVE PARTITION ID ?
TO TABLE {{ $.Identifier }};
{{ end }}

{{ define "existsStoreTable" }}
SELECT count() FROM system.tables
WHERE database = currentDatabase()
  AND name = {{ template "storeTableNameString" . }};
{{ end }}

{{ define "dropStoreTable" }}
DROP TABLE IF EXISTS {{ template "storeTableNameIdentifier" . }};
{{ end }}
`)

	return templates{
		createTargetTable:            tplAll.Lookup("createTargetTable"),
		alterTargetColumns:           tplAll.Lookup("alterTargetColumns"),
		createLoadTable:              tplAll.Lookup("createLoadTable"),
		insertLoadTable:              tplAll.Lookup("insertLoadTable"),
		queryLoadTable:               tplAll.Lookup("queryLoadTable"),
		queryLoadTableNoFlowDocument: tplAll.Lookup("queryLoadTableNoFlowDocument"),
		dropLoadTable:                tplAll.Lookup("dropLoadTable"),
		createStoreTable:             tplAll.Lookup("createStoreTable"),
		insertStoreTable:             tplAll.Lookup("insertStoreTable"),
		queryStoreParts:              tplAll.Lookup("queryStoreParts"),
		moveStorePartition:           tplAll.Lookup("moveStorePartition"),
		existsStoreTable:             tplAll.Lookup("existsStoreTable"),
		dropStoreTable:               tplAll.Lookup("dropStoreTable"),
	}
}

func renderTableAndRangeKey(table sql.Table, rangeKey uint32, tpl *template.Template) (rendered string, err error) {
	v := struct {
		sql.Table
		RangeKey string
	}{
		Table:    table,
		RangeKey: strconv.FormatInt(int64(rangeKey), 16),
	}

	var w strings.Builder
	if err = tpl.Execute(&w, &v); err != nil {
		return
	}
	rendered = w.String()
	log.WithFields(log.Fields{
		"rendered":  rendered,
		"table":     table,
		"range-key": rangeKey,
	}).Debug("rendered template")
	return
}
