package connector

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"text/template"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

// Identifiers matching the this pattern do not need to be quoted. See
// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#identifiers.
var simpleIdentifierRegexp = regexp.MustCompile(`(?i)^[a-z_][a-z0-9_]*$`)

// Up until recently, BigQuery had a number of restrictions on allowable characters in column names.
// Because of this, we convert non-alphanumeric characters of identifiers (both table names and
// columns) to underscores, with the exception of hyphens.

// With BigQuery supporting "Flexible Column Names"
// (https://cloud.google.com/bigquery/docs/schemas#flexible-column-names), it may now be possible
// significantly relax these conversions, but the migration for existing materializations would need
// to be figured out. So for now we continue to sanitize fields in the same historical way.

// As it is, there is a potential for an error if these "sanitized" column names collide, for
// example "field!" vs. "field?" both sanitizing to "field_".  In this case an alternate projection
// name for one of the fields will be required.
var identifierSanitizerRegexp = regexp.MustCompile(`[^\-_0-9a-zA-Z]`)

func identifierSanitizer(delegate func(string) string) func(string) string {
	return func(text string) string {
		return delegate(identifierSanitizerRegexp.ReplaceAllString(text, "_"))
	}
}

// translateFlowIdentifier returns the column or table name that will be created for a given
// collection name or field name, without any quoting applied.
func translateFlowIdentifier(f string) string {
	return identifierSanitizerRegexp.ReplaceAllString(f, "_")
}

var jsonConverter sql.ElementConverter = func(te tuple.TupleElement) (interface{}, error) {
	switch ii := te.(type) {
	case []byte:
		return string(ii), nil
	case json.RawMessage:
		return string(ii), nil
	case nil:
		// TODO(whb): This actually materializes NULL values as empty strings in
		// BigQuery. It's only used for objects and arrays, and we eventually
		// want to have those materialized as JSON anyway, so I'm leaving this
		// alone for now to maintain historical consistency in case somebody is
		// relying on the pre-existing (incorrect, I would argue) behavior.
		return string(json.RawMessage(nil)), nil
	default:
		return nil, fmt.Errorf("invalid type %#v for variant", te)
	}
}

func bqDialect(objAndArrayAsJson bool) sql.Dialect {
	objAndArrayCol := sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes))
	if !objAndArrayAsJson {
		objAndArrayCol = sql.MapStatic("STRING", sql.UsingConverter(jsonConverter))
	}

	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.ARRAY:   objAndArrayCol,
			sql.BINARY:  sql.MapStatic("STRING"),
			sql.BOOLEAN: sql.MapStatic("BOOLEAN"),
			sql.INTEGER: sql.MapSignedInt64(
				sql.MapStatic("INTEGER"),
				sql.MapStatic("BIGNUMERIC(38,0)", sql.AlsoCompatibleWith("bignumeric")),
			),
			// We used to materialize these as "BIGNUMERIC(38,0)", so
			// pre-existing columns of that type are allowed for
			// backward-compatibility.
			sql.NUMBER: sql.MapStatic("FLOAT64", sql.AlsoCompatibleWith("float", "bignumeric")),
			sql.OBJECT: objAndArrayCol,
			// Note that MULTIPLE fields have always been materialized into JSON
			// columns, so there is no configurability for these.
			sql.MULTIPLE: sql.MapStatic("JSON", sql.UsingConverter(sql.ToJsonBytes)),
			sql.STRING_INTEGER: sql.MapStringMaxLen(
				// BigQuery's table metadata APIs include the precision and
				// scale with BIGNUMERIC columns, and we strip that off when
				// creating the InfoSchema so that a BIGNUMERIC(38,0) is
				// compatible with any BIGNUMERIC column.
				sql.MapStatic("BIGNUMERIC(38,0)", sql.AlsoCompatibleWith("bignumeric"), sql.UsingConverter(sql.StrToInt)),
				sql.MapStatic("STRING", sql.UsingConverter(sql.ToStr)),
				38,
			),
			// https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_floating_point
			sql.STRING_NUMBER: sql.MapStatic("FLOAT64", sql.AlsoCompatibleWith("float"), sql.UsingConverter(sql.StrToFloat("NaN", "Infinity", "-Infinity"))),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: sql.MapStatic("STRING"),
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      sql.MapStatic("DATE", sql.UsingConverter(sql.ClampDate)),
					"date-time": sql.MapStatic("TIMESTAMP", sql.UsingConverter(sql.ClampDatetime)),
				},
			}),
		},
		sql.WithNotNullText("NOT NULL"),
	)

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"integer": {
				sql.NewMigrationSpec([]string{"bignumeric(38,0)"}, sql.WithCastSQL(toBigNumericCast)),
				sql.NewMigrationSpec([]string{"float64", "string"}),
			},
			"bignumeric": {sql.NewMigrationSpec([]string{"float64", "string"})},
			"float":      {sql.NewMigrationSpec([]string{"string"})},
			"date":       {sql.NewMigrationSpec([]string{"string"})},
			"timestamp":  {sql.NewMigrationSpec([]string{"string"}, sql.WithCastSQL(datetimeToStringCast))},
			"*":          {sql.NewMigrationSpec([]string{"json"}, sql.WithCastSQL(toJsonCast))},
		},
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			return sql.InfoTableLocation{
				TableSchema: path[1],
				TableName:   translateFlowIdentifier(path[2]),
			}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(func(schema string) string { return schema }),
		ColumnLocatorer: sql.ColumnLocatorFn(func(field string) string { return translateFlowIdentifier(field) }),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			identifierSanitizer(sql.PassThroughTransform(
				func(s string) bool {
					// Note: The BigQuery reserved words list must be in all-caps, as they are
					// listed in the BigQuery docs. Quoting must be applied regardless of case.
					return simpleIdentifierRegexp.MatchString(s) && !slices.Contains(BQ_RESERVED_WORDS, strings.ToUpper(s))
				},
				sql.QuoteTransform("`", "\\`"),
			)))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransformEscapedBackslash("'", "\\'")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		MaxColumnCharLength:    300,
		CaseInsensitiveColumns: true,
	}
}

func datetimeToStringCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%E*SZ', %s, 'UTC') `, migration.Identifier)
}

func toJsonCast(migration sql.ColumnTypeMigration) string {
	return fmt.Sprintf(`TO_JSON(%s)`, migration.Identifier)
}

func toBigNumericCast(m sql.ColumnTypeMigration) string {
	return fmt.Sprintf("CAST(%s AS BIGNUMERIC)", m.Identifier)
}

type templates struct {
	tempTableName           *template.Template
	createTargetTable       *template.Template
	alterTableColumns       *template.Template
	installFence            *template.Template
	updateFence             *template.Template
	loadQuery               *template.Template
	loadQueryNoFlowDocument *template.Template
	storeInsert             *template.Template
	storeUpdate             *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
	var tplAll = sql.MustParseTemplate(dialect, "root", `
{{ define "tempTableName" -}}
flow_temp_table_{{ $.Binding }}
{{- end }}

-- Templated creation of a materialized table definition and comments.
-- Note: BigQuery only allows a maximum of 4 columns for clustering.

{{ define "createTargetTable" -}}
CREATE TABLE IF NOT EXISTS {{$.Identifier}} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }},{{ end }}
		{{$col.Identifier}} {{$col.DDL}}
	{{- end }}
)
CLUSTER BY {{ range $ind, $key := $.Keys }}
	{{- if lt $ind 4 -}}
		{{- if $ind }}, {{end -}}
			{{$key.Identifier}}
		{{- end -}}
	{{- end}};
{{ end }}

-- Templated query which performs table alterations by adding columns and/or
-- dropping nullability constraints. BigQuery does not allow adding columns and
-- modifying columns together in the same statement, but either one of those
-- things can be grouped together in separate statements, so this template will
-- actually generate two separate statements if needed.

{{ define "alterTableColumns" }}
{{- if $.AddColumns -}}
ALTER TABLE {{$.Identifier}}
{{- range $ind, $col := $.AddColumns }}
	{{- if $ind }},{{ end }}
	ADD COLUMN {{$col.Identifier}} {{$col.NullableDDL}}
{{- end }};
{{- end -}}
{{- if $.DropNotNulls -}}
{{- if $.AddColumns }}

{{ end -}}
ALTER TABLE {{$.Identifier}}
{{- range $ind, $col := $.DropNotNulls }}
	{{- if $ind }},{{ end }}
	ALTER COLUMN {{ ColumnIdentifier $col.Name }} DROP NOT NULL
{{- end }};
{{- end }}
{{ end }}

-- Templated query which joins keys from the load table with the target table,
-- and returns values. It deliberately skips the trailing semi-colon
-- as these queries are composed with a UNION ALL.

{{ define "loadQuery" -}}
{{ if $.Document -}}
SELECT {{ $.Binding }}, l.{{$.Document.Identifier}}
	FROM {{ $.Identifier }} AS l
	JOIN {{ template "tempTableName" . }} AS r
	{{- range $ind, $bound := $.Bounds }}
		{{ if $ind }} AND {{ else }} ON {{ end -}}
		l.{{ $bound.Identifier }} = r.c{{$ind}}
		{{- if $bound.LiteralLower }} AND l.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND l.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
	{{- end }}
{{ else }}
SELECT -1, NULL LIMIT 0
{{ end }}
{{ end }}

-- Templated query for no_flow_document feature flag - reconstructs JSON from root-level columns

{{ define "loadQueryNoFlowDocument" -}}
SELECT {{ $.Binding }}, 
TO_JSON(STRUCT(
{{- range $i, $col := $.RootLevelColumns}}
	{{- if $i}}, {{end}}
	l.{{ $col.Identifier }} AS {{ $col.Field }}
{{- end}}
)) as flow_document
FROM {{ $.Identifier }} AS l
JOIN {{ template "tempTableName" . }} AS r
{{- range $ind, $bound := $.Bounds }}
	{{ if $ind }} AND {{ else }} ON {{ end -}}
	l.{{ $bound.Identifier }} = r.c{{$ind}}
	{{- if $bound.LiteralLower }} AND l.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND l.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end }}
{{ end }}

-- Templated query which bulk inserts rows from a source bucket into a target table.

{{ define "storeInsert" -}}
INSERT INTO {{ $.Identifier }} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}}
	{{- end -}}
)
SELECT {{ range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		c{{$ind}}
	{{- end }} FROM {{ template "tempTableName" . }};
{{ end }}

-- Templated query which updates an existing row in the target table:

{{ define "storeUpdate" -}}
MERGE INTO {{ $.Identifier }} AS l
USING {{ template "tempTableName" . }} AS r
ON {{ range $ind, $bound := $.Bounds }}
	{{ if $ind -}} AND {{end -}}
	l.{{$bound.Identifier}} = r.c{{$ind}}
	{{- if $bound.LiteralLower }} AND l.{{ $bound.Identifier }} >= {{ $bound.LiteralLower }} AND l.{{ $bound.Identifier }} <= {{ $bound.LiteralUpper }}{{ end }}
{{- end}}
WHEN MATCHED AND r._flow_delete THEN
	DELETE
WHEN MATCHED THEN
	UPDATE SET {{ range $ind, $val := $.Values }}
	{{- if $ind }}, {{end -}}
		l.{{$val.Identifier}} = r.c{{ Add (len $.Keys) $ind}}
	{{- end}} 
	{{- if $.Document -}}
		{{ if $.Values  }}, {{ end }}l.{{$.Document.Identifier}} = r.c{{ Add (len $.Columns) -1 }}
	{{- end }}
WHEN NOT MATCHED AND NOT r._flow_delete THEN
	INSERT (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}}
	{{- end -}}
	)
	VALUES (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		r.c{{$ind}}
	{{- end -}}
	);
{{ end }}


{{ define "installFence" }}
-- Our desired fence
DECLARE vMaterialization STRING DEFAULT {{ Literal $.Materialization.String }};
DECLARE vKeyBegin INT64 DEFAULT {{ $.KeyBegin }};
DECLARE vKeyEnd INT64 DEFAULT {{ $.KeyEnd }};

-- The current values
DECLARE curFence INT64;
DECLARE curKeyBegin INT64;
DECLARE curKeyEnd INT64;
DECLARE curCheckpoint STRING;

BEGIN TRANSACTION;

-- Increment the fence value of _any_ checkpoint which overlaps our key range.
UPDATE {{ Identifier $.TablePath }}
	SET fence=fence+1
	WHERE materialization = vMaterialization
	AND key_end >= vKeyBegin
	AND key_begin <= vKeyEnd;

-- Read the checkpoint with the narrowest [key_begin, key_end] which fully overlaps our range.
SET (curFence, curKeyBegin, curKeyEnd, curCheckpoint) = (
	SELECT AS STRUCT fence, key_begin, key_end, checkpoint
		FROM {{ Identifier $.TablePath }}
		WHERE materialization = vMaterialization
		AND key_begin <= vKeyBegin
		AND key_end >= vKeyEnd
		ORDER BY key_end - key_begin ASC
		LIMIT 1
);

-- Create a new fence if none exists.
IF curFence IS NULL THEN
	SET curFence = {{ $.Fence }};
	SET curKeyBegin = 1;
	SET curKeyEnd = 0;
	SET curCheckpoint = {{ Literal (Base64Std $.Checkpoint) }};
END IF;

-- If any of the key positions don't line up, create a new fence.
-- Either it's new or we are starting a split shard.
IF vKeyBegin <> curKeyBegin OR vKeyEnd <> curKeyEnd THEN
	INSERT INTO {{ Identifier $.TablePath }} (materialization, key_begin, key_end, fence, checkpoint)
	VALUES (vMaterialization, vKeyBegin, vKeyEnd, curFence, curCheckpoint);
END IF;

COMMIT TRANSACTION;

-- Get the current value
SELECT curFence AS fence, curCheckpoint AS checkpoint;
{{ end }}

{{ define "updateFence" }}
IF (
	SELECT fence
	FROM {{ Identifier $.TablePath }}
	WHERE materialization={{ Literal $.Materialization.String }} AND key_begin={{ $.KeyBegin }} AND key_end={{ $.KeyEnd }} AND fence={{ $.Fence }}
) IS NULL THEN
	RAISE USING MESSAGE = 'This instance was fenced off by another';
END IF;

UPDATE {{ Identifier $.TablePath }}
	SET checkpoint={{ Literal (Base64Std $.Checkpoint) }}
	WHERE materialization={{ Literal $.Materialization.String }}
	AND key_begin={{ $.KeyBegin }}
	AND key_end={{ $.KeyEnd }}
	AND fence={{ $.Fence }};
{{ end }}
`)

	return templates{
		tempTableName:           tplAll.Lookup("tempTableName"),
		createTargetTable:       tplAll.Lookup("createTargetTable"),
		alterTableColumns:       tplAll.Lookup("alterTableColumns"),
		installFence:            tplAll.Lookup("installFence"),
		updateFence:             tplAll.Lookup("updateFence"),
		loadQuery:               tplAll.Lookup("loadQuery"),
		loadQueryNoFlowDocument: tplAll.Lookup("loadQueryNoFlowDocument"),
		storeInsert:             tplAll.Lookup("storeInsert"),
		storeUpdate:             tplAll.Lookup("storeUpdate"),
	}
}

type queryParams struct {
	sql.Table
	Bounds            []sql.MergeBound
	ObjAndArrayAsJson bool
}

func renderQueryTemplate(table sql.Table, tpl *template.Template, bounds []sql.MergeBound, objAndArrayAsJson bool) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &queryParams{
		Table:             table,
		Bounds:            bounds,
		ObjAndArrayAsJson: objAndArrayAsJson,
	}); err != nil {
		return "", err
	}

	return w.String(), nil
}
