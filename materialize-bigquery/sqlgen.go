package connector

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
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
		return string(json.RawMessage(nil)), nil
	default:
		return nil, fmt.Errorf("invalid type %#v for variant", te)
	}
}

var bqDialect = func() sql.Dialect {
	var mapper sql.TypeMapper = sql.ProjectionTypeMapper{
		sql.ARRAY:    sql.NewStaticMapper("STRING", sql.WithElementConverter(jsonConverter)),
		sql.BINARY:   sql.NewStaticMapper("BYTES"),
		sql.BOOLEAN:  sql.NewStaticMapper("BOOL"),
		sql.INTEGER:  sql.NewStaticMapper("INT64"),
		sql.NUMBER:   sql.NewStaticMapper("FLOAT64"),
		sql.OBJECT:   sql.NewStaticMapper("STRING", sql.WithElementConverter(jsonConverter)),
		sql.MULTIPLE: sql.NewStaticMapper("JSON", sql.WithElementConverter(sql.JsonBytesConverter)),
		sql.STRING: sql.StringTypeMapper{
			Fallback: sql.NewStaticMapper("STRING"),
			WithFormat: map[string]sql.TypeMapper{
				"integer": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("STRING"),
					Delegate:   sql.NewStaticMapper("BIGNUMERIC(38,0)", sql.WithElementConverter(sql.StdStrToInt())),
				},
				"number": sql.PrimaryKeyMapper{
					PrimaryKey: sql.NewStaticMapper("STRING"),
					// https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions#cast_as_floating_point
					Delegate: sql.NewStaticMapper("FLOAT64", sql.WithElementConverter(sql.StdStrToFloat("NaN", "Infinity", "-Infinity"))),
				},
				"date":      sql.NewStaticMapper("DATE", sql.WithElementConverter(sql.ClampDate())),
				"date-time": sql.NewStaticMapper("TIMESTAMP", sql.WithElementConverter(sql.ClampDatetime())),
			},
		},
	}

	mapper = sql.NullableMapper{
		NotNullText: "NOT NULL",
		Delegate:    mapper,
	}

	columnValidator := sql.NewColumnValidator(
		sql.ColValidation{Types: []string{"string"}, Validate: stringCompatible},
		sql.ColValidation{Types: []string{"bool"}, Validate: sql.BooleanCompatible},
		sql.ColValidation{Types: []string{"int64"}, Validate: sql.IntegerCompatible},
		sql.ColValidation{Types: []string{"float64"}, Validate: sql.NumberCompatible},
		sql.ColValidation{Types: []string{"json"}, Validate: sql.MultipleCompatible},
		sql.ColValidation{Types: []string{"bignumeric"}, Validate: bignumericCompatible},
		sql.ColValidation{Types: []string{"date"}, Validate: sql.DateCompatible},
		sql.ColValidation{Types: []string{"timestamp"}, Validate: sql.DateTimeCompatible},
	)

	return sql.Dialect{
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			return sql.InfoTableLocation{
				TableSchema: path[1],
				TableName:   translateFlowIdentifier(path[2]),
			}
		}),
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
		Literaler: sql.LiteralFn(sql.QuoteTransform("'", "\\'")),
		Placeholderer: sql.PlaceholderFn(func(_ int) string {
			return "?"
		}),
		TypeMapper:             mapper,
		ColumnValidator:        columnValidator,
		MaxColumnCharLength:    300,
		CaseInsensitiveColumns: true,
	}
}()

// bignumericCompatible allows either integers or numbers, since we used to create number columns as
// "bignumeric" (now they are float64), and currently create strings formatted as integers as
// "bignumeric". This is needed for compatibility for older materializations.
func bignumericCompatible(p pf.Projection) bool {
	return sql.IntegerCompatible(p) || sql.NumberCompatible(p)
}

// stringCompatible allow strings of any format, arrays, or objects to be materialized.
func stringCompatible(p pf.Projection) bool {
	if sql.StringCompatible(p) {
		return true
	} else if sql.TypesOrNull(p.Inference.Types, []string{"array"}) {
		return true
	} else if sql.TypesOrNull(p.Inference.Types, []string{"object"}) {
		return true
	}

	return false
}

var (
	tplAll = sql.MustParseTemplate(bqDialect, "root", `
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
	{{- range $ind, $key := $.Keys }}
		{{ if $ind }} AND {{ else }} ON {{ end -}}
		l.{{ $key.Identifier }} = r.c{{$ind}}
	{{- end }}
{{ else }}
SELECT -1, NULL LIMIT 0
{{ end }}
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
ON {{ range $ind, $key := $.Keys }}
{{- if $ind }} AND {{end -}}
	l.{{$key.Identifier}} = r.c{{$ind}}
{{- end}}
{{- if $.Document }}
WHEN MATCHED AND r.c{{ Add (len $.Columns) -1 }} IS NULL THEN
	DELETE
{{- end }}
WHEN MATCHED THEN
	UPDATE SET {{ range $ind, $val := $.Values }}
	{{- if $ind }}, {{end -}}
		l.{{$val.Identifier}} = r.c{{ Add (len $.Keys) $ind}}
	{{- end}} 
	{{- if $.Document -}}
		{{ if $.Values  }}, {{ end }}l.{{$.Document.Identifier}} = r.c{{ Add (len $.Columns) -1 }}
	{{- end }}
WHEN NOT MATCHED THEN
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
	tplTempTableName     = tplAll.Lookup("tempTableName")
	tplCreateTargetTable = tplAll.Lookup("createTargetTable")
	tplAlterTableColumns = tplAll.Lookup("alterTableColumns")
	tplInstallFence      = tplAll.Lookup("installFence")
	tplUpdateFence       = tplAll.Lookup("updateFence")
	tplLoadQuery         = tplAll.Lookup("loadQuery")
	tplStoreInsert       = tplAll.Lookup("storeInsert")
	tplStoreUpdate       = tplAll.Lookup("storeUpdate")
)
