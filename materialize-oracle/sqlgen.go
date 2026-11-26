package connector

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

const (
	// Keys are a VARCHAR2 of at most this many characters, which always fits
	// in 4000 bytes, at up to 4 bytes per AL32UTF8 character.
	maxVarcharChars = 1000

	timestampFormat   = `YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM`
	goTimestampFormat = "2006-01-02T15:04:05.000000000-07:00"

	// timestampDDL is the column type of a date-time value.
	timestampDDL = "TIMESTAMP(9) WITH TIME ZONE"
)

// Unquoted identifiers start with a letter, and Oracle stores them in upper
// case.
var simpleIdentifierRegexp = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

func isSimpleIdentifier(s string) bool {
	return simpleIdentifierRegexp.MatchString(s) && !slices.Contains(reservedWords, strings.ToUpper(s))
}

// translateIdentifier returns the name Oracle stores for s. No identifier can
// contain a double quote, so a quoted one has them replaced.
func translateIdentifier(s string) string {
	if isSimpleIdentifier(s) {
		return strings.ToUpper(s)
	}
	return strings.ReplaceAll(s, `"`, "_")
}

// numberConverter renders an integer as decimal text for TO_NUMBER.
// the reason we use TO_NUMBER with text rendering of numeric values is that
// go-ora does not support big.Int and so we can't fit values of `NUMBER(38, 0)` into
// any go type. go-ora's own `Number` type also has shown issues with handling very large
// negative numbers where it corrupts the data. So TO_NUMBER is at the moment
// the most precise path, although not the most performant.
var numberConverter sql.ElementConverter = func(te tuple.TupleElement) (any, error) {
	switch v := te.(type) {
	case nil:
		return nil, nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case string:
		// Integers may be written with a zero fraction, like "1.0".
		if idx := strings.IndexByte(v, '.'); idx != -1 {
			v = v[:idx]
		}
		return v, nil
	default:
		return nil, fmt.Errorf("could not convert %#v (%T) to an integer", te, te)
	}
}

// floatConverter renders a number as text for TO_BINARY_DOUBLE, which also
// accepts NaN and the infinities.
var floatConverter sql.ElementConverter = func(te tuple.TupleElement) (any, error) {
	switch v := te.(type) {
	case nil:
		return nil, nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float64:
		switch {
		case math.IsNaN(v):
			return "NaN", nil
		case math.IsInf(v, 1):
			return "Inf", nil
		case math.IsInf(v, -1):
			return "-Inf", nil
		}
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case string:
		switch v {
		case "NaN":
			return "NaN", nil
		case "Infinity":
			return "Inf", nil
		case "-Infinity":
			return "-Inf", nil
		}
		return v, nil
	default:
		return nil, fmt.Errorf("could not convert %#v (%T) to a number", te, te)
	}
}

// timestampConverter renders a date-time in UTC with nanoseconds, the layout
// that TO_TIMESTAMP_TZ parses with timestampFormat, clamped to the years 1 to
// 9999 that Oracle stores.
var timestampConverter sql.ElementConverter = sql.StringCastConverter(func(str string) (any, error) {
	parsed, _, err := sql.ParseRFC3339Nano(str)
	if err != nil {
		return nil, err
	}
	parsed = parsed.UTC()
	if parsed.Year() < 1 {
		parsed = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	} else if parsed.Year() > 9999 {
		parsed = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	}
	return parsed.Format(goTimestampFormat), nil
})

func newOracleDialect(defaultSchema string, maxStringBytes int) sql.Dialect {
	jsonMapping := sql.MapStatic("CLOB", sql.UsingConverter(sql.ToJsonString))
	floatMapping := sql.MapStatic("BINARY_DOUBLE", sql.UsingConverter(floatConverter))

	stringKey := mapVarchar(maxVarcharChars)
	stringValue := sql.MapPrimaryKey(stringKey, mapText(maxStringBytes, nil))
	numberAsText := mapText(maxStringBytes, sql.ToStr)

	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER:  sql.MapSignedInt64(mapNumber, numberAsText),
			sql.NUMBER:   floatMapping,
			sql.BOOLEAN:  sql.MapStatic("BOOLEAN"),
			sql.OBJECT:   jsonMapping,
			sql.ARRAY:    jsonMapping,
			sql.MULTIPLE: jsonMapping,
			sql.BINARY: sql.MapPrimaryKey(
				sql.MapStatic("RAW(2000)", sql.AlsoCompatibleWith("RAW"), sql.UsingConverter(sql.Base64Decoder)),
				sql.MapStatic("BLOB", sql.UsingConverter(sql.Base64Decoder)),
			),
			sql.STRING_INTEGER: sql.MapStringMaxLen(mapNumber, numberAsText, maxNumberDigits),
			sql.STRING_NUMBER:  floatMapping,
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: stringValue,
				// Checkpoints outgrow a VARCHAR2
				WithContentType: map[string]sql.MapProjectionFn{
					"application/x-protobuf; proto=consumer.Checkpoint": sql.MapStatic("CLOB"),
				},
				WithFormat: map[string]sql.MapProjectionFn{
					"date":      sql.MapPrimaryKey(stringKey, sql.MapStatic("DATE", sql.UsingConverter(sql.ClampDate))),
					"date-time": sql.MapPrimaryKey(stringKey, sql.MapStatic(timestampDDL, sql.UsingConverter(timestampConverter))),
				},
			}),
		},
		// Oracle stores an empty string or binary value as NULL, so their
		// columns must accept NULL. Primary keys are NOT NULL regardless.
		sql.WithNotNullFn(func(ddl string) string {
			if storesEmptyAsNull(ddl) {
				return ddl
			}
			return ddl + " NOT NULL"
		}),
	)

	// Keys aren't migrated, so a migrated VARCHAR2 is always maxStringBytes
	var text = (&varcharColumn{Chars: maxStringBytes}).DDL()

	// toText migrates a column to a VARCHAR2 or a CLOB, converting it with format.
	var toText = func(format string) []sql.MigrationSpec {
		return []sql.MigrationSpec{
			sql.NewMigrationSpec([]string{text}, sql.WithCastSQL(castTo(format))),
			sql.NewMigrationSpec([]string{"CLOB"}, sql.WithCastSQL(castTo("TO_CLOB("+format+")"))),
		}
	}

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"number(38,0)": append(toText("TO_CHAR(%s)"),
				sql.NewMigrationSpec([]string{"BINARY_DOUBLE"}, sql.WithCastSQL(castTo("TO_BINARY_DOUBLE(%s)")))),
			"binary_double":               toText(binaryDoubleToText),
			"boolean":                     toText("LOWER(TO_CHAR(%s))"),
			"date":                        toText("TO_CHAR(%s, 'YYYY-MM-DD')"),
			"timestamp(9) with time zone": toText(`TO_CHAR(SYS_EXTRACT_UTC(%s), 'YYYY-MM-DD"T"HH24:MI:SS.FF9"Z"')`),
			"varchar2": {
				// Oracle widens a VARCHAR2 in place, without rewriting the table.
				sql.NewMigrationSpec([]string{text}, sql.WithDirectCast()),
				sql.NewMigrationSpec([]string{"CLOB"}, sql.WithCastSQL(castTo("TO_CLOB(%s)"))),
			},
		},
		DirectCastSQL: sql.DirectCastPerColumn(func(table sql.Table, m sql.ColumnTypeMigration) string {
			return fmt.Sprintf("ALTER TABLE %s MODIFY (%s %s)", table.Identifier, m.Identifier, m.BareDDL)
		}),
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			if len(path) == 1 {
				return sql.InfoTableLocation{TableSchema: defaultSchema, TableName: translateIdentifier(path[0])}
			}
			return sql.InfoTableLocation{TableSchema: translateIdentifier(path[0]), TableName: translateIdentifier(path[1])}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(translateIdentifier),
		ColumnLocatorer: sql.ColumnLocatorFn(translateIdentifier),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".",
			sql.PassThroughTransform(isSimpleIdentifier, sql.QuoteTransform(`"`, "_")))),
		Literaler: sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer: sql.PlaceholderFn(func(index int) string {
			return fmt.Sprintf(":%d", index+1)
		}),
		TypeMapper:             mapper,
		MaxColumnCharLength:    128,
		ColumnLengthInBytes:    true,
		MaxColumns:             1000,
		CaseInsensitiveColumns: false,
	}
}

// binaryDoubleToText writes a number as JSON does, in its shortest exact form,
// and NaN and the infinities as Flow names them.
const binaryDoubleToText = `CASE
	WHEN %[1]s IS NAN THEN 'NaN'
	WHEN %[1]s = BINARY_DOUBLE_INFINITY THEN 'Infinity'
	WHEN %[1]s = -BINARY_DOUBLE_INFINITY THEN '-Infinity'
	ELSE JSON_SERIALIZE(JSON_SCALAR(%[1]s))
END`

func castTo(format string) sql.CastSQLFunc {
	return func(m sql.ColumnTypeMigration) string {
		return fmt.Sprintf(format, m.Identifier)
	}
}

func storesEmptyAsNull(ddl string) bool {
	return strings.HasPrefix(ddl, "VARCHAR2") || strings.HasPrefix(ddl, "RAW") || ddl == "CLOB" || ddl == "BLOB"
}

// maxNumberDigits is the precision of NUMBER(38,0).
const maxNumberDigits = 38

// mapNumber also accepts NUMBER(*,0), which is how Oracle reports an existing
// INTEGER column.
var mapNumber = sql.MapStatic("NUMBER(38,0)", sql.AlsoCompatibleWith("NUMBER(*,0)"), sql.UsingConverter(numberConverter))

// mapText is a VARCHAR2 of the most it holds, which Store converts to a CLOB
// once a value doesn't fit (see widenToCLOB), so an existing CLOB is also
// compatible.
func mapText(maxStringBytes int, toText sql.ElementConverter) sql.MapProjectionFn {
	var col = &varcharColumn{Chars: maxStringBytes}
	var compatible = sql.CompatibleColumnTypes{col, &sql.CompatibleStringEqualFold{Inner: "CLOB"}}
	return func(*sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
		return col, compatible, toText
	}
}

// mapVarchar maps to a VARCHAR2 of the projection's maxLength, or of
// maxChars when projection has no maxLength
func mapVarchar(maxChars int) sql.MapProjectionFn {
	return func(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
		var n = maxChars
		if s := p.Inference.String_; s != nil && s.MaxLength > 0 && int(s.MaxLength) < n {
			n = int(s.MaxLength)
		}
		var col = &varcharColumn{Chars: n}
		return col, sql.CompatibleColumnTypes{col}, nil
	}
}

// varcharColumn is a VARCHAR2 with character length semantics. An existing
// VARCHAR2 is compatible when it holds at least as many characters.
type varcharColumn struct {
	Chars int
}

func (v *varcharColumn) DDL() string { return fmt.Sprintf("VARCHAR2(%d CHAR)", v.Chars) }

func (v *varcharColumn) Compatible(existing boilerplate.ExistingField) bool {
	return strings.EqualFold(existing.Type, "VARCHAR2") && existing.CharacterMaxLength >= v.Chars
}

// bindExpr is the bind at position index, wrapped so the database parses the
// bound text into the column's type.
func bindExpr(c sql.Column, index int) string {
	var placeholder = fmt.Sprintf(":%d", index+1)
	switch {
	case strings.HasPrefix(c.BareDDL, "NUMBER"):
		return "TO_NUMBER(" + placeholder + ")"
	case c.BareDDL == "BINARY_DOUBLE":
		return "TO_BINARY_DOUBLE(" + placeholder + ")"
	case strings.HasPrefix(c.BareDDL, "TIMESTAMP"):
		return "TO_TIMESTAMP_TZ(" + placeholder + ", '" + timestampFormat + "')"
	case c.BareDDL == "DATE":
		return "TO_DATE(" + placeholder + ", 'YYYY-MM-DD')"
	default:
		return placeholder
	}
}

// loadTableName names the private temporary table that holds a binding's Load
// keys. A private temporary table exists only in its session, and its name
// must start with the database's PRIVATE_TEMP_TABLE_PREFIX, ORA$PTT_ by
// default.
func loadTableName(binding int) string {
	return fmt.Sprintf("ORA$PTT_FLOW_LOAD_%d", binding)
}

type templates struct {
	createTargetTable *template.Template
	alterTableColumns *template.Template
	createLoadTable   *template.Template
	loadInsert        *template.Template
	loadQuery         *template.Template
	storeInsert       *template.Template
	storeUpdate       *template.Template
	deleteQuery       *template.Template
	updateFence       *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
	var tplAll = sql.MustParseTemplate(dialect, "root", `
{{ define "createTargetTable" -}}
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
)
{{- end }}

-- Adds columns and drops NOT NULL constraints in one statement.

{{ define "alterTableColumns" }}
ALTER TABLE {{$.Identifier}}
{{- if $.AddColumns }} ADD (
	{{- range $ind, $col := $.AddColumns }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}} {{$col.NullableDDL}}
	{{- end -}}
){{ end }}
{{- if $.DropNotNulls }} MODIFY (
	{{- range $ind, $col := $.DropNotNulls }}
		{{- if $ind }}, {{ end -}}
		{{ ColumnIdentifier $col.Name }} NULL
	{{- end -}}
){{ end }}
{{ end }}

-- Creates the temporary table of a binding's Load keys, for the session.

{{ define "createLoadTable" }}
CREATE PRIVATE TEMPORARY TABLE {{ LoadTable $.Binding }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{ end -}}
		{{$key.Identifier}} {{$key.BareDDL}}
	{{- end -}}
) ON COMMIT PRESERVE DEFINITION
{{ end }}

-- Inserts one Load key per element of the bound arrays.

{{ define "loadInsert" }}
INSERT INTO {{ LoadTable $.Binding }} (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{ end -}}
		{{$key.Identifier}}
	{{- end -}}
) VALUES (
	{{- range $ind, $key := $.Keys }}
		{{- if $ind }}, {{ end -}}
		{{ Bind $key $ind }}
	{{- end -}}
)
{{ end }}

-- Selects the documents of the keys in the load table. Without the hints the
-- optimizer may hash-join them to a full scan of the table.

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT /*+ LEADING(l r) USE_NL(r) INDEX(r) */ r.{{$.Document.Identifier}}
FROM {{ LoadTable $.Binding }} l
JOIN {{$.Identifier}} r ON {{ range $ind, $key := $.Keys }}
	{{- if $ind }} AND {{ end -}}
	r.{{$key.Identifier}} = l.{{$key.Identifier}}
{{- end }}
{{ end -}}
{{ end }}

-- Inserts one row per element of the bound arrays.

{{ define "storeInsert" }}
INSERT INTO {{$.Identifier}} (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}}
	{{- end -}}
) VALUES (
	{{- range $ind, $col := $.Columns }}
		{{- if $ind }}, {{ end -}}
		{{ Bind $col $ind }}
	{{- end -}}
)
{{ end }}

-- Updates the row of each bound key.

{{ define "storeUpdate" }}
{{- $set := slice $.Columns (len $.Keys) }}
UPDATE {{$.Identifier}} SET {{ range $ind, $col := $set }}
	{{- if $ind }}, {{ end -}}
	{{$col.Identifier}} = {{ Bind $col $ind }}
{{- end }} WHERE {{ range $ind, $key := $.Keys }}
	{{- if $ind }} AND {{ end -}}
	{{$key.Identifier}} = {{ Bind $key (Add $ind (len $set)) }}
{{- end }}
{{ end }}

-- Deletes the row of each bound key.

{{ define "deleteQuery" }}
DELETE FROM {{$.Identifier}} WHERE {{ range $ind, $key := $.Keys }}
	{{- if $ind }} AND {{ end -}}
	{{$key.Identifier}} = {{ Bind $key $ind }}
{{- end }}
{{ end }}

-- Updates the fence's checkpoint, bound as :1, if the fence still holds.

{{ define "updateFence" }}
UPDATE {{ Identifier $.TablePath }}
	SET   {{ ColumnIdentifier "checkpoint" }} = :1
	WHERE {{ ColumnIdentifier "materialization" }} = {{ Literal $.Materialization.String }}
	AND   {{ ColumnIdentifier "key_begin" }} = {{ $.KeyBegin }}
	AND   {{ ColumnIdentifier "key_end" }} = {{ $.KeyEnd }}
	AND   {{ ColumnIdentifier "fence" }} = {{ $.Fence }}
{{ end }}
`, template.FuncMap{
		"Bind":      bindExpr,
		"LoadTable": loadTableName,
	})

	return templates{
		createTargetTable: tplAll.Lookup("createTargetTable"),
		alterTableColumns: tplAll.Lookup("alterTableColumns"),
		createLoadTable:   tplAll.Lookup("createLoadTable"),
		loadInsert:        tplAll.Lookup("loadInsert"),
		loadQuery:         tplAll.Lookup("loadQuery"),
		storeInsert:       tplAll.Lookup("storeInsert"),
		storeUpdate:       tplAll.Lookup("storeUpdate"),
		deleteQuery:       tplAll.Lookup("deleteQuery"),
		updateFence:       tplAll.Lookup("updateFence"),
	}
}
