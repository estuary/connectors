package connector

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

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	go_ora "github.com/sijms/go-ora/v2"
)

var simpleIdentifier = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_$#]*$`)

func isSimpleIdentifier(name string) bool {
	return simpleIdentifier.MatchString(name) && !slices.Contains(ORACLE_RESERVED_WORDS, strings.ToLower(name))
}

func locateIdentifier(name string) string {
	if isSimpleIdentifier(name) {
		return strings.ToUpper(name)
	}
	return name
}

const (
	// VARCHAR2 holds up to 4000 bytes, and an AL32UTF8 character takes up to 4.
	maxTextBytes = 4000
	maxCharBytes = 4
	// Date-time keys keep their RFC3339 text, such as 2026-11-11T10:09:08.123456789+02:30.
	dateTimeKeyBytes = 64
	// String keys share this budget, below the 6398-byte index key limit of
	// Oracle's default 8K block size.
	keyTextBudget = 6000
	// Oracle 12.2 and later limit identifiers to 128 bytes.
	maxIdentifierBytes = 128
)

// keyTextBytes is the VARCHAR2 size of a string key column.
func keyTextBytes(p *sql.Projection) int {
	if str := p.Inference.String_; str != nil && str.Format == "date-time" {
		return dateTimeKeyBytes
	} else if str != nil && str.MaxLength > 0 && str.MaxLength <= maxTextBytes/maxCharBytes {
		return int(str.MaxLength) * maxCharBytes
	}
	return maxTextBytes
}

// sizedVarchar is compatible with an existing VARCHAR2 column of at least
// Size bytes.
type sizedVarchar struct{ Size int }

func (s sizedVarchar) Compatible(existing boilerplate.ExistingField) bool {
	return strings.EqualFold(existing.Type, "VARCHAR2") && existing.CharacterMaxLength >= s.Size
}

func mapVarchar(size int) sql.MapProjectionFn {
	return func(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
		// BYTE keeps the size in bytes under NLS_LENGTH_SEMANTICS=CHAR.
		return sql.StringDDL(fmt.Sprintf("VARCHAR2(%d BYTE)", size)), sql.CompatibleColumnTypes{sizedVarchar{size}}, nil
	}
}

// mapShortText creates a VARCHAR2(4000) column for a non-key string, but
// accepts an existing VARCHAR2 column with room for its maxLength, or a CLOB.
func mapShortText(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
	ddl, _, converter := mapVarchar(maxTextBytes)(p)
	return ddl, sql.CompatibleColumnTypes{
		sizedVarchar{int(p.Inference.String_.MaxLength) * maxCharBytes},
		&sql.CompatibleStringEqualFold{Inner: "CLOB"},
	}, converter
}

// clobText is the CLOB type of a string column, which an existing VARCHAR2
// column migrates to when the string's maxLength grows or is removed.
type clobText struct{}

func (clobText) DDL() string { return "CLOB" }

func mapClobText(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
	return clobText{}, sql.CompatibleColumnTypes{&sql.CompatibleStringEqualFold{Inner: "CLOB"}}, toClob(sql.ToStr)
}

// clobTextMigration targets string CLOBs, excluding JSON CLOBs whose values
// require JSON encoding.
type clobTextMigration struct{}

func (clobTextMigration) CanMigrate(_ boilerplate.ExistingField, mapped sql.MappedType) bool {
	_, ok := mapped.TargetType.(clobText)
	return ok
}

// characterTypes report their DATA_LENGTH as a column size.
var characterTypes = []string{"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"}

// existingCharacterLength is the size reported for an existing column, which
// is DATA_LENGTH for character types and 0 for others.
func existingCharacterLength(dataType string, dataLength int64) int {
	if slices.Contains(characterTypes, strings.ToUpper(dataType)) {
		return int(dataLength)
	}
	return 0
}

// mapNumber38 uses an alternate text mapping for integers whose inferred range
// or string length may exceed the 38 digits of NUMBER(38).
func mapNumber38(def, alt sql.MapProjectionFn) sql.MapProjectionFn {
	return func(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
		if num := p.Inference.Numeric; num != nil && (num.Minimum <= -1e38 || num.Maximum >= 1e38) {
			return alt(p)
		} else if str := p.Inference.String_; str != nil && str.MaxLength > 38 {
			return alt(p)
		}
		return def(p)
	}
}

func createOracleDialect(defaultSchema string) sql.Dialect {
	var text sql.MapProjectionFn = mapClobText
	var jsonType = sql.MapStatic("CLOB", sql.UsingConverter(toClob(sql.ToJsonString)))
	// Oracle cannot index CLOB or TIMESTAMP WITH TIME ZONE key columns.
	var keyText sql.MapProjectionFn = func(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
		return mapVarchar(keyTextBytes(p))(p)
	}
	var stringType = sql.MapPrimaryKey(keyText, func(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
		// maxLength counts characters, while VARCHAR2(4000) holds 4000 bytes.
		if p.Inference.String_ != nil && p.Inference.String_.MaxLength > 0 && p.Inference.String_.MaxLength <= maxTextBytes/maxCharBytes {
			return mapShortText(p)
		}
		return text(p)
	})
	var integer = mapNumber38(
		sql.MapStatic("NUMBER(38)", sql.AlsoCompatibleWith("NUMBER"), sql.UsingConverter(toNumber)),
		sql.MapPrimaryKey(func(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
			ddl, compatible, _ := keyText(p)
			return ddl, compatible, sql.ToStr
		}, text),
	)
	var mapper = sql.NewDDLMapper(sql.FlatTypeMappings{
		sql.INTEGER:        integer,
		sql.STRING_INTEGER: integer,
		sql.NUMBER:         sql.MapStatic("BINARY_DOUBLE", sql.UsingConverter(toDouble(nil))),
		sql.STRING_NUMBER:  sql.MapStatic("BINARY_DOUBLE", sql.UsingConverter(toDouble(sql.StrToFloat(nil, nil, nil)))),
		sql.BOOLEAN:        sql.MapStatic("NUMBER(1)", sql.AlsoCompatibleWith("NUMBER", "NUMBER(38)"), sql.UsingConverter(boolToNumber)),
		sql.OBJECT:         jsonType,
		sql.ARRAY:          jsonType,
		sql.MULTIPLE:       jsonType,
		sql.BINARY:         stringType,
		sql.STRING: sql.MapString(sql.StringMappings{
			Fallback: stringType,
			WithFormat: map[string]sql.MapProjectionFn{
				"date-time": sql.MapPrimaryKey(keyText, sql.MapStatic("TIMESTAMP(9) WITH TIME ZONE", sql.AlsoCompatibleWith("TIMESTAMP WITH TIME ZONE"), sql.UsingConverter(rfc3339ToTimestamp))),
			},
		}),
	}, sql.WithNotNullSuffix("NOT NULL"))

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"varchar2": {sql.NewMigrationSpecTarget(clobTextMigration{}, sql.WithCastSQL(func(m sql.ColumnTypeMigration) string {
				return "TO_CLOB(" + m.Identifier + ")"
			}))},
		},
		TableLocatorer: sql.TableLocatorFn(func(path []string) sql.InfoTableLocation {
			if len(path) == 1 {
				return sql.InfoTableLocation{TableSchema: locateIdentifier(defaultSchema), TableName: locateIdentifier(path[0])}
			}
			return sql.InfoTableLocation{TableSchema: locateIdentifier(path[0]), TableName: locateIdentifier(path[1])}
		}),
		SchemaLocatorer: sql.SchemaLocatorFn(locateIdentifier),
		ColumnLocatorer: sql.ColumnLocatorFn(locateIdentifier),
		Identifierer: sql.IdentifierFn(sql.JoinTransform(".", func(s string) string {
			if isSimpleIdentifier(s) {
				return strings.ToUpper(s)
			}
			return sql.QuoteTransform(`"`, `""`)(s)
		})),
		Literaler:           sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
		Placeholderer:       sql.PlaceholderFn(func(index int) string { return fmt.Sprintf(":%d", index+1) }),
		TypeMapper:          textNullableMapper{mapper},
		MaxColumnCharLength: maxIdentifierBytes,
		// Staging tables add a delete column to Oracle's 1000-column limit.
		MaxColumns: 999,
	}
}

// emptyStringIsNull reports whether a column stores strings that may be
// empty, which Oracle stores as NULL.
func emptyStringIsNull(p *sql.Projection, bareDDL string) bool {
	ft, _ := p.AsFlatType()
	return !p.IsPrimaryKey && (ft == sql.STRING || ft == sql.BINARY) &&
		isTextDDL(bareDDL)
}

// textNullableMapper keeps non-key text columns nullable, since Oracle stores
// an empty string as NULL. String-encoded number columns are also nullable,
// since NaN and Infinity are stored as NULL.
type textNullableMapper struct{ sql.DDLMapper }

func (m textNullableMapper) MapType(p *sql.Projection, fc sql.FieldConfig) sql.MappedType {
	mapped := m.DDLMapper.MapType(p, fc)
	ft, _ := p.AsFlatType()
	if emptyStringIsNull(p, mapped.BareDDL) || (ft == sql.STRING_NUMBER && mapped.BareDDL == "BINARY_DOUBLE") {
		mapped.DDL = mapped.NullableDDL
	}
	// A DDL override to a text type stores scalars as their JSON text, as
	// castToString does, which the no-flow-document load query reads back,
	// rather than Oracle's implicit conversion of a number or 1 and 0.
	if fc.DDL != "" && isTextDDL(mapped.BareDDL) {
		switch ft {
		case sql.INTEGER, sql.NUMBER, sql.BOOLEAN, sql.STRING_INTEGER, sql.STRING_NUMBER:
			mapped.Converter = toClob(sql.ToStr)
		}
	}
	return mapped
}

// isTextDDL reports whether a column type stores text, matching the "uncast"
// template.
func isTextDDL(bareDDL string) bool {
	return bareDDL == "CLOB" || strings.HasPrefix(bareDDL, "VARCHAR2")
}

// maxClobStringBytes is the longest text bound as a VARCHAR2 rather than a
// temporary LOB, which costs extra round trips per value. Oracle's legacy UTF8
// character set stores a 4-byte UTF-8 character in 6 bytes.
const maxClobStringBytes = maxTextBytes * 2 / 3

func toClob(convert sql.ElementConverter) sql.ElementConverter {
	return func(value tuple.TupleElement) (any, error) {
		converted, err := convert(value)
		if err != nil || converted == nil {
			return nil, err
		}
		if str := converted.(string); len(str) > maxClobStringBytes {
			return go_ora.Clob{String: str, Valid: true}, nil
		}
		return converted, nil
	}
}

// go-ora binds a float as an Oracle NUMBER, and encodes magnitudes outside
// NUMBER's range as wrong values. Those floats are bound as text instead,
// which Oracle converts to BINARY_DOUBLE.
const (
	minDoubleMagnitude = 1e-125
	maxDoubleMagnitude = 1e125
)

func toDouble(convert sql.ElementConverter) sql.ElementConverter {
	return func(value tuple.TupleElement) (any, error) {
		if convert != nil {
			var err error
			if value, err = convert(value); err != nil {
				return nil, err
			}
		}
		if f, ok := value.(float64); ok && f != 0 && (math.Abs(f) < minDoubleMagnitude || math.Abs(f) >= maxDoubleMagnitude) {
			return doubleText(f), nil
		}
		return value, nil
	}
}

// doubleText formats a float as an integer mantissa and exponent, such as
// 17976931348623157E292, which has no decimal separator for the session's
// NLS_NUMERIC_CHARACTERS to misread. Subnormal values, below BINARY_DOUBLE's
// documented minimum of about 2.2e-308, become zero.
func doubleText(f float64) any {
	if math.Abs(f) < math.SmallestNonzeroFloat64*(1<<52) {
		return 0.0
	}
	formatted := strconv.FormatFloat(f, 'e', -1, 64)
	mantissa, exponent, _ := strings.Cut(formatted, "e")
	exp, _ := strconv.Atoi(exponent)
	if whole, fraction, ok := strings.Cut(mantissa, "."); ok {
		mantissa = whole + fraction
		exp -= len(fraction)
	}
	return mantissa + "E" + strconv.Itoa(exp)
}

// decimalInteger matches the integer strings that Flow accepts, such as +5,
// 1.00 and 1E3. The exponent is capped at four digits to bound parsing cost;
// toNumber separately limits the value to the 38 digits NUMBER(38) holds.
var decimalInteger = regexp.MustCompile(`^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]{1,4})?$`)

func toNumber(value tuple.TupleElement) (any, error) {
	if value == nil {
		return nil, nil
	}
	// go-ora parses neither a leading '+' nor an 'E' exponent.
	if str, ok := value.(string); ok {
		var r big.Rat
		if !decimalInteger.MatchString(str) {
			return nil, fmt.Errorf("parsing integer %q", str)
		} else if _, ok := r.SetString(str); !ok || !r.IsInt() {
			return nil, fmt.Errorf("parsing integer %q", str)
		}
		value = r.Num().String()
		if digits := strings.TrimPrefix(value.(string), "-"); len(digits) > 38 {
			return nil, fmt.Errorf("parsing integer %q: more than 38 digits", str)
		}
	}
	return go_ora.NewNumber(value)
}

func boolToNumber(value tuple.TupleElement) (any, error) {
	if value == nil {
		return nil, nil
	}
	if v, ok := value.(bool); !ok {
		return nil, fmt.Errorf("expected boolean, got %T", value)
	} else if v {
		return int64(1), nil
	}
	return int64(0), nil
}

var rfc3339ToTimestamp = sql.StringCastConverter(func(value string) (any, error) {
	parsed, _, err := sql.ParseRFC3339Nano(strings.ToUpper(value))
	if err != nil {
		return nil, fmt.Errorf("parsing RFC3339 timestamp: %w", err)
	} else if parsed.Year() < 1 || parsed.UTC().Year() < 1 {
		// Oracle has no year 0. The driver usually sends the UTC year, and
		// sends the local year when client and server time zone files differ.
		parsed = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	} else if parsed.UTC().Year() > 9999 {
		// Likewise, Oracle has no year 10000, which a late 9999 time with a
		// negative offset reaches in UTC.
		// Whole seconds keep an existing lower-precision column from
		// rounding the value up into year 10000.
		parsed = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	}
	return go_ora.TimeStampTZ(parsed), nil
})

type stagedTable struct {
	sql.Table
	LoadTable    string
	StoreTable   string
	DeleteColumn string
}

func (t stagedTable) UpdateColumns() []sql.Column {
	cols := append([]sql.Column(nil), t.Values...)
	if t.Document != nil {
		cols = append(cols, *t.Document)
	}
	return cols
}

type templates struct {
	createTargetTable       *template.Template
	createLoadTable         *template.Template
	createStoreTable        *template.Template
	loadInsert              *template.Template
	storeInsert             *template.Template
	loadQuery               *template.Template
	loadQueryNoFlowDocument *template.Template
	mergeInto               *template.Template
	directCopy              *template.Template
	deleteFrom              *template.Template
	updateFence             *template.Template
}

func renderTemplates(dialect sql.Dialect) templates {
	var tpl = sql.MustParseTemplate(dialect, "root", `
{{ define "createTargetTable" }}
CREATE TABLE {{ $.Identifier }} (
	{{- range $i, $col := $.Columns }}
	{{ if $i }}, {{ end }}{{ $col.Identifier }} {{ $col.DDL }}
	{{- end }}
	{{- if not $.DeltaUpdates }},
	PRIMARY KEY ({{ range $i, $key := $.Keys }}{{ if $i }}, {{ end }}{{ $key.Identifier }}{{ end }})
	{{- end }}
)
{{ end }}

-- Global temporary tables have session-private rows, cleared on commit.
{{ define "createLoadTable" }}
CREATE GLOBAL TEMPORARY TABLE {{ $.LoadTable }} (
	{{- range $i, $key := $.Keys }}
	{{ if $i }}, {{ end }}{{ $key.Identifier }} {{ $key.NullableDDL }}
	{{- end }}
) ON COMMIT DELETE ROWS
{{ end }}

{{ define "createStoreTable" }}
CREATE GLOBAL TEMPORARY TABLE {{ $.StoreTable }} (
	{{- range $i, $col := $.Columns }}
	{{ if $i }}, {{ end }}{{ $col.Identifier }} {{ $col.NullableDDL }}
	{{- end }},
	{{ $.DeleteColumn }} NUMBER(1)
) ON COMMIT DELETE ROWS
{{ end }}

{{ define "loadInsert" }}
INSERT INTO {{ $.LoadTable }} ({{ range $i, $key := $.Keys }}{{ if $i }}, {{ end }}{{ $key.Identifier }}{{ end }})
VALUES ({{ range $i, $key := $.Keys }}{{ if $i }}, {{ end }}{{ $key.Placeholder }}{{ end }})
{{ end }}

{{ define "storeInsert" }}
INSERT INTO {{ $.StoreTable }} ({{ range $i, $col := $.Columns }}{{ if $i }}, {{ end }}{{ $col.Identifier }}{{ end }}, {{ $.DeleteColumn }})
VALUES ({{ range $i, $col := $.Columns }}{{ if $i }}, {{ end }}{{ $col.Placeholder }}{{ end }}, :{{ Add (len $.Columns) 1 }})
{{ end }}

{{ define "joinKeys" -}}
{{ range $i, $key := $.Keys }}{{ if $i }} AND {{ end }}(l.{{ $key.Identifier }} = r.{{ $key.Identifier }} OR (l.{{ $key.Identifier }} IS NULL AND r.{{ $key.Identifier }} IS NULL)){{ end }}
{{- end }}

{{ define "loadQuery" }}
{{ if $.Document -}}
SELECT r.{{ $.Document.Identifier }} FROM {{ $.Identifier }} r
JOIN {{ $.LoadTable }} l ON ({{ template "joinKeys" . }})
{{ else -}}
SELECT TO_CLOB('null') FROM DUAL WHERE 1 = 0
{{ end }}
{{ end }}

{{ define "uncast" -}}
{{- $ident := printf "r.%s" $.Identifier -}}
{{- $text := or (eq $.BareDDL "CLOB") (eq (printf "%.8s" $.BareDDL) "VARCHAR2") -}}
{{- if and $text (or (eq $.AsFlatType "integer") (eq $.AsFlatType "number") (eq $.AsFlatType "boolean")) -}}
{{ $ident }} FORMAT JSON
{{- else if and $text (or (eq $.AsFlatType "string_integer") (eq $.AsFlatType "string_number")) -}}
{{ $ident }}
{{- else if or (eq $.AsFlatType "string_integer") (eq $.AsFlatType "string_number") -}}
TO_CHAR({{ $ident }})
{{- else if eq $.AsFlatType "boolean" -}}
CASE {{ $ident }} WHEN 1 THEN 'true' WHEN 0 THEN 'false' END FORMAT JSON
{{- else if or (eq $.AsFlatType "object") (eq $.AsFlatType "array") (eq $.AsFlatType "multiple") -}}
{{ $ident }} FORMAT JSON
{{- else if eq $.BareDDL "TIMESTAMP(9) WITH TIME ZONE" -}}
TO_CHAR({{ $ident }}, 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')
{{- else -}}
{{ $ident }}
{{- end -}}
{{- end }}

{{ define "loadQueryNoFlowDocument" }}
SELECT JSON_OBJECT(
	{{- range $i, $col := $.RootPropertyColumns }}
	{{ if $i }}, {{ end }}{{ Literal $col.RootProperty }} VALUE {{ template "uncast" (ColumnWithAlias $col "r") }}
	{{- end }}
	NULL ON NULL RETURNING CLOB
) FROM {{ $.Identifier }} r JOIN {{ $.LoadTable }} l ON ({{ template "joinKeys" . }})
{{ end }}

{{ define "mergeInto" }}
MERGE INTO {{ $.Identifier }} l
USING (SELECT * FROM {{ $.StoreTable }} WHERE {{ $.DeleteColumn }} = 0) r
ON ({{ template "joinKeys" . }})
{{ if $.UpdateColumns -}}
WHEN MATCHED THEN UPDATE SET
	{{- range $i, $col := $.UpdateColumns }}
	{{ if $i }}, {{ end }}l.{{ $col.Identifier }} = r.{{ $col.Identifier }}
	{{- end }}
{{ end -}}
WHEN NOT MATCHED THEN INSERT ({{ range $i, $col := $.Columns }}{{ if $i }}, {{ end }}{{ $col.Identifier }}{{ end }})
VALUES ({{ range $i, $col := $.Columns }}{{ if $i }}, {{ end }}r.{{ $col.Identifier }}{{ end }})
{{ end }}

{{ define "directCopy" }}
INSERT INTO {{ $.Identifier }} ({{ range $i, $col := $.Columns }}{{ if $i }}, {{ end }}{{ $col.Identifier }}{{ end }})
SELECT {{ range $i, $col := $.Columns }}{{ if $i }}, {{ end }}{{ $col.Identifier }}{{ end }} FROM {{ $.StoreTable }}
{{ end }}

{{ define "deleteFrom" }}
DELETE FROM {{ $.Identifier }} l WHERE EXISTS (
	SELECT 1 FROM {{ $.StoreTable }} r WHERE r.{{ $.DeleteColumn }} = 1 AND {{ template "joinKeys" . }}
)
{{ end }}

{{ define "updateFence" }}
UPDATE {{ Identifier $.TablePath }} SET CHECKPOINT = :1
WHERE MATERIALIZATION = :2 AND KEY_BEGIN = :3 AND KEY_END = :4 AND FENCE = :5
{{ end }}
`)
	return templates{
		createTargetTable:       tpl.Lookup("createTargetTable"),
		createLoadTable:         tpl.Lookup("createLoadTable"),
		createStoreTable:        tpl.Lookup("createStoreTable"),
		loadInsert:              tpl.Lookup("loadInsert"),
		storeInsert:             tpl.Lookup("storeInsert"),
		loadQuery:               tpl.Lookup("loadQuery"),
		loadQueryNoFlowDocument: tpl.Lookup("loadQueryNoFlowDocument"),
		mergeInto:               tpl.Lookup("mergeInto"),
		directCopy:              tpl.Lookup("directCopy"),
		deleteFrom:              tpl.Lookup("deleteFrom"),
		updateFence:             tpl.Lookup("updateFence"),
	}
}
