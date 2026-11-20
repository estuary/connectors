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
	"unicode/utf8"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

const (
	// VARCHAR2 holds at most 4000 bytes with MAX_STRING_SIZE=STANDARD, and an
	// AL32UTF8 character takes up to 4 bytes.
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

// numberConverter renders an integer as decimal text for TO_NUMBER, which
// keeps the bound type of a column uniform and the value exact.
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
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return nil, fmt.Errorf("could not convert %q to a number: %w", v, err)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("could not convert %#v (%T) to a number", te, te)
	}
}

// timestampConverter renders a date-time in UTC with nanoseconds, the layout
// that TO_TIMESTAMP_TZ parses with timestampFormat. Oracle has no year 0.
var timestampConverter sql.ElementConverter = sql.StringCastConverter(func(str string) (any, error) {
	parsed, _, err := sql.ParseRFC3339Nano(str)
	if err != nil {
		return nil, err
	} else if parsed.Year() == 0 {
		parsed = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return parsed.UTC().Format(goTimestampFormat), nil
})

// newOracleDialect returns the dialect for tables whose resource path names no
// schema, which live in defaultSchema, given as Oracle stores it.
func newOracleDialect(defaultSchema string) sql.Dialect {
	jsonMapping := sql.MapStatic("CLOB", sql.UsingConverter(sql.ToJsonString))

	stringKey := mapVarchar(maxVarcharChars)
	stringValue := sql.MapPrimaryKey(stringKey, mapStringValue)

	mapper := sql.NewDDLMapper(
		sql.FlatTypeMappings{
			sql.INTEGER:  mapInteger,
			sql.NUMBER:   sql.MapStatic("BINARY_DOUBLE", sql.UsingConverter(floatConverter)),
			sql.BOOLEAN:  sql.MapStatic("BOOLEAN"),
			sql.OBJECT:   jsonMapping,
			sql.ARRAY:    jsonMapping,
			sql.MULTIPLE: jsonMapping,
			sql.BINARY: sql.MapPrimaryKey(
				sql.MapStatic("RAW(2000)", sql.AlsoCompatibleWith("RAW"), sql.UsingConverter(sql.Base64Decoder)),
				sql.MapStatic("BLOB", sql.UsingConverter(sql.Base64Decoder)),
			),
			sql.STRING_INTEGER: sql.MapStringMaxLen(
				mapNumber,
				mapText(sql.ToStr),
				maxNumberDigits,
			),
			sql.STRING_NUMBER: sql.MapStatic("BINARY_DOUBLE", sql.UsingConverter(floatConverter)),
			sql.STRING: sql.MapString(sql.StringMappings{
				Fallback: stringValue,
				WithFormat: map[string]sql.MapProjectionFn{
					// Keys are strings: a TIMESTAMP WITH TIME ZONE can't be in a
					// primary key, and the text round-trips exactly.
					"date":      sql.MapPrimaryKey(stringKey, sql.MapStatic("DATE", sql.UsingConverter(sql.ClampDate))),
					"date-time": sql.MapPrimaryKey(stringKey, sql.MapStatic(timestampDDL, sql.UsingConverter(timestampConverter))),
					"time":      sql.MapPrimaryKey(stringKey, mapVarchar(maxVarcharChars)),
				},
			}),
		},
		// Oracle stores an empty string as NULL, so string columns must accept
		// NULL. Primary keys are NOT NULL regardless.
		sql.WithNotNullFn(func(ddl string) string {
			if isStringDDL(ddl) {
				return ddl
			}
			return ddl + " NOT NULL"
		}),
	)

	var directCast = sql.WithDirectCast()

	return sql.Dialect{
		MigratableTypes: sql.MigrationSpecs{
			"number(38,0)": {
				sql.NewMigrationSpec([]string{"BINARY_DOUBLE"}, sql.WithCastSQL(castTo("TO_BINARY_DOUBLE(%s)"))),
				sql.NewMigrationSpecTarget(anyVarchar{}, sql.WithCastSQL(castTo("TO_CHAR(%s)"))),
				sql.NewMigrationSpec([]string{"CLOB"}, sql.WithCastSQL(castTo("TO_CLOB(TO_CHAR(%s))"))),
			},
			"binary_double": {sql.NewMigrationSpec([]string{"CLOB"}, sql.WithCastSQL(castTo("TO_CLOB(TO_CHAR(%s, 'TM'))")))},
			"boolean":       {sql.NewMigrationSpec([]string{"CLOB"}, sql.WithCastSQL(castTo("CASE WHEN %[1]s THEN TO_CLOB('true') WHEN NOT %[1]s THEN TO_CLOB('false') END")))},
			"date":          {sql.NewMigrationSpec([]string{"CLOB"}, sql.WithCastSQL(castTo("TO_CLOB(TO_CHAR(%s, 'YYYY-MM-DD'))")))},
			"timestamp(9) with time zone": {sql.NewMigrationSpec([]string{"CLOB"},
				sql.WithCastSQL(castTo(`TO_CLOB(TO_CHAR(SYS_EXTRACT_UTC(%s), 'YYYY-MM-DD"T"HH24:MI:SS.FF9"Z"'))`)))},
			"varchar2": {
				sql.NewMigrationSpecTarget(&varcharWidening{}, directCast),
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
		MaxColumns:             1000,
		CaseInsensitiveColumns: false,
	}
}

func castTo(format string) sql.CastSQLFunc {
	return func(m sql.ColumnTypeMigration) string {
		return fmt.Sprintf(format, m.Identifier)
	}
}

func isStringDDL(ddl string) bool {
	return strings.HasPrefix(ddl, "VARCHAR2") || ddl == "CLOB"
}

// maxNumberDigits is the precision of NUMBER(38,0).
const maxNumberDigits = 38

// mapNumber also accepts NUMBER(*,0), which is how Oracle reports an existing
// INTEGER column.
var mapNumber = sql.MapStatic("NUMBER(38,0)", sql.AlsoCompatibleWith("NUMBER(*,0)"), sql.UsingConverter(numberConverter))

// mapInteger maps to NUMBER(38,0) unless the schema's range needs more
// digits, in which case the integer is stored as text.
func mapInteger(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
	var digits = 0
	if n := p.Inference.Numeric; n != nil {
		for _, bound := range []float64{n.Minimum, n.Maximum} {
			digits = max(digits, len(strconv.FormatFloat(math.Abs(bound), 'f', 0, 64)))
		}
	}
	if digits <= maxNumberDigits {
		return mapNumber(p)
	} else if digits+1 <= maxVarcharChars {
		return varcharMapping(digits+1, sql.ToStr)
	}
	return sql.MapStatic("CLOB", sql.UsingConverter(sql.ToStr))(p)
}

// mapStringValue sizes a VARCHAR2 from the schema's maxLength, and uses a CLOB
// when there is none or it doesn't fit.
func mapStringValue(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
	return mapText(nil)(p)
}

// mapText is mapStringValue with a converter to text applied first.
func mapText(toText sql.ElementConverter) sql.MapProjectionFn {
	return func(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
		if s := p.Inference.String_; s != nil && s.MaxLength > 0 && s.MaxLength <= maxVarcharChars {
			return varcharMapping(int(s.MaxLength), toText)
		}
		return sql.MapStatic("CLOB", sql.UsingConverter(toText))(p)
	}
}

// mapVarchar maps to a VARCHAR2 of the projection's maxLength, or of size
// characters when the projection has no smaller maxLength.
func mapVarchar(size int) sql.MapProjectionFn {
	return func(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
		var n = size
		if s := p.Inference.String_; s != nil && s.MaxLength > 0 && int(s.MaxLength) < n {
			n = int(s.MaxLength)
		}
		return varcharMapping(n, nil)
	}
}

// varcharMapping is a VARCHAR2 of n characters. A longer value fails with an
// error naming the limit: the driver would send it as a LONG bind, which
// Oracle reports as an unrelated bind error.
func varcharMapping(n int, toText sql.ElementConverter) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
	var col = &varcharColumn{Chars: n}
	return col, sql.CompatibleColumnTypes{col}, func(te tuple.TupleElement) (any, error) {
		var v any = te
		if toText != nil {
			var err error
			if v, err = toText(te); err != nil {
				return nil, err
			}
		}
		if s, ok := v.(string); ok && utf8.RuneCountInString(s) > n {
			return nil, fmt.Errorf("a value of %d characters is longer than the column's %s", utf8.RuneCountInString(s), col.DDL())
		}
		return v, nil
	}
}

// anyVarchar is a migration target of any VARCHAR2 size.
type anyVarchar struct{}

func (anyVarchar) CanMigrate(_ boilerplate.ExistingField, desired sql.MappedType) bool {
	_, ok := desired.TargetType.(*varcharColumn)
	return ok
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

// varcharWidening grows a VARCHAR2 in place, which Oracle does without
// rewriting the table.
type varcharWidening struct{}

func (*varcharWidening) CanMigrate(existing boilerplate.ExistingField, desired sql.MappedType) bool {
	target, ok := desired.TargetType.(*varcharColumn)
	return ok && strings.EqualFold(existing.Type, "VARCHAR2") && target.Chars >= existing.CharacterMaxLength
}

// isLOB reports whether a column's values may exceed 4000 bytes. The driver
// sends such values as LONG binds, which Oracle accepts only after every other
// bind of a statement.
func isLOB(c *sql.Column) bool {
	return strings.Contains(strings.ToUpper(c.BareDDL), "LOB")
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

// storeColumns orders a table's columns for INSERT, with the LOB columns last
// from position lobStart. order maps each position to the index of its value
// in Table.ConvertAll's output.
func storeColumns(t *sql.Table) (cols []*sql.Column, order []int, lobStart int) {
	return lobsLast(t.Columns(), 0)
}

// updateColumns orders a table's columns for UPDATE: the values to set, with
// the LOB columns last from position lobStart, and then the keys, which only
// order covers.
func updateColumns(t *sql.Table) (set []*sql.Column, order []int, lobStart int) {
	set, order, lobStart = lobsLast(t.Columns(), len(t.Keys))
	for i := range t.Keys {
		order = append(order, i)
	}
	return set, order, lobStart
}

// lobsLast orders the columns from index first on, with the LOB columns last
// from position lobStart. order maps each position to the column's index.
func lobsLast(all []*sql.Column, first int) (cols []*sql.Column, order []int, lobStart int) {
	for _, lob := range []bool{false, true} {
		if lob {
			lobStart = len(cols)
		}
		for i := first; i < len(all); i++ {
			if isLOB(all[i]) == lob {
				cols = append(cols, all[i])
				order = append(order, i)
			}
		}
	}
	return cols, order, lobStart
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

-- Inserts one row per element of the bound arrays, with LOB columns last.

{{ define "storeInsert" }}
INSERT INTO {{$.Identifier}} (
	{{- range $ind, $col := StoreColumns $ }}
		{{- if $ind }}, {{ end -}}
		{{$col.Identifier}}
	{{- end -}}
) VALUES (
	{{- range $ind, $col := StoreColumns $ }}
		{{- if $ind }}, {{ end -}}
		{{ Bind $col $ind }}
	{{- end -}}
)
{{ end }}

-- Updates the row of each bound key, with LOB columns last among the values.

{{ define "storeUpdate" }}
{{- $set := UpdateColumns $ }}
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
		"StoreColumns": func(t sql.Table) []*sql.Column {
			var cols, _, _ = storeColumns(&t)
			return cols
		},
		"UpdateColumns": func(t sql.Table) []*sql.Column {
			var set, _, _ = updateColumns(&t)
			return set
		},
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
