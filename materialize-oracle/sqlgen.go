package connector

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
)

// Unquoted identifiers start with a letter, and Oracle stores them in upper
// case.
var simpleIdentifierRegexp = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

func isSimpleIdentifier(s string) bool {
	return simpleIdentifierRegexp.MatchString(s) && !slices.Contains(reservedWords, strings.ToUpper(s))
}

// Neither quoted nor unquoted identifiers can contain a double quote.
func sanitizeIdentifier(s string) string {
	return strings.ReplaceAll(s, `"`, "_")
}

// translateIdentifier returns the name Oracle stores for s.
func translateIdentifier(s string) string {
	if isSimpleIdentifier(s) {
		return strings.ToUpper(s)
	}
	return sanitizeIdentifier(s)
}

func quoteIdentifier(s string) string {
	if isSimpleIdentifier(s) {
		return s
	}
	return `"` + sanitizeIdentifier(s) + `"`
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
				sql.MapStatic("NUMBER(38,0)", sql.UsingConverter(numberConverter)),
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
					"date-time": sql.MapPrimaryKey(stringKey, sql.MapStatic("TIMESTAMP(9) WITH TIME ZONE", sql.UsingConverter(timestampConverter))),
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
		Identifierer:    sql.IdentifierFn(sql.JoinTransform(".", quoteIdentifier)),
		Literaler:       sql.ToLiteralFn(sql.QuoteTransform("'", "''")),
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
		return sql.MapStatic("NUMBER(38,0)", sql.UsingConverter(numberConverter))(p)
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

// bindExpr wraps a placeholder so the database parses the bound text into the
// column's type.
func bindExpr(c *sql.Column, placeholder string) string {
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

// storeColumns orders a table's columns for INSERT, with LOB columns last.
// The returned permutation maps each position to the index of the value from
// Table.ConvertAll.
func storeColumns(t *sql.Table) ([]*sql.Column, []int) {
	var cols = t.Columns()
	var order = make([]int, len(cols))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		var la, lb = isLOB(cols[a]), isLOB(cols[b])
		switch {
		case la == lb:
			return 0
		case lb:
			return -1
		default:
			return 1
		}
	})
	var out = make([]*sql.Column, len(cols))
	for i, idx := range order {
		out[i] = cols[idx]
	}
	return out, order
}

// A table's staged rows wait in a stage table beside it, which has the
// table's columns and these two, until their transaction commits.
const (
	stageTokenColumn = "flow_token"
	stageOpColumn    = "flow_op"
	stagePrefix      = "flow_stage_"
	// Oracle's limit on the length of a name.
	maxIdentifierBytes = 128
)

// Operations of staged rows.
const (
	opInsert = "I" // a key the table doesn't have
	opUpdate = "U" // replaces the row of an existing key
	opDelete = "D" // deletes the row of an existing key, and has only the keys
)

// stagePath is the path of the stage table of the table at path.
func stagePath(path []string) []string {
	var table = path[len(path)-1]
	var name = stagePrefix + table
	if len(name) > maxIdentifierBytes {
		var sum = sha256.Sum256([]byte(table))
		var suffix = "_" + hex.EncodeToString(sum[:8])
		name = name[:maxIdentifierBytes-len(suffix)]
		for !utf8.ValidString(name) {
			name = name[:len(name)-1]
		}
		name += suffix
	}
	return append(slices.Clone(path[:len(path)-1]), name)
}

// createStageSQL creates the stage table of t. Its columns take their types
// from t's, but are nullable: CTAS doesn't carry NOT NULL to outer-joined
// columns.
func createStageSQL(t *sql.Table, stage string) string {
	var cols = []string{
		"CAST(NULL AS VARCHAR2(36)) " + stageTokenColumn,
		"CAST(NULL AS CHAR(1)) " + stageOpColumn,
	}
	for _, c := range t.Columns() {
		cols = append(cols, "t."+c.Identifier)
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS SELECT %s FROM dual LEFT JOIN %s t ON 1 = 0 WHERE 1 = 0",
		stage, strings.Join(cols, ", "), t.Identifier)
}

// stageInsertSQL inserts one row per element of the bound arrays into the
// stage table: the token, the operation, then t's columns in storeColumns
// order.
func stageInsertSQL(t *sql.Table, stage string) string {
	var cols, _ = storeColumns(t)
	var names = []string{stageTokenColumn, stageOpColumn}
	var binds = []string{":1", ":2"}
	for i, c := range cols {
		names = append(names, c.Identifier)
		binds = append(binds, bindExpr(c, fmt.Sprintf(":%d", i+3)))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", stage, strings.Join(names, ", "), strings.Join(binds, ", "))
}

// tokenBinds lists n placeholders, for tokens bound in order.
func tokenBinds(n int) string {
	var binds = make([]string, n)
	for i := range binds {
		binds[i] = fmt.Sprintf(":%d", i+1)
	}
	return strings.Join(binds, ", ")
}

// applyDeleteSQL deletes the rows of t whose keys are staged with op, by the
// transactions of the bound tokens.
func applyDeleteSQL(t *sql.Table, stage, op string, tokens int) string {
	var conds []string
	for _, k := range t.Keys {
		conds = append(conds, fmt.Sprintf("s.%s = r.%s", k.Identifier, k.Identifier))
	}
	return fmt.Sprintf("DELETE FROM %s r WHERE EXISTS (SELECT 1 FROM %s s WHERE s.%s IN (%s) AND s.%s = '%s' AND %s)",
		t.Identifier, stage, stageTokenColumn, tokenBinds(tokens), stageOpColumn, op, strings.Join(conds, " AND "))
}

// applyInsertSQL inserts into t the rows staged by the transactions of the
// bound tokens, other than deletions.
func applyInsertSQL(t *sql.Table, stage string, tokens int) string {
	var names []string
	for _, c := range t.Columns() {
		names = append(names, c.Identifier)
	}
	var cols = strings.Join(names, ", ")
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s IN (%s) AND %s <> '%s'",
		t.Identifier, cols, cols, stage, stageTokenColumn, tokenBinds(tokens), stageOpColumn, opDelete)
}

// clearStageSQL deletes the rows staged by the transactions of the bound
// tokens.
func clearStageSQL(stage string, tokens int) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", stage, stageTokenColumn, tokenBinds(tokens))
}

// pruneStageSQL deletes the rows of tokens that sort before the token bound as
// :1, other than the n tokens bound after it.
func pruneStageSQL(stage string, n int) string {
	var query = fmt.Sprintf("DELETE FROM %s WHERE %s < :1", stage, stageTokenColumn)
	if n > 0 {
		var binds = make([]string, n)
		for i := range binds {
			binds[i] = fmt.Sprintf(":%d", i+2)
		}
		query += fmt.Sprintf(" AND %s NOT IN (%s)", stageTokenColumn, strings.Join(binds, ", "))
	}
	return query
}

// keyKind is how a key column is sent to JSON_TABLE and compared to the
// table's column.
type keyKind int

const (
	keyNumber keyKind = iota
	keyString
	keyRaw
	keyBoolean
)

func keyKindOf(c *sql.Column) keyKind {
	switch {
	case strings.HasPrefix(c.BareDDL, "NUMBER"):
		return keyNumber
	case strings.HasPrefix(c.BareDDL, "RAW"):
		return keyRaw
	case c.BareDDL == "BOOLEAN":
		return keyBoolean
	default:
		return keyString
	}
}

// loadSQL selects the documents of a JSON array of key tuples, bound as :1.
// A key that fails to convert is an error rather than a silent miss.
func loadSQL(t *sql.Table) string {
	var cols, conds []string
	for i, k := range t.Keys {
		var lk = fmt.Sprintf("l.k%d", i)
		var path = fmt.Sprintf("'$[%d]'", i)
		switch keyKindOf(&k) {
		case keyNumber:
			cols = append(cols, fmt.Sprintf("k%d NUMBER PATH %s ERROR ON ERROR", i, path))
		case keyString:
			cols = append(cols, fmt.Sprintf("k%d VARCHAR2(4000) PATH %s ERROR ON ERROR", i, path))
		case keyRaw:
			cols = append(cols, fmt.Sprintf("k%d VARCHAR2(4000) PATH %s ERROR ON ERROR", i, path))
			lk = "HEXTORAW(" + lk + ")"
		case keyBoolean:
			cols = append(cols, fmt.Sprintf("k%d VARCHAR2(5) PATH %s ERROR ON ERROR", i, path))
			lk = "TO_BOOLEAN(" + lk + ")"
		}
		conds = append(conds, fmt.Sprintf("r.%s = %s", k.Identifier, lk))
	}

	// The optimizer has no row estimate for JSON_TABLE over a bind, and
	// without one hash-joins it to a full scan of the table, even with join
	// hints. The cardinality hint makes it look up each key in the primary key.
	return fmt.Sprintf(
		"SELECT /*+ LEADING(l r) USE_NL(r) INDEX(r) CARDINALITY(l 10) */ r.%s\nFROM JSON_TABLE(:1, '$[*]' ERROR ON ERROR COLUMNS (%s)) l\nJOIN %s r ON %s",
		t.Document.Identifier,
		strings.Join(cols, ", "),
		t.Identifier,
		strings.Join(conds, " AND "),
	)
}

// appendLoadKey appends a converted key as a JSON array to buf.
func appendLoadKey(buf []byte, t *sql.Table, converted []any) ([]byte, error) {
	buf = append(buf, '[')
	for i, v := range converted {
		if i > 0 {
			buf = append(buf, ',')
		}
		var elem any
		switch keyKindOf(&t.Keys[i]) {
		case keyNumber:
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("key %s: unexpected %T for a number", t.Keys[i].Field, v)
			}
			elem = json.Number(s)
		case keyRaw:
			b, ok := v.([]byte)
			if !ok {
				return nil, fmt.Errorf("key %s: unexpected %T for binary", t.Keys[i].Field, v)
			}
			elem = hex.EncodeToString(b)
		case keyBoolean:
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("key %s: unexpected %T for a boolean", t.Keys[i].Field, v)
			}
			elem = strconv.FormatBool(b)
		default:
			elem = v
		}
		enc, err := json.Marshal(elem)
		if err != nil {
			return nil, fmt.Errorf("encoding key %s: %w", t.Keys[i].Field, err)
		}
		buf = append(buf, enc...)
	}
	return append(buf, ']'), nil
}

type templates struct {
	createTargetTable *template.Template
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
`)

	return templates{
		createTargetTable: tplAll.Lookup("createTargetTable"),
	}
}
