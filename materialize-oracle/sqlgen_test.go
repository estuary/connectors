package connector

import (
	stdsql "database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
)

var testDialect = createOracleDialect("test")
var testTemplates = renderTemplates(testDialect)

func TestIdentifiers(t *testing.T) {
	for _, tc := range []struct {
		field      string
		identifier string
		location   string
	}{
		{"id", "ID", "ID"},
		{"flow_document", "FLOW_DOCUMENT", "FLOW_DOCUMENT"},
		{"MixedCase", "MIXEDCASE", "MIXEDCASE"},
		{"a$1#", "A$1#", "A$1#"},
		{"select", `"select"`, "select"},
		{"Order Items", `"Order Items"`, "Order Items"},
		{"_meta/op", `"_meta/op"`, "_meta/op"},
		{"1st", `"1st"`, "1st"},
		{"a.b", `"a.b"`, "a.b"},
		{"日本語", `"日本語"`, "日本語"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			require.Equal(t, tc.identifier, testDialect.Identifier(tc.field))
			require.Equal(t, tc.location, testDialect.ColumnLocator(tc.field))
		})
	}
	require.Equal(t, "TEST.ORDERS", testDialect.Identifier("test", "orders"))
	require.Equal(t, sql.InfoTableLocation{TableSchema: "TEST", TableName: "ORDERS"}, testDialect.TableLocator([]string{"orders"}))
	require.Equal(t, sql.InfoTableLocation{TableSchema: "report schema", TableName: "ORDERS"}, testDialect.TableLocator([]string{"report schema", "orders"}))
	require.Equal(t, "'it''s literal'", testDialect.Literal("it's literal"))
	require.Equal(t, ":1", testDialect.Placeholder(0))
	require.Equal(t, ":12", testDialect.Placeholder(11))
}

func TestTypeMapping(t *testing.T) {
	for _, tc := range []struct {
		name      string
		types     []string
		str       *pf.Inference_String
		num       *pf.Inference_Numeric
		key       bool
		ddl       string
		nullable  bool
		input     any
		converted any
	}{
		{name: "integer", types: []string{"integer"}, ddl: "NUMBER(38)", input: nil, converted: nil},
		{name: "number", types: []string{"number"}, ddl: "BINARY_DOUBLE", input: 10.5, converted: 10.5},
		{name: "boolean", types: []string{"boolean"}, ddl: "NUMBER(1)", input: true, converted: int64(1)},
		{name: "false", types: []string{"boolean"}, ddl: "NUMBER(1)", input: false, converted: int64(0)},
		{name: "bounded string", types: []string{"string"}, str: &pf.Inference_String{MaxLength: 1000}, ddl: "VARCHAR2(4000 BYTE)", nullable: true, input: "hello", converted: "hello"},
		{name: "empty string", types: []string{"string"}, str: &pf.Inference_String{MaxLength: 1000}, ddl: "VARCHAR2(4000 BYTE)", nullable: true, input: "", converted: ""},
		{name: "multibyte overflow", types: []string{"string"}, str: &pf.Inference_String{MaxLength: 1001}, ddl: "CLOB", nullable: true, input: "é", converted: "é"},
		{name: "unbounded string", types: []string{"string"}, ddl: "CLOB", nullable: true, input: "hello", converted: "hello"},
		{name: "long unbounded string", types: []string{"string"}, ddl: "CLOB", nullable: true, input: strings.Repeat("x", maxClobStringBytes+1), converted: go_ora.Clob{String: strings.Repeat("x", maxClobStringBytes+1), Valid: true}},
		{name: "longest string bound as text", types: []string{"string"}, ddl: "CLOB", nullable: true, input: strings.Repeat("x", maxClobStringBytes), converted: strings.Repeat("x", maxClobStringBytes)},
		{name: "number at upper range", types: []string{"number"}, ddl: "BINARY_DOUBLE", input: 9.99e124, converted: 9.99e124},
		{name: "number at lower range", types: []string{"number"}, ddl: "BINARY_DOUBLE", input: -1e-125, converted: -1e-125},
		{name: "zero", types: []string{"number"}, ddl: "BINARY_DOUBLE", input: 0.0, converted: 0.0},
		{name: "integral number", types: []string{"number"}, ddl: "BINARY_DOUBLE", input: int64(7), converted: int64(7)},
		{name: "string number", types: []string{"string"}, str: &pf.Inference_String{Format: "number"}, ddl: "BINARY_DOUBLE", nullable: true, input: "1.5", converted: 1.5},
		{name: "string NaN", types: []string{"string"}, str: &pf.Inference_String{Format: "number"}, ddl: "BINARY_DOUBLE", nullable: true, input: "NaN", converted: nil},
		{name: "string Infinity", types: []string{"string"}, str: &pf.Inference_String{Format: "number"}, ddl: "BINARY_DOUBLE", nullable: true, input: "Infinity", converted: nil},
		{name: "string -Infinity", types: []string{"string"}, str: &pf.Inference_String{Format: "number"}, ddl: "BINARY_DOUBLE", nullable: true, input: "-Infinity", converted: nil},
		{name: "string key", types: []string{"string"}, key: true, ddl: "VARCHAR2(4000 BYTE)", input: "key", converted: "key"},
		{name: "sized string key", types: []string{"string"}, str: &pf.Inference_String{MaxLength: 36}, key: true, ddl: "VARCHAR2(144 BYTE)", input: "key", converted: "key"},
		{name: "long string key", types: []string{"string"}, str: &pf.Inference_String{MaxLength: 1001}, key: true, ddl: "VARCHAR2(4000 BYTE)", input: "key", converted: "key"},
		{name: "object", types: []string{"object"}, ddl: "CLOB", input: []byte(`{"n":1}`), converted: `{"n":1}`},
		{name: "array", types: []string{"array"}, ddl: "CLOB", input: []byte(`[1,2]`), converted: `[1,2]`},
		{name: "document", types: []string{"object"}, ddl: "CLOB", input: json.RawMessage(`{"id":1}`), converted: `{"id":1}`},
		{name: "multiple", types: []string{"boolean", "string"}, ddl: "CLOB", input: "hello", converted: `"hello"`},
		{name: "timestamp", types: []string{"string"}, str: &pf.Inference_String{Format: "date-time"}, ddl: "TIMESTAMP(9) WITH TIME ZONE", input: "2026-11-11T10:09:08.123456789+02:30", converted: go_ora.TimeStampTZ(time.Date(2026, 11, 11, 10, 9, 8, 123456789, time.FixedZone("", 9000)))},
		{name: "integer beyond 38 digits", types: []string{"integer"}, num: &pf.Inference_Numeric{HasMaximum: true, Maximum: 3.4e38}, ddl: "CLOB", input: uint64(18446744073709551615), converted: "18446744073709551615"},
		{name: "integer key beyond 38 digits", types: []string{"integer"}, num: &pf.Inference_Numeric{HasMinimum: true, Minimum: -1e39}, key: true, ddl: "VARCHAR2(4000 BYTE)", input: 1e39, converted: "1000000000000000000000000000000000000000"},
		{name: "string integer of 38 digits", types: []string{"string"}, str: &pf.Inference_String{Format: "integer", MaxLength: 38}, ddl: "NUMBER(38)", input: nil, converted: nil},
		{name: "string integer beyond 38 digits", types: []string{"string"}, str: &pf.Inference_String{Format: "integer", MaxLength: 39}, ddl: "CLOB", input: "123456789012345678901234567890123456789", converted: "123456789012345678901234567890123456789"},
		{name: "string integer key beyond 38 digits", types: []string{"string"}, str: &pf.Inference_String{Format: "integer", MaxLength: 77}, key: true, ddl: "VARCHAR2(308 BYTE)", input: "-1", converted: "-1"},
		{name: "timestamp with space", types: []string{"string"}, str: &pf.Inference_String{Format: "date-time"}, ddl: "TIMESTAMP(9) WITH TIME ZONE", input: "2026-11-11 10:09:08z", converted: go_ora.TimeStampTZ(time.Date(2026, 11, 11, 10, 9, 8, 0, time.UTC))},
		{name: "timestamp in year zero", types: []string{"string"}, str: &pf.Inference_String{Format: "date-time"}, ddl: "TIMESTAMP(9) WITH TIME ZONE", input: "0000-01-01T00:00:00Z", converted: go_ora.TimeStampTZ(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))},
		{name: "timestamp in year zero in UTC", types: []string{"string"}, str: &pf.Inference_String{Format: "date-time"}, ddl: "TIMESTAMP(9) WITH TIME ZONE", input: "0001-01-01T00:00:00+01:00", converted: go_ora.TimeStampTZ(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))},
		{name: "timestamp in year 10000 in UTC", types: []string{"string"}, str: &pf.Inference_String{Format: "date-time"}, ddl: "TIMESTAMP(9) WITH TIME ZONE", input: "9999-12-31T23:59:59-05:00", converted: go_ora.TimeStampTZ(time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC))},
		{name: "timestamp key", types: []string{"string"}, str: &pf.Inference_String{Format: "date-time"}, key: true, ddl: "VARCHAR2(64 BYTE)", input: "2026-11-11T10:09:08.123456789+02:30", converted: "2026-11-11T10:09:08.123456789+02:30"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mapped := testDialect.MapType(&sql.Projection{Projection: pf.Projection{
				Field: "a_field", IsPrimaryKey: tc.key,
				Inference: pf.Inference{Types: tc.types, String_: tc.str, Numeric: tc.num, Exists: pf.Inference_MUST},
			}}, sql.FieldConfig{})
			require.Equal(t, tc.ddl, mapped.BareDDL)
			if tc.nullable {
				require.Equal(t, tc.ddl, mapped.DDL)
			} else {
				require.Equal(t, tc.ddl+" NOT NULL", mapped.DDL)
			}
			converted, err := mapped.Converter(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.converted, converted)
			converted, err = mapped.Converter(nil)
			require.NoError(t, err)
			require.Nil(t, converted)
		})
	}
}

func TestNumberConversion(t *testing.T) {
	for _, value := range []any{int64(-9223372036854775807), uint64(18446744073709551615), "99999999999999999999999999999999999999"} {
		converted, err := toNumber(value)
		require.NoError(t, err)
		actual, err := converted.(*go_ora.Number).String()
		require.NoError(t, err)
		want, err := sql.ToStr(value)
		require.NoError(t, err)
		require.Equal(t, want, actual)
	}
	// Flow accepts any decimal string with an integer value.
	for value, want := range map[string]string{
		"+5": "5", "+0": "0", "-0": "0", "0123": "123", "-0123": "-123", "1E3": "1000", "1e3": "1000",
		"1e+3": "1000", "1.00": "1", "1.5e1": "15", "120e-1": "12", "-12E1": "-120",
		"-12345678901234567890123456789012345678": "-12345678901234567890123456789012345678",
	} {
		converted, err := toNumber(value)
		require.NoError(t, err, value)
		actual, err := converted.(*go_ora.Number).String()
		require.NoError(t, err)
		require.Equal(t, want, actual, value)
	}
	for _, value := range []string{"", "1.5", "1e-1", "0x10", "1/1", "1_000", "abc", "1e99999",
		"1e200", "1E39", "-1e200", "123456789012345678901234567890123456789"} {
		_, err := toNumber(value)
		require.ErrorContains(t, err, "parsing integer", value)
	}

	// Floats outside the range of Oracle NUMBER are bound as text without a
	// decimal separator, and subnormal floats as zero.
	number := testDialect.MapType(&sql.Projection{Projection: pf.Projection{Field: "f", Inference: pf.Inference{Types: []string{"number"}}}}, sql.FieldConfig{})
	stringNumber := testDialect.MapType(&sql.Projection{Projection: pf.Projection{Field: "f", Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "number"}}}}, sql.FieldConfig{})
	for value, want := range map[float64]any{
		1.7976931348623157e308:  "17976931348623157E292",
		-1e200:                  "-1E200",
		1e125:                   "1E125",
		1.5e-200:                "15E-201",
		-1e-126:                 "-1E-126",
		2.2250738585072014e-308: "22250738585072014E-324",
		5e-324:                  0.0,
		-2.225073858507201e-308: 0.0,
		9.99e124:                9.99e124,
		1e-125:                  1e-125,
	} {
		converted, err := number.Converter(value)
		require.NoError(t, err, value)
		require.Equal(t, want, converted, value)
		converted, err = stringNumber.Converter(strconv.FormatFloat(value, 'g', -1, 64))
		require.NoError(t, err, value)
		require.Equal(t, want, converted, value)
	}
	_, err := rfc3339ToTimestamp("not a timestamp")
	require.Error(t, err)
	_, err = boolToNumber("true")
	require.Error(t, err)
}

func TestSQLGeneration(t *testing.T) {
	snap, tables := sql.RunSqlGenTests(t, testDialect, func(table string) []string { return []string{"test", table} }, sql.TestTemplates{
		TableTemplates: []*template.Template{testTemplates.createTargetTable},
		TplUpdateFence: testTemplates.updateFence,
	})
	for _, table := range tables {
		staged := stagedTable{Table: table, LoadTable: "TEST.FLOW_LOAD_TEST", StoreTable: "TEST.FLOW_STORE_TEST", DeleteColumn: `"_flow_delete"`}
		for _, tpl := range []*template.Template{
			testTemplates.createLoadTable, testTemplates.createStoreTable,
			testTemplates.loadInsert, testTemplates.storeInsert,
			testTemplates.loadQuery, testTemplates.loadQueryNoFlowDocument,
			testTemplates.mergeInto, testTemplates.directCopy, testTemplates.deleteFrom,
		} {
			var query strings.Builder
			require.NoError(t, tpl.Execute(&query, &staged))
			snap.WriteString("--- " + table.Identifier + " " + tpl.Name() + " ---\n" + query.String() + "\n")
			require.NotContains(t, query.String(), ";", "Oracle driver statements must not end in a semicolon")
		}
	}
	checkpoints, err := sql.ResolveTable(*sql.FlowCheckpointsTable([]string{"test"}), testDialect)
	require.NoError(t, err)
	require.Equal(t, "TEST.FLOW_CHECKPOINTS_V1", checkpoints.Identifier)
	require.Equal(t, "VARCHAR2(4000 BYTE)", checkpoints.Keys[0].BareDDL)
	require.Equal(t, "CLOB", checkpoints.Values[1].BareDDL)
	ddl, err := sql.RenderTableTemplate(checkpoints, testTemplates.createTargetTable)
	require.NoError(t, err)
	snap.WriteString("--- checkpoints ---\n" + ddl)
	cupaloy.SnapshotT(t, snap.String())
}

func TestLoadQueryTextColumns(t *testing.T) {
	// Values cast to strings are stored as text, which the no-flow-document
	// load query must read back as JSON and not as Oracle numbers.
	cast := json.RawMessage(`{"castToString": true}`)
	table, err := sql.ResolveTable(sql.TableShape{
		Path: sql.TablePath{"test", "text_columns"},
		Keys: []sql.Projection{{Projection: pf.Projection{
			Field: "key", Ptr: "/key", IsPrimaryKey: true,
			Inference: pf.Inference{Types: []string{"boolean"}, Exists: pf.Inference_MUST},
		}}},
		Values: []sql.Projection{
			{Projection: pf.Projection{Field: "flag", Ptr: "/flag", Inference: pf.Inference{Types: []string{"boolean"}}}},
			{Projection: pf.Projection{Field: "ratio", Ptr: "/ratio", Inference: pf.Inference{Types: []string{"number"}}}},
			{Projection: pf.Projection{Field: "plain", Ptr: "/plain", Inference: pf.Inference{Types: []string{"boolean"}}}},
			{Projection: pf.Projection{Field: "flag_ddl", Ptr: "/flag_ddl", Inference: pf.Inference{Types: []string{"boolean"}}}},
			{Projection: pf.Projection{Field: "ratio_ddl", Ptr: "/ratio_ddl", Inference: pf.Inference{Types: []string{"number"}}}},
		},
		FieldConfigJsonMap: map[string]json.RawMessage{
			"key": cast, "flag": cast, "ratio": cast,
			"flag_ddl":  json.RawMessage(`{"DDL": "VARCHAR2(5)"}`),
			"ratio_ddl": json.RawMessage(`{"DDL": "CLOB"}`),
		},
	}, testDialect)
	require.NoError(t, err)
	require.Equal(t, "VARCHAR2(4000 BYTE)", table.Keys[0].BareDDL)
	require.Equal(t, "CLOB", table.Values[0].BareDDL)
	require.Equal(t, "CLOB", table.Values[1].BareDDL)

	// DDL overrides to text store the same JSON text as castToString.
	for _, tc := range []struct {
		col   sql.Column
		input any
		want  any
	}{
		{table.Values[3], true, "true"},
		{table.Values[3], false, "false"},
		{table.Values[4], 0.5, "0.5"},
		{table.Values[0], true, "true"},
		{table.Values[2], true, int64(1)},
	} {
		converted, err := tc.col.Converter(tc.input)
		require.NoError(t, err)
		require.Equal(t, tc.want, converted, tc.col.Field)
	}

	var query strings.Builder
	require.NoError(t, testTemplates.loadQueryNoFlowDocument.Execute(&query, &stagedTable{Table: table, LoadTable: "TEST.FLOW_LOAD_TEST"}))
	require.Contains(t, query.String(), "'key' VALUE r.KEY FORMAT JSON")
	require.Contains(t, query.String(), "'flag' VALUE r.FLAG FORMAT JSON")
	require.Contains(t, query.String(), "'ratio' VALUE r.RATIO FORMAT JSON")
	require.Contains(t, query.String(), "'plain' VALUE CASE r.PLAIN WHEN 1 THEN 'true' WHEN 0 THEN 'false' END FORMAT JSON")
	require.Contains(t, query.String(), "'flag_ddl' VALUE r.FLAG_DDL FORMAT JSON")
	require.Contains(t, query.String(), "'ratio_ddl' VALUE r.RATIO_DDL FORMAT JSON")
	require.NotContains(t, query.String(), "CASE r.FLAG")
	require.NotContains(t, query.String(), "CASE r.KEY")
}

func TestKeyColumns(t *testing.T) {
	projection := func(field string, key bool, str *pf.Inference_String) sql.Projection {
		return sql.Projection{Projection: pf.Projection{
			Field: field, Ptr: "/" + field, IsPrimaryKey: key,
			Inference: pf.Inference{Types: []string{"string"}, String_: str, Exists: pf.Inference_MUST},
		}}
	}
	table, err := sql.ResolveTable(sql.TableShape{
		Path: sql.TablePath{"test", "keyed"},
		Keys: []sql.Projection{
			projection("tenant_id", true, nil),
			projection("sku", true, &pf.Inference_String{MaxLength: 36}),
			projection("event_time", true, &pf.Inference_String{Format: "date-time"}),
		},
		Values: []sql.Projection{
			projection("code", false, &pf.Inference_String{MaxLength: 100}),
			projection("note", false, nil),
			projection("updated_at", false, &pf.Inference_String{Format: "date-time"}),
		},
	}, testDialect)
	require.NoError(t, err)
	ddl, err := sql.RenderTableTemplate(table, testTemplates.createTargetTable)
	require.NoError(t, err)
	require.Equal(t, `
CREATE TABLE TEST.KEYED (
	TENANT_ID VARCHAR2(4000 BYTE) NOT NULL
	, SKU VARCHAR2(144 BYTE) NOT NULL
	, EVENT_TIME VARCHAR2(64 BYTE) NOT NULL
	, CODE VARCHAR2(4000 BYTE)
	, NOTE CLOB
	, UPDATED_AT TIMESTAMP(9) WITH TIME ZONE NOT NULL,
	PRIMARY KEY (TENANT_ID, SKU, EVENT_TIME)
)
`, ddl)

	// Oracle stores empty strings as NULL, so required text values stay nullable.
	for _, col := range table.Values[:2] {
		converted, err := col.Converter("")
		require.NoError(t, err)
		require.Contains(t, []any{"", go_ora.Clob{Valid: true}}, converted)
	}
	converted, err := table.Keys[2].Converter("2026-11-11T10:09:08.123456789+02:30")
	require.NoError(t, err)
	require.Equal(t, "2026-11-11T10:09:08.123456789+02:30", converted)
}

func TestVarcharCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name     string
		str      *pf.Inference_String
		key      bool
		existing int
		want     bool
	}{
		{name: "key fits", str: &pf.Inference_String{MaxLength: 16}, key: true, existing: 64, want: true},
		{name: "key in wider column", str: &pf.Inference_String{MaxLength: 16}, key: true, existing: 4000, want: true},
		{name: "key maxLength grew", str: &pf.Inference_String{MaxLength: 100}, key: true, existing: 64},
		{name: "key maxLength removed", key: true, existing: 64},
		{name: "short text in smaller column", str: &pf.Inference_String{MaxLength: 1000}, existing: 1000},
		{name: "short text fits", str: &pf.Inference_String{MaxLength: 1000}, existing: 4000, want: true},
		{name: "short text in user-sized column", str: &pf.Inference_String{MaxLength: 100}, existing: 400, want: true},
		{name: "short text in former key column", str: &pf.Inference_String{MaxLength: 16}, existing: 64, want: true},
		{name: "short text in too small column", str: &pf.Inference_String{MaxLength: 100}, existing: 399},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mapped := testDialect.MapType(&sql.Projection{Projection: pf.Projection{
				Field: "id", Ptr: "/id", IsPrimaryKey: tc.key,
				Inference: pf.Inference{Types: []string{"string"}, String_: tc.str, Exists: pf.Inference_MUST},
			}}, sql.FieldConfig{})
			require.Equal(t, tc.want, mapped.Compatible(boilerplate.ExistingField{Name: "ID", Type: "VARCHAR2", CharacterMaxLength: tc.existing}))
		})
	}
}

func TestStringColumnMigration(t *testing.T) {
	for _, tc := range []struct {
		name        string
		types       []string
		str         *pf.Inference_String
		existing    boilerplate.ExistingField
		wantDDL     string
		wantCompat  bool
		wantMigrate bool
	}{
		{name: "maxLength grows past VARCHAR2", types: []string{"string"}, str: &pf.Inference_String{MaxLength: 1001}, existing: boilerplate.ExistingField{Type: "VARCHAR2", CharacterMaxLength: 4000}, wantDDL: "CLOB", wantMigrate: true},
		{name: "maxLength removed", types: []string{"string"}, existing: boilerplate.ExistingField{Type: "VARCHAR2", CharacterMaxLength: 4000}, wantDDL: "CLOB", wantMigrate: true},
		{name: "maxLength shrinks below CLOB", types: []string{"string"}, str: &pf.Inference_String{MaxLength: 500}, existing: boilerplate.ExistingField{Type: "CLOB"}, wantDDL: "VARCHAR2(4000 BYTE)", wantCompat: true},
		{name: "maxLength still fits", types: []string{"string"}, str: &pf.Inference_String{MaxLength: 1000}, existing: boilerplate.ExistingField{Type: "VARCHAR2", CharacterMaxLength: 4000}, wantDDL: "VARCHAR2(4000 BYTE)", wantCompat: true},
		{name: "JSON is not migrated", types: []string{"object"}, existing: boilerplate.ExistingField{Type: "VARCHAR2", CharacterMaxLength: 4000}, wantDDL: "CLOB"},
		{name: "keys are not migrated", types: []string{"string"}, existing: boilerplate.ExistingField{Type: "CLOB"}, wantDDL: "VARCHAR2(4000 BYTE)"},
		{name: "integer to unbounded string is not migrated", types: []string{"string"}, existing: boilerplate.ExistingField{Type: "NUMBER(38)"}, wantDDL: "CLOB"},
		{name: "wider integer string is not migrated", types: []string{"string"}, str: &pf.Inference_String{Format: "integer", MaxLength: 39}, existing: boilerplate.ExistingField{Type: "NUMBER(38)"}, wantDDL: "CLOB"},
		{name: "integer to bounded string is not migrated", types: []string{"string"}, str: &pf.Inference_String{MaxLength: 10}, existing: boilerplate.ExistingField{Type: "NUMBER(38)"}, wantDDL: "VARCHAR2(4000 BYTE)"},
		{name: "integer to JSON is not migrated", types: []string{"object"}, existing: boilerplate.ExistingField{Type: "NUMBER(38)"}, wantDDL: "CLOB"},
		{name: "boolean to string is not migrated", types: []string{"string"}, existing: boilerplate.ExistingField{Type: "NUMBER(1)"}, wantDDL: "CLOB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mapped := testDialect.MapType(&sql.Projection{Projection: pf.Projection{
				Field: "a_field", IsPrimaryKey: tc.name == "keys are not migrated",
				Inference: pf.Inference{Types: tc.types, String_: tc.str},
			}}, sql.FieldConfig{})
			mapped.MigratableTypes = testDialect.MigratableTypes
			tc.existing.Name, tc.existing.Nullable = "A_FIELD", true
			require.Equal(t, tc.wantDDL, mapped.BareDDL)
			require.Equal(t, tc.wantCompat, mapped.Compatible(tc.existing))
			require.Equal(t, tc.wantMigrate, mapped.CanMigrate(tc.existing))
		})
	}

	mapped := testDialect.MapType(&sql.Projection{Projection: pf.Projection{
		Field: "note", Inference: pf.Inference{Types: []string{"string"}},
	}}, sql.FieldConfig{})
	existing := boilerplate.ExistingField{Name: "NOTE", Type: "VARCHAR2", CharacterMaxLength: 4000, Nullable: true}
	spec := testDialect.MigratableTypes.FindMigrationSpec(existing, mapped)
	require.NotNil(t, spec)
	c := &client{ep: &sql.Endpoint[config]{Dialect: testDialect}}
	for _, tc := range []struct {
		name               string
		progress, original bool
		want               []string
	}{
		{name: "new migration", original: true, want: []string{
			"ALTER TABLE TEST.ORDERS ADD (NOTE_FLOWTMP1 CLOB)",
			"UPDATE TEST.ORDERS SET NOTE_FLOWTMP1 = TO_CLOB(NOTE)",
			"ALTER TABLE TEST.ORDERS DROP COLUMN NOTE",
			"ALTER TABLE TEST.ORDERS RENAME COLUMN NOTE_FLOWTMP1 TO NOTE",
		}},
		{name: "resumed after the copy", progress: true, original: true, want: []string{
			"UPDATE TEST.ORDERS SET NOTE_FLOWTMP1 = TO_CLOB(NOTE)",
			"ALTER TABLE TEST.ORDERS DROP COLUMN NOTE",
			"ALTER TABLE TEST.ORDERS RENAME COLUMN NOTE_FLOWTMP1 TO NOTE",
		}},
		{name: "resumed after the drop", progress: true, want: []string{
			"ALTER TABLE TEST.ORDERS RENAME COLUMN NOTE_FLOWTMP1 TO NOTE",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, _, err := c.AlterTable(t.Context(), sql.TableAlter{
				Table: sql.Table{Identifier: "TEST.ORDERS"},
				ColumnTypeChanges: []sql.ColumnTypeMigration{{
					Column:               sql.Column{Projection: sql.Projection{Projection: pf.Projection{Field: "note"}}, MappedType: mapped, Identifier: "NOTE"},
					MigrationSpec:        *spec,
					ProgressColumnExists: tc.progress,
					OriginalColumnExists: tc.original,
				}},
			})
			require.NoError(t, err)
			require.Equal(t, strings.Join(tc.want, "\n"), stmts)
		})
	}
}

func TestIntegerColumnMigrationUnsupported(t *testing.T) {
	recording := new(recordingSQL)
	db := stdsql.OpenDB(recording)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	c := &client{db: db, ep: &sql.Endpoint[config]{Dialect: testDialect}}
	d := testDriver(t, &validationTestClient{
		Client: c,
		tables: map[string][]boilerplate.ExistingField{
			"ORDERS": {
				{Name: "ID", Type: "NUMBER(38)"},
				{Name: "STATUS", Type: "NUMBER(38)", Nullable: true},
				{Name: "FLOW_DOCUMENT", Type: "CLOB"},
			},
			"FLOW_CHECKPOINTS_V1": {},
		},
	})
	last := testMaterialization(t, testConfig())
	last.Bindings[0].Collection.GetProjection("status").Inference.String_ = &pf.Inference_String{Format: "uint64", MaxLength: 20}
	next := testMaterialization(t, testConfig())
	binding := next.Bindings[0]
	validated, err := d.Validate(t.Context(), &pm.Request_Validate{
		Name: next.Name, ConfigJson: next.ConfigJson, ConnectorType: next.ConnectorType, LastMaterialization: last,
		Bindings: []*pm.Request_Validate_Binding{{ResourceConfigJson: binding.ResourceConfigJson, Collection: binding.Collection}},
	})
	require.NoError(t, err)
	var found bool
	for _, pc := range validated.Bindings[0].ProjectionConstraints {
		if pc.Field == "status" {
			found = true
			require.Equal(t, pm.Response_Validated_Constraint_INCOMPATIBLE, pc.Constraint.Type)
		}
	}
	require.True(t, found)
	_, err = d.Apply(t.Context(), &pm.Request_Apply{Materialization: next, LastMaterialization: last, Version: "next", LastVersion: "previous"})
	require.NoError(t, err)
	require.Empty(t, recording.queries())
}

func TestExistingColumnType(t *testing.T) {
	null := stdsql.NullInt64{}
	n := func(v int64) stdsql.NullInt64 { return stdsql.NullInt64{Int64: v, Valid: true} }
	for _, tc := range []struct {
		dataType         string
		precision, scale stdsql.NullInt64
		want             string
	}{
		{"NUMBER", n(1), n(0), "NUMBER(1)"},
		{"NUMBER", n(38), n(0), "NUMBER(38)"},
		{"NUMBER", n(10), n(2), "NUMBER(10,2)"},
		{"NUMBER", null, null, "NUMBER"},
		{"NUMBER", null, n(0), "NUMBER"},
		{"NUMBER", null, n(2), "NUMBER(*,2)"},
		{"TIMESTAMP(9) WITH TIME ZONE", null, n(9), "TIMESTAMP WITH TIME ZONE"},
		{"VARCHAR2", null, null, "VARCHAR2"},
	} {
		require.Equal(t, tc.want, existingColumnType(tc.dataType, tc.precision, tc.scale), tc.want)
	}

	// A boolean column can't hold integers, but an integer column can hold booleans.
	integer := testDialect.MapType(&sql.Projection{Projection: pf.Projection{Field: "n", Inference: pf.Inference{Types: []string{"integer"}}}}, sql.FieldConfig{})
	boolean := testDialect.MapType(&sql.Projection{Projection: pf.Projection{Field: "b", Inference: pf.Inference{Types: []string{"boolean"}}}}, sql.FieldConfig{})
	for _, tc := range []struct {
		existing         string
		integer, boolean bool
	}{
		{"NUMBER(1)", false, true},
		{"NUMBER(38)", true, true},
		{"NUMBER", true, true},
		{"NUMBER(10,2)", false, false},
	} {
		existing := boilerplate.ExistingField{Name: "N", Type: tc.existing, Nullable: true}
		require.Equal(t, tc.integer, integer.Compatible(existing), tc.existing)
		require.Equal(t, tc.boolean, boolean.Compatible(existing), tc.existing)
	}
}

func TestExistingCharacterLength(t *testing.T) {
	for _, tc := range []struct {
		dataType string
		length   int64
		want     int
	}{
		{"VARCHAR2", 400, 400},
		{"NVARCHAR2", 200, 200},
		{"CHAR", 1, 1},
		{"NCHAR", 2, 2},
		{"RAW", 16, 16},
		{"NUMBER", 22, 0},
		{"CLOB", 4000, 0},
		{"BINARY_DOUBLE", 8, 0},
		{"TIMESTAMP WITH TIME ZONE", 13, 0},
	} {
		require.Equal(t, tc.want, existingCharacterLength(tc.dataType, tc.length), tc.dataType)
	}
}
