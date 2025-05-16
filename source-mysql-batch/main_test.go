package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/go-mysql-org/go-mysql/client"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbAddress     = flag.String("db_address", "localhost:3306", "The database server address to use for tests")
	dbControlUser = flag.String("db_control_user", "root", "The user for test setup/control operations")
	dbControlPass = flag.String("db_control_pass", "secret1234", "The password the the test setup/control user")
	dbCaptureUser = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")
	dbCapturePass = flag.String("db_capture_pass", "secret1234", "The password for the capture user")

	testFeatureFlags = flag.String("feature_flags", "", "Feature flags to apply to all test captures.")
)

func TestMain(m *testing.M) {
	flag.Parse()
	if level, err := log.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		log.SetLevel(level)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	os.Exit(m.Run())
}

func testCaptureSpec(t testing.TB) *st.CaptureSpec {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var endpointSpec = &Config{
		Address:  *dbAddress,
		User:     *dbCaptureUser,
		Password: *dbCapturePass,
		Advanced: advancedConfig{
			PollSchedule: "200ms",
			FeatureFlags: *testFeatureFlags,
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"<TIMESTAMP>"`] = regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)

	return &st.CaptureSpec{
		Driver:       mysqlDriver,
		EndpointSpec: endpointSpec,
		Validator:    &st.OrderedCaptureValidator{IncludeSourcedSchemas: true},
		Sanitizers:   sanitizers,
	}
}

func discoverBindings(ctx context.Context, t testing.TB, cs *st.CaptureSpec, matchers ...*regexp.Regexp) []*pf.CaptureSpec_Binding {
	t.Helper()

	var discovery = cs.Discover(ctx, t, matchers...)
	var bindings []*pf.CaptureSpec_Binding
	for _, discovered := range discovery {
		var res Resource
		require.NoError(t, json.Unmarshal(discovered.ResourceConfigJson, &res))
		bindings = append(bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: discovered.ResourceConfigJson,
			Collection: pf.CollectionSpec{
				Name:           pf.Collection("acmeCo/test/" + discovered.RecommendedName),
				ReadSchemaJson: discovered.DocumentSchemaJson,
				Key:            discovered.Key,
			},
			ResourcePath: []string{res.Name},
			StateKey:     res.Name,
		})
	}
	return bindings
}

func testMySQLClient(t testing.TB) *client.Conn {
	t.Helper()
	var control, err = client.Connect(*dbAddress, *dbControlUser, *dbControlPass, "mysql")
	require.NoError(t, err)
	t.Cleanup(func() { control.Close() })
	return control
}

func testTableName(t *testing.T, uniqueID string) (name, id string) {
	t.Helper()
	const testSchemaName = "test"
	var baseName = strings.ToLower(strings.TrimPrefix(t.Name(), "Test"))
	for _, str := range []string{"/", "=", "(", ")"} {
		baseName = strings.ReplaceAll(baseName, str, "_")
	}
	return fmt.Sprintf("%s.%s_%s", testSchemaName, baseName, uniqueID), uniqueID
}

func createTestTable(t testing.TB, control *client.Conn, tableName, definition string) {
	t.Helper()
	control.Execute(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	control.Execute(fmt.Sprintf("CREATE TABLE %s %s", tableName, definition))
	t.Cleanup(func() { control.Execute(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
}

func summarizeBindings(t testing.TB, bindings []*pf.CaptureSpec_Binding) string {
	t.Helper()
	var summary = new(strings.Builder)
	for idx, binding := range bindings {
		fmt.Fprintf(summary, "Binding %d:\n", idx)
		bs, err := json.MarshalIndent(binding, "  ", "  ")
		require.NoError(t, err)
		io.Copy(summary, bytes.NewReader(bs))
		fmt.Fprintf(summary, "\n")
	}
	if len(bindings) == 0 {
		fmt.Fprintf(summary, "(no output)")
	}
	return summary.String()
}

func executeControlQuery(t testing.TB, control *client.Conn, query string, args ...any) {
	t.Helper()
	var results, err = control.Execute(query, args...)
	require.NoError(t, err)
	results.Close()
}

func setShutdownAfterQuery(t testing.TB, setting bool) {
	var oldSetting = TestShutdownAfterQuery
	TestShutdownAfterQuery = setting
	t.Cleanup(func() { TestShutdownAfterQuery = oldSetting })
}

func setResourceCursor(t testing.TB, binding *pf.CaptureSpec_Binding, cursor ...string) {
	var res Resource
	require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &res))
	res.Cursor = cursor
	var bs, err = json.Marshal(res)
	require.NoError(t, err)
	binding.ResourceConfigJson = bs
}

func uniqueTableID(t testing.TB, extra ...string) string {
	t.Helper()
	var h = sha256.New()
	h.Write([]byte(t.Name()))
	for _, x := range extra {
		h.Write([]byte{':'})
		h.Write([]byte(x))
	}
	var x = binary.BigEndian.Uint32(h.Sum(nil)[0:4])
	return fmt.Sprintf("%d", (x%900000)+100000)
}

// TestSpec verifies the connector's response to the Spec RPC against a snapshot.
func TestSpec(t *testing.T) {
	response, err := mysqlDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// TestQueryTemplate is a unit test which verifies that the default query template produces
// the expected output for initial/subsequent polling queries with different cursors.
func TestQueryTemplate(t *testing.T) {
	var res = &Resource{
		Name:       "test_foobar",
		SchemaName: "test",
		TableName:  "foobar",
	}

	tmpl, err := template.New("query").Funcs(templateFuncs).Parse(tableQueryTemplate)
	require.NoError(t, err)

	for _, tc := range []struct {
		Name    string
		IsFirst bool
		Cursor  []string
	}{
		{Name: "FirstNoCursor", IsFirst: true, Cursor: nil},
		{Name: "SubsequentNoCursor", IsFirst: false, Cursor: nil},
		{Name: "FirstOneCursor", IsFirst: true, Cursor: []string{"`ka`"}},
		{Name: "SubsequentOneCursor", IsFirst: false, Cursor: []string{"`ka`"}},
		{Name: "FirstTwoCursor", IsFirst: true, Cursor: []string{"`ka`", "`kb`"}},
		{Name: "SubsequentTwoCursor", IsFirst: false, Cursor: []string{"`ka`", "`kb`"}},
		{Name: "FirstThreeCursor", IsFirst: true, Cursor: []string{"`ka`", "`kb`", "`kc`"}},
		{Name: "SubsequentThreeCursor", IsFirst: false, Cursor: []string{"`ka`", "`kb`", "`kc`"}},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			var buf = new(strings.Builder)
			require.NoError(t, tmpl.Execute(buf, map[string]any{
				"IsFirstQuery": tc.IsFirst,
				"CursorFields": tc.Cursor,
				"SchemaName":   res.SchemaName,
				"TableName":    res.TableName,
			}))
			cupaloy.SnapshotT(t, buf.String())
		})
	}
}

// TestQueryPlaceholderExpansion is a unit test which verifies that the expandQueryPlaceholders
// function is doing its job properly.
func TestQueryPlaceholderExpansion(t *testing.T) {
	var querySource = `SELECT * FROM "test"."foobar" WHERE (ka > @flow_cursor_value[0]) OR (ka = @flow_cursor_value[0] AND kb > @flow_cursor_value[1]) OR (ka = @flow_cursor_value[0] AND kb = @flow_cursor_value[1] AND kc > @flow_cursor_value[2]) OR (x > ?) OR (y > ?);`
	var argvals = []any{1, "two", 3.0, "xval", "yval"}
	var query, args, err = expandQueryPlaceholders(querySource, argvals)
	require.NoError(t, err)
	var buf = new(strings.Builder)
	fmt.Fprintf(buf, "Query: %s\n\n---\n", query)
	for i, arg := range args {
		fmt.Fprintf(buf, "Argument %d: %#v\n", i, arg)
	}
	cupaloy.SnapshotT(t, buf.String())
}

// TestSimpleCapture exercises the simplest use-case of a capture first doing an initial
// backfill and subsequently capturing new rows using ["id"] as the cursor.
func TestSimpleCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName), i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)
		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName), i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestAsyncCapture performs a capture with periodic restarts, in parallel with a bunch of inserts.
func TestAsyncCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT)")

	// Have to sanitize the index within the polling interval because we're running
	// the capture in parallel with changes.
	cs.Sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		// Spawn a worker thread which will insert 250 rows of data over the course of 25 seconds.
		go func() {
			for i := 0; i < 250; i++ {
				time.Sleep(100 * time.Millisecond)
				control.Execute(fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName), i, fmt.Sprintf("Value for row %d", i))
				log.WithField("i", i).Debug("inserted row")
			}
		}()

		// Perform six captures each running for 5 seconds, then verify that
		// the resulting data is correct.
		for i := 0; i < 6; i++ {
			var captureCtx, cancelCapture = context.WithCancel(ctx)
			time.AfterFunc(5*time.Second, cancelCapture)
			cs.Capture(captureCtx, t, nil)
		}
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestIntegerTypes exercises discovery and capture of the integer types
// TINYINT, SMALLINT, MEDIUMINT, INTEGER, and BIGINT.
func TestIntegerTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        tinyint_col TINYINT,
        smallint_col SMALLINT,
        mediumint_col MEDIUMINT,
        integer_col INTEGER,
        bigint_col BIGINT
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, -128, -32768, -8388608, -2147483648, -9223372036854775808},
			{1, 127, 32767, 8388607, 2147483647, 9223372036854775807},
			{2, 0, 0, 0, 0, 0},
			{3, nil, nil, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestNumericTypes exercises discovery and capture of the numeric types
// FLOAT, DOUBLE, DECIMAL, BIT, and BOOLEAN.
func TestNumericTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        float_col FLOAT,
        double_col DOUBLE,
        decimal_col DECIMAL(10, 2),
        bit_col BIT(1),
        boolean_col BOOLEAN
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, 1.23, 2.34, 123.45, 1, true},
			{1, -1.23, -2.34, -123.45, 0, false},
			{2, 0.0, 0.0, 0.00, 1, true},
			{3, nil, nil, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestDateAndTimeTypes exercises discovery and capture of the date and time types
// DATE, DATETIME, TIMESTAMP, TIME, and YEAR.
func TestDateAndTimeTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        date_col DATE,
        datetime_col DATETIME,
        timestamp_col TIMESTAMP,
        time_col TIME,
        year_col YEAR
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, "2025-02-13", "2025-02-13 12:34:56", "2025-02-13 12:34:56", "12:34:56", 2025},
			{1, "2000-01-01", "2000-01-01 00:00:00", "2000-01-01 00:00:00", "00:00:00", 2000},
			{2, "1999-12-31", "1999-12-31 23:59:59", "1999-12-31 23:59:59", "23:59:59", 1999},
			{3, nil, nil, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestStringTypes exercises discovery and capture of the string types
// CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, and LONGTEXT.
func TestStringTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        char_col CHAR(10),
        varchar_col VARCHAR(255),
        tinytext_col TINYTEXT,
        text_col TEXT,
        mediumtext_col MEDIUMTEXT,
        longtext_col LONGTEXT
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, "char_val", "varchar_val", "tinytext_val", "text_val", "mediumtext_val", "longtext_val"},
			{1, "short", "short", "short", "short", "short", "short"},
			{2, "", "", "", "", "", ""},
			{3, nil, nil, nil, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestBinaryTypes exercises discovery and capture of the binary types
// BINARY, VARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, and LONGBLOB.
func TestBinaryTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        binary_col BINARY(16),
        varbinary_col VARBINARY(255),
        tinyblob_col TINYBLOB,
        blob_col BLOB,
        mediumblob_col MEDIUMBLOB,
        longblob_col LONGBLOB
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, []byte("binary_data_1234"), []byte("varbinary_data"), []byte("tinyblob_data"), []byte("blob_data"), []byte("mediumblob_data"), []byte("longblob_data")},
			{1, []byte("short"), []byte("short"), []byte("short"), []byte("short"), []byte("short"), []byte("short")},
			{2, []byte(""), []byte(""), []byte(""), []byte(""), []byte(""), []byte("")},
			{3, nil, nil, nil, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestEnumAndSetTypes exercises discovery and capture of the domain-specific string types ENUM and SET.
func TestEnumAndSetTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        enum_col ENUM('value1', 'value2', 'value3'),
        set_col SET('a', 'b', 'c', 'd')
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, "value1", "a,b"},
			{1, "value2", "b,c"},
			{2, "value3", "c,d"},
			{3, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestSpatialTypes exercises discovery and capture of the spatial types
// GEOMETRY, POINT, LINESTRING, and POLYGON.
func TestSpatialTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        geometry_col GEOMETRY,
        point_col POINT,
        linestring_col LINESTRING,
        polygon_col POLYGON
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, "POINT(1 1)", "POINT(1 1)", "LINESTRING(0 0, 1 1, 2 2)", "POLYGON((0 0, 1 1, 1 0, 0 0))"},
			{1, "POINT(2 2)", "POINT(2 2)", "LINESTRING(2 2, 3 3, 4 4)", "POLYGON((1 1, 2 2, 2 1, 1 1))"},
			{2, "POINT(3 3)", "POINT(3 3)", "LINESTRING(3 3, 4 4, 5 5)", "POLYGON((2 2, 3 3, 3 2, 2 2))"},
			{3, nil, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ST_GeomFromText(?), ST_GeomFromText(?), ST_GeomFromText(?), ST_GeomFromText(?))", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestJSONType exercises discovery and capture of the JSON column type.
func TestJSONType(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        json_col JSON
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, `{"key1": "value1", "key2": 123, "key3": true}`},
			{1, `"A bare string"`},
			{2, `12345`},
			{3, `true`},
			{4, `null`},
			{5, nil},
			{6, `{"nested": {"key": "value", "array": [1, 2, 3]}}`},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFullRefresh exercises the scenario of a table without a configured cursor,
// which causes the capture to perform a full refresh every time it runs.
func TestFullRefresh(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName), i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)
		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName), i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithUpdatedAtCursor exercises the use-case of a capture using a non-primary-key updated_at column as the cursor.
func TestCaptureWithUpdatedAtCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT, updated_at TIMESTAMP)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)
		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithTwoColumnCursor exercises the use-case of a capture using a multiple-column compound cursor.
func TestCaptureWithTwoColumnCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, col1 INTEGER, col2 INTEGER, data TEXT)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "col1", "col2")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, 1, 1, "Value for row 0"}, {1, 1, 2, "Value for row 1"}, {2, 1, 3, "Value for row 2"},
			{3, 2, 1, "Value for row 3"}, {4, 2, 2, "Value for row 4"}, {5, 2, 3, "Value for row 5"},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, col1, col2, data) VALUES (?, ?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		for _, row := range [][]any{
			{6, 0, 9, "Value ignored because col1 is too small"},
			{7, 2, 0, "Value ignored because col2 is too small"},
			{8, 3, 1, "Value for row 8"}, {9, 3, 2, "Value for row 9"}, {10, 3, 3, "Value for row 10"},
			{11, 4, 1, "Value for row 11"}, {12, 4, 2, "Value for row 12"}, {13, 4, 3, "Value for row 13"},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, col1, col2, data) VALUES (?, ?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithModifications exercises the use-case of a capture using an updated_at
// cursor where some rows are modified and deleted between captures.
func TestCaptureWithModifications(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT, updated_at TIMESTAMP)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
				i, fmt.Sprintf("Initial value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// Update and delete some rows, as well as inserting a few more.
		executeControlQuery(t, control, fmt.Sprintf("UPDATE %s SET data = ?, updated_at = ? WHERE id = ?", tableName),
			"Modified value for row 3", baseTime.Add(15*time.Minute).Format("2006-01-02 15:04:05"), 3)
		executeControlQuery(t, control, fmt.Sprintf("UPDATE %s SET data = ?, updated_at = ? WHERE id = ?", tableName),
			"Modified value for row 7", baseTime.Add(16*time.Minute).Format("2006-01-02 15:04:05"), 7)
		executeControlQuery(t, control, fmt.Sprintf("DELETE FROM %s WHERE id IN (2, 5)", tableName))
		for i := 10; i < 15; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i+10)*time.Minute).Format("2006-01-02 15:04:05"))
		}

		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithEmptyPoll exercises the scenario where a polling interval finds no new rows.
func TestCaptureWithEmptyPoll(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT, updated_at TIMESTAMP)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// First batch of rows
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// No changes
		cs.Capture(ctx, t, nil)

		// Second batch of rows with later timestamps
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i+10)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithNullCursor exercises the handling of NULL values in cursor columns.
func TestCaptureWithNullCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT, sort_col INTEGER)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "sort_col")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		// First batch with mix of NULL and non-NULL cursor values
		for _, row := range [][]any{
			{0, "Value with NULL cursor", nil},
			{1, "Value with cursor 10", 10},
			{2, "Another NULL cursor", nil},
			{3, "Value with cursor 20", 20},
			{4, "Third NULL cursor", nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, sort_col) VALUES (?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)

		// Second batch testing NULL handling after initial cursor
		for _, row := range [][]any{
			{5, "Late NULL cursor", nil},    // Will not be captured
			{6, "Value with cursor 15", 15}, // Will not be captured (cursor 20 is already seen)
			{7, "Value with cursor 25", 25},
			{8, "Another late NULL", nil}, // Will not be captured
			{9, "Final value cursor 30", 30},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, sort_col) VALUES (?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestQueryTemplateOverride exercises a capture configured with an explicit query template
// in the resource spec rather than a blank template and specified table name/schema.
//
// This is the behavior of preexisting bindings which were created before the table/schema
// change in February 2025.
func TestQueryTemplateOverride(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT, updated_at TIMESTAMP)")

	// Create a binding with a query template override instead of table/schema
	var res = Resource{
		Name:     "query_template_override",
		Template: fmt.Sprintf(`SELECT * FROM %[1]s {{if not .IsFirstQuery}} WHERE updated_at > @flow_cursor_value[0] {{end}} ORDER BY updated_at`, tableName),
		Cursor:   []string{"updated_at"},
	}
	bs, err := json.Marshal(res)
	require.NoError(t, err)
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	cs.Bindings[0].ResourceConfigJson = bs

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial rows
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// More rows with later timestamps
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i+5)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessCapture exercises discovery and capture from a table without
// a defined primary key, but using a user-specified updated_at cursor.
func TestKeylessCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(data TEXT, value INTEGER, updated_at TIMESTAMP)") // No PRIMARY KEY

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial batch of data
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value, updated_at) VALUES (?, ?, ?)", tableName),
				fmt.Sprintf("Initial row %d", i), i,
				baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// Add more rows with later timestamps
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value, updated_at) VALUES (?, ?, ?)", tableName),
				fmt.Sprintf("Additional row %d", i), i,
				baseTime.Add(time.Duration(i+5)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// Update some rows with new timestamps
		executeControlQuery(t, control, fmt.Sprintf(
			"UPDATE %s SET data = CONCAT(data, ' (updated)'), updated_at = ? WHERE value >= 3 AND value <= 7",
			tableName), baseTime.Add(15*time.Minute).Format("2006-01-02 15:04:05"))
		cs.Capture(ctx, t, nil)

		// Delete and reinsert some rows
		executeControlQuery(t, control, fmt.Sprintf("DELETE FROM %s WHERE value IN (4, 6)", tableName))
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (data, value, updated_at) VALUES (?, ?, ?), (?, ?, ?)",
			tableName),
			"Reinserted row 4", 4, baseTime.Add(20*time.Minute).Format("2006-01-02 15:04:05"),
			"Reinserted row 6", 6, baseTime.Add(20*time.Minute).Format("2006-01-02 15:04:05"))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessFullRefreshCapture exercises discovery and capture from a table
// without a defined primary key, and with the cursor left empty to test
// full-refresh behavior.
// TestKeylessFullRefreshCapture exercises discovery and capture from a table
// without a defined primary key, and with the cursor left empty to test
// full-refresh behavior.
func TestKeylessFullRefreshCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(data TEXT, value INTEGER)") // No PRIMARY KEY

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// Initial batch of data
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value) VALUES (?, ?)", tableName),
				fmt.Sprintf("Initial row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Add more rows - these should appear in the next full refresh
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value) VALUES (?, ?)", tableName),
				fmt.Sprintf("Additional row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Modify some existing rows - changes should appear in next full refresh
		executeControlQuery(t, control, fmt.Sprintf(
			"UPDATE %s SET data = CONCAT(data, ' (updated)') WHERE value >= 3 AND value <= 7",
			tableName))
		cs.Capture(ctx, t, nil)

		// Delete some rows and add new ones - changes should appear in next full refresh
		executeControlQuery(t, control, fmt.Sprintf("DELETE FROM %s WHERE value IN (4, 6)", tableName))
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (data, value) VALUES (?, ?), (?, ?)",
			tableName),
			"New row A", 20,
			"New row B", 21)
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFeatureFlagKeylessRowID exercises discovery and capture from a keyless
// full-refresh table (as in TestKeylessFullRefreshCapture), but with the
// keyless_row_id feature flag explicitly set to true and false in distinct
// subtests.
func TestFeatureFlagKeylessRowID(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag string
	}{
		{"Enabled", "keyless_row_id"},
		{"Disabled", "no_keyless_row_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ctx, control = context.Background(), testMySQLClient(t)
			var tableName, uniqueID = testTableName(t, uniqueTableID(t))
			createTestTable(t, control, tableName, "(data TEXT, value INTEGER)") // No PRIMARY KEY

			// Create capture spec with specific feature flag
			var cs = testCaptureSpec(t)
			cs.EndpointSpec.(*Config).Advanced.FeatureFlags = tc.flag

			// Discover the table and verify discovery snapshot
			cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))

			// Empty cursor forces full refresh behavior
			setResourceCursor(t, cs.Bindings[0])

			t.Run("Discovery", func(t *testing.T) {
				cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings))
			})

			t.Run("Capture", func(t *testing.T) {
				setShutdownAfterQuery(t, true)

				// Initial data batch
				for i := 0; i < 5; i++ {
					executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value) VALUES (?, ?)", tableName),
						fmt.Sprintf("Initial row %d", i), i)
				}
				cs.Capture(ctx, t, nil)

				// Add more rows
				for i := 5; i < 10; i++ {
					executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value) VALUES (?, ?)", tableName),
						fmt.Sprintf("Additional row %d", i), i)
				}
				cs.Capture(ctx, t, nil)

				// Modify some existing rows
				executeControlQuery(t, control, fmt.Sprintf(
					"UPDATE %s SET data = CONCAT(data, ' (updated)') WHERE value >= 3 AND value <= 7",
					tableName))
				cs.Capture(ctx, t, nil)

				// Delete and add new rows
				executeControlQuery(t, control, fmt.Sprintf("DELETE FROM %s WHERE value IN (4, 6)", tableName))
				executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (data, value) VALUES (?, ?)", tableName),
					"New row A", 20)
				cs.Capture(ctx, t, nil)

				cupaloy.SnapshotT(t, cs.Summary())
			})
		})
	}
}

// TestCaptureFromView exercises discovery and capture from a view with an updated_at cursor.
func TestCaptureFromView(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testMySQLClient(t)
	var baseTableName, tableID = testTableName(t, uniqueTableID(t))
	var viewName, viewID = testTableName(t, uniqueTableID(t, "view"))

	// Create base table and view
	createTestTable(t, control, baseTableName, `(
        id INTEGER PRIMARY KEY,
        name TEXT,
        visible BOOLEAN,
        updated_at TIMESTAMP
    )`)
	executeControlQuery(t, control, fmt.Sprintf(`
        CREATE VIEW %s AS
        SELECT id, name, updated_at
        FROM %s
        WHERE visible = true`, viewName, baseTableName))
	t.Cleanup(func() { executeControlQuery(t, control, fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)) })

	// By default views should not be discovered.
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(tableID), regexp.MustCompile(viewID))
	t.Run("DiscoveryWithoutViews", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	// Enable view discovery and re-discover bindings, then set a cursor for capturing the view.
	cs.EndpointSpec.(*Config).Advanced.DiscoverViews = true
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(tableID), regexp.MustCompile(viewID))
	t.Run("DiscoveryWithViews", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	// Update both the base table and the view incrementally using the updated_at column.
	setResourceCursor(t, cs.Bindings[0], "updated_at")
	setResourceCursor(t, cs.Bindings[1], "updated_at")

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// First batch: Insert rows into base table, some visible in view
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, name, visible, updated_at) VALUES (?, ?, ?, ?)", baseTableName),
				i, fmt.Sprintf("Row %d", i), i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// Second batch: More rows with later timestamps
		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, name, visible, updated_at) VALUES (?, ?, ?, ?)", baseTableName),
				i, fmt.Sprintf("Row %d", i), i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i)*time.Minute).Format("2006-01-02 15:04:05"))
		}
		cs.Capture(ctx, t, nil)

		// Update some rows to change their visibility
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET visible = NOT visible, updated_at = ?
            WHERE id IN (2, 3, 4)`, baseTableName),
			baseTime.Add(20*time.Minute).Format("2006-01-02 15:04:05"))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFeatureFlagEmitSourcedSchemas runs a capture with the `emit_sourced_schemas` feature flag set.
func TestFeatureFlagEmitSourcedSchemas(t *testing.T) {
	var ctx, control = context.Background(), testMySQLClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY, data TEXT)`)

	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, 'hello'), (2, 'world')", tableName))

	for _, tc := range []struct {
		name string
		flag string
	}{
		{"Default", ""},
		{"Enabled", "emit_sourced_schemas"},
		{"Disabled", "no_emit_sourced_schemas"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cs = testCaptureSpec(t)
			cs.EndpointSpec.(*Config).Advanced.FeatureFlags = tc.flag
			cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
			setShutdownAfterQuery(t, true)

			cs.Capture(ctx, t, nil)
			cupaloy.SnapshotT(t, cs.Summary())
		})
	}
}
