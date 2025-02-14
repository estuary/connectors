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
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"<TIMESTAMP>"`] = regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)

	return &st.CaptureSpec{
		Driver:       mysqlDriver,
		EndpointSpec: endpointSpec,
		Validator:    &st.OrderedCaptureValidator{},
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
	res, err := mysqlDriver.GenerateResource("test_foobar", "test", "foobar", "BASE TABLE")
	require.NoError(t, err)

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
