package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbName = flag.String("db_name", "test", "Connect to the named database for tests")

	dbControlAddress = flag.String("db_control_addr", "127.0.0.1:1433", "The database server address to use for test setup/control operations")
	dbControlUser    = flag.String("db_control_user", "sa", "The user for test setup/control operations")
	dbControlPass    = flag.String("db_control_pass", "gf6w6dkD", "The password the the test setup/control user")

	dbCaptureAddress = flag.String("db_capture_addr", "127.0.0.1:1433", "The database server address to use for test captures")
	dbCaptureUser    = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")
	dbCapturePass    = flag.String("db_capture_pass", "we2rie1E", "The password for the capture user")

	testSchemaName   = flag.String("test_schema_name", "dbo", "The schema in which to create test tables.")
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
		Address:  *dbCaptureAddress,
		User:     *dbCaptureUser,
		Password: *dbCapturePass,
		Database: *dbName,
		Advanced: advancedConfig{
			PollSchedule: "200ms",
			FeatureFlags: *testFeatureFlags,
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"polled":"<TIMESTAMP>"`] = regexp.MustCompile(`"polled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"LastPolled":"<TIMESTAMP>"`] = regexp.MustCompile(`"LastPolled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)

	return &st.CaptureSpec{
		Driver:       sqlserverDriver,
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

func testControlClient(t testing.TB) *sql.DB {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var controlSpec = &Config{
		Address:  *dbControlAddress,
		User:     *dbControlUser,
		Password: *dbControlPass,
		Database: *dbName,
		Advanced: advancedConfig{
			PollSchedule: "200ms",
			FeatureFlags: *testFeatureFlags,
		},
	}

	var controlURI = controlSpec.ToURI()
	log.WithField("uri", controlURI).Debug("opening database control connection")

	var conn, err = sql.Open("sqlserver", controlURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.Ping())
	return conn
}

func testTableName(t *testing.T, uniqueID string) (name, id string) {
	t.Helper()
	var baseName = strings.ToLower(strings.TrimPrefix(t.Name(), "Test"))
	for _, str := range []string{"/", "=", "(", ")"} {
		baseName = strings.ReplaceAll(baseName, str, "_")
	}
	return fmt.Sprintf("%s.%s_%s", *testSchemaName, baseName, uniqueID), uniqueID
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

func createTestTable(t testing.TB, control *sql.DB, tableName, definition string) {
	t.Helper()
	executeControlQuery(t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	executeControlQuery(t, control, fmt.Sprintf("CREATE TABLE %s %s", tableName, definition))
	t.Cleanup(func() { executeControlQuery(t, control, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })
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
		fmt.Fprintf(summary, "(no bindings)")
	}
	return summary.String()
}

func executeControlQuery(t testing.TB, client *sql.DB, query string, args ...interface{}) {
	t.Helper()
	log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing setup query")
	var _, err = client.Exec(query, args...)
	require.NoError(t, err)
}

func setShutdownAfterQuery(t testing.TB, setting bool) {
	t.Helper()
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

// TestSpec verifies the connector's response to the Spec RPC against a snapshot.
func TestSpec(t *testing.T) {
	response, err := sqlserverDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// TestSimpleCapture exercises the simplest use-case of a capture first doing an initial
// backfill and subsequently capturing new rows using an updated_at column for the cursor.
func TestSimpleCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data NVARCHAR(MAX), updated_at DATETIME2)")
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(
				"INSERT INTO %s (id, data, updated_at) VALUES (@p1, @p2, @p3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf(
				"INSERT INTO %s (id, data, updated_at) VALUES (@p1, @p2, @p3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestAsyncCapture performs a capture with periodic restarts, in parallel with a bunch of inserts.
func TestAsyncCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INT PRIMARY KEY, data NVARCHAR(MAX))")

	// Have to sanitize the index within the polling interval because we're running
	// the capture in parallel with changes.
	cs.Sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		// Spawn a worker thread which will insert 50 rows of data in parallel with the capture.
		var insertsDone atomic.Bool
		go func() {
			for i := 0; i < 250; i++ {
				time.Sleep(100 * time.Millisecond)
				executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2)", tableName), i, fmt.Sprintf("Value for row %d", i))
				log.WithField("i", i).Debug("inserted row")
			}
			time.Sleep(1 * time.Second)
			insertsDone.Store(true)
		}()

		// Run the capture over and over for 5 seconds each time until all inserts have finished, then verify results.
		for !insertsDone.Load() {
			var captureCtx, cancelCapture = context.WithCancel(ctx)
			time.AfterFunc(5*time.Second, cancelCapture)
			cs.Capture(captureCtx, t, nil)
		}
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeyDiscovery exercises the connector's ability to discover types of primary key columns.
func TestKeyDiscovery(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
		k_smallint SMALLINT,
		k_int INT,
		k_bigint BIGINT,
		k_bool BIT,
		k_str VARCHAR(8),
		data NVARCHAR(MAX),
		PRIMARY KEY (k_smallint, k_int, k_bigint, k_bool, k_str)
	)`)
	cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
}

// TestSchemaFilter exercises the 'discover_schemas' advanced option.
func TestSchemaFilter(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INT PRIMARY KEY, data NVARCHAR(MAX))")

	// Run discovery with several schema filters and snapshot the results
	t.Run("Unfiltered", func(t *testing.T) {
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{}
		cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
	})
	t.Run("FilteredOut", func(t *testing.T) {
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"foo", "bar"}
		cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
	})
	t.Run("FilteredIn", func(t *testing.T) {
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"foo", *testSchemaName}
		cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
	})
}

// TestIntegerTypes exercises discovery and capture of the integer types
// INT, TINYINT, SMALLINT, BIGINT, and BIT.
func TestIntegerTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INT PRIMARY KEY,
        int_col INT,
        smallint_col SMALLINT,
        bigint_col BIGINT,
        tinyint_col TINYINT,
        bit_col BIT
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, 42, 42, 42, 42, 1},                               // Regular row with normal values
			{1, -2147483648, -32768, -9223372036854775808, 0, 0}, // Minimum values
			{2, 2147483647, 32767, 9223372036854775807, 255, 1},  // Maximum values
			{3, 0, 0, 0, 0, 0},                                   // Zero values
			{4, nil, nil, nil, nil, nil},                         // Null values
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, int_col, smallint_col, bigint_col, tinyint_col, bit_col) VALUES (@p1, @p2, @p3, @p4, @p5, @p6)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestNumericTypes exercises discovery and capture of the numeric types
// FLOAT, REAL, NUMERIC, DECIMAL, MONEY, and SMALLMONEY.
func TestNumericTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INT PRIMARY KEY,
        real_col REAL,
        float_col FLOAT,
        decimal_col DECIMAL(10,2),
        numeric_col NUMERIC(10,2),
        money_col MONEY,
        smallmoney_col SMALLMONEY
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Regular values
			{0, 123.456, 123.456, 123.45, 123.45, 123.45, 123.45},

			// Minimum/maximum values
			{1, -3.4e38, -1.7e308, -99999999.99, -99999999.99, "-922337203685477.5808", "-214748.3648"},
			{2, 3.4e38, 1.7e308, 99999999.99, 99999999.99, "922337203685477.5807", "214748.3647"},

			// Zero values
			{3, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0},

			// Null values
			{4, nil, nil, nil, nil, nil, nil},

			// Very precise decimal number (limited to SQL Server's precision)
			{5, 0.0, 0.0, 12345678.91, 12345678.91, "123456789123456.7891", "123456.7891"},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, real_col, float_col, decimal_col, numeric_col,
                    money_col, smallmoney_col
                ) VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestBinaryTypes exercises discovery and capture of the binary types
// BINARY, VARBINARY, and IMAGE.
func TestBinaryTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INT PRIMARY KEY,
        binary_col BINARY(5),
        varbinary_col VARBINARY(10),
        image_col IMAGE
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, []byte("Hello"), []byte("Hello"), []byte("Hello")},
			{1, []byte{0, 0, 0, 0, 0}, []byte{0, 0, 0}, []byte{0, 0, 0}},
			{2, []byte{255, 255, 255, 255, 255}, []byte{255, 255, 255}, []byte{255, 255, 255}},
			{3, []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE}, []byte{0xAA, 0xBB, 0xCC}, []byte{0xAA, 0xBB, 0xCC}},
			{4, []byte{}, []byte{}, []byte{}},
			{5, []byte(nil), []byte(nil), []byte(nil)},
			{6, []byte{0, 1, 2, 3, 4}, []byte{0, 1, 2, 3, 4, 5, 0xFF}, []byte{0, 1, 2, 3, 4, 5, 0xFF}},
			{7, []byte("Hello"), []byte("Hello 世"), []byte("Hello 世界")},
			{8, []byte{0x00, 0xFF, 0x7F, 0x80, 0x01},
				[]byte{0x00, 0xFF, 0x7F, 0x80, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB},
				[]byte{0x00, 0xFF, 0x7F, 0x80, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, binary_col, varbinary_col, image_col
                ) VALUES (@p1, @p2, @p3, @p4)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestStringTypes exercises discovery and capture of the string types
// CHAR, VARCHAR, TEXT, NCHAR, NVARCHAR, and NTEXT.
func TestStringTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INT PRIMARY KEY,
        varchar_col VARCHAR(16),
        char_col CHAR(16),
        nvarchar_col NVARCHAR(16),
        nchar_col NCHAR(16),
        text_col TEXT,
        ntext_col NTEXT
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, "Hello", "Hello", "Hello", "Hello", "Hello", "Hello"},
			{1, "A", "B", "C", "D", "E", "F"},
			{2, "1234567890123456", "1234567890123456", "1234567890123456", "1234567890123456", "1234567890123456", "1234567890123456"},
			{3, "Hello world", "Hello world", "Hello 世界", "Hello 世界", "Hello world", "Hello 世界"},
			{4, "Tab\tNewline\n", "Tab\tNewline\n", "Tab\tNewline\n", "Tab\tNewline\n", "Tab\tNewline\n", "Tab\tNewline\n"},
			{5, "", "", "", "", "", ""},
			{6, nil, nil, nil, nil, nil, nil},
			{7, "  trim test  ", "  trim test  ", "  trim test  ", "  trim test  ", "  trim test  ", "  trim test  "},
			{8, "Short", "Short", "Short", "Short", strings.Repeat("Long text ", 1000), strings.Repeat("Long text ", 1000)},
			{9, `Special "quotes"`, `Special "quotes"`, `Special "quotes"`, `Special "quotes"`, `Special "quotes" and \backslashes\`, `Special "quotes" and \backslashes\`},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, varchar_col, char_col, nvarchar_col, nchar_col, text_col, ntext_col
                ) VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestDateAndTimeTypes exercises discovery and capture of the date and time types
// DATE, TIME, DATETIME, DATETIME2, SMALLDATETIME, and DATETIMEOFFSET.
func TestDateAndTimeTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INT PRIMARY KEY,
        date_col DATE,
        datetime_col DATETIME,
        datetime2_col DATETIME2,
        smalldatetime_col SMALLDATETIME,
        datetimeoffset_col DATETIMEOFFSET,
        time_col TIME
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, "2025-02-14", "2025-02-14 14:44:29", "2025-02-14 14:44:29.1234567",
				"2025-02-14 14:44:00", "2025-02-14 14:44:29.1234567 +09:00", "14:44:29.1234567"},
			{1, "1969-07-20", "1969-07-20 20:17:00", "1969-07-20 20:17:00",
				"1969-07-20 20:17:00", "1969-07-20 20:17:00 +00:00", "20:17:00"},
			{2, "2077-07-20", "2077-07-20 15:30:00", "2077-07-20 15:30:00",
				"2077-07-20 15:30:00", "2077-07-20 15:30:00 +09:00", "15:30:00"},
			{3, "0001-01-01", "1753-01-01 00:00:00", "0001-01-01 00:00:00",
				"1900-01-01 00:00:00", "0001-01-01 00:00:00 +00:00", "00:00:00"},
			{4, "9999-12-31", "9999-12-31 23:59:59.997", "9999-12-31 23:59:59.9999999",
				"2079-06-06 23:59:00", "9999-12-31 23:59:59.9999999 +00:00", "23:59:59.9999999"},
			{5, "2024-02-14", "2024-02-14 15:30:45", "2024-02-14 15:30:45",
				"2024-02-14 15:31:00", "2024-02-14 15:30:45 +00:00", "15:30:45"},
			{6, "2024-02-14", "2024-02-14 15:30:45", "2024-02-14 15:30:45",
				"2024-02-14 15:31:00", "2024-02-14 15:30:45 +00:00", "15:30:45"},
			{7, "2024-02-14", "2024-02-14 15:30:45.123", "2024-02-14 15:30:45.1234567",
				"2024-02-14 15:31:00", "2024-02-14 15:30:45.1234567 +00:00", "15:30:45.1234567"},
			{8, "2024-02-14", "2024-02-14 15:30:45", "2024-02-14 15:30:45.123456",
				"2024-02-14 15:31:00", "2024-02-14 15:30:45.123456 +00:00", "15:30:45.123456"},
			{9, nil, nil, nil, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, date_col, datetime_col, datetime2_col, smalldatetime_col,
                    datetimeoffset_col, time_col
                ) VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestDatetimeLocation exercises capture of the DATETIME and DATETIME2
// types with different time zones.
func TestDatetimeLocation(t *testing.T) {
	var ctx, control = context.Background(), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, `(id INT PRIMARY KEY, datetime_col DATETIME, datetime2_col DATETIME2)`)
	setShutdownAfterQuery(t, true)

	for _, row := range [][]any{
		{0, nil, nil, nil, nil, nil, nil},
		{1, "2025-02-27 14:30:00", "2025-02-27 14:30:00"},             // Regular date and time
		{2, "1753-01-01 00:00:00", "0001-01-01 00:00:00"},             // Minimum values (different for DATETIME vs DATETIME2). Often reveals interesting offsets for dates very long ago.
		{3, "9999-12-31 23:59:59", "9999-12-31 23:59:59.9999999"},     // Near maximum values
		{4, "2025-02-27 14:30:45.123", "2025-02-27 14:30:45.1234567"}, // With fractional seconds
		{5, "2000-01-01 00:00:00", "2000-01-01 00:00:00"},             // Y2K date
		{6, "2025-12-31 23:59:59", "2025-12-31 23:59:59"},             // End of year
		{7, "2025-03-09 03:30:00", "2025-03-09 03:30:00"},             // During DST start
	} {
		executeControlQuery(t, control, fmt.Sprintf(`INSERT INTO %s (id, datetime_col, datetime2_col) VALUES (@p1, @p2, @p3)`, tableName), row...)
	}

	for _, tc := range []struct{ name, timezone string }{
		{"UTC", "UTC"},
		{"Chicago", "America/Chicago"}, // Exercises DST and also historical "Local Mean Time" for years before 1883.
		{"Tokyo", "Asia/Tokyo"},        // Also has an interesting local time for historical dates.
		{"ExplicitPositiveOffset", "+04:00"},
		{"ExplicitNegativeOffset", "-05:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cs = testCaptureSpec(t)
			cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
			cs.EndpointSpec.(*Config).Advanced.Timezone = tc.timezone
			cs.Capture(ctx, t, nil)
			cupaloy.SnapshotT(t, cs.Summary())
		})
	}
}

// TestUUIDType exercises discovery and capture of the UNIQUEIDENTIFIER type.
func TestUUIDType(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INT PRIMARY KEY,
        uuid_col UNIQUEIDENTIFIER
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Standard UUID v4 format (random)
			{0, "d36af820-6ac3-4052-ad19-7eba916a020b"},

			// UUID with all characters as same value
			{1, "11111111-1111-1111-1111-111111111111"},
			{2, "00000000-0000-0000-0000-000000000000"},
			{3, "ffffffff-ffff-ffff-ffff-ffffffffffff"},

			// Different versions of UUID
			{4, "a0eebc99-9c0b-11eb-a8b3-0242ac130003"}, // v1 time-based
			{5, "a0eebc99-9c0b-21eb-a8b3-0242ac130003"}, // v2 DCE security
			{6, "a0eebc99-9c0b-31eb-a8b3-0242ac130003"}, // v3 name-based MD5
			{7, "a0eebc99-9c0b-41eb-a8b3-0242ac130003"}, // v4 random
			{8, "a0eebc99-9c0b-51eb-a8b3-0242ac130003"}, // v5 name-based SHA-1
			{9, "018df60a-0040-7000-a000-0242ac130003"}, // v7 time-ordered

			// Null value
			{10, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, uuid_col) VALUES (@p1, @p2)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestXMLType exercises discovery and capture of the XML type.
func TestXMLType(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INT PRIMARY KEY,
        xml_col XML
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "id")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Simple XML
			{0, "<root><element>Hello</element></root>"},

			// Complex nested structure
			{1, `<library>
                <book id="1">
                    <title>Sample Book</title>
                    <author>John Doe</author>
                    <published>2024</published>
                    <genres>
                        <genre>Fiction</genre>
                        <genre>Adventure</genre>
                    </genres>
                </book>
            </library>`},

			// XML with attributes
			{2, `<user id="123" active="true">
                <name>Alice</name>
                <email>alice@example.com</email>
            </user>`},

			// XML with special characters
			{3, `<text><![CDATA[Special & <characters> " ' in CDATA]]></text>`},

			// XML with namespaces
			{4, `<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
                <soap:Header><auth>token123</auth></soap:Header>
                <soap:Body><message>Hello World</message></soap:Body>
            </soap:Envelope>`},

			// XML with unicode characters
			{5, `<greeting language="multi">Hello 世界 🌍</greeting>`},

			// Empty elements
			{6, `<empty/>`},

			// Self-closing tags
			{7, `<document><header/><body/><footer/></document>`},

			// Large XML document
			{8, fmt.Sprintf(`<root><content>%s</content></root>`,
				strings.Repeat("This is a long text content. ", 100))},

			// Well-formed XML with mixed content
			{9, `<article>
                <title>Mixed Content</title>
                <para>This is <emphasis>important</emphasis> text with <link href="#ref">links</link>.</para>
            </article>`},

			// Null value
			{10, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, xml_col) VALUES (@p1, @p2)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestModificationsAndDeletions exercises the capture of modified and deleted rows
// by performing a series of INSERT, UPDATE, and DELETE operations.
func TestModificationsAndDeletions(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        version INTEGER,
        description NVARCHAR(MAX),
        updated_at DATETIME2
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial inserts
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, version, description, updated_at)
                VALUES (@p1, @p2, @p3, @p4)`, tableName),
				i, 1, fmt.Sprintf("Initial version of row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Modify some rows
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET version = version + 1,
            description = description + ' (updated)',
            updated_at = @p1
            WHERE id IN (1, 3)`, tableName), baseTime.Add(5*time.Minute))
		cs.Capture(ctx, t, nil)

		// Delete some rows
		executeControlQuery(t, control, fmt.Sprintf(`
            DELETE FROM %s WHERE id IN (2, 4)`, tableName))
		cs.Capture(ctx, t, nil)

		// Insert new rows with reused IDs
		executeControlQuery(t, control, fmt.Sprintf(`
            INSERT INTO %s (id, version, description, updated_at)
            VALUES (@p1, @p2, @p3, @p4), (@p5, @p6, @p7, @p8)`, tableName),
			2, 1, "Reused ID 2", baseTime.Add(6*time.Minute),
			4, 1, "Reused ID 4", baseTime.Add(6*time.Minute))
		cs.Capture(ctx, t, nil)

		// Final update to all remaining rows
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET version = version + 1,
            description = description + ' (final update)',
            updated_at = @p1`, tableName), baseTime.Add(7*time.Minute))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithEmptyPoll exercises the scenario where a polling interval finds no new rows.
func TestCaptureWithEmptyPoll(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        data NVARCHAR(MAX),
        updated_at DATETIME2
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// First batch of rows
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, data, updated_at) VALUES (@p1, @p2, @p3)`, tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// No changes - this capture should find no rows
		cs.Capture(ctx, t, nil)

		// Second batch of rows
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, data, updated_at) VALUES (@p1, @p2, @p3)`, tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i+10)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFullRefresh exercises the scenario of a table without a configured cursor,
// which causes the capture to perform a full refresh every time it runs.
func TestFullRefresh(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data NVARCHAR(MAX))")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0]) // Empty cursor means full refresh behavior

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2)", tableName),
				i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)

		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2)", tableName),
				i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithUpdatedAtCursor exercises the use-case of a capture using
// an updated_at timestamp column as the cursor.
func TestCaptureWithUpdatedAtCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        data NVARCHAR(MAX),
        updated_at DATETIME2
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2, @p3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2, @p3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithTwoColumnCursor exercises the use-case of a capture using
// a multiple-column compound cursor.
func TestCaptureWithTwoColumnCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        major INTEGER,
        minor INTEGER,
        data NVARCHAR(MAX)
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "major", "minor")

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// First batch with lower version numbers
		for _, row := range [][]any{
			{0, 1, 1, "v1.1"}, {1, 1, 2, "v1.2"}, {2, 1, 3, "v1.3"},
			{3, 2, 1, "v2.1"}, {4, 2, 2, "v2.2"}, {5, 2, 3, "v2.3"},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2, @p3, @p4)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)

		// Second batch with higher version numbers
		for _, row := range [][]any{
			{6, 0, 9, "Value ignored because major is too small"},
			{7, 2, 0, "Value ignored because minor is too small"},
			{8, 3, 1, "v3.1"}, {9, 3, 2, "v3.2"}, {10, 3, 3, "v3.3"},
			{11, 4, 1, "v4.1"}, {12, 4, 2, "v4.2"}, {13, 4, 3, "v4.3"},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2, @p3, @p4)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestQueryTemplates exercises the selection and execution of query templates
// for various combinations of resource spec and stream state.
func TestQueryTemplates(t *testing.T) {
	var testCases = []struct {
		name         string
		cursor       []string
		cursorValues []any
	}{
		{name: "SingleCursorFirstQuery", cursor: []string{"updated_at"}},
		{name: "SingleCursorSubsequentQuery", cursor: []string{"updated_at"}, cursorValues: []any{"2024-02-20 12:00:00"}},
		{name: "MultiCursorFirstQuery", cursor: []string{"major", "minor"}},
		{name: "MultiCursorSubsequentQuery", cursor: []string{"major", "minor"}, cursorValues: []any{1, 2}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var resource = &Resource{
				Name:       "test_foobar",
				SchemaName: "test",
				TableName:  "foobar",
				Cursor:     tc.cursor,
			}
			var state = &streamState{
				CursorNames:  tc.cursor,
				CursorValues: tc.cursorValues,
			}
			var query, err = sqlserverDriver.buildQuery(resource, state)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, query)
		})
	}
}

// TestCaptureFromView exercises discovery and capture from a view with an updated_at cursor.
func TestCaptureFromView(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var baseTableName, tableID = testTableName(t, uniqueTableID(t))
	var viewName, viewID = testTableName(t, uniqueTableID(t, "view"))

	// Create base table and view
	createTestTable(t, control, baseTableName, `(
		id INTEGER PRIMARY KEY,
		name NVARCHAR(MAX),
		visible BIT,
		updated_at DATETIME2
	)`)
	executeControlQuery(t, control, fmt.Sprintf(`
		CREATE VIEW %s AS
		SELECT id, name, updated_at
		FROM %s
		WHERE visible = 1`, viewName, baseTableName))
	t.Cleanup(func() { executeControlQuery(t, control, fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)) })

	// By default views should not be discovered.
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(tableID), regexp.MustCompile(viewID))
	t.Run("DiscoveryWithoutViews", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	// Enable view discovery and re-discover bindings.
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
			executeControlQuery(t, control, fmt.Sprintf(`INSERT INTO %s VALUES (@p1, @p2, @p3, @p4)`, baseTableName),
				i, fmt.Sprintf("Row %d", i), i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Second batch: More rows with later timestamps
		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`INSERT INTO %s VALUES (@p1, @p2, @p3, @p4)`, baseTableName),
				i, fmt.Sprintf("Row %d", i), i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Update some rows to change their visibility
		executeControlQuery(t, control, fmt.Sprintf(`
			UPDATE %s SET visible = ~visible, updated_at = @p1
			WHERE id IN (2, 3, 4)`, baseTableName),
			baseTime.Add(20*time.Minute))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessCapture exercises discovery and capture from a table without
// a defined primary key, but using a timestamp column for the cursor.
func TestKeylessCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        data NVARCHAR(MAX),
        value INTEGER,
        updated_at DATETIME2
    )`) // Intentionally no PRIMARY KEY

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial batch of data
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (data, value, updated_at) VALUES (@p1, @p2, @p3)`, tableName),
				fmt.Sprintf("Initial row %d", i), i, baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Add more rows
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (data, value, updated_at) VALUES (@p1, @p2, @p3)`, tableName),
				fmt.Sprintf("Additional row %d", i), i, baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Update some rows
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET data = data + ' (updated)', updated_at = @p1
            WHERE value >= 3 AND value <= 7`, tableName),
			baseTime.Add(10*time.Minute))
		cs.Capture(ctx, t, nil)

		// Delete and reinsert rows
		executeControlQuery(t, control, fmt.Sprintf(`
            DELETE FROM %s WHERE value = 4 OR value = 6`, tableName))
		executeControlQuery(t, control, fmt.Sprintf(`
            INSERT INTO %s (data, value, updated_at) VALUES (@p1, @p2, @p3), (@p4, @p5, @p6)`, tableName),
			"Reinserted row 4", 4, baseTime.Add(11*time.Minute),
			"Reinserted row 6", 6, baseTime.Add(11*time.Minute))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessFullRefreshCapture exercises discovery and capture from a table
// without a defined primary key, and with the cursor explicitly unset to test
// full-refresh behavior.
func TestKeylessFullRefreshCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(data NVARCHAR(MAX), value INTEGER)`) // No PRIMARY KEY

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))

	// Empty cursor forces full refresh behavior
	setResourceCursor(t, cs.Bindings[0])

	t.Run("Discovery", func(t *testing.T) {
		cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings))
	})

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// Initial batch of data
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control,
				fmt.Sprintf("INSERT INTO %s (data, value) VALUES (@p1, @p2)", tableName),
				fmt.Sprintf("Initial row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Add more rows - these should appear in the next full refresh
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control,
				fmt.Sprintf("INSERT INTO %s (data, value) VALUES (@p1, @p2)", tableName),
				fmt.Sprintf("Additional row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Modify some existing rows - changes should appear in next full refresh
		executeControlQuery(t, control, fmt.Sprintf(
			"UPDATE %s SET data = data + ' (updated)' WHERE value >= 3 AND value <= 7",
			tableName))
		cs.Capture(ctx, t, nil)

		// Delete some rows and add new ones - changes should appear in next full refresh
		executeControlQuery(t, control,
			fmt.Sprintf("DELETE FROM %s WHERE value = 4 OR value = 6", tableName))
		executeControlQuery(t, control,
			fmt.Sprintf("INSERT INTO %s (data, value) VALUES (@p1, @p2)", tableName),
			"New row A", 20)
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFeatureFlagEmitSourcedSchemas runs a capture with the `emit_sourced_schemas` feature flag set.
func TestFeatureFlagEmitSourcedSchemas(t *testing.T) {
	var ctx, control = context.Background(), testControlClient(t)
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
