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
	"math"
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
	dbAddress     = flag.String("db_address", "localhost:5432", "The database server address to use for tests")
	dbName        = flag.String("db_name", "postgres", "Use the named database for tests")
	dbControlUser = flag.String("db_control_user", "postgres", "The user for test setup/control operations")
	dbControlPass = flag.String("db_control_pass", "postgres", "The password the the test setup/control user")
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
	// Some tested behaviors (TestDateAndTimeTypes) are timezone-sensitive. This
	// is arguably a bug, but the goal of the current work is just to document
	// via test snapshots the current behavior. So we set the timezone to a known
	// value to avoid test failures due to timezone differences, especially in CI.
	os.Setenv("TZ", "America/New_York")
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
		Database: *dbName,
		Advanced: advancedConfig{
			PollSchedule: "200ms",
			FeatureFlags: *testFeatureFlags,
		},
	}

	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"polled":"<TIMESTAMP>"`] = regexp.MustCompile(`"polled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"LastPolled":"<TIMESTAMP>"`] = regexp.MustCompile(`"LastPolled":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"BaseXID":999999`] = regexp.MustCompile(`"BaseXID":[0-9]+`)
	sanitizers[`"txid":999999`] = regexp.MustCompile(`"txid":[0-9]+`)
	sanitizers[`"CursorNames":["txid"],"CursorValues":[999999]`] = regexp.MustCompile(`"CursorNames":\["txid"\],"CursorValues":\[[0-9]+\]`)

	return &st.CaptureSpec{
		Driver:       postgresDriver,
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

func testControlClient(t testing.TB) *sql.DB {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var controlUser = *dbControlUser
	if controlUser == "" {
		controlUser = *dbCaptureUser
	}
	var controlPass = *dbControlPass
	if controlPass == "" {
		controlPass = *dbCapturePass
	}
	var controlURI = fmt.Sprintf(`postgres://%s:%s@%s/%s`, controlUser, controlPass, *dbAddress, *dbName)
	log.WithField("uri", controlURI).Debug("opening database control connection")
	var conn, err = sql.Open("pgx", controlURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.Ping())
	return conn
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
	response, err := postgresDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// TestSimpleCapture exercises the simplest use-case of a capture first doing an initial
// backfill and subsequently capturing new rows using the default XMIN cursor.
func TestSimpleCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableName), i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)
		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableName), i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestAsyncCapture performs a capture with periodic restarts, in parallel with a bunch of inserts.
func TestAsyncCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT)")

	// Have to sanitize the index within the polling interval because we're running
	// the capture in parallel with changes.
	cs.Sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		// Spawn a worker thread which will insert 50 rows of data in parallel with the capture.
		var insertsDone atomic.Bool
		go func() {
			for i := 0; i < 250; i++ {
				time.Sleep(100 * time.Millisecond)
				executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableName), i, fmt.Sprintf("Value for row %d", i))
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
		k_int INTEGER,
		k_bigint BIGINT,
		k_bool BOOLEAN,
		k_str VARCHAR(8),
		data TEXT,
		PRIMARY KEY (k_smallint, k_int, k_bigint, k_bool, k_str)
	)`)
	cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
}

// TestSchemaFilter exercises the 'discover_schemas' advanced option.
func TestSchemaFilter(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT)")

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
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"foo", "test"}
		cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
	})
}

// TestIntegerTypes exercises discovery and capture of the integer types
// INTEGER, SMALLINT, BIGINT, SERIAL, SMALLSERIAL, BIGSERIAL, and OID.
func TestIntegerTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        int_col INTEGER,
        smallint_col SMALLINT,
        bigint_col BIGINT,
        serial_col SERIAL,
        smallserial_col SMALLSERIAL,
        bigserial_col BIGSERIAL,
        oid_col OID
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, 42, 42, 42, 42, 42, 42, 42},                                                                 // Regular row with normal values
			{1, -2147483648, -32768, -9223372036854775808, 1, 1, 1, 0},                                      // Minimum values
			{2, 2147483647, 32767, 9223372036854775807, 2147483647, 32767, 9223372036854775807, 4294967295}, // Maximum values
			{3, 0, 0, 0, 0, 0, 0, 0},                                                                        // Zero values
			{4, nil, nil, nil, 5, 5, 5, nil},                                                                // Null values (except for SERIAL columns which can't be null)
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, int_col, smallint_col, bigint_col, serial_col, smallserial_col, bigserial_col, oid_col) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestNumericTypes exercises discovery and capture of the numeric types
// REAL, DOUBLE PRECISION, DECIMAL, NUMERIC, NUMERIC(200, 100), and MONEY.
func TestNumericTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        real_col REAL,
        double_col DOUBLE PRECISION,
        decimal_col DECIMAL(10,2),
        numeric_col NUMERIC(10,2),
        numeric_large_col NUMERIC(200,100),
        money_col MONEY
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Regular values
			{0, 123.456, 123.456, 123.45, 123.45, 123.45, 123.45},

			// Minimum/maximum values
			{1, -3.4e38, -1.7e308, -999999.99, -999999.99,
				"-9.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999e+99",
				"-92233720368547758.08"},
			{2, 3.4e38, 1.7e308, 999999.99, 999999.99,
				"9.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999e+99",
				"92233720368547758.07"},

			// Special floating point values
			{3, float32(math.NaN()), math.NaN(), 0.0, 0.0, 0.0, 0.0},
			{4, float32(math.Inf(1)), math.Inf(1), 1.0, 1.0, 1.0, 1.0},
			{5, float32(math.Inf(-1)), math.Inf(-1), -1.0, -1.0, -1.0, -1.0},

			// Zero values
			{6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0},

			// Null values
			{7, nil, nil, nil, nil, nil, nil},

			// Very precise decimal number
			{8, 0.0, 0.0, 0.0, 0.0,
				"9876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210.0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
				0.0},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, real_col, double_col, decimal_col, numeric_col, 
                    numeric_large_col, money_col
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestBinaryTypes exercises discovery and capture of the binary types
// BOOLEAN, BIT, BIT(3), BIT VARYING, BIT VARYING(5), and BYTEA.
func TestBinaryTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        bool_col BOOLEAN,
        bit_col BIT,
        bit3_col BIT(3),
        bitvar_col BIT VARYING,
        bitvar5_col BIT VARYING(5),
        bytea_col BYTEA
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, true, "1", "101", "1010", "10101", []byte("Hello")},
			{1, false, "0", "000", "0", "00000", []byte{0, 0, 0}},
			{2, true, "1", "111", "111111", "11111", []byte{255, 255, 255}},
			{3, true, "0", "110", "1100", "10110", []byte{0xAA, 0xBB, 0xCC}},
			{4, false, "0", "000", "", "", []byte{}},
			{5, nil, nil, nil, nil, nil, nil},
			{6, true, "1", "101", "1010", "10101", []byte{0, 1, 2, 3, 4, 5, 0xFF}},
			{7, false, "0", "010", "0101", "01010", []byte("Hello 世界")},
			{8, true, "1", "111", "1111", "11111",
				[]byte{0x00, 0xFF, 0x7F, 0x80, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, bool_col, bit_col, bit3_col, bitvar_col, 
                    bitvar5_col, bytea_col
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestStringTypes exercises discovery and capture of the string types
// VARCHAR(16), CHAR(16), BPCHAR, and TEXT.
func TestStringTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        varchar_col VARCHAR(16),
        char_col CHAR(16),
        bpchar_col BPCHAR,
        text_col TEXT
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, "Hello", "Hello", "Hello", "Hello"},
			{1, "A", "B", "C", "D"},
			{2, "1234567890123456", "1234567890123456", "1234567890123456", "1234567890123456"},
			{3, "Hello 世界", "Hello 世界", "Hello 世界", "Hello 世界"},
			{4, "Tab\tNewline\n", "Tab\tNewline\n", "Tab\tNewline\n", "Tab\tNewline\n"},
			{5, "", "", "", ""},
			{6, nil, nil, nil, nil},
			{7, "  trim test  ", "  trim test  ", "  trim test  ", "  trim test  "},
			{8, "Short", "Short", "Short", strings.Repeat("Long text ", 1000)},
			{9, `Special "quotes"`, `Special "quotes"`, `Special "quotes"`, `Special "quotes" and \backslashes\`},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, varchar_col, char_col, bpchar_col, text_col
                ) VALUES ($1, $2, $3, $4, $5)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestDateAndTimeTypes exercises discovery and capture of the date and time types
// DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE, TIME, TIME WITH TIME ZONE, and INTERVAL.
func TestDateAndTimeTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        date_col DATE,
        ts_col TIMESTAMP,
        tstz_col TIMESTAMP WITH TIME ZONE,
        time_col TIME,
        timetz_col TIME WITH TIME ZONE,
        interval_col INTERVAL
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		var tokyo, _ = time.LoadLocation("Asia/Tokyo")
		for _, row := range [][]any{
			{0, "2025-02-14", "2025-02-14 14:44:29",
				time.Date(2025, 2, 14, 14, 44, 29, 0, tokyo),
				"14:44:29", "14:44:29-05:00", "1 year 2 months 3 days 4 hours 5 minutes 6 seconds"},
			{1, "1969-07-20", "1969-07-20 20:17:00",
				time.Date(1969, 7, 20, 20, 17, 0, 0, time.UTC),
				"20:17:00", "20:17:00-04:00", "50 years"},
			{2, "2077-07-20", "2077-07-20 15:30:00",
				time.Date(2077, 7, 20, 15, 30, 0, 0, tokyo),
				"15:30:00", "15:30:00+09:00", "100 years"},
			{3, "0001-01-01", "0001-01-01 00:00:00",
				time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
				"00:00:00", "00:00:00Z", "1 microsecond"},
			{4, "9999-12-31", "9999-12-31 23:59:59.999999",
				time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
				"23:59:59.999999", "23:59:59.999999Z", "1000 years"},
			{5, "2024-02-14", "2024-02-14 15:30:45",
				time.Date(2024, 2, 14, 15, 30, 45, 0, time.UTC),
				"15:30:45", "15:30:45Z", "-178000000 years"},
			{6, "2024-02-14", "2024-02-14 15:30:45",
				time.Date(2024, 2, 14, 15, 30, 45, 0, time.UTC),
				"15:30:45", "15:30:45Z", "178000000 years"},
			{7, "2024-02-14", "2024-02-14 15:30:45.123456",
				time.Date(2024, 2, 14, 15, 30, 45, 123456000, time.UTC),
				"15:30:45.123456", "15:30:45.123456Z", "1.123456 seconds"},
			{8, "2024-02-14", "2024-02-14 15:30:45",
				time.Date(2024, 2, 14, 15, 30, 45, 0, time.UTC),
				"15:30:45", "15:30:45Z",
				"2 years 3 months 4 days 12 hours 30 minutes 45.123456 seconds"},
			{9, nil, nil, nil, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, date_col, ts_col, tstz_col, time_col, 
                    timetz_col, interval_col
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestGeometricTypes exercises discovery and capture of the geometric types
// POINT, LINE, LSEG, BOX, PATH, POLYGON, and CIRCLE.
func TestGeometricTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        point_col POINT,
        line_col LINE,
        lseg_col LSEG,
        box_col BOX,
        path_col PATH,
        polygon_col POLYGON,
        circle_col CIRCLE
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Regular geometric values
			{0,
				"(1,2)",               // POINT: (x,y)
				"{1,-1,0}",            // LINE: {A,B,C} where Ax + By + C = 0
				"[(1,1),(2,2)]",       // LSEG: segment from point 1 to point 2
				"((1,1),(2,2))",       // BOX: upper right and lower left corners
				"((1,1),(2,2),(3,3))", // PATH: series of points forming a path
				"((1,1),(2,2),(3,3))", // POLYGON: series of points forming a polygon
				"<(0,0),1>",           // CIRCLE: center point and radius
			},

			// Complex shapes
			{1,
				"(0,0)",
				"{0,1,-1}",
				"[(0,0),(100,100)]",
				"((100,100),(-100,-100))",
				"((0,0),(1,1),(2,0),(1,-1),(0,0))", // Closed path
				"((0,0),(2,0),(2,2),(0,2))",        // Square
				"<(5,5),5>",
			},

			// Negative coordinates
			{2,
				"(-1,-1)",
				"{1,1,0}",
				"[(-1,-1),(-2,-2)]",
				"((-1,-1),(-2,-2))",
				"((-1,-1),(-2,-2),(-3,-3))",
				"((-1,-1),(-1,1),(1,1),(1,-1))",
				"<(-5,-5),3>",
			},

			// Decimal coordinates
			{3,
				"(1.5,2.5)",
				"{1.5,-2.5,3.5}",
				"[(1.1,1.1),(2.2,2.2)]",
				"((1.5,1.5),(2.5,2.5))",
				"((1.1,1.1),(2.2,2.2),(3.3,3.3))",
				"((0.0,0.0),(1.5,1.5),(3.0,0.0))",
				"<(2.5,2.5),1.5>",
			},

			// Large coordinates
			{4,
				"(1000000,-1000000)",
				"{1000,2000,3000}",
				"[(9999,9999),(-9999,-9999)]",
				"((9999,9999),(-9999,-9999))",
				"((1000,1000),(2000,2000),(3000,3000))",
				"((1000,1000),(2000,2000),(3000,1000))",
				"<(0,0),999999>",
			},

			// Special cases
			{5,
				"(0,0)",               // Origin point
				"{1,0,0}",             // Vertical line x=0
				"[(0,0),(0,1)]",       // Vertical segment
				"((0,0),(0,0))",       // Box with zero area
				"[(0,0)]",             // Single point path (square brackets for open path)
				"((0,0),(0,1),(1,0))", // Triangle
				"<(0,0),0>",           // Circle with zero radius
			},

			// Null values
			{6, nil, nil, nil, nil, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, point_col, line_col, lseg_col, box_col,
                    path_col, polygon_col, circle_col
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestNetworkTypes exercises discovery and capture of the network address types
// INET, CIDR, MACADDR, and MACADDR8.
func TestNetworkTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        inet_col INET,
        cidr_col CIDR,
        macaddr_col MACADDR,
        macaddr8_col MACADDR8
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Regular values
			{0, "192.168.1.1", "192.168.1.0/24", "08:00:2b:01:02:03", "08:00:2b:01:02:03:04:05"},

			// IPv4 addresses and networks
			{1, "127.0.0.1", "127.0.0.1/32", "00:00:00:00:00:00", "00:00:00:00:00:00:00:00"},
			{2, "0.0.0.0", "0.0.0.0/0", "ff:ff:ff:ff:ff:ff", "ff:ff:ff:ff:ff:ff:ff:ff"},
			{3, "255.255.255.255", "255.255.255.0/24", "02:42:ac:11:00:02", "02:42:ac:11:00:02:00:00"},

			// IPv6 addresses and networks
			{4, "::1", "::/128", "08:00:2b:01:02:03", "08:00:2b:01:02:03:04:05"},
			{5, "2001:db8::1", "2001:db8::/32", "00:11:22:33:44:55", "00:11:22:33:44:55:66:77"},
			{6, "fe80::1", "fe80::/10", "52:54:00:ff:fe:01", "52:54:00:ff:fe:01:00:00"},

			// INET with explicit subnet masks
			{7, "192.168.1.5/24", "10.0.0.0/8", "aa:bb:cc:dd:ee:ff", "aa:bb:cc:dd:ee:ff:00:11"},
			{8, "2001:db8::1/64", "172.16.0.0/12", "f0:0d:ca:fe:be:ef", "f0:0d:ca:fe:be:ef:00:00"},

			// Null values
			{9, nil, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, inet_col, cidr_col, macaddr_col, macaddr8_col
                ) VALUES ($1, $2, $3, $4, $5)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestJSONTypes exercises discovery and capture of the JSON types
// JSON, JSONB, and JSONPATH.
func TestJSONTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        json_col JSON,
        jsonb_col JSONB,
        jsonpath_col JSONPATH
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Simple values
			{0, `{"number": 42, "string": "hello"}`,
				`{"number": 42, "string": "hello"}`,
				`$.number`},

			// Complex nested structure
			{1, `{
                "object": {"nested": true},
                "array": [1,2,3],
                "null": null,
                "number": 123.456,
                "string": "test",
                "boolean": false
            }`, `{
                "object": {"nested": true},
                "array": [1,2,3],
                "null": null,
                "number": 123.456,
                "string": "test",
                "boolean": false
            }`, `$.object.nested`},

			// Array with mixed types
			{2, `[1, "two", true, null, {"key": "value"}]`,
				`[1, "two", true, null, {"key": "value"}]`,
				`$[4].key`},

			// Special characters
			{3, `{"special\nchars": "tab\there\nand\"quotes\""}`,
				`{"special\nchars": "tab\there\nand\"quotes\""}`,
				`$."special\nchars"`},

			// Very large structure
			{4, fmt.Sprintf(`{"key": "%s"}`, strings.Repeat("long string ", 1000)),
				fmt.Sprintf(`{"key": "%s"}`, strings.Repeat("long string ", 1000)),
				`$.key`},

			// Unicode content
			{5, `{"unicode": "Hello 世界 🌍"}`,
				`{"unicode": "Hello 世界 🌍"}`,
				`$.unicode`},

			// Complex JSONPATH expressions
			{6, `{"a": [{"b": 1}, {"b": 2}, {"b": 3}]}`,
				`{"a": [{"b": 1}, {"b": 2}, {"b": 3}]}`,
				`$.a[*].b ? (@ > 1)`},

			// Empty objects and arrays
			{7, `{"empty_obj": {}, "empty_arr": []}`,
				`{"empty_obj": {}, "empty_arr": []}`,
				`$.empty_arr[*]`},

			// Numeric edge cases
			{8, `{
                "int_max": 2147483647,
                "int_min": -2147483648,
                "big_num": 1.23456789e+300,
                "small_num": 1.23456789e-300
            }`, `{
                "int_max": 2147483647,
                "int_min": -2147483648,
                "big_num": 1.23456789e+300,
                "small_num": 1.23456789e-300
            }`, `$.big_num`},

			// Null values
			{9, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, json_col, jsonb_col, jsonpath_col
                ) VALUES ($1, $2, $3, $4)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestUUIDType exercises discovery and capture of the UUID type.
func TestUUIDType(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        uuid_col UUID
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
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
                INSERT INTO %s (id, uuid_col) VALUES ($1, $2)`, tableName),
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
        id INTEGER PRIMARY KEY,
        xml_col XML
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
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
                INSERT INTO %s (id, xml_col) VALUES ($1, $2)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestArrayTypes exercises discovery and capture of the array types
// INTEGER[], REAL[], TEXT[], and UUID[].
func TestArrayTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        int_array INTEGER[],
        real_array REAL[],
        text_array TEXT[],
        uuid_array UUID[]
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Simple one-dimensional arrays
			{0,
				"{1,2,3}",
				"{1.1,2.2,3.3}",
				"{\"one\",\"two\",\"three\"}",
				"{550e8400-e29b-41d4-a716-446655440000,6ba7b810-9dad-11d1-80b4-00c04fd430c8}",
			},

			// Empty arrays
			{1,
				"{}",
				"{}",
				"{}",
				"{}",
			},

			// Arrays with NULL elements
			{2,
				"{1,NULL,3}",
				"{1.1,NULL,3.3}",
				"{\"one\",NULL,\"three\"}",
				"{550e8400-e29b-41d4-a716-446655440000,NULL}",
			},

			// Multi-dimensional arrays
			{3,
				"{{1,2},{3,4}}",
				"{{1.1,2.2},{3.3,4.4}}",
				"{{\"a\",\"b\"},{\"c\",\"d\"}}",
				"{{550e8400-e29b-41d4-a716-446655440000},{6ba7b810-9dad-11d1-80b4-00c04fd430c8}}",
			},

			// Arrays with special values
			{4,
				"{2147483647,-2147483648}",
				"{Infinity,-Infinity,NaN}",
				"{\"line\\nbreak\",\"tab\\there\",\"quote\\\"mark\"}",
				"{00000000-0000-0000-0000-000000000000,ffffffff-ffff-ffff-ffff-ffffffffffff}",
			},

			// Large arrays
			{5,
				fmt.Sprintf("{%s}", strings.Join(strings.Fields(strings.Repeat("42 ", 100)), ",")),
				fmt.Sprintf("{%s}", strings.Join(strings.Fields(strings.Repeat("3.14 ", 100)), ",")),
				fmt.Sprintf("{%s}", strings.Join(strings.Fields(strings.Repeat("\"text\" ", 100)), ",")),
				fmt.Sprintf("{%s}", strings.Join(strings.Fields(strings.Repeat("550e8400-e29b-41d4-a716-446655440000 ", 10)), ",")),
			},

			// Arrays with quoted strings and special characters
			{6,
				"{1234,5678}",
				"{1.234,5.678}",
				"{\"Hello, World!\",\"Contains \\\"quotes\\\"\",\"Special chars: @#$%\"}",
				"{550e8400-e29b-41d4-a716-446655440000}",
			},

			// Arrays with Unicode content
			{7,
				"{789,012}",
				"{7.89,0.12}",
				"{\"Hello 世界\",\"Привет мир\",\"🌍🌎🌏\"}",
				"{6ba7b810-9dad-11d1-80b4-00c04fd430c8}",
			},

			// NULL arrays
			{8, nil, nil, nil, nil},

			// Single-element arrays
			{9,
				"{42}",
				"{3.14159}",
				"{\"single element\"}",
				"{550e8400-e29b-41d4-a716-446655440000}",
			},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, int_array, real_array, text_array, uuid_array
                ) VALUES ($1, $2, $3, $4, $5)`, tableName),
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
        description TEXT
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// Initial inserts
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, version, description)
                VALUES ($1, $2, $3)`, tableName),
				i, 1, fmt.Sprintf("Initial version of row %d", i))
		}
		cs.Capture(ctx, t, nil)

		// Modify some rows
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET version = version + 1,
            description = description || ' (updated)'
            WHERE id IN (1, 3)`, tableName))
		cs.Capture(ctx, t, nil)

		// Delete some rows (not captured because we're using XMIN)
		executeControlQuery(t, control, fmt.Sprintf(`
            DELETE FROM %s WHERE id IN (2, 4)`, tableName))
		cs.Capture(ctx, t, nil)

		// Insert new rows with reused IDs
		executeControlQuery(t, control, fmt.Sprintf(`
            INSERT INTO %s (id, version, description)
            VALUES ($1, $2, $3), ($4, $5, $6)`, tableName),
			2, 1, "Reused ID 2",
			4, 1, "Reused ID 4")
		cs.Capture(ctx, t, nil)

		// Final update to all remaining rows
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET version = version + 1,
            description = description || ' (final update)'`, tableName))
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
        data TEXT
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// First batch of rows
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, data) VALUES ($1, $2)`, tableName),
				i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)

		// No changes - this capture should find no rows
		cs.Capture(ctx, t, nil)

		// Second batch of rows
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, data) VALUES ($1, $2)`, tableName),
				i, fmt.Sprintf("Value for row %d", i))
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
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY, data TEXT)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0]) // Empty cursor means full refresh behavior

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableName),
				i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)

		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableName),
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
        data TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3)", tableName),
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
        data TEXT
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
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3, $4)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)

		// Second batch with higher version numbers
		for _, row := range [][]any{
			{6, 0, 9, "Value ignored because major is too small"},
			{7, 2, 0, "Value ignored because minor is too small"},
			{8, 3, 1, "v3.1"}, {9, 3, 2, "v3.2"}, {10, 3, 3, "v3.3"},
			{11, 4, 1, "v4.1"}, {12, 4, 2, "v4.2"}, {13, 4, 3, "v4.3"},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3, $4)", tableName), row...)
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
		{name: "XMinFirstQuery", cursor: []string{"txid"}},
		{name: "XMinSubsequentQuery", cursor: []string{"txid"}, cursorValues: []any{12345}},
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
			var query, err = postgresDriver.buildQuery(resource, state)
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
	setResourceCursor(t, cs.Bindings[1], "updated_at")

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// First batch: Insert rows into base table, some visible in view
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3, $4)`, baseTableName),
				i, fmt.Sprintf("Row %d", i), i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Second batch: More rows with later timestamps
		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2, $3, $4)`, baseTableName),
				i, fmt.Sprintf("Row %d", i), i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Update some rows to change their visibility
		executeControlQuery(t, control, fmt.Sprintf(`
			UPDATE %s SET visible = NOT visible, updated_at = $1
			WHERE id IN (2, 3, 4)`, baseTableName),
			baseTime.Add(20*time.Minute))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessCapture exercises discovery and capture from a table without
// a defined primary key, but using the default XMIN cursor behavior.
func TestKeylessCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(data TEXT, value INTEGER)`) // Intentionally no PRIMARY KEY

	// Set REPLICA IDENTITY FULL to permit updates/deletions, just in case there's a
	// FOR ALL TABLES publication on the test database.
	executeControlQuery(t, control, fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL`, tableName))

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// Initial batch of data
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (data, value) VALUES ($1, $2)`, tableName),
				fmt.Sprintf("Initial row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Add more rows
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (data, value) VALUES ($1, $2)`, tableName),
				fmt.Sprintf("Additional row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Update some rows
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET data = data || ' (updated)'
            WHERE value >= 3 AND value <= 7`, tableName))
		cs.Capture(ctx, t, nil)

		// Delete and reinsert rows to ensure XMIN tracking works
		executeControlQuery(t, control, fmt.Sprintf(`
            DELETE FROM %s WHERE value = 4 OR value = 6`, tableName))
		executeControlQuery(t, control, fmt.Sprintf(`
            INSERT INTO %s (data, value) VALUES ($1, $2), ($3, $4)`, tableName),
			"Reinserted row 4", 4,
			"Reinserted row 6", 6)
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessFullRefreshCapture exercises discovery and capture from a table
// without a defined primary key, and with the cursor explicitly unset to test
// full-refresh behavior. In real use-cases this is more likely to occur when
// capturing a view, but there's no reason to include the extra complexity in
// this test setup.
func TestKeylessFullRefreshCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(data TEXT, value INTEGER)`) // No PRIMARY KEY

	// Set REPLICA IDENTITY FULL to permit updates/deletions, just in case there's a
	// FOR ALL TABLES publication on the test database.
	executeControlQuery(t, control, fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL`, tableName))

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
				fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2)", tableName),
				fmt.Sprintf("Initial row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Add more rows - these should appear in the next full refresh
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control,
				fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2)", tableName),
				fmt.Sprintf("Additional row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Modify some existing rows - changes should appear in next full refresh
		executeControlQuery(t, control, fmt.Sprintf(
			"UPDATE %s SET data = data || ' (updated)' WHERE value >= 3 AND value <= 7",
			tableName))
		cs.Capture(ctx, t, nil)

		// Delete some rows and add new ones - changes should appear in next full refresh
		executeControlQuery(t, control,
			fmt.Sprintf("DELETE FROM %s WHERE value = 4 OR value = 6", tableName))
		executeControlQuery(t, control,
			fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2), ($3, $4)", tableName),
			"New row A", 20, "New row B", 21)
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
			var ctx, control = context.Background(), testControlClient(t)
			var tableName, uniqueID = testTableName(t, uniqueTableID(t))
			createTestTable(t, control, tableName, `(data TEXT, value INTEGER)`) // No PRIMARY KEY

			// Set REPLICA IDENTITY FULL for safety with publications
			executeControlQuery(t, control, fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL`, tableName))

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
					executeControlQuery(t, control,
						fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2)", tableName),
						fmt.Sprintf("Initial row %d", i), i)
				}
				cs.Capture(ctx, t, nil)

				// Add more rows
				for i := 5; i < 10; i++ {
					executeControlQuery(t, control,
						fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2)", tableName),
						fmt.Sprintf("Additional row %d", i), i)
				}
				cs.Capture(ctx, t, nil)

				// Modify some existing rows
				executeControlQuery(t, control, fmt.Sprintf(
					"UPDATE %s SET data = data || ' (updated)' WHERE value >= 3 AND value <= 7",
					tableName))
				cs.Capture(ctx, t, nil)

				// Delete and add new rows
				executeControlQuery(t, control,
					fmt.Sprintf("DELETE FROM %s WHERE value = 4 OR value = 6", tableName))
				executeControlQuery(t, control,
					fmt.Sprintf("INSERT INTO %s (data, value) VALUES ($1, $2)", tableName),
					"New row A", 20)
				cs.Capture(ctx, t, nil)

				cupaloy.SnapshotT(t, cs.Summary())
			})
		})
	}
}
