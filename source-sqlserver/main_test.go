package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
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

	enableCDCWhenCreatingTables = flag.Bool("enable_cdc_when_creating_tables", true, "Set to true if CDC should be enabled before the test capture runs")
	testSchemaName              = flag.String("test_schema_name", "dbo", "The schema in which to create test tables.")
	testFeatureFlags            = flag.String("feature_flags", "", "Feature flags to apply to all test captures.")
)

func TestMain(m *testing.M) {
	flag.Parse()

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		level, err := log.ParseLevel(logLevel)
		if err != nil {
			log.WithField("level", logLevel).Fatal("invalid log level")
		}
		log.SetLevel(level)
	}

	// Set a 900MiB memory limit, same as we use in production.
	debug.SetMemoryLimit(900 * 1024 * 1024)

	os.Exit(m.Run())
}

func sqlserverTestBackend(t testing.TB) *testBackend {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return nil
	}

	// Open control connection
	var controlURI = (&Config{
		Address:  *dbControlAddress,
		User:     *dbControlUser,
		Password: *dbControlPass,
		Database: *dbName,
	}).ToURI()
	log.WithFields(log.Fields{
		"user": *dbControlUser,
		"addr": *dbControlAddress,
	}).Info("opening control connection")
	var conn, err = sql.Open("sqlserver", controlURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	// Construct the capture config
	var captureConfig = Config{
		Address:  *dbCaptureAddress,
		User:     *dbCaptureUser,
		Password: *dbCapturePass,
		Database: *dbName,
	}
	captureConfig.Advanced.FeatureFlags = *testFeatureFlags
	// Other connectors use 16 in tests, but going below 128 here
	// appears to change database backfill row ordering in the
	// 'DuplicatedScanKey' test.
	captureConfig.Advanced.BackfillChunkSize = 128
	if err := captureConfig.Validate(); err != nil {
		t.Fatalf("error validating capture config: %v", err)
	}
	captureConfig.SetDefaults()

	return &testBackend{control: conn, config: captureConfig}
}

type testBackend struct {
	control *sql.DB
	config  Config
}

func (tb *testBackend) UpperCaseMode() bool { return false }

func (tb *testBackend) CaptureSpec(ctx context.Context, t testing.TB, streamMatchers ...*regexp.Regexp) *st.CaptureSpec {
	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"<TIMESTAMP>"`] = regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"cursor":"AAAAAAAAAAAAAA=="`] = regexp.MustCompile(`"cursor":"[0-9A-Za-z+/=]+"`)
	sanitizers[`"lsn":"AAAAAAAAAAAAAA=="`] = regexp.MustCompile(`"lsn":"[0-9A-Za-z+/=]+"`)
	sanitizers[`"seqval":"AAAAAAAAAAAAAA=="`] = regexp.MustCompile(`"seqval":"[0-9A-Za-z+/=]+"`)

	var cfg = tb.config
	var cs = &st.CaptureSpec{
		Driver:       sqlserverDriver,
		EndpointSpec: &cfg,
		Validator:    &st.OrderedCaptureValidator{IncludeSourcedSchemas: true},
		Sanitizers:   sanitizers,
	}
	if strings.Contains(*testFeatureFlags, "replica_fencing") {
		cs.CaptureDelay = 1 * time.Second
	}
	if len(streamMatchers) > 0 {
		cs.Bindings = tests.DiscoverBindings(ctx, t, tb, streamMatchers...)
	}
	return cs
}

// CreateTable creates a new database table whose name is based on the current test
// name. If `suffix` is non-empty it should be included at the end of the new table's
// name. The table will be registered with `t.Cleanup()` to be deleted at the end of
// the current test.
func (tb *testBackend) CreateTable(ctx context.Context, t testing.TB, suffix string, tableDef string) string {
	t.Helper()

	var tableName = tb.TableName(t, suffix)
	var quotedTableName = fmt.Sprintf("[%s].[%s]", *testSchemaName, tableName)

	log.WithFields(log.Fields{"table": quotedTableName, "cols": tableDef}).Debug("creating test table")

	tb.Query(ctx, t, fmt.Sprintf("IF OBJECT_ID('%[1]s', 'U') IS NOT NULL DROP TABLE %[1]s;", quotedTableName))
	tb.Query(ctx, t, fmt.Sprintf("CREATE TABLE %s%s;", quotedTableName, tableDef))

	if *enableCDCWhenCreatingTables {
		time.Sleep(1 * time.Second) // Sleep to make deadlocks less likely
		var query = fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = '%s', @source_name = '%s', @role_name = '%s';`,
			*testSchemaName,
			tableName,
			*dbCaptureUser,
		)
		tb.Query(ctx, t, query)
	}

	t.Cleanup(func() {
		log.WithField("table", quotedTableName).Debug("destroying test table")
		tb.Query(ctx, t, fmt.Sprintf("IF OBJECT_ID('%[1]s', 'U') IS NOT NULL DROP TABLE %[1]s;", quotedTableName))
	})

	return quotedTableName
}

func (tb *testBackend) TableName(t testing.TB, suffix string) string {
	t.Helper()
	var tableName = "test_" + strings.TrimPrefix(t.Name(), "Test")
	if suffix != "" {
		tableName += "_" + suffix
	}
	for _, str := range []string{"/", "=", "(", ")"} {
		tableName = strings.ReplaceAll(tableName, str, "_")
	}
	return tableName
}

// Insert adds all provided rows to the specified table in a single transaction.
func (tb *testBackend) Insert(ctx context.Context, t testing.TB, table string, rows [][]interface{}) {
	t.Helper()

	if len(rows) < 1 {
		t.Fatalf("must insert at least one row")
	}
	var tx, err = tb.control.BeginTx(ctx, nil)
	require.NoErrorf(t, err, "begin transaction")

	log.WithFields(log.Fields{"table": table, "count": len(rows)}).Debug("inserting data")
	var query = fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, argsTuple(len(rows[0])))
	for _, row := range rows {
		log.WithFields(log.Fields{"table": table, "row": row, "query": query}).Trace("inserting row")
		require.Equal(t, len(row), len(rows[0]), "incorrect number of values in row")
		var _, err = tx.ExecContext(ctx, query, row...)
		require.NoError(t, err, "insert row")
	}
	require.NoErrorf(t, tx.Commit(), "commit transaction")
}

func argsTuple(argc int) string {
	var tuple = "(@p1"
	for idx := 1; idx < argc; idx++ {
		tuple += fmt.Sprintf(",@p%d", idx+1)
	}
	return tuple + ")"
}

// Update modifies preexisting rows to a new value.
func (tb *testBackend) Update(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}, setCol string, setVal interface{}) {
	t.Helper()
	var query = fmt.Sprintf(`UPDATE %s SET %s = @p1 WHERE %s = @p2;`, table, setCol, whereCol)
	log.WithField("query", query).Debug("updating rows")
	tb.Query(ctx, t, query, setVal, whereVal)
}

// Delete removes preexisting rows.
func (tb *testBackend) Delete(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}) {
	t.Helper()
	var query = fmt.Sprintf(`DELETE FROM %s WHERE %s = @p1;`, table, whereCol)
	log.WithField("query", query).Debug("deleting rows")
	tb.Query(ctx, t, query, whereVal)
}

func (tb *testBackend) Query(ctx context.Context, t testing.TB, query string, args ...any) {
	t.Helper()
	var _, err = tb.control.ExecContext(ctx, query, args...)
	require.NoError(t, err)
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

func setShutdownAfterCaughtUp(t testing.TB, setting bool) {
	t.Helper()
	var prevSetting = sqlcapture.TestShutdownAfterCaughtUp
	sqlcapture.TestShutdownAfterCaughtUp = setting
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = prevSetting })
}

// TestGeneric runs the generic sqlcapture test suite.
func TestGeneric(t *testing.T) {
	var tb = sqlserverTestBackend(t)
	tests.Run(context.Background(), t, tb)
}

func TestColumnNameQuoting(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "79126849"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "([id] INTEGER, [data] INTEGER, [CAPITALIZED] INTEGER, [unique] INTEGER, [type] INTEGER, PRIMARY KEY ([id], [data], [capitalized], [unique], [type]))")
	tb.Insert(ctx, t, tableName, [][]any{{0, 0, 0, 0, 0}, {1, 1, 1, 1, 1}, {2, 2, 2, 2, 2}})
	tests.VerifiedCapture(ctx, t, tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID)))
}

func TestTextCollation(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "89620867"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id VARCHAR(8) PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableName, [][]any{{"AAA", "1"}, {"BBB", "2"}, {"-J C", "3"}, {"H R", "4"}})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	tests.VerifiedCapture(ctx, t, cs)
}

// TestDiscoveryIrrelevantConstraints verifies that discovery works correctly
// even when there are other non-primary-key constraints on a table.
func TestDiscoveryIrrelevantConstraints(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "44516719"
	tb.CreateTable(ctx, t, uniqueID, "(id VARCHAR(8) PRIMARY KEY, foo INTEGER UNIQUE, data TEXT)")
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}

func TestUUIDCaptureOrder(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "1794630882"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id UNIQUEIDENTIFIER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableName, [][]any{
		{"00ffffff-ffff-ffff-ffff-ffffffffffff", "sixteen"},
		{"ff00ffff-ffff-ffff-ffff-ffffffffffff", "fifteen"},
		{"ffff00ff-ffff-ffff-ffff-ffffffffffff", "fourteen"},
		{"ffffff00-ffff-ffff-ffff-ffffffffffff", "thirteen"},
		{"ffffffff-00ff-ffff-ffff-ffffffffffff", "twelve"},
		{"ffffffff-ff00-ffff-ffff-ffffffffffff", "eleven"},
		{"ffffffff-ffff-00ff-ffff-ffffffffffff", "ten"},
		{"ffffffff-ffff-ff00-ffff-ffffffffffff", "nine"},
		{"ffffffff-ffff-ffff-00ff-ffffffffffff", "seven"},
		{"ffffffff-ffff-ffff-ff00-ffffffffffff", "eight"},
		{"ffffffff-ffff-ffff-ffff-00ffffffffff", "one"},
		{"ffffffff-ffff-ffff-ffff-ff00ffffffff", "two"},
		{"ffffffff-ffff-ffff-ffff-ffff00ffffff", "three"},
		{"ffffffff-ffff-ffff-ffff-ffffff00ffff", "four"},
		{"ffffffff-ffff-ffff-ffff-ffffffff00ff", "five"},
		{"ffffffff-ffff-ffff-ffff-ffffffffff00", "six"},
	})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	tests.VerifiedCapture(ctx, t, cs)
}

func TestManyTables(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniquePrefix = "2546318"

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var tableNames []string
	var streamMatchers []*regexp.Regexp
	for i := 0; i < 20; i++ {
		var uniqueID = fmt.Sprintf("%s_%03d", uniquePrefix, i)
		var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
		tableNames = append(tableNames, tableName)
		streamMatchers = append(streamMatchers, regexp.MustCompile(uniqueID))
	}
	var cs = tb.CaptureSpec(ctx, t, streamMatchers...)

	for i := 0; i < 20; i++ {
		tb.Insert(ctx, t, tableNames[i], [][]any{{0, fmt.Sprintf("table %d row zero", i)}, {1, fmt.Sprintf("table %d row one", i)}})
	}
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	for i := 0; i < 20; i++ {
		tb.Insert(ctx, t, tableNames[i], [][]any{{2, fmt.Sprintf("table %d row two", i)}, {3, fmt.Sprintf("table %d row three", i)}})
	}
	t.Run("capture1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	for i := 0; i < 10; i++ {
		tb.Insert(ctx, t, tableNames[i], [][]any{{4, fmt.Sprintf("table %d row four", i)}, {5, fmt.Sprintf("table %d row five", i)}})
	}
	t.Run("capture2", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestCaptureInstanceCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var tb, ctx = sqlserverTestBackend(t), context.Background()
	for _, tc := range []struct {
		Name     string
		UniqueID string
		WithDBO  bool
	}{
		{"WithDBO", "71329622", true},
		{"WithoutDBO", "72928064", false},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			var fullTableName = tb.CreateTable(ctx, t, tc.UniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
			var changeTableName = fmt.Sprintf("cdc.%s_%s_CT", *testSchemaName, tb.TableName(t, tc.UniqueID))
			var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(tc.UniqueID))
			cs.EndpointSpec.(*Config).Advanced.AutomaticChangeTableCleanup = true

			// The db_owner permission is required to automatically clean up change tables,
			// and we need to test both with and without to verify that the capture correctly
			// handles the lack of this permission.
			if tc.WithDBO {
				tb.Query(ctx, t, fmt.Sprintf("ALTER ROLE db_owner ADD MEMBER %s", *dbCaptureUser))
				t.Cleanup(func() { tb.Query(ctx, t, fmt.Sprintf("ALTER ROLE db_owner DROP MEMBER %s", *dbCaptureUser)) })
			}

			// Run the capture once to dispense with any backfills
			sqlcapture.TestShutdownAfterCaughtUp = true
			cs.Capture(ctx, t, nil)
			cs.Reset() // Reset validator/error state
			sqlcapture.TestShutdownAfterCaughtUp = false

			// Launch the ongoing capture in a separate thread
			var captureCtx, cancelCapture = context.WithCancel(ctx)
			defer cancelCapture()
			var captureDone atomic.Bool
			go func(ctx context.Context) {
				cs.Capture(captureCtx, t, nil)
				captureDone.Store(true)
			}(captureCtx)

			// Traffic generator in a separate thread
			var trafficCtx, cancelTraffic = context.WithCancel(ctx)
			defer cancelTraffic()
			var trafficDone atomic.Bool
			go func(ctx context.Context) {
				for i := 0; i < 10; i++ {
					tb.Insert(ctx, t, fullTableName, [][]any{
						{i*100 + 1, fmt.Sprintf("iteration %d row one", i)},
						{i*100 + 2, fmt.Sprintf("iteration %d row two", i)},
						{i*100 + 3, fmt.Sprintf("iteration %d row three", i)},
						{i*100 + 4, fmt.Sprintf("iteration %d row four", i)},
					})
					time.Sleep(1 * time.Second)
				}
				time.Sleep(cdcCleanupInterval + 2*time.Second)
				trafficDone.Store(true)
			}(trafficCtx)

			// Wait until the traffic generator shuts down and then cancel the capture
			for !trafficDone.Load() {
				time.Sleep(100 * time.Millisecond)
			}
			cancelCapture()
			for !captureDone.Load() {
				time.Sleep(100 * time.Millisecond)
			}

			// Query the number of rows in the change table after everything finishes
			var changeTableRows int
			require.NoError(t, tb.control.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", changeTableName)).Scan(&changeTableRows))

			// Validate test snapshot plus change table row count
			var summary = cs.Summary()
			cs.Reset()
			summary += fmt.Sprintf("\n# Capture Instance Contained %d Rows At Finish", changeTableRows)
			cupaloy.SnapshotT(t, summary)
		})
	}
}

func TestAlterationAddColumn(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	// Grant the 'db_owner' role, it is required for the capture instance automation to work.
	tb.Query(ctx, t, fmt.Sprintf("ALTER ROLE db_owner ADD MEMBER %s", *dbCaptureUser))
	t.Cleanup(func() { tb.Query(ctx, t, fmt.Sprintf("ALTER ROLE db_owner DROP MEMBER %s", *dbCaptureUser)) })

	for _, tc := range []struct {
		Name     string
		UniqueID string
		Manual   bool
		Automate bool
	}{
		{"Off", "39517707", false, false},
		{"Manual", "22713060", true, false},
		{"Automatic", "81310450", false, true},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			var tableName = tb.CreateTable(ctx, t, tc.UniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
			var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(tc.UniqueID))
			cs.Validator = &st.OrderedCaptureValidator{}
			cs.EndpointSpec.(*Config).Advanced.AutomaticCaptureInstances = tc.Automate

			// Get the backfill out of the way
			tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}})
			cs.Capture(ctx, t, nil)

			// Add a column. Since the changes occur before the first capture here, it won't
			// be able to get the new column, but the second capture should.
			tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD extra TEXT;", tableName))
			tb.Insert(ctx, t, tableName, [][]any{{2, "two", "aaa"}, {3, "three", "bbb"}})
			time.Sleep(1 * time.Second) // Sleep to let the alteration make its way to cdc.ddl_history
			if tc.Manual {
				tb.Query(ctx, t, fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = '%[1]s', @source_name = '%[2]s', @role_name = '%[3]s', @capture_instance = '%[1]s_%[2]s_v2';`, *testSchemaName, tb.TableName(t, tc.UniqueID), *dbCaptureUser))
				time.Sleep(1 * time.Second)
			}
			cs.Capture(ctx, t, nil)
			tb.Insert(ctx, t, tableName, [][]any{{4, "four", "ccc"}, {5, "five", "ddd"}})
			cs.Capture(ctx, t, nil)
			if tc.Manual {
				time.Sleep(1 * time.Second)
				tb.Query(ctx, t, fmt.Sprintf(`EXEC sys.sp_cdc_disable_table @source_schema = '%[1]s', @source_name = '%[2]s', @capture_instance = '%[1]s_%[2]s';`, *testSchemaName, tb.TableName(t, tc.UniqueID)))
				time.Sleep(1 * time.Second)
			}

			// Add a second column. Since each table alteration requires a new capture instance
			// this should demonstrate that cleanup is working correctly as well.
			tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD evenmore TEXT;", tableName))
			tb.Insert(ctx, t, tableName, [][]any{{6, "six", "eee", "foo"}, {7, "seven", "fff", "bar"}})
			time.Sleep(1 * time.Second) // Sleep to let the alteration make its way to cdc.ddl_history
			if tc.Manual {
				tb.Query(ctx, t, fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = '%[1]s', @source_name = '%[2]s', @role_name = '%[3]s', @capture_instance = '%[1]s_%[2]s_v3';`, *testSchemaName, tb.TableName(t, tc.UniqueID), *dbCaptureUser))
				time.Sleep(1 * time.Second)
			}
			cs.Capture(ctx, t, nil)
			tb.Insert(ctx, t, tableName, [][]any{{8, "eight", "ggg", "baz"}, {9, "nine", "hhh", "asdf"}})
			cs.Capture(ctx, t, nil)
			if tc.Manual {
				time.Sleep(1 * time.Second)
				tb.Query(ctx, t, fmt.Sprintf(`EXEC sys.sp_cdc_disable_table @source_schema = '%[1]s', @source_name = '%[2]s', @capture_instance = '%[1]s_%[2]s_v2';`, *testSchemaName, tb.TableName(t, tc.UniqueID)))
				time.Sleep(1 * time.Second)
			}

			// Validate test snapshot
			cupaloy.SnapshotT(t, cs.Summary())
		})
	}
}

func TestDeletedTextColumn(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "41823101"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, v_text TEXT NOT NULL, v_varchar VARCHAR(32), v_int INTEGER)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}

	tb.Insert(ctx, t, tableName, [][]any{{0, "zero", "zero", 100}, {1, "one", "one", 101}})
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]any{{2, "two", "two", 102}, {3, "three", "three", 103}})
	tb.Delete(ctx, t, tableName, "id", 1)
	tb.Delete(ctx, t, tableName, "id", 2)
	t.Run("main", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestComputedColumn(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "18851564"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, a VARCHAR(32), b VARCHAR(32), computed AS ISNULL(a, ISNULL(b, 'default')))")

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })
	sqlcapture.TestShutdownAfterCaughtUp = true

	t.Run("discovery", func(t *testing.T) {
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})

	t.Run("capture", func(t *testing.T) {
		tb.Insert(ctx, t, tableName, [][]any{{0, "a0", "b0"}, {1, nil, "b1"}, {2, "a2", nil}, {3, nil, nil}})
		cs.Capture(ctx, t, nil)
		tb.Insert(ctx, t, tableName, [][]any{{4, "a4", "b4"}, {5, nil, "b5"}, {6, "a6", nil}, {7, nil, nil}})
		cs.Capture(ctx, t, nil)
		tb.Update(ctx, t, tableName, "id", 4, "a", "a4-modified")
		tb.Update(ctx, t, tableName, "id", 5, "b", "b5-modified")
		tb.Update(ctx, t, tableName, "id", 6, "a", "a6-modified")
		cs.Capture(ctx, t, nil)
		tb.Delete(ctx, t, tableName, "id", 4)
		tb.Delete(ctx, t, tableName, "id", 5)
		tb.Delete(ctx, t, tableName, "id", 6)
		tb.Delete(ctx, t, tableName, "id", 7)
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

func TestDroppedAndRecreatedTable(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "37815596"
	var tableDef = "(id INTEGER PRIMARY KEY, data TEXT)"
	var tableName = tb.CreateTable(ctx, t, uniqueID, tableDef)

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	// Initial backfill
	tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}, {2, "two"}})
	cs.Capture(ctx, t, nil)

	// Some replication
	tb.Insert(ctx, t, tableName, [][]any{{3, "three"}, {4, "four"}, {5, "five"}})
	cs.Capture(ctx, t, nil)

	// Drop and recreate the table, then fill it with some new data.
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE %s;`, tableName))
	tb.CreateTable(ctx, t, uniqueID, tableDef)
	tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}, {2, "two"}})
	tb.Insert(ctx, t, tableName, [][]any{{6, "six"}, {7, "seven"}, {8, "eight"}})
	cs.Capture(ctx, t, nil)

	// Followed by some more replication
	tb.Insert(ctx, t, tableName, [][]any{{9, "nine"}, {10, "ten"}, {11, "eleven"}})
	cs.Capture(ctx, t, nil)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestFilegroupAndRole(t *testing.T) {
	// Turn off the test logic that creates CDC instances when creating tables, for this one test.
	var oldEnableCDCWhenCreatingTables = *enableCDCWhenCreatingTables
	*enableCDCWhenCreatingTables = false
	t.Cleanup(func() { *enableCDCWhenCreatingTables = oldEnableCDCWhenCreatingTables })

	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "93932362"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")

	// Grant the 'db_owner' role, it is required to create a CDC instance automatically.
	tb.Query(ctx, t, fmt.Sprintf("ALTER ROLE db_owner ADD MEMBER %s", *dbCaptureUser))
	t.Cleanup(func() { tb.Query(ctx, t, fmt.Sprintf("ALTER ROLE db_owner DROP MEMBER %s", *dbCaptureUser)) })

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	cs.EndpointSpec.(*Config).Advanced.Filegroup = "PRIMARY"
	cs.EndpointSpec.(*Config).Advanced.RoleName = "flow_capture"
	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}, {2, "two"}})
	cs.Capture(ctx, t, nil)
	tb.Insert(ctx, t, tableName, [][]any{{3, "three"}, {4, "four"}, {5, "five"}})
	cs.Capture(ctx, t, nil)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestPrimaryKeyUpdate(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "63510878"
	var tableDef = "(id INTEGER PRIMARY KEY, data TEXT)"
	var tableName = tb.CreateTable(ctx, t, uniqueID, tableDef)

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	// Initial backfill
	tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}, {2, "two"}})
	cs.Capture(ctx, t, nil)

	// Some replication
	tb.Insert(ctx, t, tableName, [][]any{{3, "three"}, {4, "four"}, {5, "five"}})
	cs.Capture(ctx, t, nil)

	// Primary key updates
	tb.Update(ctx, t, tableName, "id", 1, "id", 6)
	tb.Update(ctx, t, tableName, "id", 4, "id", 7)
	cs.Capture(ctx, t, nil)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestComputedPrimaryKey(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "10118243"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(actual_id INTEGER NOT NULL, data VARCHAR(32), id AS actual_id PRIMARY KEY)")

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })
	sqlcapture.TestShutdownAfterCaughtUp = true

	t.Run("discovery", func(t *testing.T) {
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})

	t.Run("capture", func(t *testing.T) {
		tb.Insert(ctx, t, tableName, [][]any{{0, "zero"}, {1, "one"}, {2, "two"}})
		cs.Capture(ctx, t, nil)
		tb.Insert(ctx, t, tableName, [][]any{{3, "three"}, {4, "four"}, {5, "five"}})
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFeatureFlagEmitSourcedSchemas runs a capture with the `emit_sourced_schemas` feature flag set.
func TestFeatureFlagEmitSourcedSchemas(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "64029092"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data VARCHAR(32))")

	tb.Insert(ctx, t, tableName, [][]any{{1, "hello"}, {2, "world"}})

	for _, tc := range []struct {
		name string
		flag string
	}{
		{"Default", ""},
		{"Enabled", "emit_sourced_schemas"},
		{"Disabled", "no_emit_sourced_schemas"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cs = tb.CaptureSpec(ctx, t)
			cs.EndpointSpec.(*Config).Advanced.FeatureFlags = tc.flag
			cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))

			sqlcapture.TestShutdownAfterCaughtUp = true
			t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

			cs.Capture(ctx, t, nil)
			cupaloy.SnapshotT(t, cs.Summary())
		})
	}
}
