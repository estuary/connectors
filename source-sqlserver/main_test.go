package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture/tests"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbAddress = flag.String("db_addr", "127.0.0.1:1433", "Connect to the specified address/port for tests")
	dbName    = flag.String("db_name", "test", "Connect to the named database for tests")

	dbControlUser = flag.String("db_control_user", "sa", "The user for test setup/control operations")
	dbControlPass = flag.String("db_control_pass", "gf6w6dkD", "The password the the test setup/control user")
	dbCaptureUser = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")
	dbCapturePass = flag.String("db_capture_pass", "we2rie1E", "The password for the capture user")

	enableCDCWhenCreatingTables = flag.Bool("enable_cdc_when_creating_tables", true, "Set to true if CDC should be enabled before the test capture runs")
	testSchemaName              = flag.String("test_schema_name", "dbo", "The schema in which to create test tables.")
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

	// SQL Server captures aren't just tailing the WAL directly, instead they're
	// polling change tables which are updated by an asynchronous worker which in
	// turn is reading the WAL, which has a higher latency than in other DBs. This
	// means that our capture shutdown delay has to be substantially longer if we
	// want to avoid having tests flake out sometimes.
	tests.CaptureShutdownDelay = 15 * time.Second

	os.Exit(m.Run())
}

func sqlserverTestBackend(t *testing.T) *testBackend {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return nil
	}

	// Open control connection
	var controlURI = (&Config{
		Address:  *dbAddress,
		User:     *dbControlUser,
		Password: *dbControlPass,
		Database: *dbName,
	}).ToURI()
	log.WithFields(log.Fields{
		"user": *dbControlUser,
		"addr": *dbAddress,
	}).Info("opening control connection")
	var conn, err = sql.Open("sqlserver", controlURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	// Construct the capture config
	var captureConfig = Config{
		Address:  *dbAddress,
		User:     *dbCaptureUser,
		Password: *dbCapturePass,
		Database: *dbName,
	}
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

func (tb *testBackend) CaptureSpec(ctx context.Context, t testing.TB, streamMatchers ...*regexp.Regexp) *st.CaptureSpec {
	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"<TIMESTAMP>"`] = regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|-[0-9]+:[0-9]+)"`)
	sanitizers[`"cursor":"AAAAAAAAAAAAAA=="`] = regexp.MustCompile(`"cursor":"[0-9A-Za-z+/=]+"`)
	sanitizers[`"lsn":"AAAAAAAAAAAAAA=="`] = regexp.MustCompile(`"lsn":"[0-9A-Za-z+/=]+"`)
	sanitizers[`"seqval":"AAAAAAAAAAAAAA=="`] = regexp.MustCompile(`"seqval":"[0-9A-Za-z+/=]+"`)

	var cfg = tb.config
	var cs = &st.CaptureSpec{
		Driver:       sqlserverDriver,
		EndpointSpec: &cfg,
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   sanitizers,
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

	var tableName = "test_" + strings.TrimPrefix(t.Name(), "Test")
	if suffix != "" {
		tableName += "_" + suffix
	}
	for _, str := range []string{"/", "=", "(", ")"} {
		tableName = strings.ReplaceAll(tableName, str, "_")
	}
	var quotedTableName = fmt.Sprintf("[%s].[%s]", *testSchemaName, tableName)

	log.WithFields(log.Fields{"table": quotedTableName, "cols": tableDef}).Debug("creating test table")

	tb.Query(ctx, t, fmt.Sprintf("IF OBJECT_ID('%[1]s', 'U') IS NOT NULL DROP TABLE %[1]s;", quotedTableName))
	tb.Query(ctx, t, fmt.Sprintf("CREATE TABLE %s%s;", quotedTableName, tableDef))

	if *enableCDCWhenCreatingTables {
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

// Insert adds all provided rows to the specified table in a single transaction.
func (tb *testBackend) Insert(ctx context.Context, t testing.TB, table string, rows [][]interface{}) {
	t.Helper()

	if len(rows) < 1 {
		t.Fatalf("must insert at least one row")
	}
	var tx, err = tb.control.BeginTx(ctx, nil)
	require.NoErrorf(t, err, "begin transaction")

	log.WithFields(log.Fields{"table": table, "count": len(rows), "first": rows[0]}).Debug("inserting data")
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
