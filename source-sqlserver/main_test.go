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

	dbCaptureUser     = flag.String("db_user", "flow_capture", "Connect as the specified user for test captures")
	dbCapturePassword = flag.String("db_password", "we2rie1E", "Password for the specified database test capture user")
	dbTestUser        = flag.String("db_test_user", "sa", "Connect as the specified user for test manipulations")
	dbTestPassword    = flag.String("db_test_pass", "gf6w6dkD", "Password for the test-manipulation user")

	enableCDCWhenCreatingTables = flag.Bool("enable_cdc_when_creating_tables", true, "Set to true if CDC should be enabled before the test capture runs")
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

	var testCfg = &Config{
		Address:  *dbAddress,
		User:     *dbTestUser,
		Password: *dbTestPassword,
		Database: *dbName,
	}
	log.WithFields(log.Fields{
		"address": testCfg.Address,
		"user":    testCfg.User,
	}).Info("connecting to test database")
	var conn, err = sql.Open("sqlserver", testCfg.ToURI())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return &testBackend{
		cfg: Config{
			Address:  *dbAddress,
			User:     *dbCaptureUser,
			Password: *dbCapturePassword,
			Database: *dbName,
		},
		conn: conn,
	}
}

type testBackend struct {
	cfg  Config
	conn *sql.DB
}

func (tb *testBackend) CaptureSpec(t testing.TB, streamIDs ...string) *st.CaptureSpec {
	var cfg = tb.cfg
	return &st.CaptureSpec{
		Driver:       sqlserverDriver,
		EndpointSpec: &cfg,
		Bindings:     tests.ResourceBindings(t, streamIDs...),
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers:   CaptureSanitizers,
	}
}

var CaptureSanitizers = make(map[string]*regexp.Regexp)

func init() {
	for k, v := range st.DefaultSanitizers {
		CaptureSanitizers[k] = v
	}
	CaptureSanitizers[`"cursor":"AAAAAAAAAAAAAA=="`] = regexp.MustCompile(`"cursor":"[0-9A-Za-z+/=]+"`)
	CaptureSanitizers[`"lsn":"AAAAAAAAAAAAAA=="`] = regexp.MustCompile(`"lsn":"[0-9A-Za-z+/=]+"`)
	CaptureSanitizers[`"seqval":"AAAAAAAAAAAAAA=="`] = regexp.MustCompile(`"seqval":"[0-9A-Za-z+/=]+"`)
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
	var fullTableName = "dbo." + tableName

	log.WithFields(log.Fields{"table": fullTableName, "cols": tableDef}).Debug("creating test table")

	var _, err = tb.conn.ExecContext(ctx, fmt.Sprintf("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s;", fullTableName, fullTableName))
	require.NoError(t, err)

	_, err = tb.conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s%s;", fullTableName, tableDef))
	require.NoError(t, err)

	if *enableCDCWhenCreatingTables {
		var instanceName = "dbo_" + strings.ToLower(tableName)
		var query = fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = '%s', @role_name = '%s', @capture_instance = '%s';`,
			tableName,
			*dbCaptureUser,
			instanceName,
		)
		_, err = tb.conn.ExecContext(ctx, query)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		log.WithField("table", fullTableName).Debug("destroying test table")
		_, err = tb.conn.ExecContext(ctx, fmt.Sprintf("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s;", fullTableName, fullTableName))
		require.NoError(t, err)
	})

	return fullTableName
}

// Insert adds all provided rows to the specified table in a single transaction.
func (tb *testBackend) Insert(ctx context.Context, t testing.TB, table string, rows [][]interface{}) {
	t.Helper()

	if len(rows) < 1 {
		t.Fatalf("must insert at least one row")
	}
	var tx, err = tb.conn.BeginTx(ctx, nil)
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
	var _, err = tb.conn.ExecContext(ctx, query, setVal, whereVal)
	require.NoError(t, err, "update rows")
}

// Delete removes preexisting rows.
func (tb *testBackend) Delete(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}) {
	t.Helper()
	var query = fmt.Sprintf(`DELETE FROM %s WHERE %s = @p1;`, table, whereCol)
	log.WithField("query", query).Debug("deleting rows")
	var _, err = tb.conn.ExecContext(ctx, query, whereVal)
	require.NoError(t, err, "delete rows")
}

// TestGeneric runs the generic sqlcapture test suite.
func TestGeneric(t *testing.T) {
	var tb = sqlserverTestBackend(t)
	tests.Run(context.Background(), t, tb)
}
