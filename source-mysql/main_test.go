package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	_ "github.com/go-mysql-org/go-mysql/driver"

	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbAddress  = flag.String("db_addr", "127.0.0.1:3306", "Connect to the specified address/port for tests")
	dbUser     = flag.String("db_user", "root", "Connect as the specified user for tests")
	dbPassword = flag.String("db_password", "flow", "Password for the specified database test user")
	dbName     = flag.String("db_name", "test", "Connect to the named database for tests")
)

var (
	TestBackend *mysqlTestBackend
)

func TestMain(m *testing.M) {
	flag.Parse()

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		level, err := logrus.ParseLevel(logLevel)
		if err != nil {
			logrus.WithField("level", logLevel).Fatal("invalid log level")
		}
		logrus.SetLevel(level)
	}
	fixMysqlLogging()

	// Initialize test config and database connection
	var cfg = Config{
		Address:  *dbAddress,
		User:     *dbUser,
		Password: *dbPassword,
		Advanced: advancedConfig{
			DBName: *dbName,
		},
	}
	if err := cfg.Validate(); err != nil {
		logrus.WithFields(logrus.Fields{"err": err, "config": cfg}).Fatal("error validating test config")
	}
	cfg.SetDefaults("Test Config")

	var conn, err = client.Connect(cfg.Address, cfg.User, cfg.Password, cfg.Advanced.DBName)
	if err != nil {
		logrus.WithField("err", err).Fatal("error connecting to database")
	}

	TestBackend = &mysqlTestBackend{conn: conn, cfg: cfg}

	var exitCode = m.Run()
	os.Exit(exitCode)
}

func lowerTuningParameters(t testing.TB) {
	// Within the scope of a single test, adjust some tuning parameters so that it's
	// easier to exercise backfill chunking and replication buffering behavior.
	var prevChunkSize = TestBackend.cfg.Advanced.BackfillChunkSize
	t.Cleanup(func() { TestBackend.cfg.Advanced.BackfillChunkSize = prevChunkSize })
	TestBackend.cfg.Advanced.BackfillChunkSize = 16

	var prevBufferSize = replicationBufferSize
	t.Cleanup(func() { replicationBufferSize = prevBufferSize })
	replicationBufferSize = 0
}

type mysqlTestBackend struct {
	conn *client.Conn
	cfg  Config
}

func (tb *mysqlTestBackend) CaptureSpec(t testing.TB, streamIDs ...string) *st.CaptureSpec {
	var cfg = tb.cfg
	return &st.CaptureSpec{
		Driver:       mysqlDriver,
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
	CaptureSanitizers[`"binlog.000123:56789"`] = regexp.MustCompile(`"binlog\.[0-9]+:[0-9]+"`)
	CaptureSanitizers[`"ts_ms":1111111111111`] = regexp.MustCompile(`"ts_ms":[0-9]+`)
}

func (tb *mysqlTestBackend) CreateTable(ctx context.Context, t testing.TB, suffix string, tableDef string) string {
	t.Helper()

	var tableName = "test." + strings.TrimPrefix(t.Name(), "Test")
	if suffix != "" {
		tableName += "_" + suffix
	}
	for _, str := range []string{"/", "=", "(", ")"} {
		tableName = strings.ReplaceAll(tableName, str, "_")
	}

	logrus.WithFields(logrus.Fields{"table": tableName, "cols": tableDef}).Debug("creating test table")
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %s%s;`, tableName, tableDef))
	t.Cleanup(func() {
		logrus.WithField("table", tableName).Debug("destroying test table")
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE %s;`, tableName))
	})
	return tableName
}

func (tb *mysqlTestBackend) Insert(ctx context.Context, t testing.TB, table string, rows [][]interface{}) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	if err := tb.conn.Begin(); err != nil {
		t.Fatalf("error beginning transaction: %v", err)
	}
	var argc = len(rows[0])
	var query = fmt.Sprintf("INSERT INTO %s VALUES %s", table, argsTuple(argc))
	for _, row := range rows {
		if len(row) != argc {
			t.Fatalf("incorrect number of values in row %q (expected %d)", row, len(rows[0]))
		}
		tb.Query(ctx, t, query, row...)
	}
	if err := tb.conn.Commit(); err != nil {
		t.Fatalf("error committing transaction: %v", err)
	}
}

func argsTuple(argc int) string {
	var tuple = "(?"
	for idx := 1; idx < argc; idx++ {
		tuple += fmt.Sprintf(",?")
	}
	return tuple + ")"
}

func (tb *mysqlTestBackend) Update(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}, setCol string, setVal interface{}) {
	t.Helper()
	tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?;", table, setCol, whereCol), setVal, whereVal)
}

func (tb *mysqlTestBackend) Delete(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}) {
	t.Helper()
	tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE %s = ?;", table, whereCol), whereVal)
}

func (tb *mysqlTestBackend) Query(ctx context.Context, t testing.TB, query string, args ...interface{}) {
	t.Helper()
	logrus.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing query")
	var result, err = tb.conn.Execute(query, args...)
	if err != nil {
		t.Fatalf("error executing query %q: %v", query, err)
	}
	defer result.Close()
}

// TestGeneric runs the generic sqlcapture test suite.
func TestGeneric(t *testing.T) {
	lowerTuningParameters(t)
	tests.Run(context.Background(), t, TestBackend)
}

func TestAlterTable(t *testing.T) {
	var tb, ctx = TestBackend, context.Background()
	var tableA = tb.CreateTable(ctx, t, "aaa", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, "bbb", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableC = tb.CreateTable(ctx, t, "ccc", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "abc"}, {2, "def"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{3, "ghi"}, {4, "jkl"}})
	tb.Insert(ctx, t, tableC, [][]interface{}{{5, "mno"}, {6, "pqr"}})

	var cs = tb.CaptureSpec(t, tableA, tableB)
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Altering tableC, which is not being captured, should be fine
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra TEXT;", tableC))
	t.Run("capture1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Altering tableB, which is being captured, should result in an error
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra TEXT;", tableB))
	t.Run("capture2-fails", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Restarting the capture won't fix this
	t.Run("capture3-fails", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// But removing the problematic table should fix it
	cs.Bindings = tests.ResourceBindings(t, tableA)
	t.Run("capture4", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// And we can then re-add the table and it should start over after the problem
	cs.Bindings = tests.ResourceBindings(t, tableA, tableB)
	t.Run("capture5", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Finally we exercise the trickiest edge case, in which a new table (C)
	// is added to the capture *when it was also altered after the last state
	// checkpoint*. This should still work, because tables only become active
	// after the first stream-to-watermark operation.
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN evenmore TEXT;", tableC))
	cs.Bindings = tests.ResourceBindings(t, tableA, tableB, tableC)
	t.Run("capture6", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

// TestBinlogExpirySanityCheck verifies that the "dangerously short binlog expiry"
// sanity check is working as intended.
func TestBinlogExpirySanityCheck(t *testing.T) {
	var tb, ctx = TestBackend, context.Background()
	var cs = tb.CaptureSpec(t)

	for idx, tc := range []struct {
		VarName     string
		VarValue    int
		SkipCheck   bool
		ExpectError bool
		Message     string
	}{
		{"binlog_expire_logs_seconds", 2592000, false, false, "A 30-day expiry should not produce an error"},
		{"binlog_expire_logs_seconds", 604800, false, false, "A 7-day expiry should not produce an error"},
		{"binlog_expire_logs_seconds", 518400, false, true, "A 6-day expiry *should* produce an error"},
		{"binlog_expire_logs_seconds", 518400, true, false, "A 6-day expiry should not produce an error if we skip the sanity check"},
		{"expire_logs_days", 30, false, false, "A 30-day expiry should not produce an error"},
		{"expire_logs_days", 7, false, false, "A 7-day expiry should not produce an error"},
		{"expire_logs_days", 6, false, true, "A 6-day expiry *should* produce an error"},
		{"expire_logs_days", 6, true, false, "A 6-day expiry should not produce an error if we skip the sanity check"},
		{"binlog_expire_logs_seconds", 0, false, false, "A value of zero should also not produce an error"},
		{"binlog_expire_logs_seconds", 2592000, false, false, "Resetting expiry back to the default value"},
	} {
		t.Run(fmt.Sprintf("%d_%s_%d", idx, tc.VarName, tc.VarValue), func(t *testing.T) {
			// Set both expiry variables to their desired values. We start by setting them
			// both to zero because MySQL only allows one at a time to be nonzero.
			tb.Query(ctx, t, "SET GLOBAL binlog_expire_logs_seconds = 0;")
			tb.Query(ctx, t, "SET GLOBAL expire_logs_days = 0;")
			tb.Query(ctx, t, fmt.Sprintf("SET GLOBAL %s = %d;", tc.VarName, tc.VarValue))

			// Perform validation, which should run the sanity check
			cs.EndpointSpec.(*Config).Advanced.SkipBinlogRetentionCheck = tc.SkipCheck
			var _, err = cs.Validate(ctx, t)

			// Verify the result
			if tc.ExpectError {
				require.Error(t, err, tc.Message)
			} else {
				require.NoError(t, err, tc.Message)
			}
		})
	}
}

func TestSkipBackfills(t *testing.T) {
	// Set up three tables with some data in them, a catalog which captures all three,
	// but a configuration which specifies that tables A and C should skip backfilling
	// and only capture new changes.
	var tb, ctx = TestBackend, context.Background()
	var tableA = tb.CreateTable(ctx, t, "aaa", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, "bbb", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableC = tb.CreateTable(ctx, t, "ccc", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "one"}, {2, "two"}, {3, "three"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{4, "four"}, {5, "five"}, {6, "six"}})
	tb.Insert(ctx, t, tableC, [][]interface{}{{7, "seven"}, {8, "eight"}, {9, "nine"}})

	var cs = tb.CaptureSpec(t, tableA, tableB, tableC)
	cs.EndpointSpec.(*Config).Advanced.SkipBackfills = fmt.Sprintf("%s,%s", tableA, tableC)

	// Run an initial capture, which should only backfill events from table B
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Insert additional data and verify that all three tables report new events
	tb.Insert(ctx, t, tableA, [][]interface{}{{10, "ten"}, {11, "eleven"}, {12, "twelve"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{13, "thirteen"}, {14, "fourteen"}, {15, "fifteen"}})
	tb.Insert(ctx, t, tableC, [][]interface{}{{16, "sixteen"}, {17, "seventeen"}, {18, "eighteen"}})
	t.Run("main", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

// TestCursorResume sets up a capture with a (string, int) primary key and
// and repeatedly restarts it after each row of capture output.
func TestCursorResume(t *testing.T) {
	var tb, ctx = TestBackend, context.Background()
	var tableName = tb.CreateTable(ctx, t, "", "(epoch VARCHAR(8), count INTEGER, data TEXT, PRIMARY KEY (epoch, count))")
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{"aaa", 1, "bvzf"}, {"aaa", 2, "ukwh"}, {"aaa", 3, "lntg"}, {"bbb", -100, "bycz"},
		{"bbb", 2, "ajgp"}, {"bbb", 333, "zljj"}, {"bbb", 4096, "lhnw"}, {"bbb", 800000, "iask"},
		{"ccc", 1234, "bikh"}, {"ddd", -10000, "dhqc"}, {"x", 1, "djsf"}, {"y", 1, "iwnx"},
		{"z", 1, "qmjp"}, {"", 0, "xakg"}, {"", -1, "kvxr"}, {"   ", 3, "gboj"},
	})
	var cs = tb.CaptureSpec(t, tableName)

	// Reduce the backfill chunk size to 1 row. Since the capture will be killed and
	// restarted after each scan key update, this means we'll advance over the keys
	// one by one.
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 1
	var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
	cupaloy.SnapshotT(t, summary)
}

// TestComplexDataset tries to throw together a bunch of different bits of complexity
// to synthesize something vaguely "realistic". It features a multiple-column primary
// key, a dataset large enough that the initial table scan gets divided across many
// "chunks", two connector restarts at different points in the initial table scan, and
// some concurrent modifications to row ranges already-scanned and not-yet-scanned.
func TestComplexDataset(t *testing.T) {
	var tb, ctx = TestBackend, context.Background()
	var tableName = tb.CreateTable(ctx, t, "", "(year INTEGER, state VARCHAR(2), fullname VARCHAR(64), population INTEGER, PRIMARY KEY (year, state))")
	tests.LoadCSV(ctx, t, tb, tableName, "statepop.csv", 0)
	var cs = tb.CaptureSpec(t, tableName)

	// Reduce the backfill chunk size to 10 rows for this test.
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 10

	t.Run("init", func(t *testing.T) {
		var summary, states = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)
		cs.Checkpoint = states[13] // Next restart between (1940, 'NV') and (1940, 'NY')
		logrus.WithField("checkpoint", string(cs.Checkpoint)).Warn("restart at")
	})

	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // An insert prior to the first restart, which will be reported once replication begins
		{1970, "XX", "No Such State", 12345},  // An insert between the two restarts, which will be visible in the table scan and should be filtered during replication
		{1990, "XX", "No Such State", 123456}, // An insert after the second restart, which will be visible in the table scan and should be filtered during replication
	})
	t.Run("restart1", func(t *testing.T) {
		var summary, states = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)
		cs.Checkpoint = states[10] // Next restart in the middle of 1980 data
	})

	tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE state = 'XX';", tableName))
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1970, "XX", "No Such State", 12345},  // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1990, "XX", "No Such State", 123456}, // Deleting/reinserting this row will be filtered since this portion of the table has yet to be scanned
	})
	t.Run("restart2", func(t *testing.T) {
		var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)
	})
}
