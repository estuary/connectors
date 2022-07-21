package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
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

	backfillChunkSize = 16
	replicationBufferSize = 0

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
	cfg.SetDefaults()

	var conn, err = client.Connect(cfg.Address, cfg.User, cfg.Password, cfg.Advanced.DBName)
	if err != nil {
		logrus.WithField("err", err).Fatal("error connecting to database")
	}

	TestBackend = &mysqlTestBackend{conn: conn, cfg: cfg}

	var exitCode = m.Run()
	os.Exit(exitCode)
}

func TestAlterTable(t *testing.T) {
	var tb, ctx = TestBackend, context.Background()
	var tableA = tb.CreateTable(ctx, t, "aaa", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, "bbb", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableC = tb.CreateTable(ctx, t, "ccc", "(id INTEGER PRIMARY KEY, data TEXT)")

	var catalogA = tests.ConfiguredCatalog(ctx, t, tb, tableA)
	var catalogAB = tests.ConfiguredCatalog(ctx, t, tb, tableA, tableB)
	var catalogABC = tests.ConfiguredCatalog(ctx, t, tb, tableA, tableB, tableC)
	var state = sqlcapture.PersistentState{}
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "abc"}, {2, "def"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{3, "ghi"}, {4, "jkl"}})
	tb.Insert(ctx, t, tableC, [][]interface{}{{5, "mno"}, {6, "pqr"}})
	tests.VerifiedCapture(ctx, t, tb, &catalogAB, &state, "init")

	// Altering tableC, which is not being captured, should be fine
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra TEXT;", tableC))
	tests.VerifiedCapture(ctx, t, tb, &catalogAB, &state, "capture1")

	// Altering tableB, which is being captured, should result in an error
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra TEXT;", tableB))
	tests.VerifiedCapture(ctx, t, tb, &catalogAB, &state, "capture2_fails")

	// Restarting the capture won't fix this
	tests.VerifiedCapture(ctx, t, tb, &catalogAB, &state, "capture3_fails")

	// But removing the problematic table should fix it
	tests.VerifiedCapture(ctx, t, tb, &catalogA, &state, "capture4")

	// And we can then re-add the table and it should start over after the problem
	tests.VerifiedCapture(ctx, t, tb, &catalogAB, &state, "capture5")

	// Finally we exercise the trickiest edge case, in which a new table (C)
	// is added to the capture *when it was also altered after the last state
	// checkpoint*. This should still work, because tables only become active
	// after the first stream-to-watermark operation.
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN evenmore TEXT;", tableC))
	tests.VerifiedCapture(ctx, t, tb, &catalogABC, &state, "capture6")
}

type mysqlTestBackend struct {
	conn *client.Conn
	cfg  Config
}

func (tb *mysqlTestBackend) CreateTable(ctx context.Context, t testing.TB, suffix string, tableDef string) string {
	t.Helper()

	var tableName = "test_" + strings.TrimPrefix(t.Name(), "Test")
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

func (tb *mysqlTestBackend) GetDatabase() sqlcapture.Database {
	var cfg = tb.cfg
	return &mysqlDatabase{config: &cfg}
}

// TestGeneric runs the generic sqlcapture test suite.
func TestGeneric(t *testing.T) {
	tests.Run(context.Background(), t, TestBackend)
}

// TestBinlogExpirySanityCheck verifies that the "dangerously short binlog expiry"
// sanity check is working as intended.
func TestBinlogExpirySanityCheck(t *testing.T) {
	var ctx = context.Background()

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
			TestBackend.Query(ctx, t, "SET GLOBAL binlog_expire_logs_seconds = 0;")
			TestBackend.Query(ctx, t, "SET GLOBAL expire_logs_days = 0;")
			TestBackend.Query(ctx, t, fmt.Sprintf("SET GLOBAL %s = %d;", tc.VarName, tc.VarValue))

			// Connect to the database, which may run the sanity-check
			db := TestBackend.GetDatabase()
			if tc.SkipCheck {
				db.(*mysqlDatabase).config.Advanced.SkipBinlogRetentionCheck = true
			}
			err := db.Connect(ctx)
			db.Close(ctx)

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
	var tb, ctx = &mysqlTestBackend{conn: TestBackend.conn, cfg: TestBackend.cfg}, context.Background()
	var tableA = tb.CreateTable(ctx, t, "aaa", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, "bbb", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableC = tb.CreateTable(ctx, t, "ccc", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "one"}, {2, "two"}, {3, "three"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{4, "four"}, {5, "five"}, {6, "six"}})
	tb.Insert(ctx, t, tableC, [][]interface{}{{7, "seven"}, {8, "eight"}, {9, "nine"}})
	tb.cfg.Advanced.SkipBackfills = fmt.Sprintf("test.%s,test.%s", tableA, tableC)

	// Run an initial capture, which should only backfill events from table B
	var catalog = tests.ConfiguredCatalog(ctx, t, tb, tableA, tableB, tableC)
	var state = sqlcapture.PersistentState{}
	tests.VerifiedCapture(ctx, t, tb, &catalog, &state, "init")

	// Insert additional data and verify that all three tables report new events
	tb.Insert(ctx, t, tableA, [][]interface{}{{10, "ten"}, {11, "eleven"}, {12, "twelve"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{13, "thirteen"}, {14, "fourteen"}, {15, "fifteen"}})
	tb.Insert(ctx, t, tableC, [][]interface{}{{16, "sixteen"}, {17, "seventeen"}, {18, "eighteen"}})
	tests.VerifiedCapture(ctx, t, tb, &catalog, &state, "")
}
