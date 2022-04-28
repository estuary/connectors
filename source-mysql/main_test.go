package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
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
	dbServerID = flag.Int("db_serverid", 12345, "Unique server ID for replication")
)

var (
	TestBackend *mysqlTestBackend
)

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Verbose() {
		logrus.SetLevel(logrus.InfoLevel)
	}

	backfillChunkSize = 16
	replicationBufferSize = 0

	// Initialize test config and database connection
	var cfg = Config{
		Address:         *dbAddress,
		User:            *dbUser,
		Password:        *dbPassword,
		DBName:          *dbName,
		WatermarksTable: "flow.watermarks",
		ServerID:        *dbServerID,
	}
	if err := cfg.Validate(); err != nil {
		logrus.WithFields(logrus.Fields{"err": err, "config": cfg}).Fatal("error validating test config")
	}
	cfg.SetDefaults()

	var conn, err = client.Connect(cfg.Address, cfg.User, cfg.Password, cfg.DBName)
	if err != nil {
		logrus.WithField("err", err).Fatal("error connecting to database")
	}

	TestBackend = &mysqlTestBackend{conn: conn, cfg: cfg}

	var exitCode = m.Run()
	os.Exit(exitCode)
}

type mysqlTestBackend struct {
	conn *client.Conn
	cfg  Config
}

func (tb *mysqlTestBackend) CreateTable(ctx context.Context, t *testing.T, suffix string, tableDef string) string {
	t.Helper()

	var tableName = "test_" + strings.TrimPrefix(t.Name(), "Test")
	if suffix != "" {
		tableName += "_" + suffix
	}
	for _, str := range []string{"/", "=", "(", ")"} {
		tableName = strings.ReplaceAll(tableName, str, "_")
	}
	tableName = strings.ToLower(tableName)

	logrus.WithFields(logrus.Fields{"table": tableName, "cols": tableDef}).Debug("creating test table")
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %s%s;`, tableName, tableDef))
	t.Cleanup(func() {
		logrus.WithField("table", tableName).Debug("destroying test table")
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE %s;`, tableName))
	})
	return tableName
}

func (tb *mysqlTestBackend) Insert(ctx context.Context, t *testing.T, table string, rows [][]interface{}) {
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

func (tb *mysqlTestBackend) Update(ctx context.Context, t *testing.T, table string, whereCol string, whereVal interface{}, setCol string, setVal interface{}) {
	t.Helper()
	tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?;", table, setCol, whereCol), setVal, whereVal)
}

func (tb *mysqlTestBackend) Delete(ctx context.Context, t *testing.T, table string, whereCol string, whereVal interface{}) {
	t.Helper()
	tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE %s = ?;", table, whereCol), whereVal)
}

func (tb *mysqlTestBackend) Query(ctx context.Context, t *testing.T, query string, args ...interface{}) {
	t.Helper()
	logrus.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing query")
	var result, err = tb.conn.Execute(query, args...)
	if err != nil {
		t.Fatalf("error executing query %q: %v", query, err)
	}
	defer result.Close()

	// At tracing level, attempt to print result rows readably
	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		for _, row := range result.Values {
			var vals []string
			for _, val := range row {
				var str = string(val.AsString())
				if str == "" {
					str = strconv.FormatInt(val.AsInt64(), 10)
				}
				vals = append(vals, str)
			}
			logrus.WithField("values", vals).Trace("query result row")
		}
	}
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
				db.(*mysqlDatabase).config.SkipBinlogRetentionCheck = true
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
