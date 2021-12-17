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
)

var (
	dbAddress = flag.String("db_addr", "127.0.0.1:3306", "Connect to the specified address/port for tests")
	dbUser    = flag.String("db_user", "root", "Connect as the specified user for tests")
	dbName    = flag.String("db_name", "test", "Connect to the named database for tests")
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
		// TODO(wgd): Make configurable via a flag
		Address:         "127.0.0.1:3306",
		User:            "root",
		Pass:            "flow",
		DBName:          "test",
		WatermarksTable: "test.flow_watermarks",
		ServerID:        1234, // TODO(wgd): How do we handle this?
	}
	if err := cfg.Validate(); err != nil {
		logrus.WithFields(logrus.Fields{"err": err, "config": cfg}).Fatal("error validating test config")
	}

	var conn, err = client.Connect(cfg.Address, cfg.User, cfg.Pass, cfg.DBName)
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

// TestDatatypes runs the discovery test on various datatypes.
func TestDatatypes(t *testing.T) {
	var ctx = context.Background()
	tests.TestDatatypes(ctx, t, TestBackend, []tests.DatatypeTestCase{
		{
			ColumnType:  "integer",
			ExpectType:  `{"type":["integer","null"]}`,
			InputValue:  123,
			ExpectValue: `123`,
		},
		{
			ColumnType:  "integer",
			ExpectType:  `{"type":["integer","null"]}`,
			InputValue:  nil,
			ExpectValue: `null`,
		},
		{
			ColumnType:  "integer not null",
			ExpectType:  `{"type":"integer"}`,
			InputValue:  123,
			ExpectValue: `123`,
		},
		{
			ColumnType:  "varchar(32)",
			ExpectType:  `{"type":["string","null"]}`,
			InputValue:  "hello",
			ExpectValue: `"hello"`,
		},
		{
			ColumnType:  "text",
			ExpectType:  `{"type":["string","null"]}`,
			InputValue:  "hello",
			ExpectValue: `"hello"`,
		},
	})
}
